# -*- coding: utf-8 -*-
"""
datanode.py — DataNode：块存储节点（模拟）
=============================================
职责：
  * 块本体存储：blocks/<block_id>.dat + 索引（genstamp / 校验和 / 状态），
    索引持久化为 node_state.json（原子写）；
  * 向 NameNode 发送心跳（携带存储水位、块数、IO 计数、版本向量、事件），
    接收命令（replicate / delete / report / pull_docs）；
  * 周期性全量块汇报（block report），供 NameNode 对账副本表；
  * 数据巡检（scrub）：抽样重算校验和，发现损坏立即上报，
    NameNode 据此触发副本恢复（难点二）；
  * 副本流水线：接收 PUT /block 时按 X-Forward-To 头链式转发给下一节点，
    每一跳校验 sha256（难点一：副本一致性）；
  * 元数据文档同步：基于版本向量拉取 NameNode 的 cluster 文档缓存到本地，
    concurrent 冲突按"NameNode 权威"解决并记录审计（难点五）；
  * 故障演练：kill / revive / corrupt 由 NameNode 的模拟接口驱动。

节点间通信全部走 HTTP（urllib），端口见 config.DATANODE_PORTS。
"""

import json
import os
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import config
from .util import (RateCounter, atomic_write_json, b64e, gen_id, http_json,
                   http_request, now, parse_range, read_json, sha256_bytes,
                   to_rate_units, vv_compare, vv_merge, content_range_value,
                   HttpError)


class DataNodeError(Exception):
    pass


class DataNode:
    def __init__(self, node_id, port, rack, nn_url, data_dir,
                 capacity=None, cluster_key=None):
        self.node_id = node_id
        self.port = port
        self.rack = rack
        self.nn_url = nn_url.rstrip("/")
        self.cluster_key = cluster_key or config.CLUSTER_KEY
        self.data_dir = data_dir
        self.block_dir = os.path.join(data_dir, "blocks")
        self.cold_dir = os.path.join(data_dir, "cold")
        self.cache_dir = os.path.join(data_dir, "doc_cache")
        os.makedirs(self.block_dir, exist_ok=True)
        os.makedirs(self.cold_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)

        self.capacity = capacity or config.NODE_CAPACITY

        # ---- 持久状态：node_state.json（原子写） ----
        self.state_path = os.path.join(data_dir, "node_state.json")
        state = read_json(self.state_path, default=None) or {}
        self.index = state.get("blocks", {})        # bid -> {genstamp, checksum, size, state, stored_at}
        self.vv = state.get("vv", {})               # 本文档的版本向量
        self.doc_vv = state.get("doc_vv", {})       # 已缓存同步文档的版本向量
        self.counters = state.get("counters", {})
        self._state_dirty = False
        self._state_lock = threading.RLock()

        # ---- 运行时状态 ----
        self.running = False
        self.killed = False                          # 被演练杀掉的标记
        self.started_at = None
        self.httpd = None
        self._threads = []
        self._stop_evt = threading.Event()
        self._events = []                            # 待随下次心跳上报的事件
        self._events_lock = threading.Lock()
        self._hb_failures = 0
        self._last_hb_ack = None
        self._last_report_at = 0.0
        self._report_now = threading.Event()

        # ---- IO 统计 ----
        self.io = {
            "reads": self.counters.get("reads", 0),
            "writes": self.counters.get("writes", 0),
            "bytes_read": self.counters.get("bytes_read", 0),
            "bytes_written": self.counters.get("bytes_written", 0),
            "replicate_in": self.counters.get("replicate_in", 0),
            "replicate_out": self.counters.get("replicate_out", 0),
            "scrubbed": self.counters.get("scrubbed", 0),
            "corrupt_found": self.counters.get("corrupt_found", 0),
        }
        self.read_rate = RateCounter(window=10)
        self.write_rate = RateCounter(window=10)
        self._reconcile_index_with_disk()

    # ==================================================================
    # 持久化
    # ==================================================================
    def _persist_state(self, force=False):
        with self._state_lock:
            if not self._state_dirty and not force:
                return
            self.counters = {k: self.io.get(k, 0) for k in
                             ("reads", "writes", "bytes_read", "bytes_written",
                              "replicate_in", "replicate_out", "scrubbed",
                              "corrupt_found")}
            payload = {
                "node_id": self.node_id,
                "vv": self.vv,
                "doc_vv": self.doc_vv,
                "counters": self.counters,
                "blocks": self.index,
                "updated_at": now(),
            }
            atomic_write_json(self.state_path, payload)   # 原子写
            self._state_dirty = False

    def _touch_state(self):
        """本地状态变更：推进版本向量分量 + 标脏。"""
        with self._state_lock:
            self.vv = {**self.vv, self.node_id: self.vv.get(self.node_id, 0) + 1}
            self._state_dirty = True

    def _reconcile_index_with_disk(self):
        """启动时对账：磁盘上有而索引没有的块 => 孤儿（汇报给 NN 决定删除）。"""
        orphans = []
        try:
            hot = {f[:-4] for f in os.listdir(self.block_dir)
                   if f.endswith(".dat")}
        except OSError:
            hot = set()
        try:
            cold = {f[:-4] for f in os.listdir(self.cold_dir)
                    if f.endswith(".dat")}
        except OSError:
            cold = set()
        on_disk = hot | cold
        for bid in list(self.index.keys()):
            if bid not in on_disk:
                del self.index[bid]
                self._state_dirty = True
        for bid in on_disk - set(self.index.keys()):
            orphans.append(bid)
        self._orphans = orphans
        return orphans

    def _block_path(self, bid):
        return os.path.join(self.block_dir, f"{bid}.dat")

    def _cold_path(self, bid):
        return os.path.join(self.cold_dir, f"{bid}.dat")

    def _locate_block(self, bid):
        """按索引 tier 定位块文件：热目录或冷介质目录。"""
        meta = self.index.get(bid) or {}
        if meta.get("tier") == "cold":
            cold = self._cold_path(bid)
            if os.path.exists(cold):
                return cold
        return self._block_path(bid)

    # ==================================================================
    # 冷介质分级（生命周期归档 / 取回）
    # ==================================================================
    def archive_to_cold(self, bid):
        """把块从热介质目录移动到冷介质目录（同盘 rename，内容/校验和不变）。"""
        with self._state_lock:
            meta = self.index.get(bid)
        if not meta:
            raise DataNodeError(f"块不存在: {bid}")
        src, dst = self._block_path(bid), self._cold_path(bid)
        if os.path.exists(dst):
            moved = False     # 幂等：冷介质已有
        elif not os.path.exists(src):
            raise DataNodeError(f"热介质上找不到块文件: {bid}")
        else:
            os.replace(src, dst)    # 原子改名，数据不重写、不丢内容
            moved = True
        with self._state_lock:
            m = self.index.get(bid)
            if m is not None:
                m["tier"] = "cold"
                m["cold_at"] = now()
                self._touch_state()
        if moved:
            self.push_event({"type": "cold_archived", "block_id": bid})
            self._persist_state()
        return {"ok": True, "block_id": bid, "tier": "cold",
                "node_id": self.node_id, "already": not moved}

    def restore_from_cold(self, bid):
        """把块从冷介质目录取回热介质目录（访问触发，内容/校验和不变）。"""
        with self._state_lock:
            meta = self.index.get(bid)
        if not meta:
            raise DataNodeError(f"块不存在: {bid}")
        src, dst = self._cold_path(bid), self._block_path(bid)
        if os.path.exists(dst):
            moved = False     # 幂等
        elif not os.path.exists(src):
            raise DataNodeError(f"冷介质上找不到块文件: {bid}")
        else:
            os.replace(src, dst)
            moved = True
        with self._state_lock:
            m = self.index.get(bid)
            if m is not None:
                m["tier"] = "hot"
                m.pop("cold_at", None)
                self._touch_state()
        if moved:
            self.push_event({"type": "cold_restored", "block_id": bid})
            self._persist_state()
        return {"ok": True, "block_id": bid, "tier": "hot",
                "node_id": self.node_id, "already": not moved}

    # ==================================================================
    # 启动 / 停止 / 故障演练
    # ==================================================================
    def start(self):
        if self.running:
            return
        self._stop_evt.clear()
        self.killed = False
        self.running = True
        self.started_at = now()
        self.httpd = ThreadingHTTPServer((config.HOST, self.port),
                                         DataNodeHandler)
        self.httpd.datanode = self
        self.httpd.daemon_threads = True
        t = threading.Thread(target=self.httpd.serve_forever,
                             kwargs={"poll_interval": 0.2},
                             name=f"http-{self.node_id}", daemon=True)
        t.start()
        self._threads = [t]
        for target, name in ((self._heartbeat_loop, "heartbeat"),
                             (self._block_report_loop, "block-report"),
                             (self._scrub_loop, "scrub"),
                             (self._persist_loop, "persist")):
            th = threading.Thread(target=target, name=f"{name}-{self.node_id}",
                                  daemon=True)
            th.start()
            self._threads.append(th)
        # 启动即做一次全量汇报（携带孤儿块，NN 决定去留）
        self._report_now.set()

    def stop(self, mark_killed=True):
        self.running = False
        if mark_killed:
            self.killed = True
        self._stop_evt.set()
        self._report_now.set()
        if self.httpd:
            try:
                self.httpd.shutdown()
                self.httpd.server_close()
            except Exception:
                pass
            self.httpd = None
        self._persist_state(force=True)

    def kill_sim(self):
        """故障演练：进程假死（停 HTTP + 停心跳，磁盘数据保留）。"""
        self.stop(mark_killed=True)

    def revive_sim(self):
        """故障演练：节点复活，重新注册并全量汇报。"""
        if self.running:
            return
        self._reconcile_index_with_disk()
        self.start()
        self.push_event({"type": "revived", "node_id": self.node_id})

    def corrupt_block_sim(self, bid):
        """故障演练：悄悄翻转块数据若干字节（校验和不随之更新）。"""
        path = self._block_path(bid)
        if not os.path.exists(path):
            raise DataNodeError(f"本节点没有块 {bid}")
        with open(path, "r+b") as f:
            data = bytearray(f.read())
            if data:
                for _ in range(3):
                    pos = random.randrange(len(data))
                    data[pos] = (data[pos] + 0x37) & 0xFF
                f.seek(0)
                f.write(data)
                f.truncate()
        return True

    # ==================================================================
    # 事件队列（随心跳捎带上报）
    # ==================================================================
    def push_event(self, event):
        event.setdefault("ts", now())
        event.setdefault("node_id", self.node_id)
        with self._events_lock:
            self._events.append(event)
            if len(self._events) > 500:
                self._events = self._events[-500:]

    def drain_events(self, limit=100):
        with self._events_lock:
            out, self._events = self._events[:limit], self._events[limit:]
            return out

    # ==================================================================
    # 块存取
    # ==================================================================
    def store_block(self, bid, data, genstamp, checksum, size=None):
        """校验并落盘一个块，更新索引（原子写 node_state）。"""
        actual = sha256_bytes(data)
        if checksum and actual != checksum:
            raise DataNodeError(
                f"块 {bid} 校验和不匹配: 期望 {checksum[:12]}… 实际 {actual[:12]}…")
        if size is not None and size != len(data):
            raise DataNodeError(f"块 {bid} 长度不匹配: {size} != {len(data)}")
        path = self._block_path(bid)
        atomic_write_bytes_local(path, data)
        with self._state_lock:
            self.index[bid] = {
                "genstamp": int(genstamp or config.GENSTAMP_INITIAL),
                "checksum": actual,
                "size": len(data),
                "state": "ok",
                "stored_at": now(),
            }
            self.io["writes"] += 1
            self.io["bytes_written"] += len(data)
            self.write_rate.hit(to_rate_units(len(data),
                                              config.IO_RATE_SCALE))
            self._touch_state()
        self._persist_state()
        return self.index[bid]

    def read_block(self, bid, start=None, end=None, verify=True):
        """
        读取块（可选 Range）。读取前重算校验和：
        不一致 => 标记 corrupt、上报事件、抛错（NN 会故障转移到其它副本）。
        """
        with self._state_lock:
            meta = self.index.get(bid)
        if not meta:
            raise DataNodeError(f"块不存在: {bid}")
        path = self._locate_block(bid)
        try:
            with open(path, "rb") as f:
                data = f.read()
        except OSError as e:
            raise DataNodeError(f"块读取失败: {bid}: {e}")
        if verify:
            actual = sha256_bytes(data)
            if actual != meta["checksum"]:
                with self._state_lock:
                    meta["state"] = "corrupt"
                    self.io["corrupt_found"] += 1
                    self._touch_state()
                self.push_event({"type": "corrupt", "block_id": bid,
                                 "reason": "读取时校验和不匹配",
                                 "expected": meta["checksum"][:12],
                                 "actual": actual[:12]})
                self._persist_state()
                raise DataNodeError(f"块 {bid} 已损坏（校验和不匹配）")
        self.io["reads"] += 1
        self.io["bytes_read"] += len(data)
        self.read_rate.hit(to_rate_units(len(data), config.IO_RATE_SCALE))
        if start is None:
            return data, meta
        return data[start:end + 1], meta

    def delete_block(self, bid, reason=""):
        existed = False
        for path in (self._block_path(bid), self._cold_path(bid)):
            if os.path.exists(path):
                try:
                    os.unlink(path)
                    existed = True
                except OSError:
                    pass
        with self._state_lock:
            if bid in self.index:
                del self.index[bid]
                existed = True
            self._touch_state()
        if existed:
            self.push_event({"type": "deleted", "block_id": bid,
                             "reason": reason})
            self._persist_state()
        return existed

    def replicate_from(self, bid, src_url, genstamp, checksum, size=None):
        """从源节点 HTTP 拉取块并本地落盘（恢复流程的执行端）。"""
        url = f"{src_url.rstrip('/')}/block/{bid}"
        try:
            _status, _hdrs, data = http_request(
                url, "GET", timeout=20,
                headers={"X-Cluster-Key": self.cluster_key})
        except HttpError as e:
            self.push_event({"type": "replicate_failed", "block_id": bid,
                             "reason": f"拉取失败: {e}"})
            return False
        try:
            self.store_block(bid, data, genstamp, checksum, size)
        except DataNodeError as e:
            self.push_event({"type": "replicate_failed", "block_id": bid,
                             "reason": str(e)})
            return False
        with self._state_lock:
            self.io["replicate_in"] += 1
        self.push_event({"type": "replicate_done", "block_id": bid,
                         "genstamp": int(genstamp), "checksum": checksum,
                         "size": len(data)})
        return True

    def forward_block(self, bid, data, genstamp, checksum, forward_urls):
        """
        流水线转发：把块 PUT 给链上下一个节点（携带剩余转发列表）。
        返回每一跳的结果 [{"node","ok","error"?}]。
        """
        results = []
        if not forward_urls:
            return results
        nxt, rest = forward_urls[0], forward_urls[1:]
        headers = {"X-Cluster-Key": self.cluster_key,
                   "Content-Type": "application/octet-stream"}
        if rest:
            headers["X-Forward-To"] = ",".join(rest)
        url = (nxt if nxt.startswith("http") else
               f"{nxt.rstrip('/')}/block/{bid}"
               f"?genstamp={genstamp}&checksum={checksum}&size={len(data)}")
        if not url.startswith("http"):
            url = f"{nxt}/block/{bid}?genstamp={genstamp}&checksum={checksum}&size={len(data)}"
        try:
            _status, _hdrs, body = http_request(url, "PUT", data=data,
                                                headers=headers, timeout=30)
            resp = json.loads(body.decode("utf-8")) if body else {}
            with self._state_lock:
                self.io["replicate_out"] += 1
            results.append({"node": resp.get("node_id", nxt), "ok": True,
                            "forwarded": resp.get("forwarded", [])})
        except Exception as e:  # noqa: BLE001
            results.append({"node": nxt, "ok": False, "error": str(e)[:200]})
        return results

    # ==================================================================
    # 心跳（携带事件 + 接收命令）
    # ==================================================================
    def _heartbeat_payload(self):
        used = self.used_bytes()
        return {
            "node_id": self.node_id,
            "rack": self.rack,
            "port": self.port,
            "url": f"http://{config.HOST}:{self.port}",
            "storage": {"capacity": self.capacity, "used": used,
                        "free": max(0, self.capacity - used)},
            "block_count": len(self.index),
            "io": dict(self.io),
            "rates": {"read": round(self.read_rate.rate(), 1),
                      "write": round(self.write_rate.rate(), 1)},
            "uptime": (now() - self.started_at) if self.started_at else 0,
            "vv": dict(self.vv),
            "doc_vv": dict(self.doc_vv),
            "events": self.drain_events(),
        }

    def _heartbeat_loop(self):
        url = f"{self.nn_url}/internal/heartbeat"
        while not self._stop_evt.is_set():
            self._stop_evt.wait(config.HEARTBEAT_INTERVAL)
            if not self.running:
                continue
            try:
                resp = http_json(url, "POST", self._heartbeat_payload(),
                                 timeout=4,
                                 headers={"X-Cluster-Key": self.cluster_key})
                self._hb_failures = 0
                self._last_hb_ack = now()
                self._handle_commands(resp.get("commands", []))
                for doc in resp.get("pull_docs", []):
                    self._pull_sync_doc(doc)
            except Exception:
                self._hb_failures += 1
                if self._hb_failures in (1, 5, 20):
                    self.push_event({"type": "hb_failed",
                                     "count": self._hb_failures})

    def _handle_commands(self, commands):
        for cmd in commands:
            ctype = cmd.get("type")
            if ctype == "replicate":
                t = threading.Thread(
                    target=self.replicate_from,
                    args=(cmd["block_id"], cmd["src"], cmd.get("genstamp", 1),
                          cmd.get("checksum"), cmd.get("size")),
                    name=f"repl-{cmd['block_id'][-6:]}", daemon=True)
                t.start()
            elif ctype == "delete":
                self.delete_block(cmd["block_id"],
                                  reason=cmd.get("reason", "namenode 命令"))
            elif ctype == "report":
                self._report_now.set()

    # ==================================================================
    # 块汇报
    # ==================================================================
    def _block_report_loop(self):
        url = f"{self.nn_url}/internal/block_report"
        while not self._stop_evt.is_set():
            triggered = self._report_now.wait(timeout=config.BLOCK_REPORT_INTERVAL)
            if not self.running:
                self._report_now.clear()
                continue
            self._report_now.clear()
            blocks = []
            with self._state_lock:
                for bid, meta in self.index.items():
                    blocks.append({"id": bid, "genstamp": meta["genstamp"],
                                   "size": meta["size"],
                                   "checksum": meta["checksum"],
                                   "state": meta["state"],
                                   "tier": meta.get("tier", "hot")})
                orphans = list(getattr(self, "_orphans", []))
                self._orphans = []
            payload = {"node_id": self.node_id, "blocks": blocks,
                       "orphans": orphans, "full": True,
                       "vv": dict(self.vv), "sent_at": now()}
            try:
                resp = http_json(url, "POST", payload, timeout=8,
                                 headers={"X-Cluster-Key": self.cluster_key})
                self._last_report_at = now()
                self._handle_commands(resp.get("commands", []))
            except Exception:
                self._report_now.set()   # 失败则尽快重试

    # ==================================================================
    # 数据巡检（scrub）：主动发现静默损坏
    # ==================================================================
    def _scrub_loop(self):
        while not self._stop_evt.is_set():
            self._stop_evt.wait(config.SCRUB_INTERVAL)
            if not self.running:
                continue
            with self._state_lock:
                bids = [b for b, m in self.index.items() if m["state"] == "ok"]
            if not bids:
                continue
            sample = random.sample(bids, min(config.SCRUB_BATCH, len(bids)))
            for bid in sample:
                try:
                    with open(self._locate_block(bid), "rb") as f:
                        data = f.read()
                    actual = sha256_bytes(data)
                    with self._state_lock:
                        meta = self.index.get(bid)
                        self.io["scrubbed"] += 1
                        if meta and actual != meta["checksum"]:
                            meta["state"] = "corrupt"
                            self.io["corrupt_found"] += 1
                            self._touch_state()
                            self.push_event({
                                "type": "corrupt", "block_id": bid,
                                "reason": "巡检发现校验和不匹配",
                                "expected": meta["checksum"][:12],
                                "actual": actual[:12]})
                except OSError:
                    continue
            self._persist_state()

    def _persist_loop(self):
        while not self._stop_evt.is_set():
            self._stop_evt.wait(2.0)
            self._persist_state()

    # ==================================================================
    # 文档同步（版本向量）
    # ==================================================================
    def _pull_sync_doc(self, doc_name):
        """从 NameNode 拉取文档信封，按版本向量合并到本地缓存。"""
        url = f"{self.nn_url}/internal/meta/{doc_name}"
        try:
            envelope = http_json(url, "GET", timeout=5,
                                 headers={"X-Cluster-Key": self.cluster_key})
        except HttpError:
            return
        remote_vv = envelope.get("vv", {})
        local_vv = self.doc_vv.get(doc_name, {})
        rel = vv_compare(remote_vv, local_vv)
        cache_path = os.path.join(self.cache_dir, f"{doc_name}.json")
        if rel in ("after", "equal") or not local_vv:
            atomic_write_json(cache_path, envelope)          # 原子写缓存
            with self._state_lock:
                self.doc_vv[doc_name] = vv_merge(local_vv, remote_vv)
                self._touch_state()
            self.push_event({"type": "doc_synced", "doc": doc_name,
                             "relation": "applied",
                             "vv": dict(self.doc_vv[doc_name])})
        elif rel == "concurrent":
            # 冲突：cluster 文档以 NameNode 为权威，采纳远端并留痕
            atomic_write_json(cache_path, envelope)
            with self._state_lock:
                self.doc_vv[doc_name] = vv_merge(local_vv, remote_vv)
                self._touch_state()
            self.push_event({"type": "doc_synced", "doc": doc_name,
                             "relation": "conflict-nn-wins",
                             "vv": dict(self.doc_vv[doc_name])})
        else:  # before：远端落后，跳过
            self.push_event({"type": "doc_synced", "doc": doc_name,
                             "relation": "skipped"})
        self._persist_state()

    # ==================================================================
    # 状态视图
    # ==================================================================
    def used_bytes(self):
        with self._state_lock:
            return sum(m["size"] for m in self.index.values())

    def status(self):
        with self._state_lock:
            corrupt = sum(1 for m in self.index.values()
                          if m["state"] == "corrupt")
            cold = sum(1 for m in self.index.values()
                       if m.get("tier") == "cold")
            return {
                "node_id": self.node_id,
                "rack": self.rack,
                "port": self.port,
                "running": self.running,
                "killed": self.killed,
                "started_at": self.started_at,
                "uptime": (now() - self.started_at) if self.started_at else 0,
                "capacity": self.capacity,
                "used": self.used_bytes(),
                "free": max(0, self.capacity - self.used_bytes()),
                "blocks": len(self.index),
                "cold_blocks": cold,
                "corrupt_blocks": corrupt,
                "io": dict(self.io),
                "rates": {"read": round(self.read_rate.rate(), 1),
                          "write": round(self.write_rate.rate(), 1)},
                "vv": dict(self.vv),
                "doc_vv": dict(self.doc_vv),
                "hb_failures": self._hb_failures,
                "last_hb_ack": self._last_hb_ack,
                "last_report_at": self._last_report_at,
            }

    def block_list(self, limit=200, offset=0):
        with self._state_lock:
            items = sorted(self.index.items(),
                           key=lambda kv: kv[1].get("stored_at", 0),
                           reverse=True)
            total = len(items)
            out = []
            for bid, m in items[offset:offset + limit]:
                out.append({"id": bid, "genstamp": m["genstamp"],
                            "size": m["size"], "state": m["state"],
                            "checksum": m["checksum"][:16],
                            "stored_at": m["stored_at"]})
            return {"total": total, "blocks": out}


def atomic_write_bytes_local(path, data):
    """块文件原子写（tmp + os.replace），避免半截块被读到。"""
    from .util import atomic_write_bytes
    atomic_write_bytes(path, data)


# ============================================================================
# DataNode HTTP 服务
# ============================================================================

class DataNodeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DFSVS-DataNode/1.0"

    # ------------------------------------------------------------ 基础设施
    @property
    def dn(self):
        return self.server.datanode

    def log_message(self, fmt, *args):     # 静默默认访问日志
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, X-Cluster-Key, Range")
        self.send_header("Access-Control-Expose-Headers",
                         "Content-Range, X-Node-Id, X-Checksum")

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, data, status=200, mime="application/octet-stream",
                    extra_headers=None):
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self._cors()
        self.end_headers()
        self.wfile.write(data)

    def _authed(self):
        key = self.headers.get("X-Cluster-Key", "")
        return key == self.dn.cluster_key

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        return self.rfile.read(length) if length else b""

    def _parsed(self):
        u = urlparse(self.path)
        return u.path.rstrip("/") or "/", parse_qs(u.query)

    # ------------------------------------------------------------ 路由
    def do_GET(self):
        path, query = self._parsed()
        try:
            if path == "/status":
                return self._send_json(self.dn.status())
            if path.startswith("/block/"):
                return self._get_block(path, query)
            if path.startswith("/meta/"):
                if not self._authed():
                    return self._send_json({"error": "forbidden"}, 403)
                return self._get_meta(path)
            self._send_json({"error": f"unknown path {path}"}, 404)
        except DataNodeError as e:
            self._send_json({"error": str(e)}, 404)
        except Exception as e:  # noqa: BLE001
            self._send_json({"error": str(e)}, 500)

    def do_PUT(self):
        path, query = self._parsed()
        try:
            if not self._authed():
                return self._send_json({"error": "forbidden"}, 403)
            if path.startswith("/block/"):
                return self._put_block(path, query)
            self._send_json({"error": f"unknown path {path}"}, 404)
        except DataNodeError as e:
            self._send_json({"error": str(e)}, 400)
        except Exception as e:  # noqa: BLE001
            self._send_json({"error": str(e)}, 500)

    def do_POST(self):
        path, _query = self._parsed()
        try:
            if not self._authed():
                return self._send_json({"error": "forbidden"}, 403)
            if path == "/replicate":
                body = json.loads(self._read_body().decode("utf-8") or "{}")
                ok = self.dn.replicate_from(
                    body.get("block_id"), body.get("src_url"),
                    body.get("genstamp", 1), body.get("checksum"),
                    body.get("size"))
                return self._send_json({"ok": ok,
                                        "node_id": self.dn.node_id})
            if path.startswith("/cold/archive/"):
                bid = path.split("/")[-1]
                return self._send_json(self.dn.archive_to_cold(bid))
            if path.startswith("/cold/restore/"):
                bid = path.split("/")[-1]
                return self._send_json(self.dn.restore_from_cold(bid))
            if path == "/corrupt":
                body = json.loads(self._read_body().decode("utf-8") or "{}")
                self.dn.corrupt_block_sim(body.get("block_id"))
                return self._send_json({"ok": True,
                                        "node_id": self.dn.node_id})
            self._send_json({"error": f"unknown path {path}"}, 404)
        except DataNodeError as e:
            self._send_json({"error": str(e)}, 400)
        except Exception as e:  # noqa: BLE001
            self._send_json({"error": str(e)}, 500)

    def do_DELETE(self):
        path, _query = self._parsed()
        try:
            if not self._authed():
                return self._send_json({"error": "forbidden"}, 403)
            if path.startswith("/block/"):
                bid = path.split("/")[-1]
                existed = self.dn.delete_block(bid, reason="HTTP DELETE")
                return self._send_json({"ok": True, "existed": existed,
                                        "node_id": self.dn.node_id})
            self._send_json({"error": f"unknown path {path}"}, 404)
        except Exception as e:  # noqa: BLE001
            self._send_json({"error": str(e)}, 500)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ------------------------------------------------------------ 处理器
    def _get_block(self, path, query):
        if not self._authed():
            return self._send_json({"error": "forbidden"}, 403)
        bid = path.split("/")[-1]
        data, meta = self.dn.read_block(bid)
        rng = parse_range(self.headers.get("Range"), len(data))
        extra = {"X-Node-Id": self.dn.node_id,
                 "X-Checksum": meta["checksum"],
                 "X-Genstamp": str(meta["genstamp"])}
        if rng:
            start, end = rng
            extra["Content-Range"] = content_range_value(start, end, len(data))
            return self._send_bytes(data[start:end + 1], 206,
                                    extra_headers=extra)
        return self._send_bytes(data, 200, extra_headers=extra)

    def _put_block(self, path, query):
        bid = path.split("/")[-1]
        data = self._read_body()
        genstamp = int((query.get("genstamp", ["1"])[0]))
        checksum = query.get("checksum", [None])[0]
        size = query.get("size", [None])[0]
        size = int(size) if size else None
        forward_hdr = self.headers.get("X-Forward-To", "")
        forward_urls = [u for u in forward_hdr.split(",") if u]
        meta = self.dn.store_block(bid, data, genstamp, checksum, size)
        forwarded = self.dn.forward_block(bid, data, genstamp,
                                          meta["checksum"], forward_urls)
        self._send_json({
            "ok": True,
            "node_id": self.dn.node_id,
            "block_id": bid,
            "genstamp": meta["genstamp"],
            "checksum": meta["checksum"],
            "size": meta["size"],
            "forwarded": forwarded,
        })

    def _get_meta(self, path):
        doc = path.split("/")[-1]
        cache_path = os.path.join(self.dn.cache_dir, f"{doc}.json")
        payload = read_json(cache_path, default=None)
        if payload is None:
            return self._send_json({"error": f"doc not cached: {doc}"}, 404)
        self._send_json(payload)


# ============================================================================
# 独立进程运行入口：python -m backend.datanode --id dn4 --port 8024
# ============================================================================

def run_standalone(node_id=None, port=None, nn_url=None, rack=None):
    node_id = node_id or "dn-standalone"
    default_port, default_rack = config.DATANODE_PORTS.get(
        node_id, (8029, "rack-x"))
    port = port or default_port
    rack = rack or default_rack
    nn_url = nn_url or f"http://{config.HOST}:{config.NAMENODE_PORT}"
    data_dir = os.path.join(config.DATANODE_ROOT, node_id)
    dn = DataNode(node_id, port, rack, nn_url, data_dir)
    dn.start()
    print(f"[DataNode {node_id}] listening on {config.HOST}:{port} "
          f"rack={rack} nn={nn_url}")
    try:
        while True:
            import time as _t
            _t.sleep(1)
    except KeyboardInterrupt:
        dn.stop(mark_killed=False)
        print(f"[DataNode {node_id}] stopped")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="DFSVS DataNode (standalone)")
    parser.add_argument("--id", default="dn4")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--nn", default=None)
    parser.add_argument("--rack", default=None)
    args = parser.parse_args()
    run_standalone(args.id, args.port, args.nn, args.rack)
