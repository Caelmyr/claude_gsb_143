# -*- coding: utf-8 -*-
"""
metadata.py — 元数据集中存储（JSON 文档 + 原子写 + 版本向量）
================================================================
难点五的实现核心：
  * 元数据集中：NameNode 把全部元数据组织为若干 JSON 文档
    （fs / blocks / versions / users / perms / logs / recycle / stats / cluster），
    每个文档一个文件，统一由 MetadataStore 管理；
  * 原子写：所有落盘走 util.atomic_write_json
    （同目录临时文件 -> flush -> fsync -> os.replace -> 目录 fsync），
    崩溃/并发读者永远不会看到半截 JSON；
  * 版本向量：每个文档携带 vv = {node_id: logical_clock}。
    本地修改调用 touch() 推进本机分量；
    跨节点同步（NameNode <-> DataNode）用 vv_compare 判定
    before/after/concurrent——concurrent 即写冲突，按"权威节点优先 +
    合并回调"策略解决，并记录冲突审计日志；
  * 脏文档延迟刷盘：高频文档（logs/stats）可标记 flush=False，
    由后台线程按 META_FLUSH_INTERVAL 批量原子落盘，兼顾吞吐与持久性。

并发模型：一把 store 级 RLock 保护所有文档的读改写；
模拟集群规模下（<= 4 节点、单机部署）足够，且保证元数据事务性。
"""

import os
import threading

from . import config
from .util import (atomic_write_json, now, read_json, vv_compare, vv_merge,
                   vv_increment)


class MetaError(Exception):
    pass


class Doc:
    """一个 JSON 元数据文档。"""

    def __init__(self, name, path, node_id):
        self.name = name
        self.path = path
        self.node_id = node_id
        self.data = {}
        self.vv = {}                 # 版本向量 {node_id: counter}
        self.updated_at = 0.0
        self.dirty = False
        self.write_count = 0
        self.conflict_count = 0

    def envelope(self):
        """磁盘/网络传输格式：{vv, updated_at, data}。"""
        return {
            "doc": self.name,
            "vv": self.vv,
            "updated_at": self.updated_at,
            "data": self.data,
        }

    def load(self):
        payload = read_json(self.path, default=None)
        if payload is None:
            self.data = {}
            self.vv = {}
            self.updated_at = 0.0
        else:
            self.data = payload.get("data", {})
            self.vv = payload.get("vv", {})
            self.updated_at = payload.get("updated_at", 0.0)
        return self

    def save(self):
        atomic_write_json(self.path, self.envelope())
        self.dirty = False
        self.write_count += 1


class MetadataStore:
    """
    集中元数据仓库。

    使用范式（NameNode 内）：
        with store.lock:
            data = store.get("blocks")
            data["blocks"][bid] = {...}
            store.touch("blocks")            # 推进 vv + 立即原子落盘
        # 高频文档：
        with store.lock:
            store.get("logs")["items"].append(entry)
            store.touch("logs", flush=False)  # 标脏，后台批量落盘
    """

    def __init__(self, meta_dir, node_id="namenode"):
        self.meta_dir = meta_dir
        self.node_id = node_id
        self.lock = threading.RLock()
        self.docs = {}
        self._dirty = set()
        self._stop = threading.Event()
        self._flusher = None
        os.makedirs(meta_dir, exist_ok=True)
        for name in config.META_DOCS:
            doc = Doc(name, os.path.join(meta_dir, f"{name}.json"), node_id)
            doc.load()
            self.docs[name] = doc
        self._stats = {"atomic_writes": 0, "conflicts": 0, "syncs_applied": 0,
                       "syncs_skipped": 0}

    # ------------------------------------------------------------------ 基础
    def get(self, name):
        """取文档数据（调用方需持有 store.lock 或在单线程上下文）。"""
        doc = self.docs.get(name)
        if doc is None:
            raise MetaError(f"未知元数据文档: {name}")
        return doc.data

    def doc(self, name):
        return self.docs.get(name)

    def touch(self, name, flush=True):
        """本地修改后调用：推进版本向量分量，标脏或立即原子落盘。"""
        doc = self.docs[name]
        doc.vv = vv_increment(doc.vv, self.node_id)
        doc.updated_at = now()
        doc.dirty = True
        if flush:
            self.flush(name)
        else:
            self._dirty.add(name)

    def flush(self, name=None):
        """原子落盘一个或全部脏文档。"""
        with self.lock:
            names = [name] if name else list(self.docs.keys())
            for n in names:
                doc = self.docs[n]
                if name or doc.dirty or n in self._dirty:
                    doc.save()
                    self._stats["atomic_writes"] += 1
                    self._dirty.discard(n)

    def flush_dirty(self):
        with self.lock:
            for n in list(self._dirty):
                self.docs[n].save()
                self._stats["atomic_writes"] += 1
            self._dirty.clear()

    # ------------------------------------------------------------ 后台刷盘
    def start_flusher(self):
        if self._flusher and self._flusher.is_alive():
            return
        self._stop.clear()
        self._flusher = threading.Thread(target=self._flush_loop,
                                         name="meta-flusher", daemon=True)
        self._flusher.start()

    def _flush_loop(self):
        while not self._stop.is_set():
            self._stop.wait(config.META_FLUSH_INTERVAL)
            try:
                self.flush_dirty()
            except Exception:
                pass

    def stop(self):
        self._stop.set()
        if self._flusher:
            self._flusher.join(timeout=2)
        self.flush_dirty()   # 退出前保证脏数据落盘

    # -------------------------------------------------------- 同步（版本向量）
    def export_doc(self, name):
        """导出文档信封（供其它节点拉取）。"""
        with self.lock:
            return self.docs[name].envelope()

    def import_doc(self, name, envelope, merge_fn=None, authority=None):
        """
        接收远端文档信封并按版本向量合并：
          * remote 支配 local（after）  => 直接采纳；
          * local 支配 remote（before/equal）=> 跳过（对方落后）；
          * concurrent => 写冲突：
              - 提供 merge_fn(local_data, remote_data) 则调用其合并，
                vv 取逐分量 max；
              - 否则按 authority 判定：远端为权威节点则采纳远端，
                否则保留本地；两种情况都记录冲突计数。
        返回 (result, merged_vv)，result ∈ {applied, skipped, merged, kept_local}
        """
        with self.lock:
            doc = self.docs[name]
            remote_vv = envelope.get("vv", {})
            rel = vv_compare(remote_vv, doc.vv)
            if rel in ("before", "equal"):
                self._stats["syncs_skipped"] += 1
                return "skipped", doc.vv

            if rel == "after":
                doc.data = envelope.get("data", {})
                doc.vv = vv_merge(doc.vv, remote_vv)
                doc.updated_at = now()
                doc.save()
                self._stats["syncs_applied"] += 1
                self._stats["atomic_writes"] += 1
                return "applied", doc.vv

            # concurrent：冲突路径
            doc.conflict_count += 1
            self._stats["conflicts"] += 1
            merged_vv = vv_merge(doc.vv, remote_vv)
            if merge_fn is not None:
                doc.data = merge_fn(doc.data, envelope.get("data", {}))
                doc.vv = merged_vv
                doc.updated_at = now()
                doc.save()
                self._stats["atomic_writes"] += 1
                return "merged", doc.vv

            remote_is_authority = (authority in remote_vv
                                   and remote_vv.get(authority, 0)
                                   > doc.vv.get(authority, 0))
            if remote_is_authority:
                doc.data = envelope.get("data", {})
                doc.vv = merged_vv
                doc.updated_at = now()
                doc.save()
                self._stats["atomic_writes"] += 1
                return "applied", doc.vv
            doc.vv = merged_vv     # 保留本地数据，但吸收向量（避免重复冲突）
            doc.updated_at = now()
            doc.save()
            return "kept_local", doc.vv

    def vv_snapshot(self):
        """所有文档的版本向量快照（心跳/块汇报携带，用于增量同步判定）。"""
        with self.lock:
            return {name: dict(doc.vv) for name, doc in self.docs.items()}

    def stats(self):
        with self.lock:
            out = dict(self._stats)
            out["docs"] = {
                name: {"vv": dict(doc.vv), "dirty": doc.dirty,
                       "updated_at": doc.updated_at,
                       "writes": doc.write_count,
                       "conflicts": doc.conflict_count,
                       "bytes": os.path.getsize(doc.path)
                       if os.path.exists(doc.path) else 0}
                for name, doc in self.docs.items()
            }
            return out
