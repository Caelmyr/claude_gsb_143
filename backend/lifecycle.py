# -*- coding: utf-8 -*-
"""
lifecycle.py — 生命周期管理（自动归档 / 自动回收站 / 撤销宽限 / 冷数据层）
=============================================================================
元数据存储在 meta['lifecycle'] JSON 文档：
    {
      "rules":     [ 规则 ],
      "pending":   { "<inode_id>:<action>": 待触发动作（宽限期内可撤销） },
      "history":   [ 执行/撤销/取回 留痕 ],
      "exemptions":{ "<inode_id>:<action>": 撤销豁免（防止扫描器重新挂起） }
    }

规则模型
--------
每条规则绑定一个路径前缀（或精确路径）与一个动作：
  * archive —— 超过 N 天（按 最近访问 atime / 修改 mtime / 创建 ctime）没访问，
               把文件归档到冷数据层（块从 DataNode 热副本下沉到 NameNode 冷归档库，
               内容校验和不变、不丢数据；访问时透明取回）；
  * trash   —— 超时自动移入回收站（走与手动删除相同的删除保护路径）。

优先级与覆盖
------------
同一动作可能被多条规则同时命中：
  按 (priority 降序, 路径长度降序, 创建时间升序) 取第一条作为**生效规则**，
  其余命中规则标记为 **shadowed（被覆盖）**；预览与留痕中都带完整命中轨迹，
  覆盖关系一目了然。archive 与 trash 是两个独立动作，可分别命中不同规则、
  各自独立倒计时（已归档文件仍可被 trash 规则移入回收站）。

两阶段执行 + 触发前撤销
-----------------------
扫描线程周期性评估：文件首次满足条件时不立即执行，而是生成 pending 条目，
宽限期（LIFECYCLE_GRACE_SECONDS）内页面可"撤销"。撤销写入 exemption：
  * atime 规则：豁免持续到文件被重新访问（年龄跌回阈值以下）为止；
  * mtime/ctime 规则或规则被修改：豁免立即失效，按新规则重新评估。
规则被删除/停用/不再命中、或文件被访问/修改导致条件不再成立时，
pending 自动撤销并留痕。

透明取回
--------
冷数据块在 NameNode.read_block 读路径上同步取回（vault 校验 -> 重新流水线
复制到 DataNode -> 更新块表 tier=hot -> 删除 vault 副本），上层下载/预览/
缩略图/版本读取无感知；取回完成写 history(status=restored)。
"""

import threading

from . import config
from .util import gen_id, norm_path, now, ttl_seconds


class LifecycleError(Exception):
    pass


ACTION_LABEL = {"archive": "归档冷数据", "trash": "移入回收站"}
BASE_LABEL = {"atime": "最近访问", "mtime": "最近修改", "ctime": "创建时间"}


class LifecycleManager:
    def __init__(self, nn):
        self.nn = nn
        self.meta = nn.meta
        self.lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None

    # ==================================================================
    # 初始化
    # ==================================================================
    def ensure_init(self):
        with self.meta.lock:
            doc = self.meta.get("lifecycle")
            doc.setdefault("rules", [])
            doc.setdefault("pending", {})
            doc.setdefault("history", [])
            doc.setdefault("exemptions", {})
            self.meta.touch("lifecycle", flush=False)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="nn-lifecycle",
                                        daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _doc(self):
        return self.meta.get("lifecycle")

    def _loop(self):
        self._stop.wait(5.0)             # 等种子/注册完成
        while not self._stop.is_set():
            try:
                self.scan()
            except Exception as e:  # noqa: BLE001
                self.nn.log_event("ERROR", "lifecycle", "loop_error", "",
                                  "system", str(e))
            self._stop.wait(config.LIFECYCLE_SCAN_INTERVAL)

    # ==================================================================
    # 规则 CRUD
    # ==================================================================
    def list_rules(self):
        with self.meta.lock:
            rules = sorted(self._doc().get("rules", []),
                           key=self._rule_sort_key)
            return list(rules)

    @staticmethod
    def _rule_sort_key(r):
        return (-int(r.get("priority", 0)), -len(r.get("path", "/")),
                r.get("created_at", 0))

    def validate_rule(self, fields):
        path = norm_path(fields.get("path") or "/")
        action = fields.get("action", "")
        if action not in config.LIFECYCLE_ACTIONS:
            raise LifecycleError("动作必须是 archive（归档）或 trash（回收站）")
        age_base = fields.get("age_base", "atime")
        if age_base not in config.LIFECYCLE_AGE_BASES:
            raise LifecycleError("计时基准必须是 atime / mtime / ctime")
        age_unit = fields.get("age_unit", "days")
        if age_unit not in config.LIFECYCLE_AGE_UNITS:
            raise LifecycleError("时间单位非法")
        try:
            age_amount = int(fields.get("age_amount", 0))
        except (TypeError, ValueError):
            raise LifecycleError("时间数值非法")
        if age_amount <= 0:
            raise LifecycleError("时间必须为正整数")
        if ttl_seconds(age_amount, age_unit) < 5:
            raise LifecycleError("最短阈值为 5 秒")
        match = fields.get("match", "prefix")
        if match not in ("prefix", "exact"):
            match = "prefix"
        priority = int(fields.get("priority", 100) or 0)
        return {
            "name": (fields.get("name") or "").strip()[:80]
                    or f"{ACTION_LABEL[action]}规则",
            "path": path,
            "match": match,
            "action": action,
            "age_amount": age_amount,
            "age_unit": age_unit,
            "age_base": age_base,
            "priority": priority,
            "enabled": bool(fields.get("enabled", True)),
            "note": (fields.get("note") or "").strip()[:300],
        }

    def add_rule(self, fields, actor="admin"):
        clean = self.validate_rule(fields)
        rule = {
            "id": gen_id("lc"),
            **clean,
            "created_by": actor,
            "created_at": now(),
            "updated_at": now(),
        }
        with self.meta.lock:
            self._doc().setdefault("rules", []).append(rule)
            self.meta.touch("lifecycle")
        self.nn.log_event("INFO", "lifecycle", "rule_create", rule["id"], actor,
                          f"{ACTION_LABEL[rule['action']]} @ {rule['path']} "
                          f"{rule['age_amount']}{rule['age_unit']} "
                          f"按{BASE_LABEL[rule['age_base']]} priority={rule['priority']}")
        return rule

    def update_rule(self, rule_id, fields, actor="admin"):
        with self.meta.lock:
            rule = self._find_rule(rule_id)
            if not rule:
                raise LifecycleError("规则不存在")
            merged = {**rule, **fields}
            clean = self.validate_rule(merged)
            old_key = (rule["path"], rule["action"], rule["age_amount"],
                       rule["age_unit"], rule["age_base"], rule["priority"])
            rule.update(clean)
            rule["updated_at"] = now()
            # 实质内容变更：相关豁免失效，强制重新评估
            new_key = (rule["path"], rule["action"], rule["age_amount"],
                       rule["age_unit"], rule["age_base"], rule["priority"])
            if old_key != new_key:
                self._drop_exemptions_for_rule(rule_id)
                self._cancel_pending_for_rule(
                    rule_id, "规则被修改，待触发动作自动撤销，按新规则重新评估")
            self.meta.touch("lifecycle")
        self.nn.log_event("INFO", "lifecycle", "rule_update", rule_id, actor,
                          f"{ACTION_LABEL[rule['action']]} @ {rule['path']}")
        return rule

    def delete_rule(self, rule_id, actor="admin"):
        with self.meta.lock:
            doc = self._doc()
            before = len(doc.get("rules", []))
            doc["rules"] = [r for r in doc.get("rules", []) if r["id"] != rule_id]
            if len(doc["rules"]) == before:
                raise LifecycleError("规则不存在")
            self._drop_exemptions_for_rule(rule_id)
            self._cancel_pending_for_rule(rule_id, "规则被删除，待触发动作自动撤销")
            self.meta.touch("lifecycle")
        self.nn.log_event("WARN", "lifecycle", "rule_delete", rule_id, actor, "")

    def set_enabled(self, rule_id, enabled, actor="admin"):
        with self.meta.lock:
            rule = self._find_rule(rule_id)
            if not rule:
                raise LifecycleError("规则不存在")
            if rule["enabled"] != enabled:
                rule["enabled"] = enabled
                rule["updated_at"] = now()
                if not enabled:
                    self._cancel_pending_for_rule(
                        rule_id, "规则已停用，待触发动作自动撤销")
                self.meta.touch("lifecycle")
            return rule

    def _find_rule(self, rule_id):
        return next((r for r in self._doc().get("rules", [])
                     if r["id"] == rule_id), None)

    # ==================================================================
    # 规则匹配
    # ==================================================================
    @staticmethod
    def _rule_matches_path(rule, path):
        rp = rule.get("path", "/")
        if rule.get("match", "prefix") == "exact":
            return path == rp
        return (path == rp or rp == "/"
                or path.startswith(rp.rstrip("/") + "/"))

    @staticmethod
    def age_reference_ts(inode, base):
        """规则计时基准时间戳。atime 缺失时回退 mtime（新文件从未被读）。"""
        if base == "atime":
            return inode.get("last_access") or inode.get("modified_at") \
                or inode.get("created_at") or 0
        if base == "mtime":
            return inode.get("modified_at") or inode.get("created_at") or 0
        return inode.get("created_at") or inode.get("modified_at") or 0

    def _matching_rules(self, path, rules=None):
        rules = self.list_rules() if rules is None else rules
        return [r for r in rules if r.get("enabled", True)
                and self._rule_matches_path(r, path)]

    def evaluate_file(self, inode, path, t=None, rules=None):
        """
        评估单个文件：返回 {action: {"rule","due","age","threshold","shadowed"}}。
        仅评估活动文件系统内的文件（回收站/非文件不评估，调用方过滤）。
        """
        t = now() if t is None else t
        result = {}
        if inode.get("type") != "file":
            return result
        for rule in self._matching_rules(path, rules):
            action = rule["action"]
            threshold = ttl_seconds(rule["age_amount"], rule["age_unit"])
            ref = self.age_reference_ts(inode, rule["age_base"])
            age = max(0.0, t - ref)
            due = age >= threshold
            slot = result.get(action)
            hit = {"rule_id": rule["id"], "rule_name": rule["name"],
                   "priority": rule.get("priority", 0),
                   "age_base": rule["age_base"],
                   "age": age, "threshold": threshold,
                   "reference_ts": ref, "due": due,
                   "path_match": rule.get("path", "/")}
            if slot is None:
                result[action] = {"rule": rule, "winner": hit,
                                  "shadowed": []}
            else:
                slot["shadowed"].append(hit)
        return result

    def _active_files(self):
        """枚举活动文件树（不含回收站）中的 (path, inode)。"""
        with self.meta.lock:
            trash_id = self.nn.fs.trash_id
            out = []
            for path, inode in self.nn.fs.all_files():
                if inode.get("parent") == trash_id:
                    continue
                # all_files 从 root 遍历，本就不含 .trash；双保险：排除其子孙
                out.append((path, inode))
            return out

    # ==================================================================
    # 预览（页面"即将触发的动作" + 规则覆盖轨迹）
    # ==================================================================
    def preview(self, scope_path=None, limit=500):
        """
        模拟评估全部（或 scope 路径子树）文件：
          * 每个动作的生效规则、年龄/阈值、是否已到期、宽限期截止时间；
          * 被覆盖（shadowed）规则列表；
          * 当前是否已在宽限 pending 中 / 是否已归档。
        """
        scope_path = norm_path(scope_path) if scope_path else None
        t = now()
        rules = self.list_rules()
        with self.meta.lock:
            pending = dict(self._doc().get("pending", {}))
            exemptions = dict(self._doc().get("exemptions", {}))
        items = []
        for path, inode in self._active_files():
            if scope_path and not (path == scope_path or scope_path == "/"
                                   or path.startswith(
                                       scope_path.rstrip("/") + "/")):
                continue
            evals = self.evaluate_file(inode, path, t, rules)
            if not evals:
                continue
            for action, info in evals.items():
                w = info["winner"]
                key = f"{inode['id']}:{action}"
                pend = pending.get(key)
                ex = exemptions.get(key)
                if ex and not self._exemption_alive(ex, w, inode, t):
                    ex = None       # 豁免已失效（撤销后文件被重新访问/修改）
                archived = inode.get("tier") == "archived"
                # 已归档文件不再重复归档
                if action == "archive" and archived:
                    continue
                grace_left = (pend["due_at"] - t) if pend else None
                items.append({
                    "inode_id": inode["id"],
                    "path": path,
                    "name": inode["name"],
                    "size": inode.get("size", 0),
                    "action": action,
                    "action_label": ACTION_LABEL[action],
                    "rule_id": w["rule_id"],
                    "rule_name": w["rule_name"],
                    "rule_path": w["path_match"],
                    "priority": w["priority"],
                    "age_base": w["age_base"],
                    "age_base_label": BASE_LABEL[w["age_base"]],
                    "age_seconds": round(w["age"], 1),
                    "threshold_seconds": w["threshold"],
                    "reference_ts": w["reference_ts"],
                    "due": w["due"],
                    "tier": inode.get("tier", "hot"),
                    "pending_id": pend["id"] if pend else None,
                    "pending_since": pend["created_at"] if pend else None,
                    "grace_left": max(0.0, grace_left) if grace_left is not None
                    else None,
                    "exempted": bool(ex),
                    "exempted_by": ex.get("by") if ex else None,
                    "shadowed": [{
                        "rule_id": s["rule_id"], "rule_name": s["rule_name"],
                        "priority": s["priority"],
                        "rule_path": s["path_match"],
                        "would_due": s["due"],
                    } for s in info["shadowed"]],
                })
        items.sort(key=lambda x: (
            0 if x["pending_id"] else 1,
            -(x["grace_left"] if x["grace_left"] is not None else 10 ** 12),
            x["path"]))
        due = [x for x in items if x["due"]]
        in_grace = [x for x in items if x["pending_id"]]
        return {
            "t": t,
            "grace_seconds": config.LIFECYCLE_GRACE_SECONDS,
            "scan_interval": config.LIFECYCLE_SCAN_INTERVAL,
            "total": len(items),
            "due_count": len(due),
            "in_grace_count": len(in_grace),
            "items": items[:limit],
        }

    # ==================================================================
    # 扫描：同步 pending + 执行到期动作
    # ==================================================================
    def scan(self):
        """一轮完整评估：刷新待触发列表（自动撤销失效项），执行到期动作。"""
        t = now()
        created = executed = canceled = 0
        with self.meta.lock:
            rules = self.list_rules()
            doc = self._doc()
            pending = doc.setdefault("pending", {})
            exemptions = doc.setdefault("exemptions", {})

            # 当前快照：inode:action -> winner
            current = {}
            current_inodes = {}
            for path, inode in self._active_files():
                evals = self.evaluate_file(inode, path, t, rules)
                for action, info in evals.items():
                    w = info["winner"]
                    if action == "archive" and inode.get("tier") == "archived":
                        continue
                    current[f"{inode['id']}:{action}"] = (w, info["shadowed"],
                                                          path, inode)
                    current_inodes[inode["id"]] = inode

            # 1) 撤销不再成立的 pending（规则变更 / 文件被访问修改）
            for key in list(pending.keys()):
                if key in current:
                    w, _shadowed, path, inode = current[key]
                    pend = pending[key]
                    still_winner = pend["rule_id"] == w["rule_id"]
                    still_due = w["due"]
                    if still_winner and still_due:
                        continue
                    reason = ("规则/优先级变更，待触发动作自动撤销"
                              if not still_winner
                              else "文件被重新访问或修改，条件不再成立，"
                                   "倒计时自动撤销")
                else:
                    reason = "规则不再命中该文件（规则变更/文件移动），" \
                             "待触发动作自动撤销"
                self._cancel_pending_nolock(key, reason)
                canceled += 1

            # 2) 为新满足条件的文件挂起 pending（豁免除外）
            # 顺便惰性清理已失效豁免（文件在撤销后被重新访问/修改、规则已变更，
            # 或文件已不再受该规则约束）——检查不依赖"当前是否到期"
            all_hits = {}
            for path2, inode2 in self._active_files():
                ev2 = self.evaluate_file(inode2, path2, t, rules)
                for act2, info2 in ev2.items():
                    all_hits[f"{inode2['id']}:{act2}"] = (
                        info2["winner"], inode2)
            for ex_key in list(exemptions.keys()):
                ex = exemptions[ex_key]
                hit = all_hits.get(ex_key)
                if hit is None:
                    exemptions.pop(ex_key, None)
                    continue
                ex_w, ex_inode = hit
                if not self._exemption_alive(ex, ex_w, ex_inode, t):
                    exemptions.pop(ex_key, None)

            for key, (w, shadowed, path, inode) in current.items():
                if not w["due"]:
                    continue
                ex = exemptions.get(key)
                if ex and self._exemption_alive(ex, w, inode, t):
                    continue
                if ex:
                    exemptions.pop(key, None)      # 豁免已失效（被重新访问过）
                if key in pending:
                    continue
                action = key.split(":", 1)[1]
                pd = {
                    "id": gen_id("pd"),
                    "key": key,
                    "inode_id": inode["id"],
                    "path": path,
                    "name": inode["name"],
                    "size": inode.get("size", 0),
                    "action": action,
                    "rule_id": w["rule_id"],
                    "rule_name": w["rule_name"],
                    "age_base": w["age_base"],
                    "threshold_seconds": w["threshold"],
                    "reference_ts": w["reference_ts"],
                    "shadowed_by": [s["rule_id"] for s in shadowed],
                    "created_at": t,
                    "updated_at": t,
                    "due_at": t + config.LIFECYCLE_GRACE_SECONDS,
                }
                pending[key] = pd
                created += 1
                self.nn.emit(
                    "lifecycle_pending",
                    f"「{path}」将在 {int(config.LIFECYCLE_GRACE_SECONDS)} 秒后"
                    f"{ACTION_LABEL[pd['action']]}（规则 {w['rule_name']}）",
                    path=path, action=pd["action"], pending=pd["id"])

            # 3) 执行到期 pending
            due_keys = [k for k, p in pending.items() if p["due_at"] <= t]
            due_items = [(k, pending.pop(k)) for k in due_keys]
            if created or canceled or due_items:
                self.meta.touch("lifecycle", flush=False)

        for _key, pd in due_items:
            try:
                self._execute(pd)
                executed += 1
            except Exception as e:  # noqa: BLE001
                self._add_history(
                    pd, f"{pd['action']}_failed",
                    f"执行失败，将在下轮重试: {e}", by="system")
                self.nn.log_event("ERROR", "lifecycle",
                                  f"{pd['action']}_failed", pd["path"],
                                  "system", str(e)[:300])
        return {"created": created, "executed": executed, "canceled": canceled,
                "due": len(due_keys)}

    def _exemption_alive(self, ex, winner, inode, t):
        """
        豁免是否仍然压制挂起（用户撤销 = 打盹至下一个"新鲜周期"）：
          * 规则被修改/删除/停用（updated_at 晚于撤销时刻）=> 失效；
          * 文件在撤销之后被重新访问/修改（计时基准时间戳晚于撤销时刻）
            => 本轮条件已被重置，豁免完成使命而失效（文件将来再次老化时
               会作为一个新的周期重新挂起）；
          * 否则继续压制（文件一直没被动过、规则也没变）。
        """
        rule = None
        with self.meta.lock:
            rule = self._find_rule(ex.get("rule_id"))
        if not rule or not rule.get("enabled", True):
            return False
        if rule.get("updated_at", 0) > ex.get("at", 0) + 0.01:
            return False
        # 撤销之后出现了更新的访问/修改（新周期）=> 豁免失效
        return winner["reference_ts"] <= ex.get("at", 0)

    def _execute(self, pd):
        action = pd["action"]
        with self.meta.lock:
            inode = self.nn.fs.get_inode(pd["inode_id"])
            path = self.nn.fs.path_of(pd["inode_id"]) if inode else pd["path"]
        if not inode:
            self._add_history(pd, "skipped", "inode 已不存在", by="system")
            return
        rule = None
        with self.meta.lock:
            rule = self._find_rule(pd["rule_id"])
        if not rule or not rule.get("enabled", True):
            self._add_history(pd, "canceled", "规则已删除或停用", by="system")
            return

        if action == "archive":
            self._do_archive(pd, rule, inode, path)
        elif action == "trash":
            self._do_trash(pd, rule, inode, path)

    # ------------------------------------------------------------------ 归档
    def _do_archive(self, pd, rule, inode, path):
        if inode.get("tier") == "archived":
            self._add_history(pd, "skipped", "文件已处于冷归档层", by="system")
            return
        result = self.nn.archive_inode_blocks(inode)
        with self.meta.lock:
            fresh = self.nn.fs.get_inode(inode["id"])
            fresh["tier"] = "archived"
            fresh["archived_at"] = now()
            fresh["archive_rule_id"] = rule["id"]
            self.meta.touch("fs")
        self._add_history(
            pd, "archived",
            f"{result['evacuated']} 个块下沉冷归档库，"
            f"{result['mixed']} 个块仍被热数据共享而保留热副本，"
            f"释放热存储约 {result['freed_bytes']} 字节；访问时自动取回",
            by="system", extra={
                "blocks_evacuated": result["evacuated"],
                "blocks_mixed": result["mixed"],
                "freed_bytes": result["freed_bytes"],
                "vault": result["vault_blocks"],
            })
        self.nn.log_event("WARN", "lifecycle", "archive", path, "system",
                          f"规则 {rule['name']} 触发：{result['evacuated']} 块"
                          f"下沉冷数据，{result['mixed']} 块共享保留")
        self.nn.emit("lifecycle_archived",
                     f"「{path}」已归档到冷数据，访问时自动取回", path=path)

    # ---------------------------------------------------------------- 回收站
    def _do_trash(self, pd, rule, inode, path):
        # 若同一文件还有 archive 宽限任务，随删除一并撤销
        with self.meta.lock:
            doc = self._doc()
            ap = doc.get("pending", {}).get(f"{inode['id']}:archive")
        if ap:
            self.cancel_pending(ap["id"], "system",
                                note="文件已被回收站规则移走，归档倒计时撤销",
                                silent=True)
        item = self.nn.fs.delete_to_trash(
            path, actor="system", rule_id=rule["id"], rule_name=rule["name"])
        self._add_history(
            pd, "trashed",
            f"规则触发，自动移入回收站（回收站条目 {item['id']}，"
            f"保留至 {item.get('expires_at')}）",
            by="system",
            extra={"recycle_item": item["id"],
                   "expires_at": item.get("expires_at")})
        self.nn.log_event("WARN", "lifecycle", "trash", path, "system",
                          f"规则 {rule['name']} 触发：自动移入回收站 "
                          f"{item['id']}")
        self.nn.emit("lifecycle_trashed",
                     f"「{path}」已被规则自动移入回收站，可在回收站页恢复",
                     path=path)

    # ==================================================================
    # 撤销（页面触发）
    # ==================================================================
    def cancel_pending(self, pending_id, actor="admin", note="", silent=False):
        with self.meta.lock:
            doc = self._doc()
            target = next((p for p in doc.get("pending", {}).values()
                           if p["id"] == pending_id), None)
            if not target:
                raise LifecycleError("待触发动作不存在或已执行（可在历史中查看）")
            key = target["key"]
            rule = self._find_rule(target["rule_id"])
            ref = target.get("reference_ts")
            inode = self.nn.fs.get_inode(target["inode_id"])
            # 豁免：阻止扫描器立刻重新挂起
            doc.setdefault("exemptions", {})[key] = {
                "rule_id": target["rule_id"],
                "action": target["action"],
                "path": target["path"],
                "inode_id": target["inode_id"],
                "reference_ts": ref,
                "at": now(),
                "by": actor,
                "note": note or "用户在触发前撤销",
            }
            doc["pending"].pop(key, None)
            self.meta.touch("lifecycle")
            self._append_history_nolock({
                "id": gen_id("lh"), "ts": now(),
                "inode_id": target["inode_id"], "path": target["path"],
                "name": target["name"], "action": target["action"],
                "rule_id": target["rule_id"], "rule_name": target["rule_name"],
                "status": "canceled", "by": actor,
                "detail": note or "用户在触发前撤销（宽限期内）；在文件被重新"
                                  "访问/修改或规则变更前不会再次自动触发",
            })
        if not silent:
            self.nn.log_event("INFO", "lifecycle", "cancel", target["path"],
                              actor, f"撤销 {ACTION_LABEL.get(target['action'])}"
                                     f"（规则 {target['rule_name']}）")
        return {"ok": True}

    def _cancel_pending_nolock(self, key, reason):
        doc = self._doc()
        pd = doc.get("pending", {}).get(key)
        if not pd:
            return
        doc["pending"].pop(key, None)
        self._append_history_nolock({
            "id": gen_id("lh"), "ts": now(),
            "inode_id": pd["inode_id"], "path": pd["path"], "name": pd["name"],
            "action": pd["action"], "rule_id": pd["rule_id"],
            "rule_name": pd["rule_name"], "status": "canceled",
            "by": "system", "detail": reason,
        })

    def _cancel_pending_for_rule(self, rule_id, reason):
        doc = self._doc()
        for key, pd in list(doc.get("pending", {}).items()):
            if pd.get("rule_id") == rule_id:
                self._cancel_pending_nolock(key, reason)

    def _drop_exemptions_for_rule(self, rule_id):
        doc = self._doc()
        doc["exemptions"] = {k: v for k, v in doc.get("exemptions", {}).items()
                             if v.get("rule_id") != rule_id}

    # ==================================================================
    # 手动操作：立即扫描 / 手动归档 / 手动取回
    # ==================================================================
    def run_scan_now(self, actor="admin"):
        result = self.scan()
        self.nn.log_event("INFO", "lifecycle", "scan_manual", "", actor,
                          f"新增挂起 {result['created']}，执行 {result['executed']}"
                          f"，自动撤销 {result['canceled']}")
        return result

    def archive_path_now(self, path, actor="admin"):
        """跳过宽限立即归档（手动按钮）。"""
        with self.meta.lock:
            inode = self.nn.fs.resolve(path)
            if inode["type"] != "file":
                raise LifecycleError("仅支持文件归档")
            if inode.get("tier") == "archived":
                raise LifecycleError("文件已在冷归档层")
            rule = self._best_rule_for(path, "archive")
        pd = self._synthetic_pending(inode, path, rule, actor)
        # 已归档（并发/宽限恰好到期被扫描器抢先执行）则视为成功、不重复留痕
        with self.meta.lock:
            fresh = self.nn.fs.get_inode(inode["id"])
            if fresh and fresh.get("tier") == "archived":
                return {"ok": True, "already": True}
        self._do_archive(pd, rule or self._manual_rule("archive"), inode, path)
        return {"ok": True}

    def restore_path_now(self, path, actor="admin"):
        """手动取回（读路径本身也会透明取回，这里供页面显式按钮使用）。"""
        return self.nn.restore_file(path, actor, manual=True)

    def _best_rule_for(self, path, action):
        t = now()
        with self.meta.lock:
            inode = self.nn.fs.resolve(path)
            evals = self.evaluate_file(inode, path, t)
        return evals.get(action, {}).get("rule")

    def _manual_rule(self, action):
        return {"id": "", "name": f"手动{ACTION_LABEL[action]}",
                "action": action}

    def _synthetic_pending(self, inode, path, rule, actor):
        action = "archive"
        rid = rule["id"] if rule else ""
        rname = rule["name"] if rule else "手动归档"
        return {
            "id": gen_id("pd"), "key": f"{inode['id']}:{action}",
            "inode_id": inode["id"], "path": path, "name": inode["name"],
            "action": action, "rule_id": rid, "rule_name": rname,
        }

    # ==================================================================
    # 透明取回后的 inode 状态收敛（由 NN 读路径调用）
    # ==================================================================
    def note_blocks_restored(self, inode_id, restored_bids):
        """块取回后检查 inode 是否全部块回到热层，是则 tier=hot 并留痕。"""
        with self.meta.lock:
            inode = self.nn.fs.get_inode(inode_id)
            if not inode or inode.get("tier") not in ("archived", "restoring"):
                return None
            blocks_doc = self.meta.get("blocks")["blocks"]
            cold = [b for b in inode.get("block_ids", [])
                    if (blocks_doc.get(b) or {}).get("tier") == "cold"]
            if cold:
                if inode.get("tier") == "archived":
                    inode["tier"] = "restoring"
                    self.meta.touch("fs", flush=False)
                return {"state": "restoring", "cold_remaining": len(cold)}
            path = self.nn.fs.path_of(inode_id)
            archived_at = inode.get("archived_at")
            inode["tier"] = "hot"
            inode["restored_at"] = now()
            inode["last_access"] = now()
            inode.pop("archived_at", None)
            rule_id = inode.pop("archive_rule_id", None)
            self.meta.touch("fs")
            self._append_history_nolock({
                "id": gen_id("lh"), "ts": now(),
                "inode_id": inode_id, "path": path, "name": inode["name"],
                "action": "archive", "rule_id": rule_id or "",
                "rule_name": "", "status": "restored", "by": "system",
                "detail": f"冷数据访问触发透明取回，{len(restored_bids)} 个块"
                          f"已重新复制到 DataNode，内容完整可读",
                "restored_blocks": len(restored_bids),
                "archived_at": archived_at,
            })
            self.nn.log_event("INFO", "lifecycle", "restore", path, "system",
                              f"透明取回完成（{len(restored_bids)} 块）")
            self.nn.emit("lifecycle_restored",
                         f"「{path}」已从冷数据取回，访问完全透明", path=path)
            return {"state": "hot", "path": path}

    # ==================================================================
    # 历史 / 待触发视图
    # ==================================================================
    def _add_history(self, pd, status, detail, by="system", extra=None):
        entry = {
            "id": gen_id("lh"), "ts": now(),
            "inode_id": pd["inode_id"], "path": pd["path"], "name": pd["name"],
            "action": pd["action"], "rule_id": pd.get("rule_id", ""),
            "rule_name": pd.get("rule_name", ""),
            "status": status, "by": by, "detail": detail,
        }
        if extra:
            entry.update(extra)
        with self.meta.lock:
            self._append_history_nolock(entry)

    def _append_history_nolock(self, entry):
        hist = self._doc().setdefault("history", [])
        hist.append(entry)
        if len(hist) > config.LIFECYCLE_HISTORY_CAP:
            del hist[:-config.LIFECYCLE_HISTORY_CAP]
        self.meta.touch("lifecycle", flush=False)

    def list_pending(self):
        with self.meta.lock:
            items = sorted(self._doc().get("pending", {}).values(),
                           key=lambda p: p.get("due_at", 0))
            t = now()
            out = []
            for p in items:
                q = dict(p)
                q["action_label"] = ACTION_LABEL.get(p["action"], p["action"])
                q["grace_left"] = max(0.0, p["due_at"] - t)
                out.append(q)
            return {"t": t,
                    "grace_seconds": config.LIFECYCLE_GRACE_SECONDS,
                    "items": out}

    def list_history(self, path=None, rule_id=None, limit=200, action=None,
                     status=None):
        with self.meta.lock:
            items = list(self._doc().get("history", []))
        if path:
            path = norm_path(path)
            items = [h for h in items
                     if h.get("path") == path
                     or (path != "/" and (h.get("path") or "").startswith(
                         path.rstrip("/") + "/"))]
        if rule_id:
            items = [h for h in items if h.get("rule_id") == rule_id]
        if action:
            items = [h for h in items if h.get("action") == action]
        if status:
            items = [h for h in items if h.get("status") == status]
        items.reverse()
        return {"total": len(items), "items": items[:limit]}

    def file_lifecycle(self, path):
        """文件详情抽屉：当前层 / 命中规则轨迹 / 待触发 / 历史（被哪条规则动过）。"""
        with self.meta.lock:
            inode = self.nn.fs.resolve(path)
            if inode["type"] != "file":
                raise LifecycleError("仅文件有生命周期视图")
            t = now()
            rules = self.list_rules()
            evals = self.evaluate_file(inode, path, t, rules)
            history = [h for h in self._doc().get("history", [])
                       if h.get("inode_id") == inode["id"]]
            pending = [p for p in self._doc().get("pending", {}).values()
                       if p.get("inode_id") == inode["id"]]
        actions = {}
        for action, info in evals.items():
            w = info["winner"]
            actions[action] = {
                "winner": {
                    "rule_id": w["rule_id"], "rule_name": w["rule_name"],
                    "rule_path": w["path_match"], "priority": w["priority"],
                    "age_base": w["age_base"],
                    "age_base_label": BASE_LABEL[w["age_base"]],
                    "age_seconds": round(w["age"], 1),
                    "threshold_seconds": w["threshold"],
                    "due": w["due"],
                },
                "shadowed": [{"rule_id": s["rule_id"], "rule_name": s["rule_name"],
                              "priority": s["priority"],
                              "rule_path": s["path_match"]}
                             for s in info["shadowed"]],
            }
        history.reverse()
        return {
            "path": path,
            "tier": inode.get("tier", "hot"),
            "archived_at": inode.get("archived_at"),
            "restored_at": inode.get("restored_at"),
            "archive_rule_id": inode.get("archive_rule_id"),
            "last_access": inode.get("last_access"),
            "modified_at": inode.get("modified_at"),
            "created_at": inode.get("created_at"),
            "actions": actions,
            "pending": pending,
            "history": history,
        }

    def stats(self):
        with self.meta.lock:
            rules = self._doc().get("rules", [])
            pending = self._doc().get("pending", {})
            history = self._doc().get("history", [])
            archived = 0
            for _p, inode in self.nn.fs.all_files():
                if inode.get("tier") == "archived":
                    archived += 1
                elif inode.get("tier") == "restoring":
                    archived += 1
        t = now()
        return {
            "rules": len(rules),
            "enabled_rules": sum(1 for r in rules if r.get("enabled", True)),
            "pending": len(pending),
            "due_now": sum(1 for p in pending.values()
                           if p.get("due_at", 0) <= t),
            "archived_files": archived,
            "cold_blocks": self.nn.cold_block_count(),
            "history_total": len(history),
        }
