# -*- coding: utf-8 -*-
"""
lifecycle.py — 数据生命周期管理（自动归档冷数据 / 自动移入回收站）
====================================================================
规则存储在 meta['lifecycle'] JSON 文档：
    {
      "rules":   { "<rule_id>": {生命周期规则} },
      "history": [ 执行/撤销记录 ]，           # 全局留痕（有上限）
      "snoozes": { "<inode>|<rule>|<action>": 撤销记录 }
    }

核心概念
--------
* **规则（rule）**：作用于某个路径前缀（scope），可配置
  - archive_days：超过 N 天未访问 → 归档到冷数据（块降冗余、搬到 DN 冷介质目录）；
  - trash_days ：超过 M 天未访问 → 自动移入回收站（走标准删除保护流程）。
* **优先级（priority）**：同一文件命中多条规则时，priority 数值大的生效；
  平手时作用范围（scope）更具体的生效；再平手取较新的规则。
  其余被覆盖的规则在预览中标注为“被覆盖”，覆盖关系一目了然。
* **宽限期（grace_seconds）**：动作条件满足后不会立刻执行，而是先进入
  “即将触发”状态，宽限期内用户可在页面上撤销（dismiss）。
* **归档透明取回**：冷数据被访问时，NameNode 在读路径上同步把块从冷介质
  取回（retrieve），文件内容/校验和不变，用户无需换路径。

判定基准时间：inode.last_access（下载/预览/缩略图等读操作刷新），
从未访问过则依次回退 modified_at / created_at。
"""

import threading

from . import config
from .util import gen_id, norm_path, now


ACTIONS = ("archive", "trash")
ACTION_LABELS = {"archive": "归档冷数据", "trash": "移入回收站"}

TIER_HOT = "hot"
TIER_COLD = "cold"


class LifecycleError(Exception):
    pass


def scope_matches(path, scope):
    """路径前缀匹配：scope=/a 命中 /a、/a/b，不命中 /ab。"""
    path = norm_path(path)
    scope = norm_path(scope or "/")
    if scope == "/":
        return True
    return path == scope or path.startswith(scope.rstrip("/") + "/")


def _baseline_ts(inode):
    """生命周期判定基准：最后访问 → 最后修改 → 创建时间。"""
    return (inode.get("last_access")
            or inode.get("modified_at")
            or inode.get("created_at")
            or now())


class LifecycleManager:
    def __init__(self, nn):
        self.nn = nn
        self.meta = nn.meta
        self.fs = nn.fs
        self.lock = threading.RLock()
        self._restore_lock = threading.Lock()
        self._restoring = set()        # 正在同步取回的 inode_id
        self._stop = threading.Event()
        self._thread = None

    # ============================================================== 初始化
    def init_doc(self):
        with self.meta.lock:
            doc = self.meta.get("lifecycle")
            doc.setdefault("rules", {})
            doc.setdefault("history", [])
            doc.setdefault("snoozes", {})
            doc.setdefault("next_rule_seq", 1)
            self.meta.touch("lifecycle", flush=False)

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._scan_loop,
                                        name="nn-lifecycle", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    # ============================================================== 规则 CRUD
    def list_rules(self):
        with self.meta.lock:
            return [dict(r) for r in
                    sorted(self.meta.get("lifecycle").get("rules", {}).values(),
                           key=lambda r: (-int(r.get("priority", 0)),
                                          -len(r.get("scope", "")),
                                          -r.get("created_at", 0)))]

    def _get_rule(self, rule_id):
        return self.meta.get("lifecycle").get("rules", {}).get(rule_id)

    @staticmethod
    def _validate_fields(fields, partial=False):
        """归一化并校验表单字段，返回干净的字段 dict。"""
        out = {}
        if not partial or "name" in fields:
            name = (fields.get("name") or "").strip()
            if not name:
                raise LifecycleError("规则名称不能为空")
            if len(name) > 60:
                raise LifecycleError("规则名称过长（≤60 字）")
            out["name"] = name
        if not partial or "scope" in fields:
            scope = norm_path(fields.get("scope") or "/")
            out["scope"] = scope
        if not partial or "archive_days" in fields:
            out["archive_days"] = _parse_days(fields.get("archive_days"))
        if not partial or "trash_days" in fields:
            out["trash_days"] = _parse_days(fields.get("trash_days"))
        if not partial and not (out.get("archive_days") is not None or
                                out.get("trash_days") is not None):
            raise LifecycleError("请至少配置一个动作（归档 或 移入回收站）")
        if partial and "archive_days" in fields and "trash_days" in fields \
                and out["archive_days"] is None and out["trash_days"] is None:
            raise LifecycleError("请至少保留一个动作（归档 或 移入回收站）")
        if not partial or "priority" in fields:
            try:
                pri = int(fields.get("priority", 100))
            except (TypeError, ValueError):
                raise LifecycleError("优先级必须是整数")
            out["priority"] = max(0, min(pri, 10000))
        if not partial or "grace_seconds" in fields:
            try:
                grace = int(fields.get("grace_seconds",
                                       config.LIFECYCLE_GRACE_SECONDS))
            except (TypeError, ValueError):
                raise LifecycleError("宽限期必须是整数秒")
            out["grace_seconds"] = max(0, min(grace, 7 * 86400))
        if not partial or "enabled" in fields:
            out["enabled"] = bool(fields.get("enabled", True))
        return out

    def create_rule(self, fields, actor="admin"):
        clean = self._validate_fields(fields, partial=False)
        with self.meta.lock:
            doc = self.meta.get("lifecycle")
            seq = doc.get("next_rule_seq", 1)
            doc["next_rule_seq"] = seq + 1
            rule_id = gen_id("lc")
            ts = now()
            rule = {
                "id": rule_id,
                "seq": seq,
                "name": clean["name"],
                "scope": clean["scope"],
                "archive_days": clean["archive_days"],
                "trash_days": clean["trash_days"],
                "priority": clean["priority"],
                "grace_seconds": clean["grace_seconds"],
                "enabled": clean["enabled"],
                "created_at": ts,
                "created_by": actor,
                "updated_at": ts,
            }
            doc["rules"][rule_id] = rule
            self.meta.touch("lifecycle")
        self.nn.log_event("INFO", "lifecycle", "rule_create", clean["scope"],
                          actor, f"规则「{clean['name']}」({rule_id})")
        return rule

    def update_rule(self, rule_id, fields, actor="admin"):
        with self.meta.lock:
            rule = self._get_rule(rule_id)
            if not rule:
                raise LifecycleError("规则不存在")
            merged = {
                "name": fields.get("name", rule["name"]),
                "scope": fields.get("scope", rule["scope"]),
                "archive_days": fields.get("archive_days",
                                           rule.get("archive_days")),
                "trash_days": fields.get("trash_days",
                                         rule.get("trash_days")),
                "priority": fields.get("priority", rule.get("priority", 100)),
                "grace_seconds": fields.get(
                    "grace_seconds", rule.get(
                        "grace_seconds", config.LIFECYCLE_GRACE_SECONDS)),
                "enabled": fields.get("enabled", rule.get("enabled", True)),
            }
            clean = self._validate_fields(merged, partial=True)
            rule.update(clean)
            rule["updated_at"] = now()
            self.meta.touch("lifecycle")
            name = rule["name"]
        self.nn.log_event("INFO", "lifecycle", "rule_update", rule_id, actor,
                          f"更新规则「{name}」")
        return rule

    def delete_rule(self, rule_id, actor="admin"):
        with self.meta.lock:
            doc = self.meta.get("lifecycle")
            rule = doc["rules"].pop(rule_id, None)
            if not rule:
                raise LifecycleError("规则不存在")
            # 撤销记录依附于规则，规则删除后一并清理（执行历史保留）
            for key in [k for k in doc.get("snoozes", {})
                        if k.split("|")[1:2] == [rule_id]]:
                doc["snoozes"].pop(key, None)
            self.meta.touch("lifecycle")
            name = rule["name"]
        self.nn.log_event("INFO", "lifecycle", "rule_delete", rule_id, actor,
                          f"删除规则「{name}」（已归档/已删除的数据不受影响）")
        return {"ok": True, "name": name}

    # ============================================================== 规则匹配
    def matching_rules(self, path):
        """返回命中的全部启用规则，按生效优先级从高到低排序。"""
        rules = [r for r in self.list_rules()
                 if r.get("enabled", True) and scope_matches(path, r["scope"])]
        rules.sort(key=lambda r: (-int(r.get("priority", 0)),
                                  -len(r.get("scope", "/")),
                                  -int(r.get("seq", 0))))
        return rules

    def winner_for(self, path):
        rules = self.matching_rules(path)
        return rules[0] if rules else None

    # ============================================================== 评估/预览
    def _snoozed(self, inode_id, rule_id, action):
        return self.meta.get("lifecycle").get("snoozes", {}).get(
            f"{inode_id}|{rule_id}|{action}")

    def _action_states(self, inode, path, rule, t=None):
        """计算一条规则对一个文件的两个动作状态（archive/trash）。"""
        t = t if t is not None else now()
        baseline = _baseline_ts(inode)
        age = max(0.0, t - baseline)
        tier = inode.get("tier", TIER_HOT)
        out = []
        for action in ACTIONS:
            days = rule.get(f"{action}_days")
            if days is None:
                continue
            if action == "archive" and tier == TIER_COLD:
                continue    # 已在冷数据，无需重复归档
            threshold = days * config.LIFECYCLE_DAY_SECONDS
            due_at = baseline + threshold
            grace = max(0, int(rule.get("grace_seconds",
                                        config.LIFECYCLE_GRACE_SECONDS)))
            fire_at = due_at + grace
            snoozed = self._snoozed(inode["id"], rule["id"], action)
            if age < threshold:
                state = "aging"        # 条件尚未满足
            elif snoozed:
                state = "dismissed"    # 已被用户撤销
            elif t < fire_at:
                state = "pending"      # 宽限期内，即将触发（可撤销）
            else:
                state = "due"          # 宽限期满，下轮扫描即执行
            out.append({
                "action": action,
                "action_label": ACTION_LABELS[action],
                "rule_id": rule["id"],
                "rule_name": rule["name"],
                "rule_priority": rule.get("priority", 100),
                "scope": rule.get("scope"),
                "inode": inode["id"],
                "path": path,
                "name": inode.get("name"),
                "size": inode.get("size", 0),
                "tier": tier,
                "baseline": baseline,
                "age_seconds": age,
                "age_days": round(age / config.LIFECYCLE_DAY_SECONDS, 2),
                "threshold_days": days,
                "due_at": due_at,
                "fire_at": fire_at,
                "grace_seconds": grace,
                "remaining": max(0.0, fire_at - t),
                "state": state,
                "dismissed_at": snoozed.get("at") if snoozed else None,
                "dismissed_by": snoozed.get("by") if snoozed else None,
            })
        return out

    def evaluate(self, rule_id=None, state=None, q=None, limit=300, t=None):
        """
        评估活动文件树中全部文件：
          * 每个文件只由“生效规则（winner）”驱动动作；
          * 其它命中规则作为 covered_by 返回，用于展示覆盖关系。
        state: aging | pending | due | dismissed | actionable(pending+due) | all
        """
        t = t if t is not None else now()
        q = (q or "").strip().lower()
        items = []
        with self.meta.lock:
            for path, inode in self.fs.all_files():
                if q and q not in path.lower():
                    continue
                rules = self.matching_rules(path)
                if not rules:
                    continue
                winner, losers = rules[0], rules[1:]
                if rule_id and winner["id"] != rule_id:
                    continue
                actions = self._action_states(inode, path, winner, t)
                for a in actions:
                    a["covered_by"] = [
                        {"id": r["id"], "name": r["name"],
                         "scope": r["scope"],
                         "priority": r.get("priority", 100)}
                        for r in losers]
                    items.append(a)
        # 状态过滤
        if state and state != "all":
            wanted = state.split(",")
            items = [i for i in items if i["state"] in wanted]
        # 排序：待执行优先（剩余时间升序），其余按触发时间升序
        items.sort(key=lambda i: (
            {"due": 0, "pending": 1, "aging": 2, "dismissed": 3}
            .get(i["state"], 9), i["fire_at"]))
        total = len(items)
        return {"items": items[:limit], "total": total,
                "truncated": total > limit, "now": t}

    # ============================================================== 撤销/重启用
    def dismiss(self, inode_id, rule_id, action, actor="admin"):
        """在触发前撤销某文件的某个待执行动作。"""
        if action not in ACTIONS:
            raise LifecycleError("未知动作")
        with self.meta.lock:
            rule = self._get_rule(rule_id)
            if not rule:
                raise LifecycleError("规则不存在")
            inode = self.fs.get_inode(inode_id)
            if not inode:
                raise LifecycleError("文件不存在或已被删除")
            path = self.fs.path_of(inode_id)
            key = f"{inode_id}|{rule_id}|{action}"
            doc = self.meta.get("lifecycle")
            doc.setdefault("snoozes", {})[key] = {
                "inode": inode_id, "path": path, "rule_id": rule_id,
                "action": action, "by": actor, "at": now()}
            self.meta.touch("lifecycle")
        self._record_event("dismiss", inode, path, rule, actor,
                           f"撤销「{ACTION_LABELS[action]}」动作")
        self.nn.log_event("INFO", "lifecycle", "dismiss", path, actor,
                          f"撤销规则「{rule['name']}」的"
                          f"{ACTION_LABELS[action]}动作")
        return {"ok": True}

    def rearm(self, inode_id, rule_id, action, actor="admin"):
        """重新启用已撤销的动作（下轮评估重新计时/触发）。"""
        if action not in ACTIONS:
            raise LifecycleError("未知动作")
        with self.meta.lock:
            key = f"{inode_id}|{rule_id}|{action}"
            doc = self.meta.get("lifecycle")
            sn = doc.get("snoozes", {}).pop(key, None)
            if not sn:
                raise LifecycleError("该动作未被撤销，无需重新启用")
            self.meta.touch("lifecycle")
            rule = self._get_rule(rule_id)
            inode = self.fs.get_inode(inode_id)
            path = self.fs.path_of(inode_id) if inode else sn.get("path")
        if rule and inode:
            self._record_event("rearm", inode, path, rule, actor,
                               f"重新启用「{ACTION_LABELS[action]}」动作")
        self.nn.log_event("INFO", "lifecycle", "rearm", path or inode_id,
                          actor, f"重新启用规则「{rule['name'] if rule else rule_id}」")
        return {"ok": True}

    def clear_access_snoozes(self, inode_id):
        """文件被访问后，清掉它所有归档撤销记录（访问即续期）。"""
        with self.meta.lock:
            doc = self.meta.get("lifecycle")
            changed = False
            for key in [k for k in doc.get("snoozes", {})
                        if k.startswith(f"{inode_id}|") and k.endswith("|archive")]:
                doc["snoozes"].pop(key, None)
                changed = True
            if changed:
                self.meta.touch("lifecycle", flush=False)

    # ============================================================== 执行/扫描
    def scan_once(self, actor="system"):
        """扫描并执行所有已过宽限期（due）的动作。返回执行计数。"""
        # 只取 due；dismissed 被跳过
        eval_result = self.evaluate(state="due", limit=100000)
        archived = trashed = 0
        errors = []
        for item in eval_result["items"]:
            try:
                if item["action"] == "archive":
                    if self._do_archive(item):
                        archived += 1
                elif item["action"] == "trash":
                    if self._do_trash(item, actor):
                        trashed += 1
            except Exception as e:  # noqa: BLE001 单文件失败不影响其它
                errors.append(f"{item['path']}: {e}")
                self.nn.log_event("ERROR", "lifecycle",
                                  f"{item['action']}_failed", item["path"],
                                  "system", str(e)[:300])
        return {"archived": archived, "trashed": trashed,
                "errors": errors[:20]}

    def _scan_loop(self):
        while not self._stop.is_set():
            self._stop.wait(config.LIFECYCLE_SCAN_INTERVAL)
            try:
                result = self.scan_once()
                if result["archived"] or result["trashed"]:
                    self.nn.emit(
                        "lifecycle_run",
                        f"生命周期：归档 {result['archived']} 个文件、"
                        f"移入回收站 {result['trashed']} 个文件",
                        **result)
            except Exception as e:  # noqa: BLE001
                self.nn.log_event("ERROR", "lifecycle", "loop_error", "",
                                  "system", str(e)[:300])

    def _do_archive(self, item):
        """执行归档：块降冗余 + 搬到冷介质；inode 标记 cold。"""
        inode_id = item["inode"]
        rule_id = item["rule_id"]
        # 阶段一：锁内规划（再校验条件未被刷新）
        with self.meta.lock:
            inode = self.fs.get_inode(inode_id)
            rule = self._get_rule(rule_id)
            if not inode or not rule:
                return False
            if inode.get("tier") == TIER_COLD:
                return False
            # 宽限期之后又被访问过 -> 条件被刷新，本轮跳过
            if _baseline_ts(inode) != item["baseline"]:
                return False
            path = self.fs.path_of(inode_id)
            plan_doc = self.nn._plan_archive_inode(inode)
        # 阶段二：锁外执行网络搬运（删除命令 + 热->冷介质）
        executed = self.nn._execute_archive_plan(plan_doc)
        # 阶段三：锁内提交块表与 inode 状态
        with self.meta.lock:
            moved_blocks = self.nn._commit_archive_plan(
                executed, plan_doc["keep"])
            inode = self.fs.get_inode(inode_id)
            if not inode:
                return True
            inode["tier"] = TIER_COLD
            inode["archived_at"] = now()
            inode["archived_rule"] = rule_id
            # 归档撤销记录已无意义
            self.meta.get("lifecycle").get("snoozes", {}).pop(
                f"{inode_id}|{rule_id}|archive", None)
            self.meta.touch("lifecycle", flush=False)
        detail = (f"{moved_blocks} 个块降冗余至 "
                  f"{config.COLD_REPLICATION} 副本并迁移冷介质"
                  + (f"，{len(plan_doc['skipped_shared'])} 个共享块保留热层"
                     if plan_doc["skipped_shared"] else ""))
        self._record_event("archive", inode, path, rule, "system", detail)
        self.nn.log_event("WARN", "lifecycle", "archive", path, "system",
                          f"规则「{rule['name']}」归档冷数据：{detail}")
        self.nn.emit("lifecycle_archive",
                     f"「{path}」已归档到冷数据（{moved_blocks} 块）",
                     path=path, blocks=moved_blocks)
        return True

    def _do_trash(self, item, actor="system"):
        """执行移入回收站：复用标准删除保护流程，并在条目上标注规则来源。"""
        inode_id = item["inode"]
        rule_id = item["rule_id"]
        with self.meta.lock:
            inode = self.fs.get_inode(inode_id)
            rule = self._get_rule(rule_id)
            if not inode or not rule:
                return False
            if _baseline_ts(inode) != item["baseline"]:
                return False    # 宽限期后被访问/改过，条件刷新
            path = self.fs.path_of(inode_id)
        # delete_to_trash 内部自己加锁；放外层避免重入歧义
        rec_item = self.fs.delete_to_trash(path, actor="lifecycle")
        rec_item["lifecycle_rule_id"] = rule_id
        rec_item["lifecycle_rule_name"] = rule["name"]
        rec_item["auto"] = True
        with self.meta.lock:
            self.meta.touch("recycle")
            inode = self.fs.get_inode(rec_item["inode"])
            self.meta.get("lifecycle").get("snoozes", {}).pop(
                f"{inode_id}|{rule_id}|trash", None)
            self.meta.touch("lifecycle", flush=False)
        if inode:
            self._record_event("trash", inode, path, rule, "system",
                               "自动移入回收站（保留期内可恢复）")
        self.nn.log_event("WARN", "lifecycle", "trash", path, "system",
                          f"规则「{rule['name']}」到期，自动移入回收站")
        self.nn.emit("lifecycle_trash",
                     f"「{path}」被规则「{rule['name']}」移入回收站",
                     path=path, rule=rule["name"])
        return True

    # ============================================================== 透明取回
    def retrieve_file_if_cold(self, path, inode=None):
        """
        读路径钩子：文件在冷数据则同步取回。
        返回取回描述 dict（None 表示本来就在热层，无需取回）。
        """
        with self.meta.lock:
            inode = inode or self.fs.resolve(path)
            if inode.get("tier") != TIER_COLD:
                return None
            inode_id = inode["id"]
            block_ids = list(inode.get("block_ids", []))
            rule_id = inode.get("archived_rule")
            rule = self._get_rule(rule_id) if rule_id else None
        # 同文件并发读只触发一次取回，其余读等待同一把锁
        with self._restore_lock:
            if inode_id in self._restoring:
                already = True
            else:
                already = False
                self._restoring.add(inode_id)
        if already:
            # 等待正在进行的取回完成
            while inode_id in self._restoring and not self._stop.is_set():
                self._stop.wait(0.2)
            return {"path": path, "waited": True}
        try:
            with self.meta.lock:
                self.fs.get_inode(inode_id)["retrieving"] = True
            self.nn.emit("lifecycle_retrieve_begin",
                         f"「{path}」冷数据取回中…", path=path)
            self.nn.log_event("INFO", "lifecycle", "retrieve_begin", path,
                              "system", f"{len(block_ids)} 个冷块取回")
            restored = self.nn.restore_blocks_to_hot(block_ids)
            with self.meta.lock:
                node = self.fs.get_inode(inode_id)
                if node:
                    node["tier"] = TIER_HOT
                    node["retrieving"] = False
                    node["last_access"] = now()
                    node.pop("archived_at", None)
                    node.pop("archived_rule", None)
                    self.meta.touch("fs", flush=False)
            # 模拟冷介质取回耗时（对用户表现为一次稍慢的读）
            self._stop.wait(config.LIFECYCLE_RESTORE_DELAY)
            self.clear_access_snoozes(inode_id)
            with self.meta.lock:
                node = self.fs.get_inode(inode_id)
                path_now = self.fs.path_of(inode_id) if node else path
                if node:
                    self._record_event(
                        "retrieve", node, path_now, rule, "system",
                        f"访问触发透明取回，{restored} 个块恢复热层与冗余")
            self.nn.log_event("INFO", "lifecycle", "retrieve_done", path_now,
                              "system",
                              f"取回完成：{restored} 个块恢复热层，内容校验通过")
            self.nn.emit("lifecycle_retrieve_done",
                         f"「{path_now}」冷数据取回完成，可正常访问",
                         path=path_now, blocks=restored)
            return {"path": path_now, "blocks": restored, "waited": False}
        finally:
            with self._restore_lock:
                self._restoring.discard(inode_id)

    def retrieve_path(self, path, actor="admin"):
        """显式预取回（页面“立即取回”按钮）。"""
        with self.meta.lock:
            inode = self.fs.resolve(path)
        if inode.get("tier") != TIER_COLD:
            raise LifecycleError("该文件不在冷数据，无需取回")
        return self.retrieve_file_if_cold(path, inode)

    # ============================================================== 留痕
    def _append_inode_event(self, inode, event, rule, actor, detail):
        """在 inode 上追加生命周期事件（随文件移动/回收站/恢复一起走）。"""
        entry = {
            "ts": now(), "event": event,
            "rule_id": rule.get("id") if rule else None,
            "rule_name": rule.get("name") if rule else None,
            "actor": actor or "system", "detail": detail,
        }
        events = inode.setdefault("lifecycle_events", [])
        events.append(entry)
        if len(events) > config.LIFECYCLE_HISTORY_MAX:
            inode["lifecycle_events"] = events[-config.LIFECYCLE_HISTORY_MAX:]

    def _record_event(self, event, inode, path, rule, actor, detail):
        """同时写 inode 事件链与全局 history 文档。"""
        with self.meta.lock:
            self._append_inode_event(inode, event, rule, actor, detail)
            hist = self.meta.get("lifecycle").setdefault("history", [])
            hist.append({
                "id": gen_id("lh"), "ts": now(), "event": event,
                "rule_id": rule.get("id") if rule else None,
                "rule_name": rule.get("name") if rule else None,
                "path": path, "inode": inode.get("id"),
                "tier": inode.get("tier", TIER_HOT),
                "actor": actor or "system", "detail": detail,
            })
            if len(hist) > config.LIFECYCLE_HISTORY_GLOBAL_MAX:
                self.meta.get("lifecycle")["history"] = \
                    hist[-config.LIFECYCLE_HISTORY_GLOBAL_MAX:]
            self.meta.touch("lifecycle")

    def history(self, path=None, limit=100):
        with self.meta.lock:
            hist = list(self.meta.get("lifecycle").get("history", []))
        if path:
            p = norm_path(path)
            hist = [h for h in hist
                    if h.get("path") == p
                    or (h.get("path") or "").startswith(p.rstrip("/") + "/")]
        hist.sort(key=lambda h: h.get("ts", 0), reverse=True)
        return {"items": hist[:limit], "total": len(hist)}

    def file_trace(self, path):
        """单文件视角：当前层级、命中规则、被哪条规则动过。"""
        with self.meta.lock:
            inode = self.fs.resolve(path)
            if inode["type"] != "file":
                raise LifecycleError(f"不是文件: {path}")
            rules = self.matching_rules(path)
            winner = rules[0] if rules else None
            actions = (self._action_states(inode, path, winner)
                       if winner else [])
            info = {
                "path": path,
                "inode": inode["id"],
                "tier": inode.get("tier", TIER_HOT),
                "retrieving": bool(inode.get("retrieving")),
                "archived_at": inode.get("archived_at"),
                "last_access": inode.get("last_access"),
                "modified_at": inode.get("modified_at"),
                "winner": winner,
                "covered_by": [{"id": r["id"], "name": r["name"],
                                "scope": r["scope"],
                                "priority": r.get("priority", 100)}
                               for r in rules[1:]],
                "pending": actions,
                "events": list(reversed(inode.get("lifecycle_events", []))),
            }
            return info

    # ============================================================== 汇总视图
    def archived_files(self):
        out = []
        with self.meta.lock:
            for path, inode in self.fs.all_files():
                if inode.get("tier") == TIER_COLD:
                    out.append({
                        "path": path, "name": inode.get("name"),
                        "size": inode.get("size", 0),
                        "inode": inode["id"],
                        "archived_at": inode.get("archived_at"),
                        "rule_id": inode.get("archived_rule"),
                        "rule_name": (self._get_rule(inode["archived_rule"])
                                      or {}).get("name")
                        if inode.get("archived_rule") else None,
                        "retrieving": bool(inode.get("retrieving")),
                        "blocks": len(inode.get("block_ids", [])),
                    })
        out.sort(key=lambda x: x.get("archived_at") or 0, reverse=True)
        return out

    def stats(self):
        rules = self.list_rules()
        preview = self.evaluate(state="pending,due,dismissed", limit=100000)
        counts = {"pending": 0, "due": 0, "dismissed": 0}
        for it in preview["items"]:
            counts[it["state"]] = counts.get(it["state"], 0) + 1
        archived = self.archived_files()
        return {
            "rules": len(rules),
            "rules_enabled": sum(1 for r in rules if r.get("enabled", True)),
            "archived_files": len(archived),
            "archived_bytes": sum(a["size"] for a in archived),
            "actionable": counts["pending"] + counts["due"],
            "pending": counts["pending"],
            "due": counts["due"],
            "dismissed": counts["dismissed"],
            "grace_default": config.LIFECYCLE_GRACE_SECONDS,
            "scan_interval": config.LIFECYCLE_SCAN_INTERVAL,
            "cold_replication": config.COLD_REPLICATION,
            "restore_delay": config.LIFECYCLE_RESTORE_DELAY,
        }


def _parse_days(value):
    """None/空串 → None（不启用该动作）；必须为正数（天，允许小数用于演示）。"""
    if value is None or value == "":
        return None
    try:
        days = float(value)
    except (TypeError, ValueError):
        raise LifecycleError("天数必须是数字")
    if days <= 0:
        raise LifecycleError("天数必须为正数（不需要该动作请留空）")
    if days > 36500:
        raise LifecycleError("天数过大（≤36500）")
    return round(days, 4)
