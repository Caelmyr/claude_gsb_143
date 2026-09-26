# -*- coding: utf-8 -*-
"""
versioning.py — 版本控制（提交 / 分支 / 合并 / 检出 / 差异）
================================================================
难点三：版本树冲突合并。

数据模型（meta['versions'] JSON 文档）：
    {
      "head_branch": "main",
      "branches": {name: {"name","head","created_at","created_by","desc"}},
      "commits":  {cid: {
          "id","parent_ids":[..],"message","author","ts",
          "snapshot": {path: {"type","size","content_hash","block_ids","mime","owner","mode"}},
          "tree_hash","stats":{...},"conflicts":[..],"merge_info":{...}
      }}
    }

提交快照直接引用块的 block_ids——块不可变（写入后 genstamp/校验和固定），
因此历史版本天然可读：旧提交引用的块被 GC 保护（引用集 = 活动文件 ∪
全部提交快照）。检出/合并即"把某个快照物化回活动 inode 树"。

合并 = 快照级三方合并（base = LCA）：
  * 单侧变更（含增/删）        -> 直接采纳；
  * 双侧同改但内容一致         -> 采纳其一；
  * 双侧同改且不一致：
      - 文本文件：读取三方内容做 diff3 行级合并（diff_engine.merge3），
        干净则生成合并后内容写入新块；有冲突则写入冲突标记并记录；
      - 修改/删除冲突：保留修改侧并记录冲突；
      - 二进制：保留 ours 并记录冲突。
合并产生双亲提交（merge commit），冲突路径写入 commit.conflicts。
"""

import threading

from . import config
from .diff_engine import diff_opcodes, diff_stats, merge3, split_lines
from .util import (LRU, canonical_json, classify_merge_base, decode_text,
                   gen_id, is_text_mime, looks_binary, merge_label_swap,
                   now, sha256_bytes, sha256_text, short_hash, sort_by_ts)


class VersionError(Exception):
    pass


class VersionStore:
    def __init__(self, nn):
        self.nn = nn
        self.meta = nn.meta
        self.lock = threading.RLock()
        self._diff_cache = LRU(maxsize=128)

    # ---------------------------------------------------------------- 基础
    def _v(self):
        return self.meta.get("versions")

    def ensure_head(self):
        with self.meta.lock:
            v = self._v()
            v.setdefault("head_branch", config.DEFAULT_BRANCH)
            v.setdefault("branches", {})
            v.setdefault("commits", {})
            branches = v["branches"]
            if config.DEFAULT_BRANCH not in branches:
                branches[config.DEFAULT_BRANCH] = {
                    "name": config.DEFAULT_BRANCH, "head": None,
                    "created_at": now(), "created_by": "system",
                    "desc": "默认主分支",
                }
            self.meta.touch("versions")

    # ------------------------------------------------------------ 快照/树
    def snapshot_fs(self):
        """当前活动文件系统 -> 快照 {path: entry}。"""
        snap = {}
        for path, inode in self.nn.fs.all_files():
            snap[path] = {
                "type": "file",
                "size": inode.get("size", 0),
                "content_hash": inode.get("content_hash"),
                "block_ids": list(inode.get("block_ids", [])),
                "mime": inode.get("mime", ""),
                "owner": inode.get("owner", "admin"),
                "mode": inode.get("mode", "rw-r--r--"),
                "inode": inode["id"],
            }
        return snap

    @staticmethod
    def tree_hash(snapshot):
        slim = {p: {"h": e.get("content_hash"), "s": e.get("size")}
                for p, e in sorted(snapshot.items())}
        return sha256_text(canonical_json(slim))

    def branch_head(self, branch=None):
        v = self._v()
        branch = branch or v.get("head_branch", config.DEFAULT_BRANCH)
        br = v["branches"].get(branch)
        if not br:
            raise VersionError(f"分支不存在: {branch}")
        return br.get("head"), br

    def is_dirty(self, branch=None):
        head_id, _br = self.branch_head(branch)
        cur_hash = self.tree_hash(self.snapshot_fs())
        if not head_id:
            return bool(cur_hash != self.tree_hash({}))
        head = self._v()["commits"].get(head_id)
        return (head or {}).get("tree_hash") != cur_hash

    # ---------------------------------------------------------------- 提交
    def commit(self, message, author="admin", branch=None, allow_empty=False):
        message = (message or "").strip()
        if not message:
            raise VersionError("提交信息不能为空")
        with self.meta.lock:
            v = self._v()
            branch = branch or v.get("head_branch")
            head_id, br = self.branch_head(branch)
            snapshot = self.snapshot_fs()
            tree_hash = self.tree_hash(snapshot)
            if head_id:
                head = v["commits"].get(head_id)
                if head and head.get("tree_hash") == tree_hash and not allow_empty:
                    raise VersionError("工作区没有变化，无需提交")
                parents = [head_id]
                base_snap = head.get("snapshot", {})
            else:
                parents = []
                base_snap = {}

            stats = self._snapshot_change_stats(base_snap, snapshot)
            cid = self._commit_id(parents, tree_hash, message, author, snapshot)
            commit = {
                "id": cid,
                "parent_ids": parents,
                "message": message,
                "author": author,
                "ts": now(),
                "snapshot": snapshot,
                "tree_hash": tree_hash,
                "stats": stats,
                "conflicts": [],
            }
            v["commits"][cid] = commit
            br["head"] = cid
            v["head_branch"] = branch
            self.meta.touch("versions")
            self.nn.log_event("INFO", "version", "commit",
                              f"{branch}:{short_hash(cid)}", author,
                              f"提交 '{message}'：{stats['files_changed']} 个文件变更")
            return commit

    def _commit_id(self, parents, tree_hash, message, author, snapshot):
        payload = canonical_json({
            "parents": parents, "tree": tree_hash, "message": message,
            "author": author, "n": len(snapshot), "ts": now(),
        })
        return "c_" + sha256_text(payload)[:16]

    @staticmethod
    def _snapshot_change_stats(base, new):
        added = modified = deleted = 0
        bytes_delta = 0
        for p, e in new.items():
            b = base.get(p)
            if b is None:
                added += 1
                bytes_delta += e.get("size", 0)
            elif b.get("content_hash") != e.get("content_hash"):
                modified += 1
                bytes_delta += e.get("size", 0) - b.get("size", 0)
        for p, e in base.items():
            if p not in new:
                deleted += 1
                bytes_delta -= e.get("size", 0)
        return {"files_changed": added + modified + deleted,
                "added": added, "modified": modified, "deleted": deleted,
                "bytes_delta": bytes_delta}

    # ---------------------------------------------------------------- 分支
    def create_branch(self, name, from_ref=None, author="admin", desc=""):
        name = (name or "").strip()
        if not name or " " in name or "/" in name:
            raise VersionError("分支名不能为空且不含空格/斜杠")
        with self.meta.lock:
            v = self._v()
            if name in v["branches"]:
                raise VersionError(f"分支已存在: {name}")
            if from_ref:
                base_commit = self.resolve_ref(from_ref)
                head = base_commit["id"] if base_commit else None
            else:
                head, _ = self.branch_head()
            v["branches"][name] = {
                "name": name, "head": head, "created_at": now(),
                "created_by": author, "desc": desc or f"从 {from_ref or 'HEAD'} 创建",
            }
            self.meta.touch("versions")
            self.nn.log_event("INFO", "version", "branch_create", name, author,
                              f"创建分支 {name}（起点 {from_ref or 'HEAD'}）")
            return v["branches"][name]

    def delete_branch(self, name, author="admin"):
        with self.meta.lock:
            v = self._v()
            if name == v.get("head_branch"):
                raise VersionError("不能删除当前 HEAD 分支")
            if name == config.DEFAULT_BRANCH:
                raise VersionError("不能删除主分支")
            if name not in v["branches"]:
                raise VersionError(f"分支不存在: {name}")
            del v["branches"][name]
            self.meta.touch("versions")
            self.nn.log_event("WARN", "version", "branch_delete", name, author, "")

    def list_branches(self):
        with self.meta.lock:
            v = self._v()
            head_branch = v.get("head_branch")
            out = []
            for name, br in v["branches"].items():
                commit = v["commits"].get(br.get("head")) if br.get("head") else None
                out.append({
                    "name": name,
                    "head": br.get("head"),
                    "head_short": short_hash(br.get("head") or "", 8),
                    "desc": br.get("desc", ""),
                    "created_at": br.get("created_at"),
                    "created_by": br.get("created_by"),
                    "is_head": name == head_branch,
                    "commit_count": len(self.ancestors(br.get("head")))
                    if br.get("head") else 0,
                    "last_message": (commit or {}).get("message", ""),
                    "last_ts": (commit or {}).get("ts"),
                    "last_author": (commit or {}).get("author", ""),
                    "dirty": self.is_dirty(name) if name == head_branch else False,
                })
            out.sort(key=lambda b: (not b["is_head"], b["name"]))
            return {"head_branch": head_branch, "branches": out}

    def set_head_branch(self, branch):
        with self.meta.lock:
            v = self._v()
            if branch not in v["branches"]:
                raise VersionError(f"分支不存在: {branch}")
            v["head_branch"] = branch
            self.meta.touch("versions")

    # ---------------------------------------------------------------- 遍历
    def resolve_ref(self, ref):
        """ref 可以是分支名 / commit id / 短 id / HEAD。"""
        if not ref:
            return None
        with self.meta.lock:
            v = self._v()
            if ref == "HEAD":
                head_id, _ = self.branch_head()
                return v["commits"].get(head_id) if head_id else None
            br = v["branches"].get(ref)
            if br and br.get("head"):
                return v["commits"].get(br["head"])
            if ref in v["commits"]:
                return v["commits"][ref]
            for cid in v["commits"]:
                if cid.startswith(ref) or cid.endswith(ref):
                    return v["commits"][cid]
            return None

    def ancestors(self, cid, cap=2000):
        """commit 的全部祖先（含自身）id 集合。"""
        if not cid:
            return set()
        with self.meta.lock:
            commits = self._v()["commits"]
            seen = set()
            stack = [cid]
            while stack and len(seen) < cap:
                cur = stack.pop()
                if cur in seen or cur not in commits:
                    continue
                seen.add(cur)
                stack.extend(commits[cur].get("parent_ids", []))
            return seen

    def lca(self, cid1, cid2):
        """两个提交的最近公共祖先（按 ts 最大的公共祖先近似）。"""
        a1 = self.ancestors(cid1)
        a2 = self.ancestors(cid2)
        common = a1 & a2
        if not common:
            return None
        with self.meta.lock:
            commits = self._v()["commits"]
            best = max(common, key=lambda c: commits.get(c, {}).get("ts", 0))
            return best

    def list_commits(self, branch=None, limit=100, offset=0):
        with self.meta.lock:
            v = self._v()
            head_id, _ = self.branch_head(branch)
            ids = self.ancestors(head_id, cap=1000)
            commits = [v["commits"][c] for c in ids if c in v["commits"]]
            commits.sort(key=lambda c: c.get("ts", 0), reverse=True)
            # 分支引用标注
            refs = {}
            for bname, br in v["branches"].items():
                if br.get("head"):
                    refs.setdefault(br["head"], []).append(bname)
            out = []
            for c in commits[offset:offset + min(limit, config.MAX_COMMITS_PER_PAGE)]:
                out.append(self.commit_brief(c, refs.get(c["id"], [])))
            return {"total": len(commits), "commits": out,
                    "branch": branch or v.get("head_branch")}

    def commit_brief(self, c, refs=None):
        refs = refs if refs is not None else self._refs_of(c["id"])
        return {
            "id": c["id"],
            "short": short_hash(c["id"].replace("c_", ""), 8),
            "parent_ids": c.get("parent_ids", []),
            "parents_short": [short_hash(p.replace("c_", ""), 8)
                              for p in c.get("parent_ids", [])],
            "message": c.get("message", ""),
            "author": c.get("author", ""),
            "ts": c.get("ts"),
            "stats": c.get("stats", {}),
            "refs": refs,
            "is_merge": len(c.get("parent_ids", [])) > 1,
            "conflicts": c.get("conflicts", []),
            "tree_hash": short_hash(c.get("tree_hash", ""), 10),
        }

    def _refs_of(self, cid):
        v = self._v()
        return [b for b, br in v["branches"].items() if br.get("head") == cid]

    def get_commit(self, ref):
        c = self.resolve_ref(ref)
        if not c:
            raise VersionError(f"引用不存在: {ref}")
        return c

    # ---------------------------------------------------------------- 差异
    def diff_refs(self, ref_a, ref_b):
        """
        两个引用（提交/分支）之间的文件级差异。
        文本文件计算行级 adds/dels（带缓存）；二进制只报大小变化。
        """
        ca = self.get_commit(ref_a) if ref_a else None
        cb = self.get_commit(ref_b)
        snap_a = (ca or {}).get("snapshot", {})
        snap_b = cb.get("snapshot", {})
        result = self._diff_snapshots(snap_a, snap_b)
        result["a"] = (ca or {}).get("id")
        result["b"] = cb.get("id")
        result["a_label"] = ref_a or "(空)"
        result["b_label"] = ref_b
        return result

    def diff_working(self, branch=None):
        """工作区（活动文件系统）与 HEAD 提交的差异。"""
        head_id, _ = self.branch_head(branch)
        head = self._v()["commits"].get(head_id) if head_id else None
        snap_a = (head or {}).get("snapshot", {})
        snap_b = self.snapshot_fs()
        result = self._diff_snapshots(snap_a, snap_b)
        result["a"] = head_id
        result["b"] = "WORKING"
        result["a_label"] = (head_id or "(空)")[:12]
        result["b_label"] = "工作区"
        result["dirty"] = bool(result["changes"])
        return result

    def _diff_snapshots(self, snap_a, snap_b):
        changes = []
        total_adds = total_dels = 0
        paths = sorted(set(snap_a) | set(snap_b))
        for p in paths:
            ea, eb = snap_a.get(p), snap_b.get(p)
            if ea and eb and ea.get("content_hash") == eb.get("content_hash"):
                continue
            if ea and not eb:
                kind = "deleted"
            elif eb and not ea:
                kind = "added"
            else:
                kind = "modified"
            change = {
                "path": p, "kind": kind,
                "old_size": (ea or {}).get("size", 0),
                "new_size": (eb or {}).get("size", 0),
                "mime": (eb or ea or {}).get("mime", ""),
                "old_hash": short_hash((ea or {}).get("content_hash", ""), 10),
                "new_hash": short_hash((eb or {}).get("content_hash", ""), 10),
            }
            line_stats = self._line_diff_stats(ea, eb)
            if line_stats:
                change.update(line_stats)
                total_adds += line_stats.get("adds", 0)
                total_dels += line_stats.get("dels", 0)
            changes.append(change)
        return {
            "changes": changes,
            "stats": {"files": len(changes), "adds": total_adds,
                      "dels": total_dels,
                      "added": sum(1 for c in changes if c["kind"] == "added"),
                      "modified": sum(1 for c in changes if c["kind"] == "modified"),
                      "deleted": sum(1 for c in changes if c["kind"] == "deleted")},
        }

    def _line_diff_stats(self, ea, eb):
        """文本文件行级增删统计（内容按块读取，缓存按内容哈希对）。"""
        if not ea or not eb:
            return None
        if not (is_text_mime(ea.get("mime")) or is_text_mime(eb.get("mime"))):
            return None
        if max(ea.get("size", 0), eb.get("size", 0)) > config.MERGE_MAX_TEXT_BYTES:
            return {"binary": True}
        key = (ea.get("content_hash"), eb.get("content_hash"))
        cached = self._diff_cache.get(key)
        if cached is not None:
            return cached
        try:
            data_a = self.nn.read_blocks(ea.get("block_ids", []))
            data_b = self.nn.read_blocks(eb.get("block_ids", []))
            if looks_binary(data_a) or looks_binary(data_b):
                res = {"binary": True}
            else:
                la, lb = split_lines(decode_text(data_a) or ""), \
                         split_lines(decode_text(data_b) or "")
                if len(la) + len(lb) > config.DIFF_MAX_LINES:
                    res = {"binary": False, "too_large": True}
                else:
                    ops = diff_opcodes(la, lb)
                    st = diff_stats(ops)
                    res = {"adds": st["adds"], "dels": st["dels"],
                           "similarity": st["similarity"]}
        except Exception:
            res = None
        if res is not None:
            self._diff_cache.put(key, res)
        return res

    def file_at(self, ref, path):
        """读取某引用下某文件的内容（历史版本读取）。"""
        c = self.get_commit(ref)
        entry = c.get("snapshot", {}).get(path)
        if not entry:
            return {"exists": False, "path": path, "ref": c["id"]}
        data = self.nn.read_blocks(entry.get("block_ids", []))
        text = decode_text(data)
        return {
            "exists": True, "path": path, "ref": c["id"],
            "size": entry.get("size", 0),
            "content_hash": entry.get("content_hash"),
            "mime": entry.get("mime"),
            "is_text": text is not None and not looks_binary(data),
            "content": text if text is not None and not looks_binary(data) else None,
        }

    def file_history(self, path, branch=None, limit=50):
        """某文件在分支历史中的版本序列。"""
        with self.meta.lock:
            head_id, _ = self.branch_head(branch)
            ids = self.ancestors(head_id, cap=1000)
            commits = sort_by_ts([self._v()["commits"][c] for c in ids
                                  if c in self._v()["commits"]],
                                 config.HISTORY_ORDER)
            out = []
            last_hash = None
            for c in commits:
                e = c.get("snapshot", {}).get(path)
                h = (e or {}).get("content_hash")
                if e is None:
                    if last_hash is not None:
                        out.append({"commit": self.commit_brief(c),
                                    "state": "deleted", "hash": None})
                        last_hash = None
                    continue
                if h != last_hash:
                    out.append({"commit": self.commit_brief(c),
                                "state": "changed", "hash": h,
                                "size": e.get("size", 0)})
                    last_hash = h
                if len(out) >= limit:
                    break
            return out

    # ---------------------------------------------------------------- 合并
    def merge(self, source_ref, target_branch=None, author="admin"):
        """
        把 source_ref（分支/提交）合并进 target_branch（默认 HEAD 分支）。
        返回 {"kind": "noop"|"fast-forward"|"merge", ...}
        """
        with self.meta.lock:
            v = self._v()
            target_branch = target_branch or v.get("head_branch")
            head_id, tbr = self.branch_head(target_branch)
            theirs = self.resolve_ref(source_ref)
            if not theirs:
                raise VersionError(f"源引用不存在: {source_ref}")
            if source_ref == target_branch:
                raise VersionError("源分支与目标分支相同")
            ours = v["commits"].get(head_id) if head_id else None

            if not ours:
                # 目标分支还没有提交：直接快进
                tbr["head"] = theirs["id"]
                self._materialize(theirs.get("snapshot", {}), author)
                self.meta.touch("versions")
                return {"kind": "fast-forward", "commit": self.commit_brief(theirs),
                        "conflicts": [], "stats": theirs.get("stats", {})}

            base_id = self.lca(ours["id"], theirs["id"])
            base_kind = classify_merge_base(base_id, ours["id"],
                                            theirs["id"],
                                            config.MERGE_BASE_POLICY)
            if base_kind == "noop":
                return {"kind": "noop", "message": "已经是最新（源分支被目标包含）"}
            if base_kind == "fast-forward":
                # 快进合并：物化 theirs 快照
                tbr["head"] = theirs["id"]
                self._materialize(theirs.get("snapshot", {}), author)
                self.meta.touch("versions")
                self.nn.log_event("INFO", "version", "merge_ff",
                                  f"{source_ref} -> {target_branch}", author,
                                  "快进合并")
                return {"kind": "fast-forward",
                        "commit": self.commit_brief(theirs),
                        "conflicts": [], "stats": theirs.get("stats", {})}

            # 工作区脏 => 先自动提交，保证可回退
            if self.is_dirty(target_branch):
                self.commit(f"auto: 合并 {source_ref} 前的工作区快照",
                            author, target_branch)
                head_id, tbr = self.branch_head(target_branch)
                ours = v["commits"][head_id]

            base_snap = (v["commits"].get(base_id) or {}).get("snapshot", {})
            ours_snap = ours.get("snapshot", {})
            theirs_snap = theirs.get("snapshot", {})

            plan, conflicts = self._merge_snapshots(
                base_snap, ours_snap, theirs_snap,
                ours_label=target_branch, theirs_label=source_ref)

            # 物化合并结果
            self._materialize({p: e for p, (action, e, _data) in plan.items()
                               if action == "reuse"}, author)
            merged_snap = {}
            for p, (action, entry, data) in plan.items():
                if action == "delete":
                    continue
                if action == "reuse":
                    merged_snap[p] = entry
                elif action == "write":
                    info = self.nn.write_file_internal(
                        p, data, author, mime=entry.get("mime"))
                    merged_snap[p] = {
                        "type": "file", "size": info["size"],
                        "content_hash": info["content_hash"],
                        "block_ids": info["block_ids"],
                        "mime": info["mime"],
                        "owner": entry.get("owner", author),
                        "mode": entry.get("mode", "rw-r--r--"),
                        "inode": info["inode_id"],
                    }
            stats = self._snapshot_change_stats(ours_snap, merged_snap)
            cid = self._commit_id([ours["id"], theirs["id"]],
                                  self.tree_hash(merged_snap),
                                  f"merge {source_ref}", author, merged_snap)
            commit = {
                "id": cid,
                "parent_ids": [ours["id"], theirs["id"]],
                "message": f"Merge '{source_ref}' into '{target_branch}'",
                "author": author, "ts": now(),
                "snapshot": merged_snap,
                "tree_hash": self.tree_hash(merged_snap),
                "stats": stats,
                "conflicts": conflicts,
                "merge_info": {
                    "source": source_ref, "target": target_branch,
                    "base": base_id,
                    "source_head": theirs["id"], "ours_head": ours["id"],
                    "clean": not conflicts,
                },
            }
            v["commits"][cid] = commit
            tbr["head"] = cid
            self.meta.touch("versions")
            self.nn.log_event(
                "WARN" if conflicts else "INFO", "version", "merge",
                f"{source_ref} -> {target_branch}", author,
                f"合并完成：{stats['files_changed']} 文件变更，"
                f"{len(conflicts)} 处冲突")
            return {"kind": "merge", "commit": self.commit_brief(commit),
                    "conflicts": conflicts, "stats": stats,
                    "base": short_hash(base_id, 8)}

    def _merge_snapshots(self, base, ours, theirs, ours_label, theirs_label):
        """
        快照级三方合并。返回 (plan, conflicts)：
          plan[path] = (action, entry, data)
            action: reuse（直接引用旧块）| write（写新内容）| delete
          conflicts: [{"path","kind","detail"}]
        """
        plan = {}
        conflicts = []
        paths = sorted(set(base) | set(ours) | set(theirs))

        def same(x, y):
            if x is None or y is None:
                return x is None and y is None
            return x.get("content_hash") == y.get("content_hash")

        for p in paths:
            b, o, t = base.get(p), ours.get(p), theirs.get(p)
            if o is None and t is None:
                plan[p] = ("delete", None, None)
                continue
            if same(o, t):
                if o is not None:
                    plan[p] = ("reuse", o, None)
                continue
            if same(b, o):            # 我方未动 => 采纳对方（含删除）
                if t is not None:
                    plan[p] = ("reuse", t, None)
                else:
                    plan[p] = ("delete", None, None)
                continue
            if same(b, t):            # 对方未动 => 保留我方
                plan[p] = ("reuse", o, None)
                continue
            # 双侧异改
            if o is None or t is None:
                keep = o if o is not None else t
                side = "ours" if o is not None else "theirs"
                conflicts.append({
                    "path": p, "kind": "modify/delete",
                    "detail": f"一侧删除、另一侧修改；保留 {side} 版本"})
                plan[p] = ("reuse", keep, None)
                continue
            mime = o.get("mime") or t.get("mime") or ""
            too_big = max(o.get("size", 0), t.get("size", 0)) > config.MERGE_MAX_TEXT_BYTES
            if is_text_mime(mime) and not too_big:
                merged = self._merge_text_file(p, b, o, t,
                                               ours_label, theirs_label)
                if merged is not None:
                    text, file_conflicts = merged
                    plan[p] = ("write", dict(o), text.encode("utf-8"))
                    if file_conflicts:
                        conflicts.append({
                            "path": p, "kind": "content",
                            "detail": f"{file_conflicts} 处文本冲突，已写入冲突标记"})
                    continue
            conflicts.append({"path": p, "kind": "binary",
                              "detail": "二进制/超大文件双侧修改，保留 ours"})
            plan[p] = ("reuse", o, None)
        return plan, conflicts

    def _merge_text_file(self, path, b, o, t, ours_label, theirs_label):
        """读取三方内容做 diff3 行级合并；失败返回 None（按二进制冲突处理）。"""
        try:
            base_data = self.nn.read_blocks((b or {}).get("block_ids", [])) if b else b""
            ours_data = self.nn.read_blocks(o.get("block_ids", []))
            theirs_data = self.nn.read_blocks(t.get("block_ids", []))
        except Exception:
            return None
        if looks_binary(ours_data) or looks_binary(theirs_data):
            return None
        base_text = decode_text(base_data) or ""
        ours_text = decode_text(ours_data) or ""
        theirs_text = decode_text(theirs_data) or ""
        result = merge3(split_lines(base_text), split_lines(ours_text),
                        split_lines(theirs_text),
                        ours_label=ours_label, theirs_label=theirs_label,
                        label_swap=merge_label_swap(
                            config.CONFLICT_LABEL_SWAP),
                        marker_ours=config.CONFLICT_MARKER_OURS,
                        marker_sep=config.CONFLICT_MARKER_SEP,
                        marker_theirs=config.CONFLICT_MARKER_THEIRS)
        return result.text, len(result.conflicts)

    # ---------------------------------------------------------------- 检出
    def checkout(self, branch, author="admin", auto_commit=True):
        """
        切换 HEAD 分支并物化其头提交快照到活动文件系统。
        工作区有未提交修改时先自动提交（可追溯、防丢数据）。
        """
        with self.meta.lock:
            v = self._v()
            if branch not in v["branches"]:
                raise VersionError(f"分支不存在: {branch}")
            cur = v.get("head_branch")
            auto = None
            if cur == branch and not self.is_dirty(branch):
                return {"branch": branch, "auto_commit": None,
                        "message": "已在该分支且工作区干净"}
            if self.is_dirty(cur):
                if not auto_commit:
                    raise VersionError("工作区有未提交修改")
                auto = self.commit(f"auto: 切换到 {branch} 前的工作区快照",
                                   author, cur)
            v["head_branch"] = branch
            head_id = v["branches"][branch].get("head")
            snap = (v["commits"].get(head_id) or {}).get("snapshot", {}) \
                if head_id else {}
            self._materialize(snap, author)
            self.meta.touch("versions")
            self.nn.log_event("INFO", "version", "checkout", branch, author,
                              f"检出分支 {branch}（物化 {len(snap)} 个文件）")
            return {"branch": branch,
                    "auto_commit": self.commit_brief(auto) if auto else None,
                    "head": short_hash(head_id or "", 8),
                    "files": len(snap)}

    def _materialize(self, snapshot, author):
        """把快照物化为活动 inode 树：多余文件硬删除，缺失文件重建。"""
        fs = self.nn.fs
        with self.meta.lock:
            existing = {p: inode for p, inode in fs.all_files()}
            # 1. 删除不在快照中的文件
            for p, inode in existing.items():
                if p not in snapshot:
                    parent = fs.get_inode(inode.get("parent"))
                    if parent and inode["id"] in parent.get("children", []):
                        parent["children"].remove(inode["id"])
                    fs._remove_subtree(inode["id"])
            # 2. 创建/对齐快照中的文件
            for p in sorted(snapshot):
                entry = snapshot[p]
                if entry.get("type") != "file":
                    continue
                dir_path = "/".join(p.split("/")[:-1]) or "/"
                name = p.split("/")[-1]
                fs.mkdirs(dir_path, author)
                cur = existing.get(p)
                if cur and cur.get("content_hash") == entry.get("content_hash"):
                    continue
                fs.create_file(dir_path, name, entry.get("size", 0),
                               entry.get("content_hash"),
                               entry.get("block_ids", []),
                               entry.get("mime", ""),
                               entry.get("owner", author))
            # 3. 清理空目录（不属于任何快照路径前缀）
            prefixes = set()
            for p in snapshot:
                parts = [s for s in p.split("/") if s]
                for i in range(1, len(parts)):
                    prefixes.add("/" + "/".join(parts[:i]))
            self._prune_empty_dirs(fs, prefixes)
            self.meta.touch("fs")

    @staticmethod
    def _prune_empty_dirs(fs, keep_prefixes):
        inodes = fs._inodes()
        changed = True
        guard = 0
        while changed and guard < 50:
            changed = False
            guard += 1
            for iid, node in list(inodes.items()):
                if node["type"] != "dir":
                    continue
                if iid in (fs.root_id, fs.trash_id):
                    continue
                path = fs.path_of(iid)
                if path in keep_prefixes:
                    continue
                if not node.get("children"):
                    parent = inodes.get(node.get("parent"))
                    if parent and iid in parent.get("children", []):
                        parent["children"].remove(iid)
                    inodes.pop(iid, None)
                    changed = True

    # ---------------------------------------------------------------- 还原
    def restore_file(self, ref, path, author="admin"):
        """把历史版本中的单个文件恢复到活动文件系统。"""
        with self.meta.lock:
            c = self.get_commit(ref)
            entry = c.get("snapshot", {}).get(path)
            if not entry:
                raise VersionError(f"{ref} 中不存在文件: {path}")
            dir_path = "/".join(path.split("/")[:-1]) or "/"
            name = path.split("/")[-1]
            fs = self.nn.fs
            fs.mkdirs(dir_path, author)
            inode = fs.create_file(dir_path, name, entry.get("size", 0),
                                   entry.get("content_hash"),
                                   entry.get("block_ids", []),
                                   entry.get("mime", ""), author)
            self.nn.log_event("INFO", "version", "restore_file", path, author,
                              f"从 {short_hash(c['id'], 8)} 恢复文件")
            return {"path": path, "inode": inode["id"],
                    "from_commit": c["id"]}

    # ---------------------------------------------------------------- 图
    def graph(self, branch=None, limit=60):
        """时间线/图谱数据：提交列表 + 简单泳道分配（前端渲染）。"""
        with self.meta.lock:
            data = self.list_commits(branch, limit=limit)
            commits = data["commits"]
            lanes = {}
            next_lane = 0
            for c in commits:
                # 简单泳道：沿用第一个父的泳道，其余父分配新泳道
                parents = c.get("parent_ids", [])
                lane = None
                for p in parents:
                    if p in lanes:
                        lane = lanes[p]
                        break
                if lane is None:
                    lane = next_lane
                    next_lane += 1
                lanes[c["id"]] = lane
                c["lane"] = lane % 6
                for p in parents:
                    if p not in lanes:
                        lanes[p] = next_lane
                        next_lane += 1
            data["lane_count"] = min(max(lanes.values(), default=0) + 1, 6)
            return data

    def repo_stats(self):
        with self.meta.lock:
            v = self._v()
            commits = v["commits"]
            merges = sum(1 for c in commits.values()
                         if len(c.get("parent_ids", [])) > 1)
            conflicted = sum(1 for c in commits.values() if c.get("conflicts"))
            return {
                "commits": len(commits),
                "branches": len(v["branches"]),
                "head_branch": v.get("head_branch"),
                "merge_commits": merges,
                "conflicted_commits": conflicted,
                "first_ts": min((c.get("ts", now()) for c in commits.values()),
                                default=None),
            }

    def all_referenced_blocks(self):
        """全部提交快照引用到的块 id 集合（GC 保护集的一部分）。"""
        with self.meta.lock:
            refs = set()
            for c in self._v()["commits"].values():
                for e in c.get("snapshot", {}).values():
                    refs.update(e.get("block_ids", []))
            return refs
