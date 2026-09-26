# -*- coding: utf-8 -*-
"""
filesystem.py — 虚拟文件系统（inode 树 + 回收站）
====================================================
元数据集中存储在 meta['fs'] JSON 文档：
    {
      "root": "in_root",
      "trash_root": "in_trash",
      "inodes": {
         "<inode_id>": {
            "id","type"(dir|file),"name","parent","children"(dir),
            "created_at","modified_at","owner","mode",
            "size","block_ids","content_hash","mime","file_version",
            "access_count","last_access"
         }, ...
      }
    }
回收站条目存储在 meta['recycle'] 文档：
    {"items": {"<item_id>": {...}}, "retention_days": 7}

块数据不在这里——文件 inode 只持有 block_ids，块的位置/副本/校验和
由 NameNode 的块表（meta['blocks']）管理，块本体分布在各 DataNode。
"""

import threading

from . import config
from .util import gen_id, join_path, norm_path, now, safe_name, ttl_seconds


class FsError(Exception):
    pass


class VirtualFS:
    def __init__(self, meta):
        self.meta = meta
        self.lock = threading.RLock()   # 与 meta.lock 嵌套使用（RLock 可重入）

    # ---------------------------------------------------------------- 初始化
    def init_root(self):
        with self.meta.lock:
            fs = self.meta.get("fs")
            fs.setdefault("inodes", {})
            inodes = fs["inodes"]
            if "root" not in fs:
                ts = now()
                inodes["in_root"] = {
                    "id": "in_root", "type": "dir", "name": "/", "parent": None,
                    "children": [], "created_at": ts, "modified_at": ts,
                    "owner": "admin", "mode": "rwxr-xr-x",
                }
                inodes["in_trash"] = {
                    "id": "in_trash", "type": "dir", "name": ".trash",
                    "parent": None, "children": [], "created_at": ts,
                    "modified_at": ts, "owner": "admin", "mode": "rwx------",
                }
                fs["root"] = "in_root"
                fs["trash_root"] = "in_trash"
                self.meta.touch("fs")
            self.root_id = fs["root"]
            self.trash_id = fs["trash_root"]

    # ---------------------------------------------------------------- 解析
    def _inodes(self):
        return self.meta.get("fs")["inodes"]

    def get_inode(self, inode_id):
        return self._inodes().get(inode_id)

    def resolve(self, path, must_exist=True):
        """绝对路径 -> inode dict。"""
        path = norm_path(path)
        inodes = self._inodes()
        cur = inodes.get(self.root_id)
        if cur is None:
            raise FsError("文件系统未初始化")
        if path == "/":
            return cur
        for seg in [s for s in path.split("/") if s]:
            if cur["type"] != "dir":
                raise FsError(f"不是目录: {path}")
            nxt = None
            for cid in cur.get("children", []):
                child = inodes.get(cid)
                if child and child["name"] == seg:
                    nxt = child
                    break
            if nxt is None:
                if must_exist:
                    raise FsError(f"路径不存在: {path}")
                return None
            cur = nxt
        return cur

    def path_of(self, inode_id):
        """inode -> 绝对路径（回收站中的文件返回 .trash 下路径）。"""
        inodes = self._inodes()
        node = inodes.get(inode_id)
        if not node:
            return None
        parts = []
        cur = node
        guard = 0
        while cur and cur["id"] != self.root_id and guard < 512:
            parts.append(cur["name"])
            cur = inodes.get(cur.get("parent")) if cur.get("parent") else None
            guard += 1
        return "/" + "/".join(reversed(parts)) if parts else "/"

    def _child_by_name(self, dir_inode, name):
        inodes = self._inodes()
        for cid in dir_inode.get("children", []):
            child = inodes.get(cid)
            if child and child["name"] == name:
                return child
        return None

    def _touch_dir_mtime(self, dir_inode):
        dir_inode["modified_at"] = now()

    # ---------------------------------------------------------------- 目录
    def mkdir(self, path, name, owner="admin"):
        name = safe_name(name)
        with self.meta.lock:
            parent = self.resolve(path)
            if parent["type"] != "dir":
                raise FsError(f"父路径不是目录: {path}")
            if self._child_by_name(parent, name):
                raise FsError(f"已存在同名条目: {join_path(path, name)}")
            ts = now()
            inode = {
                "id": gen_id("in"), "type": "dir", "name": name,
                "parent": parent["id"], "children": [],
                "created_at": ts, "modified_at": ts,
                "owner": owner, "mode": "rwxr-xr-x",
            }
            self._inodes()[inode["id"]] = inode
            parent["children"].append(inode["id"])
            self._touch_dir_mtime(parent)
            self.meta.touch("fs")
            return inode

    def mkdirs(self, path, owner="admin"):
        """递归创建目录（幂等）。"""
        path = norm_path(path)
        with self.meta.lock:
            cur_path = ""
            for seg in [s for s in path.split("/") if s]:
                cur_path += "/" + seg
                if self.resolve(cur_path, must_exist=False) is None:
                    self.mkdir("/".join(cur_path.split("/")[:-1]) or "/", seg,
                               owner)
            return self.resolve(path)

    # ---------------------------------------------------------------- 文件
    def create_file(self, path, name, size, content_hash, block_ids, mime,
                    owner="admin"):
        """
        创建或覆盖文件 inode（块已由 NameNode 分配并复制完成）。
        覆盖时替换 block_ids——旧块若无任何版本引用，将由 GC 回收。
        """
        name = safe_name(name)
        with self.meta.lock:
            parent = self.resolve(path)
            if parent["type"] != "dir":
                raise FsError(f"父路径不是目录: {path}")
            existing = self._child_by_name(parent, name)
            ts = now()
            if existing:
                if existing["type"] != "file":
                    raise FsError(f"同名目录已存在: {name}")
                existing["size"] = size
                existing["content_hash"] = content_hash
                existing["block_ids"] = block_ids
                existing["mime"] = mime
                existing["modified_at"] = ts
                existing["file_version"] = existing.get("file_version", 1) + 1
                self._touch_dir_mtime(parent)
                self.meta.touch("fs")
                return existing
            inode = {
                "id": gen_id("in"), "type": "file", "name": name,
                "parent": parent["id"], "children": [],
                "created_at": ts, "modified_at": ts,
                "owner": owner, "mode": "rw-r--r--",
                "size": size, "block_ids": list(block_ids),
                "content_hash": content_hash, "mime": mime,
                "file_version": 1,
                "access_count": 0, "last_access": None,
            }
            self._inodes()[inode["id"]] = inode
            parent["children"].append(inode["id"])
            self._touch_dir_mtime(parent)
            self.meta.touch("fs")
            return inode

    def rename(self, path, new_name, actor="admin"):
        new_name = safe_name(new_name)
        with self.meta.lock:
            inode = self.resolve(path)
            if inode["id"] in (self.root_id, self.trash_id):
                raise FsError("根目录/回收站不可重命名")
            parent = self._inodes().get(inode["parent"])
            if parent and self._child_by_name(parent, new_name):
                raise FsError(f"已存在同名条目: {new_name}")
            inode["name"] = new_name
            inode["modified_at"] = now()
            if parent:
                self._touch_dir_mtime(parent)
            self.meta.touch("fs")
            return inode

    def move(self, src_path, dst_path, actor="admin"):
        """移动文件/目录到 dst_path（新完整路径）。"""
        dst_path = norm_path(dst_path)
        with self.meta.lock:
            inode = self.resolve(src_path)
            if inode["id"] in (self.root_id, self.trash_id):
                raise FsError("根目录/回收站不可移动")
            dst_name = safe_name(dst_path.split("/")[-1])
            dst_parent_path = "/".join(dst_path.split("/")[:-1]) or "/"
            dst_parent = self.resolve(dst_parent_path)
            if dst_parent["type"] != "dir":
                raise FsError("目标父路径不是目录")
            if self._child_by_name(dst_parent, dst_name):
                raise FsError(f"目标已存在: {dst_path}")
            # 防止把目录移动到自己的子树下
            if inode["type"] == "dir":
                cur = dst_parent
                guard = 0
                while cur and guard < 512:
                    if cur["id"] == inode["id"]:
                        raise FsError("不能把目录移动到其自身子树下")
                    cur = self._inodes().get(cur.get("parent")) if cur.get("parent") else None
                    guard += 1
            old_parent = self._inodes().get(inode["parent"])
            if old_parent and inode["id"] in old_parent.get("children", []):
                old_parent["children"].remove(inode["id"])
                self._touch_dir_mtime(old_parent)
            inode["parent"] = dst_parent["id"]
            inode["name"] = dst_name
            inode["modified_at"] = now()
            dst_parent["children"].append(inode["id"])
            self._touch_dir_mtime(dst_parent)
            self.meta.touch("fs")
            return inode

    # ---------------------------------------------------------------- 遍历
    def walk_files(self, start_id=None, prefix=""):
        """深度遍历，yield (path, inode)——只产出文件。"""
        with self.meta.lock:
            start = self._inodes().get(start_id or self.root_id)
            if not start:
                return
            stack = [(prefix, start)]
            while stack:
                pfx, node = stack.pop()
                path = join_path(pfx, node["name"]) if pfx != "/" or node["id"] != self.root_id else "/"
                if node["id"] == self.root_id:
                    path = "/"
                if node["type"] == "file":
                    yield path, node
                else:
                    for cid in node.get("children", []):
                        child = self._inodes().get(cid)
                        if child:
                            stack.append((path if path != "/" else "", child))

    def all_files(self):
        return list(self.walk_files())

    def dir_stats(self, inode):
        """递归统计目录：文件数 / 总字节 / 块数。"""
        files = blocks = size = 0
        for _p, node in self.walk_files(inode["id"]):
            files += 1
            size += node.get("size", 0)
            blocks += len(node.get("block_ids", []))
        return {"files": files, "bytes": size, "blocks": blocks}

    # ---------------------------------------------------------------- 视图
    def entry_info(self, inode, path=None):
        """UI 条目信息（不含块级细节；NN 层会补充副本健康度）。"""
        info = {
            "id": inode["id"],
            "name": inode["name"],
            "type": inode["type"],
            "path": path if path is not None else self.path_of(inode["id"]),
            "size": inode.get("size", 0),
            "mime": inode.get("mime", ""),
            "owner": inode.get("owner", ""),
            "mode": inode.get("mode", ""),
            "created_at": inode.get("created_at"),
            "modified_at": inode.get("modified_at"),
            "file_version": inode.get("file_version", 1),
            "content_hash": inode.get("content_hash"),
            "access_count": inode.get("access_count", 0),
            "last_access": inode.get("last_access"),
            "blocks": len(inode.get("block_ids", [])),
            "thumb": bool(inode.get("mime", "").startswith("image/")),
        }
        if inode["type"] == "dir":
            info["children"] = len(inode.get("children", []))
            stats = self.dir_stats(inode)
            info["size"] = stats["bytes"]
            info["files"] = stats["files"]
        return info

    def list_dir(self, path):
        with self.meta.lock:
            inode = self.resolve(path)
            if inode["type"] != "dir":
                raise FsError(f"不是目录: {path}")
            entries = []
            base = norm_path(path)
            for cid in inode.get("children", []):
                child = self._inodes().get(cid)
                if not child:
                    continue
                entries.append(self.entry_info(
                    child, join_path(base, child["name"])))
            entries.sort(key=lambda e: (e["type"] != "dir",
                                        e["name"].lower()))
            return {"path": base, "entry": self.entry_info(inode, base),
                    "items": entries}

    def tree_json(self, max_depth=8):
        """整棵目录树（前端文件浏览页）。"""
        with self.meta.lock:
            inodes = self._inodes()

            def build(node, depth):
                item = {
                    "id": node["id"], "name": node["name"], "type": node["type"],
                    "size": node.get("size", 0),
                    "mime": node.get("mime", ""),
                    "modified_at": node.get("modified_at"),
                    "blocks": len(node.get("block_ids", [])),
                }
                if node["type"] == "dir" and depth < max_depth:
                    kids = []
                    for cid in node.get("children", []):
                        child = inodes.get(cid)
                        if child:
                            kids.append(build(child, depth + 1))
                    kids.sort(key=lambda x: (x["type"] != "dir",
                                             x["name"].lower()))
                    item["children"] = kids
                    item["file_count"] = sum(
                        1 + k.get("file_count", 0) for k in kids
                        if k["type"] == "dir") + sum(
                        1 for k in kids if k["type"] == "file")
                return item

            root = inodes.get(self.root_id)
            return build(root, 0)

    # ---------------------------------------------------------------- 回收站
    def _recycle(self):
        return self.meta.get("recycle")

    def delete_to_trash(self, path, actor="admin"):
        """删除 = 摘出原父目录，挂到 .trash 下，并登记回收站条目。"""
        with self.meta.lock:
            inode = self.resolve(path)
            if inode["id"] in (self.root_id, self.trash_id):
                raise FsError("根目录/回收站不可删除")
            original_path = self.path_of(inode["id"])
            original_parent = inode.get("parent")
            parent = self._inodes().get(original_parent)
            if parent and inode["id"] in parent.get("children", []):
                parent["children"].remove(inode["id"])
                self._touch_dir_mtime(parent)

            trash = self._inodes().get(self.trash_id)
            item_id = gen_id("rc")
            inode["parent"] = self.trash_id
            inode["_orig_name"] = inode["name"]
            inode["name"] = item_id            # trash 下用 item_id 命名防冲突
            trash["children"].append(inode["id"])

            stats = (self.dir_stats(inode) if inode["type"] == "dir"
                     else {"files": 1, "bytes": inode.get("size", 0),
                           "blocks": len(inode.get("block_ids", []))})
            retention = self._recycle().get(
                "retention_days", config.TRASH_RETENTION_DAYS)
            item = {
                "id": item_id,
                "inode": inode["id"],
                "name": inode["_orig_name"],
                "type": inode["type"],
                "original_path": original_path,
                "original_parent": original_parent,
                "deleted_at": now(),
                "deleted_by": actor,
                "retention_days": retention,
                "expires_at": now() + ttl_seconds(
                    retention, config.TRASH_RETENTION_UNIT),
                "size": stats["bytes"],
                "files": stats["files"],
                "blocks": stats["blocks"],
            }
            rec = self._recycle()
            rec.setdefault("items", {})[item_id] = item
            rec.setdefault("retention_days", config.TRASH_RETENTION_DAYS)
            self.meta.touch("fs")
            self.meta.touch("recycle")
            return item

    def recycle_list(self):
        with self.meta.lock:
            items = list(self._recycle().get("items", {}).values())
            items.sort(key=lambda x: x.get("deleted_at", 0), reverse=True)
            return items

    def restore(self, item_id, actor="admin"):
        with self.meta.lock:
            rec = self._recycle()
            item = rec.get("items", {}).get(item_id)
            if not item:
                raise FsError("回收站条目不存在")
            inode = self._inodes().get(item["inode"])
            if not inode:
                raise FsError("inode 已丢失，无法恢复")
            trash = self._inodes().get(self.trash_id)
            if inode["id"] in trash.get("children", []):
                trash["children"].remove(inode["id"])

            # 恢复原路径：父目录不存在则恢复到根
            parent = self._inodes().get(item.get("original_parent"))
            if not parent or parent["type"] != "dir":
                parent = self._inodes().get(self.root_id)
            name = inode.get("_orig_name") or inode["name"]
            # 同名冲突 => "name (restored-N)"
            base_name = name
            n = 1
            while self._child_by_name(parent, name):
                stem, dot, ext = base_name.rpartition(".")
                if dot and stem:
                    name = f"{stem} (restored-{n}).{ext}"
                else:
                    name = f"{base_name} (restored-{n})"
                n += 1
            inode["name"] = name
            inode.pop("_orig_name", None)
            inode["parent"] = parent["id"]
            parent["children"].append(inode["id"])
            self._touch_dir_mtime(parent)
            del rec["items"][item_id]
            self.meta.touch("fs")
            self.meta.touch("recycle")
            return {"path": self.path_of(inode["id"]), "name": name}

    def purge(self, item_id, actor="admin"):
        """彻底删除：递归移除 inode，返回释放的 block_ids（GC 兜底）。"""
        with self.meta.lock:
            rec = self._recycle()
            item = rec.get("items", {}).get(item_id)
            if not item:
                raise FsError("回收站条目不存在")
            freed = self._remove_subtree(item["inode"])
            trash = self._inodes().get(self.trash_id)
            if item["inode"] in trash.get("children", []):
                trash["children"].remove(item["inode"])
            del rec["items"][item_id]
            self.meta.touch("fs")
            self.meta.touch("recycle")
            return freed

    def _remove_subtree(self, inode_id):
        inodes = self._inodes()
        freed = []
        stack = [inode_id]
        while stack:
            cur = stack.pop()
            node = inodes.get(cur)
            if not node:
                continue
            freed.extend(node.get("block_ids", []))
            stack.extend(node.get("children", []))
            inodes.pop(cur, None)
        return freed

    def empty_trash(self, actor="admin"):
        with self.meta.lock:
            rec = self._recycle()
            freed = []
            count = 0
            for item_id in list(rec.get("items", {}).keys()):
                freed.extend(self.purge(item_id, actor))
                count += 1
            return {"purged": count, "freed_blocks": freed}

    def purge_expired(self):
        """清理超过保留期的条目（后台线程周期调用）。"""
        t = now()
        with self.meta.lock:
            rec = self._recycle()
            expired = [iid for iid, it in rec.get("items", {}).items()
                       if it.get("expires_at", 0) < t]
            freed = []
            for iid in expired:
                freed.extend(self.purge(iid, "system"))
            return expired, freed

    def trash_stats(self):
        with self.meta.lock:
            items = self._recycle().get("items", {})
            return {
                "count": len(items),
                "bytes": sum(i.get("size", 0) for i in items.values()),
                "blocks": sum(i.get("blocks", 0) for i in items.values()),
                "retention_days": self._recycle().get(
                    "retention_days", config.TRASH_RETENTION_DAYS),
                "retention_unit": config.TRASH_RETENTION_UNIT,
            }

    # ---------------------------------------------------------------- 汇总
    def global_stats(self):
        with self.meta.lock:
            files = dirs = 0
            total_bytes = 0
            ext_bytes = {}
            ext_count = {}
            for _p, node in self.walk_files():
                files += 1
                sz = node.get("size", 0)
                total_bytes += sz
                ext = (node["name"].rsplit(".", 1)[-1].lower()
                       if "." in node["name"] else "(无扩展名)")
                ext_bytes[ext] = ext_bytes.get(ext, 0) + sz
                ext_count[ext] = ext_count.get(ext, 0) + 1
            inodes = self._inodes()
            dirs = sum(1 for n in inodes.values() if n["type"] == "dir") - 1
            return {"files": files, "dirs": max(dirs, 0),
                    "bytes": total_bytes,
                    "ext_bytes": ext_bytes, "ext_count": ext_count}
