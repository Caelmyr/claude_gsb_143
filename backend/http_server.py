# -*- coding: utf-8 -*-
"""
http_server.py — NameNode HTTP 服务：REST API + 前端静态页面 + 节点内部接口
==============================================================================
路由分三类：
  /api/*       前端 REST API（Bearer 令牌鉴权 + 角色能力 + 路径 ACL）
  /internal/*  DataNode 心跳 / 块汇报 / 文档同步（集群共享密钥鉴权）
  其它         frontend/ 目录静态文件（10+ 页面）

处理器约定：
  * 返回 dict/list           -> 200 JSON
  * 调用 ctx.send_bytes(...) -> 自定义响应（下载 / 缩略图）
  * 抛 ApiError(status, msg) -> JSON 错误
  * 抛 AuthError/FsError/NNError/VersionError -> 403/400 语义化错误
"""

import json
import mimetypes
import os
import re
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from . import config, diff_engine
from .auth import AuthError
from .filesystem import FsError
from .lifecycle import LifecycleError
from .namenode import MissingBlockError, NNError
from .util import (content_range_value, decode_text, now, parse_range,
                   sha256_bytes, short_hash, to_rate_units)
from .versioning import VersionError


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


# ============================================================================
# 请求上下文
# ============================================================================

class RequestContext:
    def __init__(self, handler, nn, method, path, query, params, body):
        self.handler = handler
        self.nn = nn
        self.method = method
        self.path = path
        self.query = {k: v[0] for k, v in query.items()}
        self.params = params
        self.body = body
        self.user = None
        self.token = None
        self._json = None
        self.responded = False

    def json(self):
        if self._json is None:
            if not self.body:
                self._json = {}
            else:
                try:
                    self._json = json.loads(self.body.decode("utf-8"))
                except json.JSONDecodeError:
                    raise ApiError(400, "请求体不是合法 JSON")
        return self._json

    def q_int(self, key, default):
        try:
            return int(self.query.get(key, default))
        except (TypeError, ValueError):
            return default

    # ---- 响应 ----
    def send_json(self, obj, status=200):
        self.handler._send_json(obj, status)
        self.responded = True

    def send_bytes(self, data, mime="application/octet-stream", status=200,
                   extra_headers=None):
        self.handler._send_bytes(data, mime, status, extra_headers)
        self.responded = True

    def actor(self):
        return self.user["username"] if self.user else "anonymous"

    def require_perm(self, path, action):
        result = self.nn.perms.require(self.user, path, action)
        return result


# ============================================================================
# 路由表
# ============================================================================

class Router:
    def __init__(self):
        self.routes = []      # (method, regex, handler, opts)

    def add(self, method, pattern, handler, auth=True, cap=None, internal=False):
        regex = re.compile("^" + re.sub(r"<(\w+)>", r"(?P<\1>[^/]+)",
                                        pattern) + "$")
        self.routes.append((method, regex, handler,
                            {"auth": auth, "cap": cap, "internal": internal}))

    def match(self, method, path):
        for m, regex, handler, opts in self.routes:
            if m != method:
                continue
            mt = regex.match(path)
            if mt:
                return handler, mt.groupdict(), opts
        return None, None, None


router = Router()


def route(method, pattern, **opts):
    def deco(fn):
        router.add(method, pattern, fn, **opts)
        return fn
    return deco


# ============================================================================
# API: 认证
# ============================================================================

@route("POST", "/api/auth/login", auth=False)
def api_login(ctx):
    body = ctx.json()
    ip = ctx.handler.client_address[0]
    try:
        token, user = ctx.nn.auth.login(body.get("username", ""),
                                        body.get("password", ""), ip)
    except AuthError as e:
        ctx.nn.log_event("WARN", "auth", "login_failed",
                         body.get("username", ""), "anonymous", str(e))
        raise ApiError(401, str(e))
    ctx.nn.log_event("INFO", "auth", "login", user["username"],
                     user["username"], f"角色 {user['role']}，来自 {ip}")
    caps = ctx.nn.auth.role_caps(user["role"])
    return {"token": token, "user": user, "caps": caps}


@route("POST", "/api/auth/logout")
def api_logout(ctx):
    if ctx.token:
        ctx.nn.auth.logout(ctx.token)
    ctx.nn.log_event("INFO", "auth", "logout", ctx.actor(), ctx.actor(), "")
    return {"ok": True}


@route("GET", "/api/auth/me")
def api_me(ctx):
    return {"user": ctx.user, "caps": ctx.nn.auth.role_caps(ctx.user["role"])}


@route("GET", "/api/auth/sessions", cap="admin")
def api_sessions(ctx):
    return {"sessions": ctx.nn.auth.list_sessions()}


# ============================================================================
# API: 文件系统
# ============================================================================

def _annotate_health(nn, entries):
    """给目录列表中的文件补充副本健康度。"""
    with nn.meta.lock:
        blocks = nn.meta.get("blocks")["blocks"]
        for e in entries:
            if e["type"] != "file":
                continue
            inode = nn.fs.get_inode(e["id"])
            if not inode:
                continue
            worst = "ok"
            live_min = None
            for bid in inode.get("block_ids", []):
                blk = blocks.get(bid)
                if not blk:
                    worst = "missing"
                    live_min = 0
                    break
                live = len(nn.live_good_replicas(blk))
                live_min = live if live_min is None else min(live_min, live)
                if live == 0:
                    worst = "missing"
                elif live < blk.get("desired", 3) and worst != "missing":
                    worst = "under"
            e["health"] = worst
            e["live_replicas"] = live_min
    return entries


@route("GET", "/api/fs/tree")
def api_fs_tree(ctx):
    return {"tree": ctx.nn.fs.tree_json(),
            "trash": ctx.nn.fs.trash_stats()}


@route("GET", "/api/fs/list")
def api_fs_list(ctx):
    path = ctx.query.get("path", "/")
    ctx.require_perm(path, "read")
    result = ctx.nn.fs.list_dir(path)
    result["items"] = _annotate_health(ctx.nn, result["items"])
    result["breadcrumb"] = [seg for seg in path.split("/") if seg]
    return result


@route("GET", "/api/fs/stat")
def api_fs_stat(ctx):
    path = ctx.query.get("path", "/")
    ctx.require_perm(path, "read")
    inode = ctx.nn.fs.resolve(path)
    info = ctx.nn.fs.entry_info(inode, path)
    return {"stat": info}


@route("POST", "/api/fs/mkdir")
def api_fs_mkdir(ctx):
    body = ctx.json()
    path = body.get("path", "/")
    name = (body.get("name") or "").strip()
    ctx.require_perm(path, "write")
    inode = ctx.nn.fs.mkdir(path, name, ctx.actor())
    ctx.nn.log_event("INFO", "fs", "mkdir", f"{path}/{name}", ctx.actor(), "")
    return {"ok": True, "id": inode["id"], "path": f"{path.rstrip('/')}/{name}"}


@route("POST", "/api/fs/rename")
def api_fs_rename(ctx):
    body = ctx.json()
    path = body.get("path", "")
    new_name = (body.get("new_name") or "").strip()
    parent = "/".join(path.split("/")[:-1]) or "/"
    ctx.require_perm(parent, "write")
    inode = ctx.nn.fs.rename(path, new_name, ctx.actor())
    ctx.nn.log_event("INFO", "fs", "rename",
                     f"{path} -> {inode['name']}", ctx.actor(), "")
    return {"ok": True, "new_path": ctx.nn.fs.path_of(inode["id"])}


@route("POST", "/api/fs/move")
def api_fs_move(ctx):
    body = ctx.json()
    src, dst = body.get("path", ""), body.get("new_path", "")
    ctx.require_perm("/".join(src.split("/")[:-1]) or "/", "write")
    ctx.require_perm("/".join(dst.split("/")[:-1]) or "/", "write")
    inode = ctx.nn.fs.move(src, dst, ctx.actor())
    ctx.nn.log_event("INFO", "fs", "move", f"{src} -> {dst}", ctx.actor(), "")
    return {"ok": True, "path": ctx.nn.fs.path_of(inode["id"])}


@route("POST", "/api/fs/delete")
def api_fs_delete(ctx):
    body = ctx.json()
    path = body.get("path", "")
    ctx.require_perm(path, "delete")
    item = ctx.nn.fs.delete_to_trash(path, ctx.actor())
    ctx.nn.log_event("WARN", "fs", "delete", path, ctx.actor(),
                     f"移入回收站（{item['retention_days']} 天后过期），"
                     f"条目 {item['id']}")
    return {"ok": True, "item": item}


@route("GET", "/api/thumbnail", auth=True)
def api_thumbnail(ctx):
    path = ctx.query.get("path", "")
    data, mime = ctx.nn.thumbnail(path)
    ctx.send_bytes(data, mime, 200, {"Cache-Control": "max-age=30"})


@route("GET", "/api/file/preview")
def api_file_preview(ctx):
    path = ctx.query.get("path", "")
    ctx.require_perm(path, "read")
    result = ctx.nn.preview_file(path)
    ctx.nn.record_access(path, "preview", ctx.actor(), 0, None)
    return result


@route("GET", "/api/file/blocks")
def api_file_blocks(ctx):
    path = ctx.query.get("path", "")
    ctx.require_perm(path, "read")
    return ctx.nn.file_blocks_detail(path)


# ============================================================================
# API: 上传（分块 + 断点续传）
# ============================================================================

@route("POST", "/api/upload/begin")
def api_upload_begin(ctx):
    body = ctx.json()
    path = body.get("path", "/")
    filename = body.get("filename", "")
    size = int(body.get("size", 0))
    ctx.require_perm(path, "write")
    view = ctx.nn.upload_begin(path, filename, size,
                               session_id=body.get("session"),
                               piece_size=body.get("piece_size"),
                               user=ctx.actor())
    return view


@route("POST", "/api/upload/chunk")
def api_upload_chunk(ctx):
    body = ctx.json()
    simulate = bool(body.get("simulate_fail"))
    result = ctx.nn.upload_chunk(body.get("session"), body.get("index"),
                                 body.get("data", ""),
                                 body.get("checksum"), simulate)
    return result


@route("POST", "/api/upload/complete")
def api_upload_complete(ctx):
    body = ctx.json()
    result = ctx.nn.upload_complete(body.get("session"), ctx.actor())
    return result


@route("GET", "/api/upload/status")
def api_upload_status(ctx):
    return ctx.nn.upload_status(ctx.query.get("session", ""))


@route("GET", "/api/upload/sessions")
def api_upload_sessions(ctx):
    return {"sessions": ctx.nn.list_sessions()}


# ============================================================================
# API: 下载（Range 分段 + 断点续传）
# ============================================================================

@route("GET", "/api/download/info")
def api_download_info(ctx):
    path = ctx.query.get("path", "")
    ctx.require_perm(path, "read")
    return ctx.nn.download_info(path)


@route("GET", "/api/download")
def api_download(ctx):
    path = ctx.query.get("path", "")
    ctx.require_perm(path, "read")
    size = ctx.nn.download_info(path)["size"]
    rng = parse_range(ctx.handler.headers.get("Range"), size)
    if ctx.query.get("offset") is not None or ctx.query.get("length"):
        offset = int(ctx.query.get("offset", 0) or 0)
        length = ctx.query.get("length")
        length = int(length) if length else None
        end = size - 1 if length is None else min(size - 1, offset + length - 1)
        rng = (offset, end) if size else None
    if rng is None and size:
        rng = (0, size - 1)
    if size == 0:
        ctx.send_bytes(b"", "application/octet-stream", 200,
                       {"X-Content-Hash": ""})
        return None
    if rng is None:
        raise ApiError(416, "无效 Range")
    start, end = rng
    data, info = ctx.nn.read_file_range(path, start, end - start + 1,
                                        ctx.actor())
    ctx.nn._record_hourly("downloads", 1)
    ctx.nn._record_hourly("bytes_out", len(data))
    headers = {
        "Content-Range": content_range_value(start, start + len(data) - 1, size),
        "Accept-Ranges": "bytes",
        "X-Served-By": ",".join(info["nodes"]),
        "X-Blocks-Touched": str(info["blocks_touched"]),
        "X-Restored-Blocks": str(info.get("restored", 0)),
        "X-From-Cold": "1" if info.get("from_cold") else "0",
        "Content-Disposition": f'attachment; filename="{os.path.basename(path)}"',
    }
    status = 206 if (start, start + len(data) - 1) != (0, size - 1) else 200
    ctx.send_bytes(data, "application/octet-stream", status, headers)


# ============================================================================
# API: 版本控制
# ============================================================================

@route("GET", "/api/version/branches")
def api_version_branches(ctx):
    return ctx.nn.versions.list_branches()


@route("GET", "/api/version/commits")
def api_version_commits(ctx):
    branch = ctx.query.get("branch") or None
    limit = ctx.q_int("limit", 60)
    offset = ctx.q_int("offset", 0)
    return ctx.nn.versions.list_commits(branch, limit, offset)


@route("GET", "/api/version/graph")
def api_version_graph(ctx):
    branch = ctx.query.get("branch") or None
    limit = ctx.q_int("limit", 60)
    return ctx.nn.versions.graph(branch, limit)


@route("POST", "/api/version/commit")
def api_version_commit(ctx):
    body = ctx.json()
    ctx.require_perm("/", "write")
    commit = ctx.nn.versions.commit(body.get("message", ""), ctx.actor(),
                                    body.get("branch"))
    return {"ok": True, "commit": ctx.nn.versions.commit_brief(commit),
            "stats": commit.get("stats", {})}


@route("POST", "/api/version/branch")
def api_version_branch_create(ctx):
    body = ctx.json()
    br = ctx.nn.versions.create_branch(body.get("name", ""),
                                       body.get("from"), ctx.actor(),
                                       body.get("desc", ""))
    return {"ok": True, "branch": br}


@route("POST", "/api/version/branch_delete")
def api_version_branch_delete(ctx):
    body = ctx.json()
    ctx.nn.versions.delete_branch(body.get("name", ""), ctx.actor())
    return {"ok": True}


@route("POST", "/api/version/checkout")
def api_version_checkout(ctx):
    body = ctx.json()
    ctx.require_perm("/", "write")
    return {"ok": True, **ctx.nn.versions.checkout(body.get("branch", ""),
                                                   ctx.actor())}


@route("POST", "/api/version/merge")
def api_version_merge(ctx):
    body = ctx.json()
    ctx.require_perm("/", "write")
    src_commit = ctx.nn.versions.resolve_ref(body.get("source", ""))
    if src_commit:
        ctx.nn.log_event("INFO", "version", "merge_request",
                         body.get("source", ""), ctx.actor(),
                         f"source head {short_hash(src_commit['id'], 8)}")
    return {"ok": True, **ctx.nn.versions.merge(body.get("source", ""),
                                                body.get("target"),
                                                ctx.actor())}


@route("GET", "/api/version/diff")
def api_version_diff(ctx):
    a = ctx.query.get("a") or None
    b = ctx.query.get("b") or "HEAD"
    return ctx.nn.versions.diff_refs(a, b)


@route("GET", "/api/version/working_diff")
def api_version_working_diff(ctx):
    branch = ctx.query.get("branch") or None
    return ctx.nn.versions.diff_working(branch)


@route("GET", "/api/version/file_at")
def api_version_file_at(ctx):
    ref = ctx.query.get("ref", "HEAD")
    path = ctx.query.get("path", "")
    return ctx.nn.versions.file_at(ref, path)


@route("GET", "/api/version/history")
def api_version_history(ctx):
    path = ctx.query.get("path", "")
    branch = ctx.query.get("branch") or None
    return {"path": path,
            "order": config.HISTORY_ORDER,
            "history": ctx.nn.versions.file_history(path, branch)}


@route("POST", "/api/version/restore")
def api_version_restore(ctx):
    body = ctx.json()
    ctx.require_perm("/", "write")
    return {"ok": True, **ctx.nn.versions.restore_file(
        body.get("ref", "HEAD"), body.get("path", ""), ctx.actor())}


@route("POST", "/api/version/diff_text")
def api_version_diff_text(ctx):
    """
    通用文本差异接口（Myers / Patience / auto）：
      * 传 a_text/b_text 直接对比；
      * 或传 a_ref/b_ref + path 对比历史版本（ref 可为 WORKING=活动文件）。
    """
    body = ctx.json()
    method = body.get("method", "auto")
    context = int(body.get("context", config.DIFF_CONTEXT_DEFAULT))
    view = body.get("view", "split")
    if body.get("a_text") is not None:
        text_a = body.get("a_text", "")
        text_b = body.get("b_text", "")
        label_a = body.get("label_a", "文本 A")
        label_b = body.get("label_b", "文本 B")
    else:
        path = body.get("path", "")
        ra = body.get("a_ref") or ""
        rb = body.get("b_ref") or "HEAD"
        text_a = _content_for_diff(ctx.nn, ra, path)
        text_b = _content_for_diff(ctx.nn, rb, path)
        label_a = f"{short_hash(ra or '(空)', 8)}:{path}"
        label_b = f"{short_hash(rb, 8)}:{path}"
    la = diff_engine.split_lines(text_a)
    lb = diff_engine.split_lines(text_b)
    if len(la) + len(lb) > config.DIFF_MAX_LINES:
        raise ApiError(413, "文本过大，超出单次 diff 上限")
    ops, timing = diff_engine.diff_timed(la, lb, method)
    result = {
        "label_a": label_a, "label_b": label_b,
        "timing": timing,
        "unified": diff_engine.render_unified(la, lb, ops, context),
    }
    if view == "split":
        rows = diff_engine.render_split(la, lb, ops, context)
        result["split"] = rows
    return result


def _content_for_diff(nn, ref, path):
    """取某 ref 下文件文本；ref 为空或 WORKING 时读活动文件系统。"""
    if not ref or ref in ("WORKING", "working"):
        try:
            inode = nn.fs.resolve(path)
            if inode["type"] != "file":
                return ""
            data = nn.read_blocks(inode.get("block_ids", []))
            return decode_text(data) or ""
        except Exception:
            return ""
    fa = nn.versions.file_at(ref, path)
    if not fa.get("exists"):
        return ""
    return fa.get("content") or ""


@route("GET", "/api/version/stats")
def api_version_stats(ctx):
    return ctx.nn.versions.repo_stats()


# ============================================================================
# API: 节点 / 演练
# ============================================================================

@route("GET", "/api/nodes")
def api_nodes(ctx):
    data = ctx.nn.nodes_view()
    matrix = ctx.nn.replica_matrix(limit=1000)
    data["health"]["under_replicated"] = sum(
        1 for r in matrix["rows"] if r["status"] != "ok")
    return data


@route("GET", "/api/nodes/blocks")
def api_node_blocks(ctx):
    node = ctx.query.get("node", "")
    limit = ctx.q_int("limit", 100)
    offset = ctx.q_int("offset", 0)
    data = ctx.nn.node_blocks(node, limit, offset)
    data["node"] = node
    return data


@route("GET", "/api/nodes/matrix")
def api_nodes_matrix(ctx):
    limit = ctx.q_int("limit", 60)
    return ctx.nn.replica_matrix(limit)


@route("GET", "/api/nodes/block_paths")
def api_node_block_paths(ctx):
    bid = ctx.query.get("block", "")
    return {"block": bid, "paths": ctx.nn.block_paths(bid)}


@route("GET", "/api/health/queue")
def api_health_queue(ctx):
    return ctx.nn.health_queue()


@route("GET", "/api/sim/events")
def api_sim_events(ctx):
    since = ctx.q_int("since", 0)
    limit = ctx.q_int("limit", 120)
    events = ctx.nn.events.items(since, limit)
    events.sort(key=lambda e: e.get("seq", 0))
    return {"events": events}


@route("POST", "/api/sim/kill", cap="sim")
def api_sim_kill(ctx):
    body = ctx.json()
    node = body.get("node", "")
    ctx.nn.log_event("WARN", "sim", "kill", node, ctx.actor(), "手动故障演练")
    return ctx.nn.sim_kill_node(node)


@route("POST", "/api/sim/revive", cap="sim")
def api_sim_revive(ctx):
    body = ctx.json()
    node = body.get("node", "")
    return ctx.nn.sim_revive_node(node)


@route("POST", "/api/sim/corrupt", cap="sim")
def api_sim_corrupt(ctx):
    body = ctx.json()
    return ctx.nn.sim_corrupt_block(body.get("block_id", ""),
                                    body.get("node_id", ""))


@route("POST", "/api/sim/chaos", cap="sim")
def api_sim_chaos(ctx):
    body = ctx.json()
    return ctx.nn.sim_chaos_mode(body.get("enabled"))


# ============================================================================
# API: 统计
# ============================================================================

@route("GET", "/api/stats/overview")
def api_stats_overview(ctx):
    return ctx.nn.overview_stats()


@route("GET", "/api/stats/hotness")
def api_stats_hotness(ctx):
    limit = ctx.q_int("limit", 12)
    return {"items": ctx.nn.hotness(limit)}


@route("GET", "/api/stats/timeline")
def api_stats_timeline(ctx):
    hours = ctx.q_int("hours", 24)
    return ctx.nn.timeline_stats(hours)


# ============================================================================
# API: 用户管理
# ============================================================================

@route("GET", "/api/users", cap="user_admin")
def api_users_list(ctx):
    return {"users": ctx.nn.auth.list_users(),
            "roles": config.ROLE_CAPABILITIES}


@route("GET", "/api/users/roles")
def api_users_roles(ctx):
    return {"roles": config.ROLE_CAPABILITIES}


@route("POST", "/api/users", cap="user_admin")
def api_users_create(ctx):
    body = ctx.json()
    user = ctx.nn.auth.create_user(body.get("username", ""),
                                   body.get("password", ""),
                                   body.get("role", "viewer"),
                                   body.get("email", ""),
                                   body.get("note", ""))
    ctx.nn.log_event("INFO", "auth", "user_create", user["username"],
                     ctx.actor(), f"角色 {user['role']}")
    return {"ok": True, "user": user}


@route("PUT", "/api/users/<name>", cap="user_admin")
def api_users_update(ctx):
    body = ctx.json()
    user = ctx.nn.auth.update_user(
        ctx.params["name"], role=body.get("role"), email=body.get("email"),
        note=body.get("note"), status=body.get("status"),
        password=body.get("password"))
    ctx.nn.log_event("INFO", "auth", "user_update", user["username"],
                     ctx.actor(), json.dumps(body, ensure_ascii=False)[:200])
    return {"ok": True, "user": user}


@route("DELETE", "/api/users/<name>", cap="user_admin")
def api_users_delete(ctx):
    name = ctx.params["name"]
    ctx.nn.auth.delete_user(name)
    ctx.nn.log_event("WARN", "auth", "user_delete", name, ctx.actor(), "")
    return {"ok": True}


@route("POST", "/api/users/revoke_sessions", cap="user_admin")
def api_users_revoke(ctx):
    body = ctx.json()
    n = ctx.nn.auth.revoke_sessions(body.get("username"))
    return {"ok": True, "revoked": n}


# ============================================================================
# API: 权限
# ============================================================================

@route("GET", "/api/perms", cap="perm_admin")
def api_perms_list(ctx):
    return ctx.nn.perms.list_rules()


@route("POST", "/api/perms", cap="perm_admin")
def api_perms_add(ctx):
    body = ctx.json()
    rule = ctx.nn.perms.add_rule(
        body.get("path", "/"), body.get("principal", ""),
        body.get("principal_type", "user"), body.get("perms"),
        body.get("effect", "allow"), body.get("priority", 100),
        body.get("note", ""))
    ctx.nn.log_event("INFO", "auth", "perm_add", rule["path"], ctx.actor(),
                     f"{rule['principal']}({rule['principal_type']}) "
                     f"{','.join(rule['perms'])} {rule['effect']}")
    return {"ok": True, "rule": rule}


@route("PUT", "/api/perms/<rule_id>", cap="perm_admin")
def api_perms_update(ctx):
    body = ctx.json()
    rule = ctx.nn.perms.update_rule(ctx.params["rule_id"], **body)
    ctx.nn.log_event("INFO", "auth", "perm_update", rule["path"], ctx.actor(), "")
    return {"ok": True, "rule": rule}


@route("DELETE", "/api/perms/<rule_id>", cap="perm_admin")
def api_perms_delete(ctx):
    rid = ctx.params["rule_id"]
    ctx.nn.perms.delete_rule(rid)
    ctx.nn.log_event("WARN", "auth", "perm_delete", rid, ctx.actor(), "")
    return {"ok": True}


@route("POST", "/api/perms/check", cap="perm_admin")
def api_perms_check(ctx):
    body = ctx.json()
    user = ctx.nn.auth.get_user(body.get("username", ""))
    if not user:
        raise ApiError(404, f"用户不存在: {body.get('username')}")
    result = ctx.nn.perms.check(user, body.get("path", "/"),
                                body.get("action", "read"))
    result["user"] = user
    return result


# ============================================================================
# API: 日志
# ============================================================================

@route("GET", "/api/logs")
def api_logs(ctx):
    level = ctx.query.get("level") or None
    source = ctx.query.get("source") or None
    user = ctx.query.get("user") or None
    q = ctx.query.get("q") or None
    limit = min(ctx.q_int("limit", 100), 1000)
    offset = ctx.q_int("offset", 0)
    return ctx.nn.query_logs(level, source, user, q, limit, offset)


@route("GET", "/api/logs/export")
def api_logs_export(ctx):
    data = ctx.nn.query_logs(limit=config.LOG_MAX_ENTRIES)
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    for it in data["items"]:
        from .util import fmt_ts
        w.writerow([fmt_ts(it["ts"]), it["level"], it["source"],
                    it.get("user", ""), it.get("action", ""),
                    it.get("target", ""), it.get("detail", "")])
    content = ("ts,level,source,user,action,target,detail\n"
               + buf.getvalue()).encode("utf-8")
    ctx.send_bytes(content, "text/csv; charset=utf-8", 200,
                   {"Content-Disposition": 'attachment; filename="dfsvs-logs.csv"'})


@route("POST", "/api/logs/clear", cap="admin")
def api_logs_clear(ctx):
    ctx.nn.clear_logs()
    ctx.nn.log_event("WARN", "api", "logs_clear", "", ctx.actor(), "清空系统日志")
    return {"ok": True}


# ============================================================================
# API: 回收站
# ============================================================================

@route("GET", "/api/recycle")
def api_recycle_list(ctx):
    items = ctx.nn.fs.recycle_list()
    return {"items": items, "stats": ctx.nn.fs.trash_stats()}


@route("POST", "/api/recycle/restore")
def api_recycle_restore(ctx):
    body = ctx.json()
    result = ctx.nn.fs.restore(body.get("id", ""), ctx.actor())
    ctx.nn.log_event("INFO", "fs", "trash_restore", result["path"],
                     ctx.actor(), f"恢复条目 {body.get('id')}")
    return {"ok": True, **result}


@route("POST", "/api/recycle/purge")
def api_recycle_purge(ctx):
    body = ctx.json()
    freed = ctx.nn.fs.purge(body.get("id", ""), ctx.actor())
    ctx.nn.log_event("WARN", "fs", "trash_purge", body.get("id", ""),
                     ctx.actor(), f"彻底删除，释放 {len(freed)} 个块引用（GC 回收）")
    return {"ok": True, "freed_blocks": len(freed)}


@route("POST", "/api/recycle/empty")
def api_recycle_empty(ctx):
    result = ctx.nn.fs.empty_trash(ctx.actor())
    ctx.nn.log_event("WARN", "fs", "trash_empty", "", ctx.actor(),
                     f"清空回收站：{result['purged']} 项")
    return {"ok": True, **result}


# ============================================================================
# API: 生命周期（规则 / 预览 / 撤销 / 历史 / 冷归档取回）
# ============================================================================

@route("GET", "/api/lifecycle/rules")
def api_lc_rules(ctx):
    return {"rules": ctx.nn.lifecycle.list_rules(),
            "grace_seconds": config.LIFECYCLE_GRACE_SECONDS,
            "actions": config.LIFECYCLE_ACTIONS,
            "age_bases": config.LIFECYCLE_AGE_BASES,
            "age_units": config.LIFECYCLE_AGE_UNITS,
            "stats": ctx.nn.lifecycle.stats()}


@route("POST", "/api/lifecycle/rules", cap="admin")
def api_lc_rule_create(ctx):
    rule = ctx.nn.lifecycle.add_rule(ctx.json(), ctx.actor())
    return {"ok": True, "rule": rule}


@route("PUT", "/api/lifecycle/rules/<rule_id>", cap="admin")
def api_lc_rule_update(ctx):
    rule = ctx.nn.lifecycle.update_rule(ctx.params["rule_id"], ctx.json(),
                                        ctx.actor())
    return {"ok": True, "rule": rule}


@route("DELETE", "/api/lifecycle/rules/<rule_id>", cap="admin")
def api_lc_rule_delete(ctx):
    ctx.nn.lifecycle.delete_rule(ctx.params["rule_id"], ctx.actor())
    return {"ok": True}


@route("POST", "/api/lifecycle/rules/<rule_id>/toggle", cap="admin")
def api_lc_rule_toggle(ctx):
    enabled = bool(ctx.json().get("enabled", True))
    rule = ctx.nn.lifecycle.set_enabled(ctx.params["rule_id"], enabled,
                                        ctx.actor())
    return {"ok": True, "rule": rule}


@route("GET", "/api/lifecycle/preview")
def api_lc_preview(ctx):
    scope = ctx.query.get("path") or None
    limit = min(2000, ctx.q_int("limit", 500))
    return ctx.nn.lifecycle.preview(scope, limit)


@route("GET", "/api/lifecycle/pending")
def api_lc_pending(ctx):
    return ctx.nn.lifecycle.list_pending()


@route("POST", "/api/lifecycle/pending/<pending_id>/cancel", cap="admin")
def api_lc_pending_cancel(ctx):
    note = (ctx.json().get("note") or "").strip()
    return ctx.nn.lifecycle.cancel_pending(ctx.params["pending_id"],
                                           ctx.actor(), note)


@route("GET", "/api/lifecycle/history")
def api_lc_history(ctx):
    return ctx.nn.lifecycle.list_history(
        path=ctx.query.get("path") or None,
        rule_id=ctx.query.get("rule_id") or None,
        action=ctx.query.get("action") or None,
        status=ctx.query.get("status") or None,
        limit=min(1000, ctx.q_int("limit", 200)))


@route("GET", "/api/lifecycle/file")
def api_lc_file(ctx):
    path = ctx.query.get("path", "")
    ctx.require_perm(path, "read")
    return ctx.nn.lifecycle.file_lifecycle(path)


@route("POST", "/api/lifecycle/scan", cap="admin")
def api_lc_scan(ctx):
    return {"ok": True, **ctx.nn.lifecycle.run_scan_now(ctx.actor())}


@route("POST", "/api/lifecycle/archive_now", cap="admin")
def api_lc_archive_now(ctx):
    path = ctx.json().get("path", "")
    ctx.require_perm(path, "admin")
    return ctx.nn.lifecycle.archive_path_now(path, ctx.actor())


@route("POST", "/api/lifecycle/restore", cap="admin")
def api_lc_restore(ctx):
    path = ctx.json().get("path", "")
    ctx.require_perm(path, "read")
    return ctx.nn.lifecycle.restore_path_now(path, ctx.actor())


# ============================================================================
# API: 系统信息
# ============================================================================

@route("GET", "/api/system/info", auth=False)
def api_system_info(ctx):
    nn = ctx.nn
    return {
        "system": "DFSVS — 分布式文件存储与版本控制系统",
        "version": "1.0.0",
        "nn_time": now(),
        "nn_uptime": now() - nn.started_at,
        "config": {
            "block_size": config.BLOCK_SIZE,
            "replication": config.DEFAULT_REPLICATION,
            "min_replication": config.MIN_REPLICATION,
            "heartbeat_interval": config.HEARTBEAT_INTERVAL,
            "heartbeat_timeout": config.HEARTBEAT_TIMEOUT,
            "chunk_strategy": config.CHUNK_STRATEGY,
            "trash_retention_days": config.TRASH_RETENTION_DAYS,
            "upload_piece_size": config.UPLOAD_PIECE_SIZE,
        },
        "nodes": {nid: n["state"] for nid, n in nn.nodes.items()},
        "authed": bool(ctx.token and nn.auth.user_for_token(ctx.token)),
    }


# ============================================================================
# 内部接口（DataNode <-> NameNode，集群密钥鉴权）
# ============================================================================

@route("POST", "/internal/heartbeat", auth=False, internal=True)
def internal_heartbeat(ctx):
    payload = ctx.json()
    return ctx.nn.handle_heartbeat(payload)


@route("POST", "/internal/block_report", auth=False, internal=True)
def internal_block_report(ctx):
    payload = ctx.json()
    return ctx.nn.handle_block_report(payload)


@route("GET", "/internal/meta/<doc>", auth=False, internal=True)
def internal_meta_doc(ctx):
    return ctx.nn.get_meta_doc(ctx.params["doc"])


# ============================================================================
# HTTP Handler
# ============================================================================

class NameNodeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DFSVS-NameNode/1.0"

    @property
    def nn(self):
        return self.server.nn

    def log_message(self, fmt, *args):
        pass    # 访问日志走系统日志页，避免刷屏

    # ------------------------------------------------------------ 响应工具
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, X-Cluster-Key, Range")
        self.send_header("Access-Control-Expose-Headers",
                         "Content-Range, X-Served-By, X-Blocks-Touched, "
                         "X-Genstamp, X-Checksum, X-Node-Id")

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send_bytes(self, data, mime="application/octet-stream", status=200,
                    extra_headers=None):
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, str(v))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0:
            return b""
        if length > 64 * 1024 * 1024:
            raise ApiError(413, "请求体过大")
        return self.rfile.read(length)

    # ------------------------------------------------------------ 分发
    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self):
        self._dispatch("GET", head_only=True)

    def _dispatch(self, method, head_only=False):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        query = parse_qs(parsed.query)
        try:
            if path.startswith("/api/") or path.startswith("/internal/"):
                self._dispatch_api(method, path, query, head_only)
            else:
                self._serve_static(path, head_only)
        except Exception as e:  # noqa: BLE001 兜底
            self._send_json({"error": f"内部错误: {e}",
                             "trace": traceback.format_exc()[-800:]}, 500)

    def _dispatch_api(self, method, path, query, head_only):
        nn = self.nn
        nn.api_rate.hit(to_rate_units(1, config.IO_RATE_SCALE))
        handler_fn, params, opts = router.match(method, path)
        if handler_fn is None:
            # HEAD 回退 GET
            if method == "HEAD":
                handler_fn, params, opts = router.match("GET", path)
            if handler_fn is None:
                raise ApiError(404, f"接口不存在: {method} {path}")
        try:
            body = b"" if method == "GET" else self._read_body()
            ctx = RequestContext(self, nn, method, path, query, params or {},
                                 body)
            # ---- 鉴权 ----
            if opts.get("internal"):
                key = self.headers.get("X-Cluster-Key", "")
                if key != nn.cluster_key:
                    raise ApiError(403, "集群密钥错误")
            elif opts.get("auth"):
                token = None
                auth_hdr = self.headers.get("Authorization", "")
                if auth_hdr.startswith("Bearer "):
                    token = auth_hdr[7:].strip()
                if not token:
                    token = query.get("token", [None])[0] \
                        if isinstance(query.get("token"), list) \
                        else query.get("token")
                ctx.token = token
                user = nn.auth.user_for_token(token) if token else None
                if not user:
                    raise ApiError(401, "未认证或令牌已过期")
                ctx.user = user
                cap = opts.get("cap")
                if cap and not nn.auth.has_cap(user, cap):
                    raise ApiError(403,
                                   f"角色 {user['role']} 缺少能力: {cap}")
            # ---- 执行 ----
            result = handler_fn(ctx)
            if head_only:
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self._cors()
                self.end_headers()
                return
            if not ctx.responded:
                if result is None:
                    result = {"ok": True}
                self._send_json(result)
        except ApiError as e:
            self._send_json({"error": e.message}, e.status)
        except AuthError as e:
            self._send_json({"error": str(e)}, 403)
        except MissingBlockError as e:
            self._send_json({"error": str(e), "degraded": True}, 503)
        except (FsError, NNError, VersionError, LifecycleError,
                ValueError) as e:
            self._send_json({"error": str(e)}, 400)
        except Exception as e:  # noqa: BLE001
            nn.log_event("ERROR", "api", "unhandled", path, "system",
                         traceback.format_exc()[-500:])
            self._send_json({"error": f"内部错误: {e}",
                             "trace": traceback.format_exc()[-800:]}, 500)

    # ------------------------------------------------------------ 静态文件
    def _serve_static(self, path, head_only=False):
        if path in ("", "/"):
            path = "/index.html"
        rel = path.lstrip("/")
        full = os.path.normpath(os.path.join(config.FRONTEND_DIR, rel))
        if not full.startswith(os.path.normpath(config.FRONTEND_DIR)):
            raise ApiError(403, "非法路径")
        if os.path.isdir(full):
            full = os.path.join(full, "index.html")
        if not os.path.exists(full):
            # SPA 回退：未知路径给 index.html（前端页面均为真实文件，一般不触发）
            full = os.path.join(config.FRONTEND_DIR, "index.html")
            if not os.path.exists(full):
                raise ApiError(404, f"页面不存在: {path}")
        mime, _ = mimetypes.guess_type(full)
        mime = mime or "application/octet-stream"
        if mime.startswith("text/") or mime in (
                "application/javascript", "application/json"):
            mime = mime.split(";")[0] + "; charset=utf-8"
        with open(full, "rb") as f:
            data = f.read()
        if head_only:
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            return
        self._send_bytes(data, mime, 200, {"Cache-Control": "no-cache"})


# ============================================================================
# 服务器启动
# ============================================================================

class NameNodeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_namenode_server(nn):
    server = NameNodeHTTPServer((nn.host, nn.port), NameNodeHandler)
    server.nn = nn
    t = threading.Thread(target=server.serve_forever,
                         kwargs={"poll_interval": 0.2},
                         name="nn-http", daemon=True)
    t.start()
    return server
