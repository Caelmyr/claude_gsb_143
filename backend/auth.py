# -*- coding: utf-8 -*-
"""
auth.py — 用户管理 / 认证 / 权限（ACL）
==========================================
  * 用户：PBKDF2-HMAC-SHA256 加盐口令散列，角色（admin/operator/viewer）；
  * 会话：登录发放不透明令牌（内存会话表 + TTL），API 以 Bearer 头鉴权；
  * 权限：路径前缀 ACL 规则表（principal = 用户或角色，effect = allow/deny，
    priority 数值大者优先；同优先级最长路径前缀优先），
    未命中规则时回落到默认策略（按角色能力）。
  * check() 返回完整判定轨迹（trace），供前端"权限测试器"可视化。
"""

import hashlib
import os
import threading

from . import config
from .util import gen_token, norm_path, now


class AuthError(Exception):
    pass


# ============================================================================
# 口令散列
# ============================================================================

def _hash_password(password, salt, iterations=config.PBKDF2_ITERATIONS):
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                             bytes.fromhex(salt), iterations,
                             dklen=config.PBKDF2_DKLEN)
    return dk.hex()


# ============================================================================
# 用户与会话
# ============================================================================

class AuthManager:
    def __init__(self, meta):
        self.meta = meta
        self.lock = threading.RLock()
        self.sessions = {}          # token -> {username, created, expires, ip}

    # ---------------------------------------------------------------- 初始化
    def ensure_seed(self):
        """首次启动时创建默认管理员。"""
        with self.meta.lock:
            users = self.meta.get("users")
            users.setdefault("users", {})
            users.setdefault("created_at", now())
            if config.DEFAULT_ADMIN_USER not in users["users"]:
                self._create_user_nolock(
                    config.DEFAULT_ADMIN_USER, config.DEFAULT_ADMIN_PASSWORD,
                    "admin", "admin@dfsvs.local", "系统内置管理员")
                self.meta.touch("users")

    # ---------------------------------------------------------------- 用户
    def _create_user_nolock(self, username, password, role, email, note=""):
        users = self.meta.get("users")["users"]
        if username in users:
            raise AuthError(f"用户已存在: {username}")
        if role not in config.ROLE_CAPABILITIES:
            raise AuthError(f"非法角色: {role}")
        if len(password or "") < 6:
            raise AuthError("口令至少 6 位")
        salt = os.urandom(16).hex()
        users[username] = {
            "username": username,
            "salt": salt,
            "pw_hash": _hash_password(password, salt),
            "role": role,
            "email": email or "",
            "note": note or "",
            "status": "active",           # active | disabled
            "created_at": now(),
            "last_login": None,
            "login_count": 0,
        }
        return users[username]

    def create_user(self, username, password, role="viewer", email="", note=""):
        username = (username or "").strip().lower()
        if not username or len(username) < 2 or len(username) > 32:
            raise AuthError("用户名需为 2~32 个字符")
        if not username.replace("_", "").replace("-", "").isalnum():
            raise AuthError("用户名仅允许字母/数字/_/-")
        with self.meta.lock:
            user = self._create_user_nolock(username, password, role, email, note)
            self.meta.touch("users")
            return self.public_user(user)

    def update_user(self, username, role=None, email=None, note=None,
                    status=None, password=None):
        with self.meta.lock:
            users = self.meta.get("users")["users"]
            user = users.get(username)
            if not user:
                raise AuthError(f"用户不存在: {username}")
            if role is not None:
                if role not in config.ROLE_CAPABILITIES:
                    raise AuthError(f"非法角色: {role}")
                user["role"] = role
            if email is not None:
                user["email"] = email
            if note is not None:
                user["note"] = note
            if status is not None:
                if status not in ("active", "disabled"):
                    raise AuthError("非法状态")
                user["status"] = status
            if password:
                if len(password) < 6:
                    raise AuthError("口令至少 6 位")
                salt = os.urandom(16).hex()
                user["salt"] = salt
                user["pw_hash"] = _hash_password(password, salt)
            self.meta.touch("users")
            return self.public_user(user)

    def delete_user(self, username):
        if username == config.DEFAULT_ADMIN_USER:
            raise AuthError("内置管理员不可删除")
        with self.meta.lock:
            users = self.meta.get("users")["users"]
            if username not in users:
                raise AuthError(f"用户不存在: {username}")
            del users[username]
            # 顺带吊销其会话
            for tok in [t for t, s in self.sessions.items()
                        if s["username"] == username]:
                self.sessions.pop(tok, None)
            self.meta.touch("users")

    def list_users(self):
        with self.meta.lock:
            users = self.meta.get("users").get("users", {})
            return sorted((self.public_user(u) for u in users.values()),
                          key=lambda x: x["created_at"])

    def get_user(self, username):
        with self.meta.lock:
            return self.public_user(self.meta.get("users")["users"].get(username))

    def public_user(self, user):
        """脱敏后的用户信息（去除盐与散列）。"""
        if not user:
            return None
        return {k: v for k, v in user.items() if k not in ("salt", "pw_hash")}

    # ---------------------------------------------------------------- 会话
    def login(self, username, password, ip=""):
        with self.meta.lock:
            user = self.meta.get("users")["users"].get((username or "").strip().lower())
            if not user:
                raise AuthError("用户名或口令错误")
            if user["status"] != "active":
                raise AuthError("账号已停用")
            if _hash_password(password or "", user["salt"]) != user["pw_hash"]:
                raise AuthError("用户名或口令错误")
            token = gen_token(config.TOKEN_PREFIX.rstrip("_"))
            self.sessions[token] = {
                "username": user["username"],
                "created": now(),
                "expires": now() + config.TOKEN_TTL,
                "ip": ip,
            }
            user["last_login"] = now()
            user["login_count"] = user.get("login_count", 0) + 1
            self.meta.touch("users", flush=False)
            return token, self.public_user(user)

    def logout(self, token):
        with self.lock:
            self.sessions.pop(token, None)

    def user_for_token(self, token):
        """令牌 -> 用户公开信息；过期/无效返回 None。"""
        if not token:
            return None
        with self.lock:
            sess = self.sessions.get(token)
            if not sess:
                return None
            if sess["expires"] < now():
                self.sessions.pop(token, None)
                return None
            with self.meta.lock:
                user = self.meta.get("users")["users"].get(sess["username"])
            if not user or user["status"] != "active":
                return None
            return self.public_user(user)

    def list_sessions(self):
        with self.lock:
            out = []
            t = now()
            for tok, s in self.sessions.items():
                if s["expires"] < t:
                    continue
                out.append({"token_tail": tok[-6:], "username": s["username"],
                            "created": s["created"], "expires": s["expires"],
                            "ip": s.get("ip", "")})
            return sorted(out, key=lambda x: x["created"], reverse=True)

    def revoke_sessions(self, username=None):
        with self.lock:
            if username is None:
                n = len(self.sessions)
                self.sessions.clear()
                return n
            toks = [t for t, s in self.sessions.items()
                    if s["username"] == username]
            for t in toks:
                self.sessions.pop(t, None)
            return len(toks)

    def prune_expired(self):
        with self.lock:
            t = now()
            for tok in [k for k, s in self.sessions.items() if s["expires"] < t]:
                self.sessions.pop(tok, None)

    def role_caps(self, role):
        return config.ROLE_CAPABILITIES.get(role, {"caps": []})["caps"]

    def has_cap(self, user, cap):
        if not user:
            return False
        return cap in self.role_caps(user.get("role"))


# ============================================================================
# 权限（路径前缀 ACL）
# ============================================================================

ACTIONS = ("read", "write", "delete", "admin")

DEFAULT_POLICY = {
    # 未命中任何规则时的角色默认能力
    "read": ["admin", "operator", "viewer"],
    "write": ["admin", "operator"],
    "delete": ["admin", "operator"],
    "admin": ["admin"],
}


class PermissionManager:
    def __init__(self, meta, auth):
        self.meta = meta
        self.auth = auth

    def ensure_seed(self):
        with self.meta.lock:
            perms = self.meta.get("perms")
            perms.setdefault("rules", [])
            perms.setdefault("default_policy", dict(DEFAULT_POLICY))
            perms.setdefault("updated_at", now())

    # ---------------------------------------------------------------- 规则
    def list_rules(self):
        with self.meta.lock:
            perms = self.meta.get("perms")
            rules = sorted(perms.get("rules", []),
                           key=lambda r: (-r.get("priority", 0), -len(r.get("path", ""))))
            return {"rules": rules,
                    "default_policy": perms.get("default_policy", DEFAULT_POLICY)}

    def add_rule(self, path, principal, principal_type="user",
                 perms=None, effect="allow", priority=100, note=""):
        path = norm_path(path)
        perms = [p for p in (perms or ["read"]) if p in ACTIONS]
        if not perms:
            raise AuthError("至少选择一个权限位")
        if principal_type not in ("user", "role"):
            raise AuthError("principal_type 必须是 user 或 role")
        if principal_type == "role" and principal not in config.ROLE_CAPABILITIES:
            raise AuthError(f"非法角色: {principal}")
        from .util import gen_id
        rule = {
            "id": gen_id("acl"),
            "path": path,
            "principal": principal,
            "principal_type": principal_type,
            "perms": perms,
            "effect": effect if effect in ("allow", "deny") else "allow",
            "priority": int(priority),
            "note": note or "",
            "created_at": now(),
        }
        with self.meta.lock:
            self.meta.get("perms")["rules"].append(rule)
            self.meta.touch("perms")
        return rule

    def update_rule(self, rule_id, **fields):
        with self.meta.lock:
            rules = self.meta.get("perms")["rules"]
            rule = next((r for r in rules if r["id"] == rule_id), None)
            if not rule:
                raise AuthError("规则不存在")
            for k in ("path", "principal", "principal_type", "perms",
                      "effect", "priority", "note"):
                if k in fields and fields[k] is not None:
                    rule[k] = fields[k]
            rule["path"] = norm_path(rule.get("path", "/"))
            self.meta.touch("perms")
            return rule

    def delete_rule(self, rule_id):
        with self.meta.lock:
            rules = self.meta.get("perms")["rules"]
            before = len(rules)
            self.meta.get("perms")["rules"] = [r for r in rules
                                               if r["id"] != rule_id]
            if len(self.meta.get("perms")["rules"]) == before:
                raise AuthError("规则不存在")
            self.meta.touch("perms")

    # ---------------------------------------------------------------- 判定
    def check(self, user, path, action):
        """
        判定 user 对 path 是否有 action 权限。
        返回 {"allowed": bool, "reason": str, "rule": dict|None, "trace": [str]}
        判定顺序：
          1. admin 角色对 action=admin 的运维操作直接放行（能力位）；
          2. 规则按 (priority desc, path 长度 desc) 排序，取第一条
             principal 匹配且路径前缀匹配的规则，其 effect 决定结果；
          3. 未命中 => 默认策略（角色能力）。
        """
        trace = []
        if not user:
            return {"allowed": False, "reason": "未认证", "rule": None,
                    "trace": ["匿名访问被拒绝"]}
        username = user.get("username")
        role = user.get("role")
        path = norm_path(path)
        if action not in ACTIONS:
            return {"allowed": False, "reason": f"未知操作 {action}",
                    "rule": None, "trace": trace}

        trace.append(f"用户 {username}（角色 {role}）请求 {action} @ {path}")

        with self.meta.lock:
            perms = self.meta.get("perms")
            rules = sorted(perms.get("rules", []),
                           key=lambda r: (-r.get("priority", 0),
                                          -len(r.get("path", "/"))))
            policy = perms.get("default_policy", DEFAULT_POLICY)

        for rule in rules:
            r_path = rule.get("path", "/")
            if not (path == r_path or path.startswith(
                    r_path.rstrip("/") + "/") or r_path == "/"):
                continue
            match_user = (rule["principal_type"] == "user"
                          and rule["principal"] == username)
            match_role = (rule["principal_type"] == "role"
                          and rule["principal"] == role)
            if not (match_user or match_role):
                continue
            hit = action in rule.get("perms", [])
            trace.append(
                f"命中规则 [{rule['id'][-6:]}] path={r_path} "
                f"principal={rule['principal']}({rule['principal_type']}) "
                f"perms={','.join(rule.get('perms', []))} effect={rule['effect']} "
                f"priority={rule.get('priority', 0)}"
                + ("" if hit else "（不含所请求权限位，继续向下匹配）"))
            if hit:
                allowed = rule["effect"] == "allow"
                trace.append(f"规则裁决: {'允许' if allowed else '拒绝'}")
                return {"allowed": allowed,
                        "reason": f"规则 {rule['id'][-6:]} ({rule['effect']})",
                        "rule": rule, "trace": trace}

        # 默认策略
        allowed_roles = policy.get(action, [])
        allowed = role in allowed_roles
        trace.append(f"未命中规则，默认策略: {action} 允许角色 {allowed_roles} "
                     f"=> {'允许' if allowed else '拒绝'}")
        return {"allowed": allowed,
                "reason": "默认策略",
                "rule": None, "trace": trace}

    def require(self, user, path, action):
        result = self.check(user, path, action)
        if not result["allowed"]:
            raise AuthError(f"权限不足: {user.get('username') if user else '匿名'} "
                            f"无 {action} @ {path}（{result['reason']}）")
        return result
