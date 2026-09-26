# -*- coding: utf-8 -*-
"""
util.py — 通用工具库
=====================
  * 时间 / ID / 哈希
  * 原子写（tmp + fsync + os.replace + 目录 fsync）——元数据 JSON 多节点安全落盘的基础
  * 版本向量（Version Vector）运算：merge / compare / increment
  * 节点间 HTTP 同步客户端（GET / PUT / POST-JSON，带重试）
  * LRU 缓存、环形缓冲、Range 解析、MIME 猜测等杂项
"""

import base64
import hashlib
import json
import mimetypes
import os
import random
import re
import string
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from urllib import error as urlerror
from urllib import request as urlrequest

# ----------------------------------------------------------------------------
# 时间
# ----------------------------------------------------------------------------

def now():
    """当前 Unix 时间戳（秒，float）。"""
    return time.time()


def now_ms():
    """当前 Unix 时间戳（毫秒，int）。"""
    return int(time.time() * 1000)


def fmt_ts(ts, fmt="%Y-%m-%d %H:%M:%S"):
    """时间戳 -> 可读字符串。"""
    if ts is None:
        return "-"
    return time.strftime(fmt, time.localtime(float(ts)))


def hour_key(ts, offset_hours=0):
    import time as _time
    return _time.strftime("%Y-%m-%dT%H",
                          _time.localtime(float(ts) + offset_hours * 3600))


def time_ago(ts):
    """时间戳 -> 'x 秒前' 风格的相对时间。"""
    if ts is None:
        return "-"
    delta = max(0.0, now() - float(ts))
    if delta < 60:
        return f"{int(delta)} 秒前"
    if delta < 3600:
        return f"{int(delta // 60)} 分钟前"
    if delta < 86400:
        return f"{int(delta // 3600)} 小时前"
    return f"{int(delta // 86400)} 天前"


# ----------------------------------------------------------------------------
# ID / 哈希
# ----------------------------------------------------------------------------

def gen_id(prefix):
    """生成带前缀的短随机 ID，如 blk_9f3a1c2d4e5f。"""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def gen_token(prefix="tok"):
    """生成高熵会话令牌。"""
    alphabet = string.ascii_letters + string.digits
    body = "".join(random.SystemRandom().choice(alphabet) for _ in range(32))
    return f"{prefix}_{body}"


def sha256_bytes(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha1_hex(text):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def short_hash(h, n=10):
    return (h or "")[:n]


# ----------------------------------------------------------------------------
# 原子写（难点五：JSON 元数据原子写）
# ----------------------------------------------------------------------------

def _fsync_dir(path):
    """对目录 fsync，确保 rename 记录本身落盘（POSIX）。"""
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def atomic_write_bytes(path, data):
    """
    原子写字节：写入同目录临时文件 -> flush -> fsync -> os.replace -> 目录 fsync。
    任何时刻读者看到的要么是旧完整文件，要么是新完整文件，绝无半截 JSON。
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)          # 原子 rename
        _fsync_dir(directory)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_text(path, text, encoding="utf-8"):
    atomic_write_bytes(path, text.encode(encoding))


def atomic_write_json(path, obj, pretty=True):
    """原子写 JSON 文档。"""
    if pretty:
        data = json.dumps(obj, ensure_ascii=False, indent=1, sort_keys=False)
    else:
        data = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    atomic_write_text(path, data)


def read_json(path, default=None):
    """容错读取 JSON：文件不存在或损坏时返回 default。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except (json.JSONDecodeError, OSError):
        # 损坏文件挪走留证，避免反复解析失败
        try:
            os.replace(path, path + f".corrupt-{now_ms()}")
        except OSError:
            pass
        return default


def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


# ----------------------------------------------------------------------------
# 版本向量（难点五：Version Vector）
# ----------------------------------------------------------------------------

def vv_merge(a, b):
    """合并两个版本向量：逐分量取最大。"""
    out = dict(a or {})
    for k, v in (b or {}).items():
        if v > out.get(k, 0):
            out[k] = v
    return out


def vv_increment(vv, node_id, step=1):
    """本地逻辑时钟推进，返回新向量（不修改原对象）。"""
    out = dict(vv or {})
    out[node_id] = out.get(node_id, 0) + step
    return out


def vv_compare(a, b):
    """
    比较版本向量:
      'equal'     a == b
      'after'     a 严格支配 b（a 更新）
      'before'    b 严格支配 a（b 更新）
      'concurrent' 并发（冲突，需要合并策略）
    """
    a = a or {}
    b = b or {}
    keys = set(a) | set(b)
    ge = all(a.get(k, 0) >= b.get(k, 0) for k in keys)
    le = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    if ge and le:
        return "equal"
    if ge:
        return "after"
    if le:
        return "before"
    return "concurrent"


def vv_dominates(a, b):
    return vv_compare(a, b) == "after"


def vv_total(vv):
    return sum((vv or {}).values())


# ----------------------------------------------------------------------------
# 节点间 HTTP 同步客户端
# ----------------------------------------------------------------------------

class HttpError(Exception):
    def __init__(self, status, message, payload=None):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message
        self.payload = payload


def http_request(url, method="GET", data=None, json_payload=None, headers=None,
                 timeout=5.0):
    """
    底层 HTTP 请求，返回 (status, headers, body_bytes)。
    非 2xx 抛 HttpError（尽量解析 JSON 错误体）。
    """
    hdrs = {"User-Agent": "dfsvs-node/1.0"}
    if headers:
        hdrs.update(headers)
    body = data
    if json_payload is not None:
        body = json.dumps(json_payload, ensure_ascii=False).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json; charset=utf-8")
    req = urlrequest.Request(url, data=body, method=method, headers=hdrs)
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urlerror.HTTPError as e:
        raw = b""
        try:
            raw = e.read()
        except Exception:
            pass
        msg = raw.decode("utf-8", "replace")[:500]
        payload = None
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            pass
        raise HttpError(e.code, msg, payload)
    except urlerror.URLError as e:
        raise HttpError(0, f"connection failed: {e.reason}")
    except TimeoutError:
        raise HttpError(0, "timeout")


def http_json(url, method="GET", payload=None, timeout=5.0, headers=None,
              retries=0, retry_delay=0.25):
    """请求并解析 JSON 响应；带可选重试（指数退避 + 抖动）。"""
    last = None
    for attempt in range(retries + 1):
        try:
            _status, _hdrs, body = http_request(
                url, method=method, json_payload=payload, headers=headers,
                timeout=timeout)
            if not body:
                return {}
            return json.loads(body.decode("utf-8"))
        except HttpError as e:
            last = e
            if e.status and 400 <= e.status < 500 and e.status != 429:
                raise  # 4xx（除限流）不重试
        if attempt < retries:
            time.sleep(retry_delay * (2 ** attempt) + random.random() * 0.05)
    raise last


def http_get_bytes(url, timeout=10.0, headers=None):
    _status, _hdrs, body = http_request(url, "GET", headers=headers, timeout=timeout)
    return body


def http_put_bytes(url, data, timeout=15.0, headers=None):
    _status, _hdrs, body = http_request(url, "PUT", data=data, headers=headers,
                                        timeout=timeout)
    try:
        return json.loads(body.decode("utf-8")) if body else {}
    except json.JSONDecodeError:
        return {}


def b64e(data):
    return base64.b64encode(data).decode("ascii")


def b64d(text):
    return base64.b64decode(text.encode("ascii"))


# ----------------------------------------------------------------------------
# 数据结构
# ----------------------------------------------------------------------------

class LRU:
    """线程安全 LRU 缓存（用于块内容缓存，加速重复 diff / 下载）。"""

    def __init__(self, maxsize=256, max_bytes=64 * 1024 * 1024):
        self._data = OrderedDict()
        self._lock = threading.Lock()
        self.maxsize = maxsize
        self.max_bytes = max_bytes
        self._bytes = 0
        self.hits = 0
        self.misses = 0

    def get(self, key, default=None):
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
                self.hits += 1
                return self._data[key]
            self.misses += 1
            return default

    def put(self, key, value):
        size = len(value) if isinstance(value, (bytes, bytearray, str, list)) else 0
        with self._lock:
            if key in self._data:
                old = self._data.pop(key)
                self._bytes -= len(old) if isinstance(old, (bytes, bytearray, str, list)) else 0
            self._data[key] = value
            self._data.move_to_end(key)
            self._bytes += size
            while self._data and (len(self._data) > self.maxsize
                                  or self._bytes > self.max_bytes):
                _k, old = self._data.popitem(last=False)
                self._bytes -= len(old) if isinstance(old, (bytes, bytearray, str, list)) else 0

    def invalidate(self, key):
        with self._lock:
            old = self._data.pop(key, None)
            if old is not None and isinstance(old, (bytes, bytearray, str, list)):
                self._bytes -= len(old)

    def clear(self):
        with self._lock:
            self._data.clear()
            self._bytes = 0

    def stats(self):
        with self._lock:
            return {"entries": len(self._data), "bytes": self._bytes,
                    "hits": self.hits, "misses": self.misses}


class RingBuffer:
    """定长环形缓冲（集群事件流 / 内存日志）。"""

    def __init__(self, maxlen=400):
        self._items = []
        self._maxlen = maxlen
        self._lock = threading.Lock()
        self._seq = 0

    def append(self, item):
        with self._lock:
            self._seq += 1
            if isinstance(item, dict):
                item = dict(item)
                item.setdefault("seq", self._seq)
            self._items.append(item)
            if len(self._items) > self._maxlen:
                self._items = self._items[-self._maxlen:]
                self._seq = 0
            return self._seq

    def items(self, since_seq=0, limit=200):
        with self._lock:
            out = [x for x in self._items
                   if not isinstance(x, dict) or x.get("seq", 0) > since_seq]
            return out[-limit:]

    def all(self):
        with self._lock:
            return list(self._items)

    def __len__(self):
        with self._lock:
            return len(self._items)


class RateCounter:
    """滑动窗口速率统计（IO 速率、请求 QPS）。"""

    def __init__(self, window=10.0):
        self.window = window
        self._events = []
        self._lock = threading.Lock()

    def hit(self, amount=1):
        t = now()
        with self._lock:
            self._events.append((t, amount))
            self._prune(t)

    def _prune(self, t):
        cutoff = t - self.window
        while self._events and self._events[0][0] < cutoff:
            self._events.pop(0)

    def rate(self):
        t = now()
        with self._lock:
            self._prune(t)
            total = sum(a for _t, a in self._events)
            return total / self.window if self._events else 0.0

    def total(self):
        with self._lock:
            return sum(a for _t, a in self._events)


# ----------------------------------------------------------------------------
# HTTP Range / MIME / 文本
# ----------------------------------------------------------------------------

_RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")


def parse_range(header_value, size):
    """解析 Range: bytes=start-end，返回 (start, end)（含端点）或 None。"""
    if not header_value:
        return None
    m = _RANGE_RE.match(header_value.strip())
    if not m:
        return None
    s, e = m.group(1), m.group(2)
    if s == "" and e == "":
        return None
    if s == "":                       # bytes=-N 取末尾 N 字节
        length = int(e)
        start = max(0, size - length)
        end = size - 1
    else:
        start = int(s)
        end = int(e) if e else size - 1
    start = max(0, min(start, size - 1)) if size else 0
    end = max(start, min(end, size - 1))
    return start, end


def content_range_value(start, end, size):
    return f"bytes {start}-{end}/{size}"


_EXTRA_MIMES = {
    ".md": "text/markdown",
    ".py": "text/x-python",
    ".js": "application/javascript",
    ".json": "application/json",
    ".svg": "image/svg+xml",
    ".yml": "text/yaml",
    ".yaml": "text/yaml",
    ".log": "text/plain",
    ".csv": "text/csv",
    ".ini": "text/plain",
    ".sh": "text/x-shellscript",
    ".html": "text/html",
    ".css": "text/css",
}


def guess_mime(name):
    ext = os.path.splitext(name or "")[1].lower()
    if ext in _EXTRA_MIMES:
        return _EXTRA_MIMES[ext]
    mime, _enc = mimetypes.guess_type(name or "")
    return mime or "application/octet-stream"


TEXT_MIME_PREFIXES = ("text/",)
TEXT_MIME_EXACT = {
    "application/json", "application/javascript", "application/xml",
    "application/x-yaml", "application/xhtml+xml", "image/svg+xml",
}


def is_text_mime(mime):
    if not mime:
        return False
    mime = mime.lower()
    if mime in TEXT_MIME_EXACT:
        return True
    return any(mime.startswith(p) for p in TEXT_MIME_PREFIXES)


def decode_text(data):
    """尽力将字节解码为文本；二进制内容返回 None。"""
    if data is None:
        return None
    if isinstance(data, str):
        return data
    for enc in ("utf-8", "gb18030", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return None


def looks_binary(data, sample=8192):
    """启发式判断二进制：前 sample 字节含 NUL 即视为二进制。"""
    if not data:
        return False
    return b"\x00" in data[:sample]


# ----------------------------------------------------------------------------
# 杂项
# ----------------------------------------------------------------------------

def human_size(n):
    n = float(n or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def needs_recovery(live, desired, minimum, trigger):
    if trigger == "min":
        return live < minimum
    return live < desired


def classify_merge_base(base_id, ours_id, theirs_id, policy):
    if policy == "swapped":
        if base_id == theirs_id:
            return "fast-forward"
        if base_id == ours_id:
            return "noop"
    else:
        if base_id == theirs_id:
            return "noop"
        if base_id == ours_id:
            return "fast-forward"
    return "merge"


def canonical_access_op(op, mapping):
    return (mapping or {}).get(op, op)


def split_multi(value, sep):
    return [v for v in (value or "").split(sep) if v]


def ttl_seconds(amount, unit):
    factor = {"seconds": 1, "minutes": 60, "hours": 3600,
              "days": 86400}.get(unit, 86400)
    return amount * factor


def to_rate_units(x, scale):
    return x / scale if scale else x


def sort_by_ts(items, order):
    return sorted(items, key=lambda x: x.get("ts", 0),
                  reverse=(order != "asc"))


def merge_label_swap(flag):
    return bool(flag)


def deep_get(d, path, default=None):
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def canonical_json(obj):
    """规范化 JSON 序列化（键排序、紧凑分隔符）——用于计算稳定的树哈希。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def safe_name(name):
    """清洗文件/目录名：去掉路径分隔符与控制字符。"""
    name = (name or "").strip().replace("\\", "/").split("/")[-1]
    name = re.sub(r"[\x00-\x1f]", "", name)
    if name in ("", ".", ".."):
        raise ValueError(f"非法名称: {name!r}")
    if len(name) > 180:
        raise ValueError("名称过长（>180 字符）")
    return name


def norm_path(path):
    """规范化绝对路径：去重斜杠、解析 . 与 ..（不触碰文件系统）。"""
    path = (path or "/").strip()
    if not path.startswith("/"):
        path = "/" + path
    parts = []
    for seg in path.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            if parts:
                parts.pop()
            continue
        parts.append(seg)
    return "/" + "/".join(parts)


def join_path(base, name):
    base = norm_path(base)
    if base == "/":
        return "/" + name
    return base + "/" + name


def retry_call(fn, attempts=3, delay=0.2, backoff=2.0, on_error=None):
    """通用重试封装。"""
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:     # noqa: BLE001 — 通用重试语义
            last = e
            if on_error:
                on_error(e, i)
            if i < attempts - 1:
                time.sleep(delay * (backoff ** i))
    raise last
