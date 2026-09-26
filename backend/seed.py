# -*- coding: utf-8 -*-
"""
seed.py — 演示数据种子
========================
首次启动时构建一个"已经在运行"的集群快照：
  * 用户（admin / alice-operator / bob-viewer / carol-ops-disabled）
  * 权限规则（/finance 只允许 admin+alice，/public 全员可读等）
  * 目录树与文件：文档 / 代码 / 数据（CSV、二进制）/ 图片（SVG 缩略图）
  * 版本历史：main 多次提交 + feature-ui 分支（含与 main 冲突的修改，
    供"合并冲突"演示）+ release 分支（供快进合并演示）
  * 回收站条目、历史访问记录（热度）、24 小时吞吐、容量历史、历史日志
"""

import os
import random

from . import config
from .auth import AuthError
from .util import hour_key, now, sha256_bytes

SEEDED_FLAG = "seeded_v1"


def is_seeded(nn):
    with nn.meta.lock:
        return bool(nn.meta.get("cluster").get(SEEDED_FLAG))


def mark_seeded(nn):
    with nn.meta.lock:
        nn.meta.get("cluster")[SEEDED_FLAG] = now()
        nn.meta.touch("cluster")


# ----------------------------------------------------------------------------
# 文本素材
# ----------------------------------------------------------------------------

README_MD = """# DFSVS 分布式文件存储与版本控制系统

欢迎使用 DFSVS。这是一个用纯 Python + 原生 HTML/JS 实现的
**教学级分布式文件系统**，模拟了 HDFS 风格的 NameNode/DataNode 架构，
并在其上叠加了 Git 风格的版本控制。

## 核心特性

- 文件分块存储（固定 64KiB 块，可选 CDC 内容定义分块）
- 多副本冗余（默认 3 副本，跨机架放置，genstamp 版本戳）
- 心跳检测与自动故障恢复（副本再复制、损坏块重建）
- 版本控制：提交 / 分支 / 合并（三方合并 + 冲突标记）
- Myers 差分算法（线性空间轨迹 + Patience 锚点分治）
- 元数据集中 JSON 存储：原子写 + 版本向量同步

## 快速开始

1. 打开"文件浏览"页查看目录树与缩略图
2. 在"上传下载"页体验分块上传与断点续传
3. 在"节点状态"页杀死一个 DataNode，观察自动恢复
4. 在"版本历史"页把 feature-ui 合并进 main，观察冲突处理

## 架构

```
浏览器 (10+ 页面)
   │ HTTP/JSON
   ▼
NameNode :8020 ──── 元数据(JSON, 原子写+版本向量)
   │  ▲ 心跳/块汇报/复制命令 (HTTP)
   ▼  │
DataNode :8021..8024 ── 块数据(.dat + sha256 校验)
```
"""

ARCH_MD = """# 架构设计说明

## 1. 元数据层

NameNode 将所有元数据组织为 9 个 JSON 文档：
fs / blocks / versions / users / perms / logs / recycle / stats / cluster。
所有写入都经过 `atomic_write_json`：临时文件 → fsync → os.replace → 目录 fsync，
保证任何崩溃点都不会产生半截文件。

## 2. 副本一致性

- 每个块携带 genstamp（单调递增版本戳）与 sha256 校验和；
- 写入采用流水线复制：NN → DN1 → DN2 → DN3，每跳校验；
- DataNode 周期性全量块汇报，NameNode 对账：
  * 未知块 / genstamp 过期 / 校验和不符 → 下发删除命令；
  * 副本缺失 → 进入 under-replicated 恢复队列。

## 3. 故障恢复

心跳超时（6s）→ 节点判 DEAD → 其上副本全部失效 →
恢复调度线程为受影响块选择 (存活源副本, 目标节点)，
通过心跳应答下发 REPLICATE 命令，目标节点 HTTP 拉取块数据。

## 4. 版本树

提交即"全量快照 + 块引用"。块不可变，历史版本天然可读。
GC 保护集 = 活动 inode ∪ 全部提交快照，未被引用的块过宽限期后回收。

## 5. 三方合并

base = LCA(ours, theirs)。快照级合并对每个路径做状态机判定，
文本冲突落到行级 diff3（Myers 支撑），二进制冲突保留 ours 并记录。
"""

MEETING_NOTES_MAIN = """# 周会纪要（2026-09 第 3 周）

参会：admin、alice、bob

## 议题一：副本恢复延迟
结论：恢复调度限流从 32 提升到 64，观察一周心跳应答大小。

## 议题二：回收站保留期
结论：保留期维持 7 天，财务目录延长到 30 天（通过 ACL 单独控制）。

## 议题三：大文件 diff 性能
结论：Myers 轨迹在 D>900 时降级 Patience，前端展示算法与耗时。

## 行动项
- [ ] admin：压测 5MB 文件的行级 diff
- [ ] alice：补充分块上传断点续传的 E2E 用例
"""

MEETING_NOTES_FEATURE = """# 周会纪要（2026-09 第 3 周）

参会：admin、alice、bob、carol

## 议题一：副本恢复延迟
结论：恢复调度限流从 32 提升到 128，并引入优先级队列（missing 优先）。

## 议题二：前端暗色主题
结论：feature-ui 分支统一暗色主题，节点页新增副本分布矩阵。

## 议题三：大文件 diff 性能
结论：Myers 轨迹在 D>900 时降级 Patience，前端展示算法与耗时。

## 行动项
- [ ] alice：完成暗色主题走查
- [ ] carol：副本分布矩阵增加机架着色
"""

ROADMAP_MD = """# 路线图

## v1.0（当前）
- [x] NameNode/DataNode 模拟
- [x] 分块 + 3 副本 + 心跳 + 自动恢复
- [x] 版本控制（提交/分支/合并/检出）
- [x] Myers/Patience 差异对比
- [x] 10+ 前端页面

## v1.1（计划）
- [ ] NameNode HA（双 NN + 版本向量合并）
- [ ] 纠删码（RS 6+3）替代三副本
- [ ] 快照 diff 增量传输

## v2.0（远期）
- [ ] 跨机房异步复制
- [ ] 基于 Raft 的元数据一致性
"""

SERVER_PY = '''"""demo_server.py — 演示用业务服务（被存储系统托管的示例代码文件）"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            body = json.dumps({"ok": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 9999), Handler).serve_forever()
'''

CLIENT_PY = '''"""demo_client.py — 调用演示服务的客户端"""
import json
import urllib.request


def health_check(url="http://127.0.0.1:9999/healthz", timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


if __name__ == "__main__":
    print(health_check())
'''

UTILS_PY = '''"""demo_utils.py — 通用小工具"""


def human_size(n):
    for unit in ("B", "K", "M", "G"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}T"


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
'''

UI_CSS = """/* ui.css — feature-ui 分支新增：暗色主题变量 */
:root {
  --bg: #0e1117;
  --panel: #161b26;
  --border: #232a3a;
  --text: #dbe2f1;
  --muted: #8b93a7;
  --accent: #4f8cff;
  --ok: #2fbf71;
  --warn: #f0a538;
  --bad: #e5534b;
}

.panel { background: var(--panel); border: 1px solid var(--border); }
.badge-ok  { color: var(--ok); }
.badge-warn{ color: var(--warn); }
.badge-bad { color: var(--bad); }
"""

LOGO_SVG = """<svg xmlns="http://www.w3.org/2000/svg" width="240" height="160" viewBox="0 0 240 160">
  <defs>
    <linearGradient id="g1" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#4f8cff"/><stop offset="1" stop-color="#9b5cff"/>
    </linearGradient>
  </defs>
  <rect width="240" height="160" rx="14" fill="#10141f"/>
  <circle cx="70" cy="60" r="26" fill="url(#g1)"/>
  <circle cx="140" cy="42" r="16" fill="#2fbf71" opacity="0.85"/>
  <circle cx="170" cy="96" r="20" fill="#f0a538" opacity="0.85"/>
  <circle cx="96" cy="112" r="14" fill="#e5534b" opacity="0.85"/>
  <line x1="70" y1="60" x2="140" y2="42" stroke="#3a4460" stroke-width="2"/>
  <line x1="70" y1="60" x2="170" y2="96" stroke="#3a4460" stroke-width="2"/>
  <line x1="70" y1="60" x2="96" y2="112" stroke="#3a4460" stroke-width="2"/>
  <line x1="140" y1="42" x2="170" y2="96" stroke="#3a4460" stroke-width="2"/>
  <text x="120" y="150" fill="#dbe2f1" font-family="monospace" font-size="13" text-anchor="middle">DFSVS cluster topology</text>
</svg>
"""

CHART_SVG = """<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180" viewBox="0 0 320 180">
  <rect width="320" height="180" rx="12" fill="#0e1117"/>
  <g stroke="#232a3a"><line x1="40" y1="20" x2="40" y2="150"/><line x1="40" y1="150" x2="300" y2="150"/></g>
  <g fill="#4f8cff">
    <rect x="60" y="90" width="24" height="60"/><rect x="100" y="60" width="24" height="90"/>
    <rect x="140" y="100" width="24" height="50"/><rect x="180" y="40" width="24" height="110"/>
    <rect x="220" y="75" width="24" height="75"/><rect x="260" y="55" width="24" height="95"/>
  </g>
  <polyline points="72,85 112,55 152,95 192,35 232,70 272,50" fill="none" stroke="#2fbf71" stroke-width="2.5"/>
  <text x="170" y="172" fill="#8b93a7" font-family="monospace" font-size="11" text-anchor="middle">weekly throughput (GiB)</text>
</svg>
"""

DIAGRAM_SVG = """<svg xmlns="http://www.w3.org/2000/svg" width="360" height="200" viewBox="0 0 360 200">
  <rect width="360" height="200" rx="12" fill="#10141f"/>
  <rect x="30" y="30" width="120" height="46" rx="8" fill="#1c2436" stroke="#4f8cff"/>
  <text x="90" y="58" fill="#dbe2f1" font-size="13" text-anchor="middle" font-family="monospace">NameNode</text>
  <rect x="210" y="20" width="120" height="34" rx="8" fill="#1c2436" stroke="#2fbf71"/>
  <text x="270" y="42" fill="#dbe2f1" font-size="12" text-anchor="middle" font-family="monospace">DataNode-1</text>
  <rect x="210" y="70" width="120" height="34" rx="8" fill="#1c2436" stroke="#2fbf71"/>
  <text x="270" y="92" fill="#dbe2f1" font-size="12" text-anchor="middle" font-family="monospace">DataNode-2</text>
  <rect x="210" y="120" width="120" height="34" rx="8" fill="#1c2436" stroke="#f0a538"/>
  <text x="270" y="142" fill="#dbe2f1" font-size="12" text-anchor="middle" font-family="monospace">DataNode-3</text>
  <line x1="150" y1="45" x2="210" y2="37" stroke="#3a4460" stroke-width="2"/>
  <line x1="150" y1="53" x2="210" y2="87" stroke="#3a4460" stroke-width="2"/>
  <line x1="150" y1="61" x2="210" y2="137" stroke="#3a4460" stroke-width="2"/>
  <text x="180" y="185" fill="#8b93a7" font-size="11" text-anchor="middle" font-family="monospace">heartbeat / block-report / replicate (HTTP)</text>
</svg>
"""


def _sales_csv(rows=2600):
    rnd = random.Random(42)
    regions = ["华东", "华北", "华南", "西南", "东北"]
    products = ["存储节点", "计算节点", "网关", "许可证", "维保服务"]
    lines = ["order_id,date,region,product,qty,unit_price,amount"]
    for i in range(rows):
        d = f"2026-{rnd.randint(1,9):02d}-{rnd.randint(1,28):02d}"
        q = rnd.randint(1, 40)
        p = round(rnd.uniform(800, 52000), 2)
        lines.append(f"SO-{20260000+i},{d},{rnd.choice(regions)},"
                     f"{rnd.choice(products)},{q},{p},{round(q*p,2)}")
    return "\n".join(lines) + "\n"


def _users_json():
    return (
        '{"contacts": [\n'
        + ",\n".join(
            f'  {{"id": {i}, "name": "user-{i:03d}", "email": "user{i:03d}@example.com", '
            f'"quota_gb": {random.Random(i).choice([5, 20, 100, 500])}}}'
            for i in range(1, 401))
        + "\n]}\n")


def _big_log_file(lines=3000):
    rnd = random.Random(7)
    levels = ["INFO", "INFO", "INFO", "WARN", "ERROR"]
    comps = ["nn.heartbeat", "dn.scrub", "api.upload", "recovery.queue",
             "gc.blocks", "version.merge"]
    out = []
    t = now() - 86400
    for i in range(lines):
        t += rnd.uniform(0.5, 30)
        out.append(f"[{t:.3f}] {rnd.choice(levels)} {rnd.choice(comps)} - "
                   f"event#{i} latency={rnd.uniform(1, 400):.1f}ms "
                   f"node=dn{rnd.randint(1,4)}")
    return "\n".join(out) + "\n"


# ----------------------------------------------------------------------------
# 种子主流程
# ----------------------------------------------------------------------------

def seed_cluster(nn, datanodes=None, verbose=True):
    def say(msg):
        if verbose:
            print(f"[seed] {msg}")

    if is_seeded(nn):
        say("已存在种子数据，跳过")
        return False

    say("创建演示用户与权限规则 …")
    _seed_users(nn)
    _seed_perms(nn)

    say("写入初始文件 …")
    files_v1 = {
        "/docs/readme.md": (README_MD.encode(), "admin"),
        "/docs/architecture.md": (ARCH_MD.encode(), "admin"),
        "/docs/meeting-notes.txt": (MEETING_NOTES_MAIN.encode(), "admin"),
        "/code/server.py": (SERVER_PY.encode(), "alice"),
        "/code/client.py": (CLIENT_PY.encode(), "alice"),
        "/code/utils.py": (UTILS_PY.encode(), "alice"),
        "/data/sales.csv": (_sales_csv().encode(), "admin"),
        "/data/users.json": (_users_json().encode(), "admin"),
        "/logs/cluster-24h.log": (_big_log_file().encode(), "system"),
        "/images/logo.svg": (LOGO_SVG.encode(), "carol"),
        "/images/throughput.svg": (CHART_SVG.encode(), "carol"),
        "/images/architecture.svg": (DIAGRAM_SVG.encode(), "carol"),
        "/finance/payroll-q3.csv": (
            ("name,base,bonus\n" +
             "".join(f"emp{i:03d},{50000+i*97},{3000+i*31}\n"
                     for i in range(1, 121))).encode(), "admin"),
        "/public/notice.txt": (
            "本目录对全体用户开放只读。\n系统状态页: /nodes.html\n".encode(),
            "admin"),
    }
    # 大二进制文件（多块 + 全副本，撑出容量水位）
    big = os.urandom(config.SEED_RANDOM_FILE_SIZE)
    files_v1["/data/blob-3mb.bin"] = (big, "admin")

    for path, (data, author) in files_v1.items():
        nn.write_file_internal(path, data, author)
    say(f"  写入 {len(files_v1)} 个文件")

    say("构建版本历史 …")
    nn.versions.commit("init: 初始化仓库（文档/代码/数据/图片）", "admin")

    # main 上第二轮修改
    nn.write_file_internal("/docs/readme.md",
                           README_MD.replace("## 快速开始",
                                             "## 快速开始（v1.0.1 修订）"),
                           "admin")
    nn.write_file_internal("/code/utils.py",
                           UTILS_PY.replace('"""demo_utils.py — 通用小工具"""',
                                            '"""demo_utils.py — 通用小工具（新增 retry）"""')
                           + "\n\ndef retry(fn, times=3):\n"
                             "    for i in range(times):\n"
                             "        try:\n            return fn()\n"
                             "        except Exception:\n            if i == times - 1:\n                raise\n",
                           "alice")
    nn.versions.commit("feat: readme 修订 + utils 增加 retry 助手", "admin")

    # feature-ui 分支（与 main 在 meeting-notes.txt 上冲突）
    nn.versions.create_branch("feature-ui", "main", "alice",
                              "前端暗色主题与节点矩阵")
    nn.versions.checkout("feature-ui", "alice")
    nn.write_file_internal("/code/ui.css", UI_CSS.encode(), "alice")
    nn.write_file_internal("/docs/meeting-notes.txt",
                           MEETING_NOTES_FEATURE.encode(), "alice")
    nn.write_file_internal("/images/logo.svg",
                           LOGO_SVG.replace("#DFSVS cluster topology",
                                            "#DFSVS cluster topology (dark)"),
                           "carol")
    nn.versions.commit("feature(ui): 暗色主题变量 + 会议纪要（分支侧修改）",
                       "alice")

    # release 分支停在 main 早期提交（供快进合并演示）
    main_head = nn.versions.branch_head("main")[0]
    nn.versions.create_branch("release-1.0", main_head, "admin",
                              "1.0 发布分支（快进合并演示）")

    # 回到 main，做与 feature-ui 冲突的修改
    nn.versions.checkout("main", "admin")
    nn.write_file_internal("/docs/meeting-notes.txt",
                           MEETING_NOTES_MAIN.replace(
                               "- [ ] admin：压测 5MB 文件的行级 diff",
                               "- [x] admin：压测 5MB 文件的行级 diff（通过）"
                               "\n- [ ] bob：整理故障演练手册"),
                           "admin")
    nn.write_file_internal("/docs/roadmap.md", ROADMAP_MD.encode(), "admin")
    nn.versions.commit("docs: 会议纪要更新（main 侧）+ 路线图", "admin")

    say("构造回收站条目 …")
    nn.write_file_internal("/tmp/draft-old.md",
                           "# 旧草稿\n\n这份文档将被删除进回收站。\n".encode(),
                           "bob")
    nn.versions.commit("chore: 临时草稿", "bob")
    nn.fs.delete_to_trash("/tmp/draft-old.md", "bob")
    nn.write_file_internal("/tmp/scratch.txt", b"scratch data 12345\n", "bob")
    nn.fs.delete_to_trash("/tmp/scratch.txt", "alice")
    nn.versions.commit("chore: 清理临时草稿（移入回收站）", "bob")

    say("生成历史访问热度与吞吐数据 …")
    _seed_stats(nn)

    say("注入历史日志 …")
    _seed_logs(nn)

    mark_seeded(nn)
    nn.meta.flush()
    say("种子数据完成 ✓")
    return True


def _seed_users(nn):
    auth = nn.auth
    for username, pw, role, email, note in (
            ("alice", "alice123", "operator", "alice@dfsvs.local", "运维一组"),
            ("bob", "bob12345", "viewer", "bob@dfsvs.local", "只读审计"),
            ("carol", "carol123", "operator", "carol@dfsvs.local", "设计资源维护")):
        try:
            auth.create_user(username, pw, role, email, note)
        except AuthError:
            pass
    try:
        auth.update_user("carol", status="disabled")
    except AuthError:
        pass


def _seed_perms(nn):
    pm = nn.perms
    rules = [
        ("/finance", "viewer", "role", ["read", "write", "delete"], "deny", 200,
         "财务目录禁止只读用户访问"),
        ("/finance", "alice", "user", ["read", "write"], "allow", 210,
         "财务目录授权 alice"),
        ("/public", "viewer", "role", ["read"], "allow", 150,
         "公告目录全员可读"),
        ("/logs", "bob", "user", ["read"], "allow", 150,
         "审计用户可读日志目录"),
        ("/tmp", "viewer", "role", ["write"], "allow", 120,
         "临时目录开放写（练习用）"),
    ]
    for path, principal, ptype, perms, effect, prio, note in rules:
        try:
            pm.add_rule(path, principal, ptype, perms, effect, prio, note)
        except AuthError:
            pass


def _seed_stats(nn):
    rnd = random.Random(99)
    t = now()
    hot_files = [
        ("/data/sales.csv", 90), ("/images/logo.svg", 70),
        ("/docs/readme.md", 55), ("/data/blob-3mb.bin", 40),
        ("/code/server.py", 32), ("/logs/cluster-24h.log", 26),
        ("/docs/meeting-notes.txt", 18), ("/images/throughput.svg", 12),
        ("/finance/payroll-q3.csv", 8), ("/public/notice.txt", 5),
    ]
    with nn.meta.lock:
        stats = nn.meta.get("stats")
        access = stats.setdefault("access", [])
        for path, weight in hot_files:
            for i in range(weight):
                age = rnd.uniform(0, 20) * 3600
                access.append({
                    "ts": t - age, "path": path,
                    "op": rnd.choice(("download", "download", "preview",
                                      "thumb")),
                    "user": rnd.choice(["admin", "alice", "bob"]),
                    "bytes": rnd.randint(2048, 900_000),
                    "node": f"dn{rnd.randint(1, 3)}",
                })
        access.sort(key=lambda e: e["ts"])
        # 24 小时吞吐
        hourly = stats.setdefault("hourly", {})
        import time as _time
        for h in range(24):
            ts = t - h * 3600
            key = hour_key(ts, 0)
            base = 12 + int(30 * abs(rnd.gauss(0, 1)))
            hourly[key] = {
                "uploads": rnd.randint(0, 6),
                "downloads": base,
                "bytes_in": rnd.randint(0, 8) * 1024 * 1024,
                "bytes_out": base * rnd.randint(200, 900) * 1024,
            }
        # 容量历史（模拟过去两天缓慢增长）
        hist = stats.setdefault("capacity_history", [])
        used0 = 9 * 1024 * 1024
        for i in range(96):
            ts = t - (95 - i) * 1800
            hist.append({
                "ts": ts,
                "used": used0 + i * rnd.randint(30_000, 260_000),
                "capacity": 3 * 512 * 1024 * 1024,
                "files": 14 + i // 6,
                "blocks": 60 + i * 2,
            })
        nn.meta.touch("stats")


def _seed_logs(nn):
    rnd = random.Random(1234)
    t = now()
    samples = [
        ("INFO", "namenode", "start", "namenode", "system", "NameNode 启动完成"),
        ("INFO", "namenode", "node_register", "dn1", "system", "DataNode dn1 注册 rack=rack-1"),
        ("INFO", "namenode", "node_register", "dn2", "system", "DataNode dn2 注册 rack=rack-2"),
        ("INFO", "namenode", "node_register", "dn3", "system", "DataNode dn3 注册 rack=rack-3"),
        ("INFO", "upload", "complete", "/data/sales.csv", "admin", "159744 字节 / 1 分片 / 3 块"),
        ("INFO", "auth", "login", "alice", "alice", "角色 operator，来自 127.0.0.1"),
        ("WARN", "auth", "login_failed", "bob", "anonymous", "用户名或口令错误"),
        ("INFO", "auth", "login", "bob", "bob", "角色 viewer，来自 127.0.0.1"),
        ("INFO", "version", "commit", "main:c_3fa1b2", "admin", "提交 'init: 初始化仓库'"),
        ("INFO", "version", "branch_create", "feature-ui", "alice", "创建分支 feature-ui"),
        ("WARN", "block", "read_failover", "blk_88aa21", "system", "副本 dn2 读取失败，切换下一副本"),
        ("ERROR", "recovery", "node_dead", "dn2", "system", "心跳超时，判定 DEAD；启动副本恢复"),
        ("INFO", "recovery", "node_revived", "dn2", "system", "节点 dn2 恢复心跳，重新标记 LIVE"),
        ("INFO", "gc", "gc_blocks", "", "system", "回收 4 个未引用块"),
        ("ERROR", "block", "corrupt", "blk_91c3de@dn3", "system", "巡检发现校验和不匹配"),
        ("INFO", "sync", "doc_synced", "cluster", "system", "dn1: relation=applied vv={'namenode': 3}"),
        ("WARN", "fs", "delete", "/tmp/draft-old.md", "bob", "移入回收站（7 天后过期）"),
        ("INFO", "fs", "trash_restore", "/tmp/scratch.txt", "alice", "恢复条目"),
        ("INFO", "download", "range", "/data/blob-3mb.bin", "bob", "Range 0-524287 由 dn1 服务"),
    ]
    with nn.meta.lock:
        items = nn.meta.get("logs").setdefault("items", [])
        base = t - 86400
        for i in range(160):
            level, source, action, target, user, detail = rnd.choice(samples)
            items.insert(0, {
                "ts": base + i * rnd.uniform(200, 900),
                "level": level, "source": source, "action": action,
                "target": target, "user": user, "detail": detail,
            })
        items.sort(key=lambda x: x["ts"])
        nn.meta.touch("logs")
