# -*- coding: utf-8 -*-
"""
config.py — 全局配置常量
=========================
分布式文件存储与版本控制系统（DFSVS）的配置中心。

包含：
  * 路径与端口规划（NameNode / DataNode / 前端静态资源）
  * 分块策略参数（固定分块 + 内容定义分块 CDC）
  * 副本与一致性参数（副本因子、genstamp、校验和）
  * 心跳 / 块汇报 / 数据巡检 / 故障恢复的时间参数
  * 元数据 JSON 文档、版本向量、原子写参数
  * 上传会话（分块上传、断点续传）与下载（Range 分段）参数
"""

import os

# ----------------------------------------------------------------------------
# 目录规划
# ----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
META_DIR = os.path.join(DATA_DIR, "meta")            # NameNode 集中元数据（JSON）
SESSION_DIR = os.path.join(DATA_DIR, "sessions")      # 分块上传的暂存区
DATANODE_ROOT = os.path.join(DATA_DIR, "datanodes")   # 各 DataNode 的块数据目录
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")     # 前端静态页面

# ----------------------------------------------------------------------------
# 网络与端口
# ----------------------------------------------------------------------------
HOST = "127.0.0.1"
NAMENODE_PORT = 8020

# DataNode 端口表：node_id -> (port, rack)
DATANODE_PORTS = {
    "dn1": (8021, "rack-1"),
    "dn2": (8022, "rack-2"),
    "dn3": (8023, "rack-3"),
    "dn4": (8024, "rack-1"),
}

# 节点间 HTTP 同步使用的集群共享密钥（轻量鉴权，防止外部伪造心跳）
CLUSTER_KEY = "dfsvs-cluster-internal-key-2026"

NODE_URL_FMT = "http://{host}:{port}"

# 每个 DataNode 的模拟容量上限（用于容量水位/放置策略演示）
NODE_CAPACITY = 512 * 1024 * 1024

# ----------------------------------------------------------------------------
# 分块策略（难点一：分块策略）
# ----------------------------------------------------------------------------
# 系统块大小：文件被切分为固定大小的块（模拟 HDFS 128MB，此处缩小便于演示）
BLOCK_SIZE = 64 * 1024                 # 64 KiB

# 内容定义分块（CDC / Rabin 风格滚动哈希）参数——可选策略
CDC_AVG_SIZE = 64 * 1024               # 期望平均块大小
CDC_MIN_SIZE = 16 * 1024               # 最小块（防止过碎）
CDC_MAX_SIZE = 256 * 1024              # 最大块（防止过长）
CDC_MASK_BITS = 16                     # 边界掩码位数: 2^16 ≈ 64K 平均
CDC_WINDOW = 48                        # 滚动哈希窗口字节数

CHUNK_STRATEGY = "fixed"               # 默认策略: fixed | cdc

# ----------------------------------------------------------------------------
# 副本与一致性（难点一：副本一致性维护）
# ----------------------------------------------------------------------------
DEFAULT_REPLICATION = 3                # 默认副本因子
MIN_REPLICATION = 2                    # 低于该值触发紧急恢复
MAX_REPLICATION = 4
REPLICATION_TIMEOUT = 30.0             # 副本复制命令超时（秒），超时重排
GENSTAMP_INITIAL = 1                   # 块版本号（generation stamp）初始值
RECOVERY_TRIGGER = "min"               # 恢复队列触发口径

# ----------------------------------------------------------------------------
# 心跳 / 汇报 / 巡检 / 恢复（难点二：故障检测与自动恢复）
# ----------------------------------------------------------------------------
HEARTBEAT_INTERVAL = 1.5               # DataNode 心跳间隔（秒）
HEARTBEAT_TIMEOUT = 6.0                # 超过该时长无心跳判定为 DEAD
BLOCK_REPORT_INTERVAL = 5.0            # 全量块汇报间隔（秒）
SCRUB_INTERVAL = 6.0                   # 数据巡检（校验和扫描）间隔（秒）
SCRUB_BATCH = 24                       # 每次巡检最多校验的块数
RECOVERY_SCAN_INTERVAL = 2.0           # 恢复调度线程扫描间隔（秒）
GC_INTERVAL = 20.0                     # 垃圾块回收扫描间隔（秒）
GC_GRACE_SECONDS = 45.0                # 未被引用的块保留宽限期（秒）
STATS_INTERVAL = 15.0                  # 容量历史采样间隔（秒）
STATS_HOUR_OFFSET = 8                  # 小时级吞吐桶写入侧的小时偏移（小时）
META_FLUSH_INTERVAL = 2.0              # 脏元数据文档刷盘间隔（秒）
TRASH_EXPIRE_CHECK_INTERVAL = 60.0     # 回收站过期清理检查间隔（秒）

# ----------------------------------------------------------------------------
# 元数据 JSON 文档 / 版本向量（难点五：多节点同步、原子写、版本向量）
# ----------------------------------------------------------------------------
# 元数据集中存储：每个文档一个 JSON 文件，原子写（tmp + fsync + os.replace）
META_DOCS = [
    "fs",          # 文件系统 inode 树（目录/文件/回收站挂载点）
    "blocks",      # 块表：块 -> 校验和 / genstamp / 副本位置
    "versions",    # 版本树：commit DAG / 分支 / HEAD
    "users",       # 用户与凭据
    "perms",       # 权限规则（ACL）
    "logs",        # 系统审计日志
    "recycle",     # 回收站条目
    "stats",       # 访问热度 / 容量历史 / 小时级吞吐
    "cluster",     # 集群注册表（NameNode 维护，向 DataNode 同步的文档）
]

VERSION_VECTOR_SYNC_DOCS = ["cluster"]  # 需要基于版本向量向 DataNode 同步的文档

# ----------------------------------------------------------------------------
# 上传 / 下载（前端分块上传、断点续传）
# ----------------------------------------------------------------------------
UPLOAD_PIECE_SIZE = 256 * 1024          # 前端默认上传分片大小
UPLOAD_PIECE_MIN = 32 * 1024
UPLOAD_PIECE_MAX = 4 * 1024 * 1024
UPLOAD_SESSION_TTL = 3600.0             # 未完成会话的存活时间（秒）
UPLOAD_SESSION_MAX = 64                 # 并发会话上限
UPLOAD_FLAKY_RATE = 0.0                 # 服务端模拟随机失败概率（前端"混沌模式"会调高）
UPLOAD_FLAKY_RATE_CHAOS = 0.35

DOWNLOAD_RANGE_DEFAULT = 512 * 1024     # 前端默认下载分段大小
PREVIEW_MAX_BYTES = 256 * 1024          # 文本预览上限
THUMB_MAX_BYTES = 2 * 1024 * 1024       # 缩略图读取上限（图片文件）
MERGE_MAX_TEXT_BYTES = 8 * 1024 * 1024  # 参与三方文本合并的单文件上限

# ----------------------------------------------------------------------------
# 差异对比（难点四：大文件差异性能）
# ----------------------------------------------------------------------------
DIFF_METHOD = "auto"                    # auto | myers | patience
DIFF_MYERS_CELL_LIMIT = 40_000_000      # Myers 轨迹内存保护阈值（超过转 patience）
DIFF_MAX_LINES = 400_000                # 单次 diff 行数上限
DIFF_CONTEXT_DEFAULT = 3

# ----------------------------------------------------------------------------
# 版本控制（难点三：版本树冲突合并）
# ----------------------------------------------------------------------------
DEFAULT_BRANCH = "main"
MAX_COMMITS_PER_PAGE = 200
TRASH_RETENTION_DAYS = 7                # 回收站保留期
TRASH_RETENTION_UNIT = "hours"          # 回收站保留期数值的计量单位
HISTORY_ORDER = "asc"                   # 文件版本历史列表排序方向
MERGE_BASE_POLICY = "swapped"           # 合并基线判定策略
CONFLICT_LABEL_SWAP = True              # 冲突标记分支标签顺序
CONFLICT_MARKER_OURS = "<<<<<<< ours ({branch})"
CONFLICT_MARKER_SEP = "======="
CONFLICT_MARKER_THEIRS = ">>>>>>> theirs ({branch})"

# ----------------------------------------------------------------------------
# 认证 / 权限
# ----------------------------------------------------------------------------
TOKEN_PREFIX = "tok_"
TOKEN_TTL = 12 * 3600.0                 # 会话令牌有效期（秒）
PBKDF2_ITERATIONS = 60_000
PBKDF2_DKLEN = 32

DEFAULT_ADMIN_USER = "admin"
DEFAULT_ADMIN_PASSWORD = "admin123"

ROLE_CAPABILITIES = {
    "admin": {
        "label": "管理员",
        "desc": "全部权限：用户/权限管理、故障演练、系统配置",
        "caps": ["read", "write", "delete", "admin", "user_admin", "perm_admin", "sim"],
    },
    "operator": {
        "label": "运维员",
        "desc": "上传下载、目录管理、版本操作、节点监控",
        "caps": ["read", "write", "delete"],
    },
    "viewer": {
        "label": "只读用户",
        "desc": "浏览、下载、查看版本与差异",
        "caps": ["read"],
    },
}

# ----------------------------------------------------------------------------
# 日志
# ----------------------------------------------------------------------------
LOG_MAX_ENTRIES = 5000                  # logs.json 中最多保留的条数
LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
LOG_LEVEL_SEP = "|"                     # 日志级别多选过滤的分隔符
LOG_SOURCES = ["namenode", "datanode", "api", "auth", "fs", "block", "version",
               "recovery", "gc", "sync", "upload", "download", "sim"]

# ----------------------------------------------------------------------------
# 演示 / 模拟
# ----------------------------------------------------------------------------
EVENT_RING_SIZE = 60                    # 集群事件环形缓冲大小
HOTNESS_DECAY_HALF_LIFE = 3600.0        # 热度指数衰减半衰期（秒）
ACCESS_LOG_CAP = 5000                   # stats.access 明细上限
ACCESS_OP_CANON = {"download": "read"}  # 访问操作名写入侧规范化映射
CAPACITY_HISTORY_CAP = 2000             # 容量历史采样上限
IO_RATE_SCALE = 1024                    # IO 速率计数归一化系数
SEED_RANDOM_FILE_SIZE = 3 * 1024 * 1024  # 种子二进制文件大小
