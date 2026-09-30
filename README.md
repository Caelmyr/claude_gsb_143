# DFSVS — 分布式文件存储与版本控制系统

> **本副本为「缺陷注入与修复题」基准版本**：在完整可运行系统之上注入了
> 10 个跨层后端缺陷（5 地狱 + 5 困难），对应 `BUG_TASKS.md` 中的 10 道修复题；
> 地狱级缺陷的候选注入位置见 `CANDIDATE_SITES.md`。
> 注入均为「结果错误但流程正常」型：服务可正常启动、页面可正常渲染、
> 接口不返回 5xx、无崩溃/死锁/未捕获异常；前端代码未被修改，仅作观察窗口。
> 若要获取无缺陷的干净实现，请按 `BUG_TASKS.md` 完成修复或对照题目逐项还原。

纯 Python（标准库，零第三方依赖）+ 原生 HTML/CSS/JS 实现的**教学级分布式文件系统**：
模拟 HDFS 风格的 NameNode / DataNode 集群（节点间全 HTTP 通信），
在其上叠加 Git 风格的版本控制（提交 / 分支 / 三方合并 / 检出），
并提供 12 个页面的管理控制台。

代码规模：**约 12,000 行**（后端 ~8,700 行 Python，前端 ~4,400 行 HTML/CSS/JS）。

---

## 1. 快速开始

```bash
# 依赖：Python 3.10+（仅标准库）；无 pip 安装步骤
python3 run.py                  # 启动 NameNode(:8020) + 3 个 DataNode(:8021-8023)，首次自动注入演示数据
python3 run.py --datanodes 4    # 4 个 DataNode
python3 run.py --reset          # 清空 data/ 后重建（重新注入种子数据）
python3 run.py --no-seed        # 空集群启动
```

浏览器打开 <http://127.0.0.1:8020/>（控制台会自动以 `admin/admin123` 登录）。

| 账号 | 口令 | 角色 |
|---|---|---|
| admin | admin123 | 管理员（含故障演练 / 用户 / 权限管理） |
| alice | alice123 | 运维员 |
| bob | bob12345 | 只读用户 |
| carol | carol123 | 运维员（已停用，演示账号状态位） |

把 DataNode 作为独立进程扩容（HTTP 自动注册进集群）：

```bash
python3 -m backend.datanode --id dn5 --port 8025
```

---

## 2. 前端页面（12 个，要求 10 个 + 仪表盘 + 生命周期）

| 页面 | 文件 | 内容 |
|---|---|---|
| 仪表盘 | `index.html` | KPI / 容量水位 / 最近提交 / 事件流 / 热点 TOP |
| 文件浏览 | `files.html` | 目录树 + 缩略图网格 + 面包屑 + 块/副本详情抽屉 + 文本预览 |
| 上传下载 | `transfer.html` | 分块上传（分片可视化、暂停/续传/混沌模式）、Range 分段下载（断点续传、sha256 校验、副本命中统计） |
| 生命周期 | `lifecycle.html` | 冷数据自动归档 / 到期移入回收站规则、优先级与覆盖关系、即将触发预览与宽限期撤销、冷数据清单与透明取回、逐文件执行留痕 |
| 版本历史 | `versions.html` | 提交时间线（泳道）、分支管理、提交/合并/检出、冲突展示、文件级历史与回滚 |
| 差异对比 | `diff.html` | 版本 diff + 文本 diff 双模式、Myers/Patience/difflib 选择、unified/双栏视图、行内字符级高亮、大文件性能试验台 |
| 节点状态 | `nodes.html` | 节点卡片（心跳/容量/IO/版本向量）、块×节点副本矩阵、恢复队列、杀死/复活/注入损坏演练、实时事件流 |
| 存储统计 | `stats.html` | 容量 donut、副本数分布、块大小直方图、24h 吞吐、容量趋势、类型分布、热度榜（sparkline）、元数据文档表 |
| 用户管理 | `users.html` | 用户 CRUD、角色能力矩阵、活动会话与吊销 |
| 权限设置 | `permissions.html` | 路径前缀 ACL 规则编辑器、默认策略、**判定轨迹测试器** |
| 系统日志 | `logs.html` | 级别/来源/用户/关键字过滤、分页、展开详情、自动刷新、CSV 导出、清空 |
| 回收站 | `recycle.html` | 保留期倒计时、恢复 / 彻底删除 / 清空 |

共享样式 `frontend/css/app.css`（暗色设计系统），共享脚本 `frontend/js/app.js`
（令牌管理、API 封装、导航、吐司/弹窗、SVG 图表库）。

---

## 3. 架构

```
┌──────────────────────────── 浏览器（12 页面）────────────────────────────┐
│  fetch /api/*（JSON）· /api/download（Range）· /api/thumbnail           │
└───────────────────────────────────┬──────────────────────────────────────┘
                                    │ HTTP（Bearer 令牌 + 路径 ACL）
┌───────────────────────────────────▼──────────────────────────────────────┐
│  NameNode :8020 （backend/namenode.py + http_server.py）                  │
│   · 元数据 10 个 JSON 文档：fs/blocks/versions/users/perms/               │
│     logs/recycle/lifecycle/stats/cluster —— 原子写 + 版本向量             │
│   · 块表（genstamp/校验和/副本位置）、放置策略、恢复调度、GC                │
│   · 生命周期：冷数据归档（块降冗余+冷介质迁移）/ 透明取回 / 回收站自动化      │
│   · 上传会话（断点续传暂存）、Range 读路径（副本轮询+故障转移）              │
│   · 版本树 VersionStore（提交/分支/merge/checkout）                        │
└──────┬───────────────────────────────────────────────────▲───────────────┘
       │ PUT /block（流水线复制 X-Forward-To）   心跳/块汇报/事件（JSON）
       │ GET /block（读、Range）                 恢复命令随心跳应答下发
       │                                         cluster 文档按版本向量拉取
┌──────▼───────────────────────────────────────────────────┴───────────────┐
│  DataNode :8021..:8024 （backend/datanode.py，可独立进程运行）              │
│   · blocks/<blk>.dat + node_state.json（原子写）                           │
│   · 心跳线程 / 全量块汇报线程 / 数据巡检(scrub)线程 / 持久化线程             │
│   · 流水线转发、拉取式再复制、静默损坏自发现                                 │
└───────────────────────────────────────────────────────────────────────────┘
```

元数据目录布局（`data/`，全部 JSON，崩溃安全）：

```
data/meta/{fs,blocks,versions,users,perms,logs,recycle,lifecycle,stats,cluster}.json
data/sessions/<upload_id>/piece_000000      # 上传分片暂存
data/datanodes/<node_id>/blocks/<blk>.dat   # 块本体（热介质）
data/datanodes/<node_id>/cold/<blk>.dat     # 归档块本体（冷介质，访问时取回）
data/datanodes/<node_id>/node_state.json    # DN 索引（原子写，含 tier 标记）
data/datanodes/<node_id>/doc_cache/*.json   # DN 同步到的元数据文档
```

---

## 4. 五大难点的实现

### 4.1 分块策略与副本一致性
* **两种分块**（`chunking.py`）：固定 64KiB；CDC 内容定义分块
  （Buzhash 滚动哈希，O(1) 滑窗，min/avg/max 约束）。
  冒烟测试验证：头部插入 16B 后 CDC 11/12 块不变（去重友好），
  固定分块则几乎全部错位。
* **内容去重**：块表维护 `by_checksum` 索引，相同内容块直接复用。
* **流水线复制**：NN PUT → DN1 → DN2 → DN3（`X-Forward-To` 链式头），
  每一跳 sha256 校验；应答嵌套展平后登记副本（含每一跳的 ack）。
* **副本一致性**：每块 `genstamp` 单调递增；块汇报对账时
  未知块/旧 genstamp/校验和不符 → 下发删除命令；
  存活副本 < 期望 → 进入 under-replicated 队列。

### 4.2 节点故障检测与自动恢复
* 心跳 1.5s；>3.6s 标 SUSPECT，>6s 判 DEAD → 其上副本全部失效评估。
* 恢复调度线程每 2s：为缺副本块选「存活好副本(源) → 空闲节点(目标)」，
  命令随该节点下次心跳应答下发；目标 DN 用 HTTP 从源拉取块。
* **坏副本主动替换**：corrupt / stale 副本先下发删除并摘除记录，
  使该节点重新成为复制候选，保证队列可收敛（不会卡死）。
* 节点复活后强制全量块汇报对账；孤儿块（磁盘有、索引无）清理。
* **静默损坏**：DN scrub 线程抽样重算校验和；读路径 NN 侧二次校验 +
  副本故障转移；注入演练见 `POST /api/sim/corrupt`。

### 4.3 版本树冲突合并
* 提交 = 全量快照 + 块引用（块不可变 ⇒ 历史版本天然可读；
  GC 保护集 = 活动 inode ∪ 全部提交快照）。
* 合并：base = LCA；快照级三方状态机（单侧变更采纳 / 双侧同内容采纳 /
  增删冲突保留修改侧）；文本双侧异改 → 行级 diff3（见 4.4），
  冲突写 `<<<<<<< / ======= / >>>>>>>` 标记并提交为待解决状态；
  二进制冲突保留 ours 并记录。合并产生双亲 commit。
* 检出 = 快照物化回活动 inode 树；工作区脏时自动提交保护（不丢数据）；
  快进合并自动识别。

### 4.4 大文件差异对比性能（diff_engine.py）
* **Myers O(ND)**：贪心 + 轨迹回溯，带公共前缀/后缀裁剪；
  轨迹内存保护 D>900 自动降级。
* **Patience**：唯一行锚点 + LIS（O(n log n)）分治，
  锚点间小段回退 Myers / 大段 difflib。
* `auto`：小输入直接 Myers，大输入 Patience。
  实测 20k 行 / 5% 修改 ≈ 85ms（907 个差异段）。
* 三方合并 `merge3`：diff3 语义，闭区间重叠判定（含相邻零宽插入），
  重叠且不同 → 冲突块；相同 → 采纳其一。
* 渲染：unified / 双栏 / 行内字符级高亮；统计与计时返回前端展示。

### 4.5 JSON 元数据多节点同步：原子写 + 版本向量
* **原子写**（`util.atomic_write_*`）：同目录临时文件 → flush → fsync →
  `os.replace` → 目录 fsync；读者永远看到完整旧/新文件。
  块文件、分片暂存、DN 索引同样走原子写。
* **版本向量**：每个文档 `vv = {node_id: 逻辑时钟}`，本地 `touch()` 推进分量；
  DN 心跳携带 `doc_vv`，NN 比较后返回 `pull_docs`；
  DN 拉取信封按 `vv_compare` 判定：
  after→采纳 / before|equal→跳过 / **concurrent→冲突**（cluster 文档以 NN 为权威，
  合并 vv 并留 `doc_synced(conflict-nn-wins)` 审计事件）。
* **锁序纪律**：全局固定 meta → node → health → cmd，
  心跳注册/复活等路径在锁外执行副作用，避免 ABBA 死锁。

---

## 5. REST API 摘要（节选）

```
POST /api/auth/login|logout      GET /api/auth/me|sessions
GET  /api/fs/tree|list|stat      POST /api/fs/mkdir|rename|move|delete
GET  /api/thumbnail|file/preview|file/blocks
POST /api/upload/begin|chunk|complete      GET /api/upload/status|sessions
GET  /api/download/info|download(Range)
GET  /api/version/branches|commits|graph|diff|working_diff|file_at|history|stats
POST /api/version/commit|branch|branch_delete|checkout|merge|restore|diff_text
GET  /api/nodes|nodes/blocks|nodes/matrix|nodes/block_paths
GET  /api/health/queue           GET /api/sim/events
POST /api/sim/kill|revive|corrupt|chaos                （admin）
GET  /api/stats/overview|hotness|timeline
GET|POST /api/users  PUT|DELETE /api/users/<name>      （user_admin）
GET|POST /api/perms  PUT|DELETE /api/perms/<id>        （perm_admin）
POST /api/perms/check
GET  /api/logs|logs/export       POST /api/logs/clear  （admin）
GET  /api/recycle                POST /api/recycle/restore|purge|empty
GET  /api/lifecycle/stats|rules|preview|archived|history|file
POST /api/lifecycle/rules|dismiss|rearm|scan|retrieve
PUT|DELETE /api/lifecycle/rules/<id>            （管理类操作需 admin）
POST /internal/heartbeat|block_report      GET /internal/meta/<doc>   （集群密钥）
```

---

## 6. 目录结构

```
gsb4/
├── run.py                     # 启动入口
├── backend/
│   ├── config.py              # 全部可调参数（块大小/副本/心跳/保留期…）
│   ├── util.py                # 原子写/版本向量/HTTP 客户端/LRU/环形缓冲
│   ├── chunking.py            # 固定 + CDC 分块、清单、重组校验
│   ├── diff_engine.py         # Myers / Patience / merge3 / 渲染器
│   ├── metadata.py            # JSON 文档仓库（原子写 + vv 同步语义）
│   ├── auth.py                # 用户/口令/会话 + 路径 ACL
│   ├── filesystem.py          # inode 树 + 回收站
│   ├── lifecycle.py           # 生命周期规则/优先级覆盖/宽限撤销/归档取回/留痕
│   ├── versioning.py          # 提交/分支/合并/检出/GC 引用集
│   ├── namenode.py            # 块表/放置/心跳/恢复/上传下载/统计/GC
│   ├── datanode.py            # 块存储/心跳/汇报/scrub/流水线/文档同步
│   ├── http_server.py         # 路由 + 静态页 + 鉴权中间件
│   ├── seed.py                # 演示数据（含冲突合并场景）
│   └── main.py                # 集群装配
├── frontend/                  # 12 页面 + css/app.css + js/app.js
└── tests/smoke_test.py        # 97 项端到端断言
```

