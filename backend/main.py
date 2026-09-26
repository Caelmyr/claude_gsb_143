# -*- coding: utf-8 -*-
"""
main.py — 集群装配入口
========================
在同一进程内拉起：
  * NameNode  HTTP :8020（元数据 + REST API + 前端静态页面）
  * DataNode  HTTP :8021..（块存储 + 心跳/汇报/复制）
节点间全部通过 HTTP 通信，因此也可以把 DataNode 拆出去独立进程运行：

    python3 main.py                       # 一键起整簇（默认 3 个 DataNode）
    python3 main.py --datanodes 4         # 4 个 DataNode
    python3 main.py --reset               # 清空数据目录后重建
    python3 main.py --no-seed             # 不注入演示数据
    python3 -m backend.datanode --id dn5 --port 8025   # 外部扩容一个节点
"""

import argparse
import os
import shutil
import signal
import sys
import time

# 允许 `python3 backend/main.py` 与 `python3 main.py` 两种方式
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from backend import config
    from backend.namenode import NameNode
    from backend.datanode import DataNode
    from backend.seed import seed_cluster, is_seeded
else:
    from . import config
    from .namenode import NameNode
    from .datanode import DataNode
    from .seed import seed_cluster, is_seeded


BANNER = r"""
  ____  _____ ____  _   __  ______
 |  _ \|  ___/ ___|| | | \ \ / / ___|   分布式文件存储与版本控制系统
 | | | | |_  \___ \| | | |\ V /\___ \   NameNode + DataNodes (模拟)
 | |_| |  _|  ___) | |_| | | |  ___) |  元数据: JSON 原子写 + 版本向量
 |____/|_|   |____/ \___/  |_| |____/   版本树: 提交/分支/三方合并
"""


class Cluster:
    """进程内集群：NameNode + N 个 DataNode。"""

    def __init__(self, datanode_count=3, nn_port=None, reset=False,
                 seed=True, verbose=True):
        self.datanode_count = datanode_count
        self.nn_port = nn_port or config.NAMENODE_PORT
        self.reset = reset
        self.seed = seed
        self.verbose = verbose
        self.nn = None
        self.dns = {}

    def _wipe(self):
        if self.reset and os.path.isdir(config.DATA_DIR):
            shutil.rmtree(config.DATA_DIR, ignore_errors=True)
            self._log("已清空数据目录 " + config.DATA_DIR)

    def _log(self, msg):
        if self.verbose:
            print(f"[cluster] {msg}", flush=True)

    def start(self):
        self._wipe()
        os.makedirs(config.DATA_DIR, exist_ok=True)

        self.nn = NameNode(port=self.nn_port)
        self.nn.start(with_http=True)
        self._log(f"NameNode  http://{config.HOST}:{self.nn_port}")

        node_ids = list(config.DATANODE_PORTS.keys())[:self.datanode_count]
        nn_url = f"http://{config.HOST}:{self.nn_port}"
        for nid in node_ids:
            port, rack = config.DATANODE_PORTS[nid]
            data_dir = os.path.join(config.DATANODE_ROOT, nid)
            dn = DataNode(nid, port, rack, nn_url, data_dir,
                          cluster_key=config.CLUSTER_KEY)
            dn.start()
            self.dns[nid] = dn
            self.nn.local_datanodes[nid] = dn
            self._log(f"DataNode  http://{config.HOST}:{port}  id={nid} "
                      f"rack={rack}")

        self.wait_ready()
        if self.seed and not is_seeded(self.nn):
            seed_cluster(self.nn, list(self.dns.values()), self.verbose)
        self.nn.meta.flush()
        self._log("集群就绪 ✔")

    def wait_ready(self, timeout=30):
        """等待全部 DataNode 注册并上报心跳。"""
        t0 = time.time()
        expect = set(self.dns.keys())
        while time.time() - t0 < timeout:
            live = {nid for nid, n in self.nn.nodes.items()
                    if n["state"] == "LIVE"}
            if expect <= live:
                # 再等第一轮块汇报完成
                time.sleep(0.6)
                return True
            time.sleep(0.25)
        self._log("警告：等待节点注册超时 "
                  f"(期望 {sorted(expect)}, 在线 {sorted(live)})")
        return False

    def stop(self):
        self._log("正在停止集群 …")
        for dn in self.dns.values():
            try:
                dn.stop(mark_killed=False)
            except Exception:
                pass
        if self.nn:
            try:
                self.nn.stop()
            except Exception:
                pass
        self._log("已停止")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="DFSVS — 分布式文件存储与版本控制系统（模拟集群）")
    parser.add_argument("--datanodes", type=int, default=3,
                        help="DataNode 数量（1~4，默认 3）")
    parser.add_argument("--port", type=int, default=None,
                        help=f"NameNode 端口（默认 {config.NAMENODE_PORT}）")
    parser.add_argument("--reset", action="store_true",
                        help="启动前清空 data/ 目录")
    parser.add_argument("--no-seed", action="store_true",
                        help="不注入演示数据")
    args = parser.parse_args(argv)

    args.datanodes = max(1, min(args.datanodes,
                                len(config.DATANODE_PORTS)))
    cluster = Cluster(datanode_count=args.datanodes, nn_port=args.port,
                      reset=args.reset, seed=not args.no_seed)

    stopping = {"flag": False}

    def _sig(_signum, _frame):
        if not stopping["flag"]:
            stopping["flag"] = True
            cluster.stop()
            sys.exit(0)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    cluster.start()
    nn_port = cluster.nn_port
    print(BANNER)
    print("  ┌──────────────────────────────────────────────────────────┐")
    print(f"  │ 控制台入口   http://{config.HOST}:{nn_port}/index.html"
          "              │")
    print("  │ 默认账号     admin / admin123                            │")
    print("  │ 演示账号     alice/alice123 (运维)  bob/bob12345 (只读)   │")
    print("  │ 故障演练     节点状态页可 杀死/复活 节点、注入块损坏       │")
    print("  └──────────────────────────────────────────────────────────┘")
    print("  按 Ctrl+C 停止集群\n", flush=True)

    try:
        while not stopping["flag"]:
            time.sleep(1)
    except KeyboardInterrupt:
        _sig(None, None)


if __name__ == "__main__":
    main()
