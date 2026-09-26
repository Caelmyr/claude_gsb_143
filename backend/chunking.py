# -*- coding: utf-8 -*-
"""
chunking.py — 文件分块策略（难点一：分块策略）
================================================
两种策略：
  1. fixed  —— 固定大小分块（默认，HDFS 风格）：
       简单、块大小可预测；缺点是插入少量数据会导致其后所有块偏移。
  2. cdc    —— 内容定义分块（Content-Defined Chunking，Rabin 风格滚动哈希）：
       块边界由内容决定，局部修改只影响边界附近的块，
       配合按校验和去重可以显著减少版本迭代产生的冗余数据。

滚动哈希采用 Buzhash 变体（循环移位 + 异或随机表），窗口滑动 O(1)。
边界判定：hash 的低 CDC_MASK_BITS 位为 0 即切块，并受
[CDC_MIN_SIZE, CDC_MAX_SIZE] 约束，保证块大小分布合理。

另提供：
  * Chunk 描述对象与 Manifest（块清单：序号/大小/校验和）
  * 重组与完整性校验（reassemble + verify）
"""

import os

from . import config
from .util import sha256_bytes

# Buzhash 随机表（固定种子生成，保证多节点边界一致）
import random as _random

_rng = _random.Random(0x5EEDDF5)
_BYTE_TABLE = [_rng.getrandbits(32) for _ in range(256)]


def _rotl32(x, r):
    x &= 0xFFFFFFFF
    return ((x << r) | (x >> (32 - r))) & 0xFFFFFFFF


class Chunk:
    """一个数据块的描述（数据本体可选持有，避免大块常驻内存）。"""

    __slots__ = ("index", "offset", "length", "data", "checksum", "strategy")

    def __init__(self, index, offset, length, data=None, checksum=None,
                 strategy="fixed"):
        self.index = index
        self.offset = offset
        self.length = length
        self.data = data
        self.checksum = checksum or (sha256_bytes(data) if data is not None else None)
        self.strategy = strategy

    def describe(self):
        return {
            "index": self.index,
            "offset": self.offset,
            "length": self.length,
            "checksum": self.checksum,
            "strategy": self.strategy,
        }


# ----------------------------------------------------------------------------
# 策略一：固定大小分块
# ----------------------------------------------------------------------------

def chunk_fixed(data, block_size=None):
    """把 bytes 按固定大小切成 Chunk 列表（携带数据与校验和）。"""
    block_size = block_size or config.BLOCK_SIZE
    chunks = []
    total = len(data)
    idx = 0
    off = 0
    while off < total:
        piece = data[off:off + block_size]
        chunks.append(Chunk(idx, off, len(piece), data=piece))
        off += block_size
        idx += 1
    if not chunks:  # 空文件也产生一个 0 长度块，简化元数据模型
        chunks.append(Chunk(0, 0, 0, data=b""))
    return chunks


def plan_fixed(total_size, block_size=None):
    """不落数据的分块计划（用于前端展示"将产生多少块"）。"""
    block_size = block_size or config.BLOCK_SIZE
    if total_size == 0:
        return [{"index": 0, "offset": 0, "length": 0}]
    plan = []
    idx = 0
    off = 0
    while off < total_size:
        length = min(block_size, total_size - off)
        plan.append({"index": idx, "offset": off, "length": length})
        off += length
        idx += 1
    return plan


# ----------------------------------------------------------------------------
# 策略二：内容定义分块（CDC / 滚动哈希）
# ----------------------------------------------------------------------------

class RollingHash:
    """Buzhash 风格滚动哈希：窗口进出一字节均为 O(1)。"""

    def __init__(self, window=None):
        self.window = window or config.CDC_WINDOW
        self._state = 0
        self._ring = bytearray()
        self._count = 0

    def push(self, byte):
        out_idx = (self._count - self.window) % self.window if self._count >= self.window else None
        out_byte = self._ring[out_idx] if out_idx is not None and out_idx < len(self._ring) else None
        pos = self._count % self.window
        if pos < len(self._ring):
            self._ring[pos] = byte
        else:
            self._ring.append(byte)
        self._count += 1
        self._state = _rotl32(self._state, 1) ^ _BYTE_TABLE[byte]
        if out_byte is not None:
            # 移出窗口的字节：撤销其贡献（rotl 的逆运算是 rotr）
            self._state ^= _rotl32(_BYTE_TABLE[out_byte], self.window % 32)
        return self._state

    def reset(self):
        self._state = 0
        self._ring = bytearray()
        self._count = 0


def chunk_cdc(data, avg=None, min_size=None, max_size=None, mask_bits=None):
    """
    内容定义分块：
      * 从 min_size 之后开始探测边界：hash 低 mask_bits 位全 0 => 切块；
      * 到 max_size 强制切块；
      * 期望平均块大小 ≈ 2^mask_bits。
    """
    avg = avg or config.CDC_AVG_SIZE
    min_size = min_size or config.CDC_MIN_SIZE
    max_size = max_size or config.CDC_MAX_SIZE
    mask_bits = mask_bits or config.CDC_MASK_BITS
    mask = (1 << mask_bits) - 1

    chunks = []
    rh = RollingHash()
    total = len(data)
    start = 0
    idx = 0
    i = 0
    while i < total:
        rh.push(data[i])
        i += 1
        cur_len = i - start
        if cur_len >= min_size:
            hit = (rh._state & mask) == 0
            if hit or cur_len >= max_size:
                piece = data[start:i]
                chunks.append(Chunk(idx, start, len(piece), data=piece,
                                  strategy="cdc"))
                start = i
                idx += 1
                rh.reset()
    if start < total or not chunks:
        piece = data[start:total]
        chunks.append(Chunk(idx, start, len(piece), data=piece, strategy="cdc"))
    return chunks


# ----------------------------------------------------------------------------
# 统一入口 / 清单 / 重组
# ----------------------------------------------------------------------------

def chunk_bytes(data, strategy=None, block_size=None):
    """按策略分块，返回 Chunk 列表。"""
    strategy = strategy or config.CHUNK_STRATEGY
    if strategy == "cdc":
        return chunk_cdc(data)
    return chunk_fixed(data, block_size)


def chunk_stream(path, strategy="fixed", block_size=None):
    """
    流式分块（大文件不整读）：yield Chunk。
    fixed 策略按块大小读取；cdc 策略退化为整读（演示规模可接受）。
    """
    size = os.path.getsize(path)
    block_size = block_size or config.BLOCK_SIZE
    if strategy == "cdc":
        with open(path, "rb") as f:
            for c in chunk_cdc(f.read()):
                yield c
        return
    idx = 0
    off = 0
    with open(path, "rb") as f:
        while off < size or idx == 0:
            piece = f.read(block_size)
            if not piece and idx > 0:
                break
            yield Chunk(idx, off, len(piece), data=piece)
            off += len(piece)
            idx += 1
            if not piece:
                break


def build_manifest(chunks, strategy=None, block_size=None, total_size=None,
                   content_hash=None):
    """
    生成块清单（写入块表/提交快照，用于完整性验证与断点续传对账）。
    """
    strategy = strategy or (chunks[0].strategy if chunks else "fixed")
    return {
        "strategy": strategy,
        "block_size": block_size or config.BLOCK_SIZE,
        "chunk_count": len(chunks),
        "total_size": total_size if total_size is not None else
                      sum(c.length for c in chunks),
        "content_hash": content_hash,
        "chunks": [{"index": c.index, "offset": c.offset, "length": c.length,
                    "checksum": c.checksum} for c in chunks],
    }


def verify_manifest(manifest, chunk_datas):
    """按清单逐块校验（重组前调用），返回 (ok, bad_indexes)。"""
    bad = []
    for meta in manifest.get("chunks", []):
        idx = meta["index"]
        data = chunk_datas.get(idx)
        if data is None or len(data) != meta["length"]:
            bad.append(idx)
            continue
        if sha256_bytes(data) != meta["checksum"]:
            bad.append(idx)
    return (not bad), bad


def reassemble(chunks_by_index, manifest):
    """
    按清单重组字节流：顺序校验 + 拼接。
    抛 ValueError 说明数据不完整/损坏（调用方触发从其它副本重读）。
    """
    ok, bad = verify_manifest(manifest, chunks_by_index)
    if not ok:
        raise ValueError(f"块校验失败: indexes={bad}")
    ordered = [chunks_by_index[m["index"]]
               for m in sorted(manifest["chunks"], key=lambda x: x["index"])]
    data = b"".join(ordered)
    if manifest.get("content_hash") and sha256_bytes(data) != manifest["content_hash"]:
        raise ValueError("整文件校验和不匹配")
    return data


def size_bucket(length):
    """块大小分布统计用的桶。"""
    if length == 0:
        return "0"
    if length < 4096:
        return "<4K"
    if length < 16 * 1024:
        return "4K-16K"
    if length < config.BLOCK_SIZE:
        return "16K-64K"
    if length == config.BLOCK_SIZE:
        return "=64K"
    return ">64K"
