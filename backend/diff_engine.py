# -*- coding: utf-8 -*-
"""
diff_engine.py — 差异对比引擎（难点四：大文件差异对比性能）
=============================================================
实现内容：
  1. Myers O(ND) 差分算法（贪心 + 轨迹回溯），带公共前缀/后缀裁剪；
  2. Patience Diff（唯一行锚点 + LIS），用于大文件——先用锚点把问题切成
     小段，再对小段跑 Myers，避免 O(D^2) 轨迹内存爆炸；
  3. difflib 兜底（极端输入下的安全网）；
  4. 三方合并 merge3（版本控制冲突合并的文本级基础，diff3 语义：
     以 base 为参照对齐两侧变更区间，重叠区间产生冲突标记）；
  5. 渲染器：unified / side-by-side / 行内字符级高亮；
  6. 计时与统计（前端展示算法性能）。

复杂度说明：
  * Myers：时间 O((N+M)·D)，轨迹内存 O(D^2)（D 为编辑距离），
    因此设置 _MAX_TRACE_D 保护，超过即降级；
  * 前后缀裁剪把大多数"小改动大文件"场景的 D 压到极小；
  * Patience：锚点划分后每段独立 diff，实践中对代码/日志类文本
    可将有效 D 降低 1~2 个数量级。
"""

import difflib
import time
from bisect import bisect_left, insort

_MAX_TRACE_D = 900            # Myers 轨迹保护：编辑距离超过则抛溢出，走降级
_SMALL_REGION = 1200          # patience 分段后小于该行数直接用 Myers


class MyersOverflow(Exception):
    """Myers 编辑距离超过轨迹保护阈值。"""


# ============================================================================
# 基础：前后缀裁剪
# ============================================================================

def _common_prefix_len(a, b):
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def _common_suffix_len(a, b, start_a=0, start_b=0):
    n = min(len(a) - start_a, len(b) - start_b)
    i = 0
    while i < n and a[len(a) - 1 - i] == b[len(b) - 1 - i]:
        i += 1
    return i


# ============================================================================
# Myers O(ND) 差分
# ============================================================================

def _myers_trace(a, b):
    """
    贪心 Myers：返回 trace（每轮 d 开始时的 V 快照列表）。
    V[k] = 对角线 k 上可达的最远 x。
    """
    n, m = len(a), len(b)
    max_d = n + m
    v = {1: 0}
    trace = []
    for d in range(max_d + 1):
        if d > _MAX_TRACE_D:
            raise MyersOverflow(f"edit distance > {_MAX_TRACE_D}")
        trace.append(dict(v))
        for k in range(-d, d + 1, 2):
            # 选择向下（插入）还是向右（删除）
            if k == -d or (k != d and v.get(k - 1, -1) < v.get(k + 1, -1)):
                x = v.get(k + 1, 0)            # 向下：来自 k+1 对角线
            else:
                x = v.get(k - 1, -1) + 1       # 向右：来自 k-1 对角线
            y = x - k
            # 沿蛇形（相等元素）前进
            while x < n and y < m and a[x] == b[y]:
                x += 1
                y += 1
            v[k] = x
            if x >= n and y >= m:
                return trace
    return trace  # 理论不可达


def _myers_backtrack(trace, a, b):
    """从轨迹回溯出编辑动作序列（正序）：('equal'|'delete'|'insert', ai, bj)。"""
    x, y = len(a), len(b)
    moves = []
    for d in range(len(trace) - 1, -1, -1):
        v = trace[d]
        k = x - y
        if k == -d or (k != d and v.get(k - 1, -1) < v.get(k + 1, -1)):
            prev_k = k + 1
        else:
            prev_k = k - 1
        prev_x = v.get(prev_k, 0)
        prev_y = prev_x - prev_k
        # 回退蛇形段
        while x > prev_x and y > prev_y:
            moves.append(("equal", x - 1, y - 1))
            x -= 1
            y -= 1
        if d > 0:
            if x == prev_x:                    # 向下 => 插入 b[prev_y]
                moves.append(("insert", -1, prev_y))
            else:                              # 向右 => 删除 a[prev_x]
                moves.append(("delete", prev_x, -1))
        x, y = prev_x, prev_y
    moves.reverse()
    return moves


def _moves_to_ops(moves):
    """把逐元素动作合并为 difflib 风格 opcodes: (tag, i1, i2, j1, j2)。"""
    ops = []
    i = j = 0
    for tag, ai, bj in moves:
        if tag == "equal":
            if ops and ops[-1][0] == "equal":
                ops[-1][2] = ai + 1
                ops[-1][4] = bj + 1
            else:
                ops.append(["equal", i, ai + 1, j, bj + 1])
            i += 1
            j += 1
        elif tag == "delete":
            if ops and ops[-1][0] == "delete":
                ops[-1][2] = ai + 1
            else:
                ops.append(["delete", ai, ai + 1, j, j])
            i += 1
        else:  # insert
            if ops and ops[-1][0] == "insert":
                ops[-1][4] = bj + 1
            else:
                ops.append(["insert", i, i, bj, bj + 1])
            j += 1
    return _merge_replace([tuple(o) for o in ops])


def _merge_replace(ops):
    """把相邻的 delete/insert 串合并为 replace，语义与 difflib 一致。"""
    out = []
    idx = 0
    n = len(ops)
    while idx < n:
        tag, i1, i2, j1, j2 = ops[idx]
        if tag == "equal":
            out.append((tag, i1, i2, j1, j2))
            idx += 1
            continue
        # 收集一段连续的非 equal 操作
        run_i1, run_i2 = i1, i2
        run_j1, run_j2 = j1, j2
        idx += 1
        while idx < n and ops[idx][0] != "equal":
            _t, i1b, i2b, j1b, j2b = ops[idx]
            run_i2 = max(run_i2, i2b)
            run_j1 = min(run_j1, j1b) if run_j1 == run_j2 else run_j1
            run_j2 = max(run_j2, j2b)
            idx += 1
        has_del = run_i2 > run_i1
        has_ins = run_j2 > run_j1
        if has_del and has_ins:
            out.append(("replace", run_i1, run_i2, run_j1, run_j2))
        elif has_del:
            out.append(("delete", run_i1, run_i2, run_j1, run_j1))
        else:
            out.append(("insert", run_i1, run_i1, run_j1, run_j2))
    return out


def myers_opcodes(a, b):
    """带前后缀裁剪的 Myers diff，返回 opcodes 列表。"""
    if a == b:
        if not a:
            return []
        return [("equal", 0, len(a), 0, len(b))]
    if not a:
        return [("insert", 0, 0, 0, len(b))]
    if not b:
        return [("delete", 0, len(a), 0, 0)]

    pre = _common_prefix_len(a, b)
    suf = _common_suffix_len(a, b, pre, pre)
    core_a = a[pre:len(a) - suf]
    core_b = b[pre:len(b) - suf]

    ops = []
    if pre:
        ops.append(("equal", 0, pre, 0, pre))
    if core_a or core_b:
        trace = _myers_trace(core_a, core_b)
        moves = _myers_backtrack(trace, core_a, core_b)
        for tag, i1, i2, j1, j2 in _moves_to_ops(moves):
            ops.append((tag, i1 + pre, i2 + pre, j1 + pre, j2 + pre))
    if suf:
        sa, sb = len(a) - suf, len(b) - suf
        ops.append(("equal", sa, sa + suf, sb, sb + suf))
    return _merge_replace(ops)


# ============================================================================
# Patience Diff（大文件策略：唯一行锚点 + 最长递增子序列）
# ============================================================================

def _lis_pairs(pairs):
    """
    对 (i, j) 序列按 j 求最长严格递增子序列（i 天然递增）。
    patience 排序 + 二分，O(n log n)。
    """
    if not pairs:
        return []
    tails = []          # tails[t] = 长度 t+1 的递增子序列的最小结尾 j 对应的 pair 下标
    pred = [-1] * len(pairs)
    for idx, (_i, j) in enumerate(pairs):
        # 在 tails 中找第一个 j 不小于当前 j 的位置（严格递增）
        lo, hi = 0, len(tails)
        while lo < hi:
            mid = (lo + hi) // 2
            if pairs[tails[mid]][1] < j:
                lo = mid + 1
            else:
                hi = mid
        pred[idx] = tails[lo - 1] if lo > 0 else -1
        if lo == len(tails):
            tails.append(idx)
        else:
            tails[lo] = idx
    # 回溯
    out = []
    cur = tails[-1] if tails else -1
    while cur != -1:
        out.append(pairs[cur])
        cur = pred[cur]
    out.reverse()
    return out


def _diff_region_fallback(a, b, off_a, off_b):
    """区域级 diff：小段用 Myers，大段用 difflib 兜底。"""
    if not a and not b:
        return []
    if not a:
        return [("insert", off_a, off_a, off_b, off_b + len(b))]
    if not b:
        return [("delete", off_a, off_a + len(a), off_b, off_b)]
    if len(a) + len(b) <= _SMALL_REGION:
        try:
            return [(t, i1 + off_a, i2 + off_a, j1 + off_b, j2 + off_b)
                    for t, i1, i2, j1, j2 in myers_opcodes(a, b)]
        except MyersOverflow:
            pass
    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
    return [(t, i1 + off_a, i2 + off_a, j1 + off_b, j2 + off_b)
            for t, i1, i2, j1, j2 in sm.get_opcodes() if t != "equal"] or \
           [("equal", off_a, off_a + len(a), off_b, off_b + len(b))]


def patience_opcodes(a, b, _depth=0):
    """
    Patience Diff：
      1. 找出两侧都恰好出现一次的公共行（唯一行）；
      2. 对这些行对求 LIS 得到锚点；
      3. 锚点之间递归（小段 Myers / 大段 difflib）。
    没有唯一公共行时退化为整体 Myers→difflib。
    """
    if a == b:
        return [("equal", 0, len(a), 0, len(b))] if a else []
    if not a:
        return [("insert", 0, 0, 0, len(b))]
    if not b:
        return [("delete", 0, len(a), 0, 0)]

    pre = _common_prefix_len(a, b)
    suf = _common_suffix_len(a, b, pre, pre)

    core_a = a[pre:len(a) - suf]
    core_b = b[pre:len(b) - suf]
    ops = []
    if pre:
        ops.append(("equal", 0, pre, 0, pre))

    if core_a or core_b:
        ops.extend(_patience_core(core_a, core_b, pre, pre, _depth))

    if suf:
        sa, sb = len(a) - suf, len(b) - suf
        ops.append(("equal", sa, sa + suf, sb, sb + suf))
    return _merge_replace(ops)


def _patience_core(core_a, core_b, off_a, off_b, depth):
    if not core_a and not core_b:
        return []
    if not core_a or not core_b or depth > 24:
        return _diff_region_fallback(core_a, core_b, off_a, off_b)

    # 统计行出现次数，找唯一公共行
    count_a = {}
    for i, line in enumerate(core_a):
        count_a[line] = count_a.get(line, 0) + 1
    pos_b = {}
    count_b = {}
    for j, line in enumerate(core_b):
        count_b[line] = count_b.get(line, 0) + 1
        if count_b[line] == 1:
            pos_b[line] = j

    pairs = []
    for i, line in enumerate(core_a):
        if count_a.get(line) == 1 and line in pos_b:
            pairs.append((i, pos_b[line]))
    anchors = _lis_pairs(pairs)
    if not anchors:
        return _diff_region_fallback(core_a, core_b, off_a, off_b)

    ops = []
    pi = pj = 0
    for ai, bj in anchors:
        ops.extend(_diff_region_or_recurse(core_a[pi:ai], core_b[pj:bj],
                                           off_a + pi, off_b + pj, depth))
        ops.append(("equal", off_a + ai, off_a + ai + 1, off_b + bj, off_b + bj + 1))
        pi, pj = ai + 1, bj + 1
    ops.extend(_diff_region_or_recurse(core_a[pi:], core_b[pj:],
                                       off_a + pi, off_b + pj, depth))
    return ops


def _diff_region_or_recurse(seg_a, seg_b, off_a, off_b, depth):
    """锚点之间的段：仍较大时递归 patience，否则 Myers/difflib。"""
    if not seg_a and not seg_b:
        return []
    total = len(seg_a) + len(seg_b)
    if total > _SMALL_REGION and depth < 24:
        sub = _patience_core(seg_a, seg_b, off_a, off_b, depth + 1) \
            if seg_a and seg_b else _diff_region_fallback(seg_a, seg_b, off_a, off_b)
        return sub
    return _diff_region_fallback(seg_a, seg_b, off_a, off_b)


# ============================================================================
# 统一入口
# ============================================================================

def diff_opcodes(a, b, method="auto"):
    """
    计算两个行序列的差异 opcodes。
    method: auto | myers | patience | difflib
    """
    if method == "myers":
        try:
            return myers_opcodes(a, b)
        except MyersOverflow:
            return patience_opcodes(a, b)
    if method == "patience":
        return patience_opcodes(a, b)
    if method == "difflib":
        sm = difflib.SequenceMatcher(None, a, b, autojunk=False)
        return [op for op in sm.get_opcodes()]
    # auto：小输入直接 Myers（轨迹可控），大输入 Patience 锚点分治
    if len(a) + len(b) <= 3000:
        try:
            return myers_opcodes(a, b)
        except MyersOverflow:
            pass
    return patience_opcodes(a, b)


def split_lines(text):
    if text is None:
        return []
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    return text.splitlines()


def diff_text(text_a, text_b, method="auto"):
    """文本级入口：返回 (lines_a, lines_b, opcodes)。"""
    la, lb = split_lines(text_a), split_lines(text_b)
    return la, lb, diff_opcodes(la, lb, method)


def diff_stats(opcodes):
    """统计：新增/删除/修改行数与相似率。"""
    adds = dels = reps = 0
    eq = 0
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "insert":
            adds += j2 - j1
        elif tag == "delete":
            dels += i2 - i1
        elif tag == "replace":
            reps += max(i2 - i1, j2 - j1)
            dels += i2 - i1
            adds += j2 - j1
        else:
            eq += i2 - i1
    total = eq + adds + dels
    ratio = (2.0 * eq / total) if total else 1.0
    return {"adds": adds, "dels": dels, "replaces": reps, "equal": eq,
            "similarity": round(ratio, 4)}


def diff_timed(lines_a, lines_b, method="auto"):
    """带计时的 diff：返回 (opcodes, timing_info)。"""
    t0 = time.perf_counter()
    ops = diff_opcodes(lines_a, lines_b, method)
    t1 = time.perf_counter()
    stats = diff_stats(ops)
    info = {
        "method": method,
        "lines_a": len(lines_a),
        "lines_b": len(lines_b),
        "elapsed_ms": round((t1 - t0) * 1000, 3),
        "hunks": sum(1 for o in ops if o[0] != "equal"),
        "stats": stats,
    }
    return ops, info


# ============================================================================
# 渲染：unified / side-by-side / 行内高亮
# ============================================================================

def render_unified(lines_a, lines_b, opcodes, context=3):
    """
    渲染 unified 视图（结构化，供前端着色）。
    返回 hunks: [{header, rows: [{kind, a_no, b_no, text}]}]
    """
    # 先把 opcodes 展开成逐行标记
    marks = []  # (kind, a_no, b_no, text)
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            for t in range(i2 - i1):
                marks.append(("ctx", i1 + t + 1, j1 + t + 1, lines_a[i1 + t]))
        elif tag == "delete":
            for t in range(i1, i2):
                marks.append(("del", t + 1, None, lines_a[t]))
        elif tag == "insert":
            for t in range(j1, j2):
                marks.append(("add", None, t + 1, lines_b[t]))
        else:  # replace
            for t in range(i1, i2):
                marks.append(("del", t + 1, None, lines_a[t]))
            for t in range(j1, j2):
                marks.append(("add", None, t + 1, lines_b[t]))

    # 按 context 聚合成 hunk
    change_idx = [i for i, m in enumerate(marks) if m[0] != "ctx"]
    if not change_idx:
        return []
    groups = []
    cur = [change_idx[0]]
    for prev, nxt in zip(change_idx, change_idx[1:]):
        if nxt - prev <= context * 2 + 1:
            cur.append(nxt)
        else:
            groups.append(cur)
            cur = [nxt]
    groups.append(cur)

    hunks = []
    for g in groups:
        start = max(0, g[0] - context)
        end = min(len(marks), g[-1] + context + 1)
        rows = [{"kind": m[0], "a_no": m[1], "b_no": m[2], "text": m[3]}
                for m in marks[start:end]]
        a_start = next((m[1] for m in marks[start:end] if m[1] is not None), 0)
        b_start = next((m[2] for m in marks[start:end] if m[2] is not None), 0)
        hunks.append({
            "header": f"@@ -{a_start or 1} +{b_start or 1} @@",
            "rows": rows,
        })
    return hunks


def render_split(lines_a, lines_b, opcodes, context=3, inline=True):
    """
    渲染 side-by-side 视图。
    返回 rows: [{cls, l_no, l_text, r_no, r_text, segs_l, segs_r}]
    cls: eq | gap | del | ins | rep
    replace 区域内做行配对 + 行内字符级高亮（segs_*）。
    """
    rows = []

    def emit_gap(n):
        # 折叠的相等区域
        if n > 0:
            rows.append({"cls": "gap", "text": f"··· 省略 {n} 行相同内容 ···"})

    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            total = i2 - i1
            if context is None:
                for t in range(total):
                    rows.append({"cls": "eq", "l_no": i1 + t + 1,
                                 "l_text": lines_a[i1 + t],
                                 "r_no": j1 + t + 1,
                                 "r_text": lines_b[j1 + t]})
            else:
                show_head = min(context, total) if rows else total
                show_tail = min(context, total)
                if total <= show_head + show_tail:
                    for t in range(total):
                        rows.append({"cls": "eq", "l_no": i1 + t + 1,
                                     "l_text": lines_a[i1 + t],
                                     "r_no": j1 + t + 1,
                                     "r_text": lines_b[j1 + t]})
                else:
                    for t in range(show_head):
                        rows.append({"cls": "eq", "l_no": i1 + t + 1,
                                     "l_text": lines_a[i1 + t],
                                     "r_no": j1 + t + 1,
                                     "r_text": lines_b[j1 + t]})
                    emit_gap(total - show_head - show_tail)
                    for t in range(total - show_tail, total):
                        rows.append({"cls": "eq", "l_no": i1 + t + 1,
                                     "l_text": lines_a[i1 + t],
                                     "r_no": j1 + t + 1,
                                     "r_text": lines_b[j1 + t]})
        elif tag == "delete":
            for t in range(i1, i2):
                rows.append({"cls": "del", "l_no": t + 1, "l_text": lines_a[t],
                             "r_no": None, "r_text": None})
        elif tag == "insert":
            for t in range(j1, j2):
                rows.append({"cls": "ins", "l_no": None, "l_text": None,
                             "r_no": t + 1, "r_text": lines_b[t]})
        else:  # replace —— 配对显示并做行内高亮
            left = list(range(i1, i2))
            right = list(range(j1, j2))
            paired = min(len(left), len(right))
            for t in range(paired):
                li, rj = left[t], right[t]
                segs_l = segs_r = None
                if inline and len(lines_a[li]) + len(lines_b[rj]) < 4000:
                    segs_l, segs_r = inline_char_diff(lines_a[li], lines_b[rj])
                rows.append({"cls": "rep", "l_no": li + 1, "l_text": lines_a[li],
                             "r_no": rj + 1, "r_text": lines_b[rj],
                             "segs_l": segs_l, "segs_r": segs_r})
            for t in range(paired, len(left)):
                li = left[t]
                rows.append({"cls": "del", "l_no": li + 1, "l_text": lines_a[li],
                             "r_no": None, "r_text": None})
            for t in range(paired, len(right)):
                rj = right[t]
                rows.append({"cls": "ins", "l_no": None, "l_text": None,
                             "r_no": rj + 1, "r_text": lines_b[rj]})
    return rows


def inline_char_diff(old, new):
    """
    行内字符级差异：返回两侧的高亮段列表 [(text, changed:bool), ...]。
    仅在行较短时调用（外层已做长度保护）。
    """
    toks_a = _tokenize_line(old)
    toks_b = _tokenize_line(new)
    try:
        ops = myers_opcodes(toks_a, toks_b) if len(toks_a) + len(toks_b) <= 1200 \
            else difflib.SequenceMatcher(None, toks_a, toks_b,
                                         autojunk=False).get_opcodes()
    except MyersOverflow:
        ops = difflib.SequenceMatcher(None, toks_a, toks_b,
                                      autojunk=False).get_opcodes()

    def collect(side_ops, toks, is_left):
        segs = []
        for tag, i1, i2, j1, j2 in side_ops:
            if is_left:
                s1, s2 = i1, i2
            else:
                s1, s2 = j1, j2
            if s2 <= s1:
                continue
            text = "".join(toks[s1:s2])
            changed = tag != "equal"
            if segs and segs[-1][1] == changed:
                segs[-1] = (segs[-1][0] + text, changed)
            else:
                segs.append((text, changed))
        return segs

    return collect(ops, toks_a, True), collect(ops, toks_b, False)


def _tokenize_line(line):
    """把一行切成词/空白 token，提高行内 diff 的可读性。"""
    import re
    return re.findall(r"\s+|\w+|[^\s\w]", line)


# ============================================================================
# 三方合并 merge3（难点三：版本树冲突合并的文本基础）
# ============================================================================

class MergeResult:
    def __init__(self, merged_lines, conflicts, clean):
        self.merged_lines = merged_lines
        self.conflicts = conflicts      # [{start, ours, theirs, base}]
        self.clean = clean              # 是否无冲突

    @property
    def text(self):
        return "\n".join(self.merged_lines) + ("\n" if self.merged_lines else "")


def merge3(base_lines, our_lines, their_lines,
           ours_label="ours", theirs_label="theirs",
           label_swap=False,
           marker_ours="<<<<<<< ours ({branch})",
           marker_sep="=======",
           marker_theirs=">>>>>>> theirs ({branch})"):
    """
    diff3 语义的三方文本合并：
      * 以 base 为参照，分别计算 ours / theirs 的变更区间；
      * 两侧变更区间（闭区间语义，含相邻）不重叠 => 各自安全应用；
      * 重叠 => 若结果相同取其一，否则产生冲突块（写入冲突标记）。
    返回 MergeResult。
    """
    changes_o = [(i1, i2, our_lines[j1:j2])
                 for tag, i1, i2, j1, j2 in diff_opcodes(base_lines, our_lines)
                 if tag != "equal"]
    changes_t = [(i1, i2, their_lines[j1:j2])
                 for tag, i1, i2, j1, j2 in diff_opcodes(base_lines, their_lines)
                 if tag != "equal"]

    merged = []
    conflicts = []
    bi = 0                     # base 游标（已消费到的位置）
    oi = ti = 0

    def emit_stable(upto):
        nonlocal bi
        if upto > bi:
            merged.extend(base_lines[bi:upto])
            bi = upto

    while oi < len(changes_o) or ti < len(changes_t):
        co = changes_o[oi] if oi < len(changes_o) else None
        ct = changes_t[ti] if ti < len(changes_t) else None

        overlapped = (co is not None and ct is not None
                      and co[0] <= ct[1] and ct[0] <= co[1])

        if overlapped:
            # 生长出一个覆盖两侧所有相邻/重叠变更的区域 [start, end)
            start = min(co[0], ct[0])
            end = max(co[1], ct[1])
            region_o, region_t = [], []
            progress = True
            while progress:
                progress = False
                while oi < len(changes_o) and changes_o[oi][0] <= end:
                    s, e, lines = changes_o[oi]
                    region_o.append((s, e, lines))
                    if e > end:
                        end = e
                    oi += 1
                    progress = True
                while ti < len(changes_t) and changes_t[ti][0] <= end:
                    s, e, lines = changes_t[ti]
                    region_t.append((s, e, lines))
                    if e > end:
                        end = e
                    ti += 1
                    progress = True
            ours_region = _render_side(base_lines, start, end, region_o)
            theirs_region = _render_side(base_lines, start, end, region_t)

            emit_stable(start)
            if ours_region == theirs_region:
                merged.extend(ours_region)
            else:
                conflicts.append({
                    "start": len(merged),
                    "base_start": start,
                    "base_end": end,
                    "base": base_lines[start:end],
                    "ours": ours_region,
                    "theirs": theirs_region,
                })
                show_ours, show_theirs = ((theirs_label, ours_label)
                                          if label_swap
                                          else (ours_label, theirs_label))
                merged.append(marker_ours.format(branch=show_ours))
                merged.extend(ours_region)
                merged.append(marker_sep)
                merged.extend(theirs_region)
                merged.append(marker_theirs.format(branch=show_theirs))
            bi = max(bi, end)
        else:
            # 不重叠：先处理位置靠前的一侧（单侧变更直接采纳）
            if co is not None and (ct is None or co[0] <= ct[0]):
                emit_stable(co[0])
                merged.extend(co[2])
                bi = max(bi, co[1])
                oi += 1
            else:
                emit_stable(ct[0])
                merged.extend(ct[2])
                bi = max(bi, ct[1])
                ti += 1

    emit_stable(len(base_lines))
    return MergeResult(merged, conflicts, not conflicts)


def _render_side(base_lines, start, end, changes):
    """把一侧在 [start, end) 区域内的变更应用到 base 片段，得到该侧结果行。"""
    out = []
    pos = start
    for s, e, lines in changes:
        if s > pos:
            out.extend(base_lines[pos:min(s, end)])
        out.extend(lines)
        pos = max(pos, e)
    if pos < end:
        out.extend(base_lines[pos:end])
    return out


def count_conflict_markers(lines):
    return sum(1 for ln in lines if ln.startswith("<<<<<<< ")
               or ln.startswith(">>>>>>> "))
