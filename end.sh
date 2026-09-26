#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# ============================================================================
# end.sh — 一键关闭前后端（停止 DFSVS 集群）
# ============================================================================
# 说明：
#   * 前端静态页面由后端 NameNode 进程(:8020)托管，前后端是同一进程，
#     因此关闭后端即同时关闭前端。
#   * 停止顺序：① 按 .server.pid 优雅停止 start.sh 启动的实例；
#               ② 兜底清理遗留的 `python3 run.py` / 独立 DataNode 进程；
#               ③ 校验端口（8020-8024）是否已释放。
#
# 用法：
#   ./end.sh                      # 关闭前后端
#   ./end.sh status               # 查看是否还有残留进程 / 占用端口
# ----------------------------------------------------------------------------
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

HOST="127.0.0.1"
PID_FILE="${SCRIPT_DIR}/.server.pid"
PORTS=(8020 8021 8022 8023 8024)

# --- 输出颜色（非 TTY 时关闭）------------------------------------------
if [ -t 1 ]; then
    C_RESET=$'\033[0m'
    C_GREEN=$'\033[1;32m'
    C_YELLOW=$'\033[1;33m'
    C_RED=$'\033[1;31m'
else
    C_RESET=""; C_GREEN=""; C_YELLOW=""; C_RED=""
fi

info() { printf '%s\n' "$*"; }
ok()   { printf '%s[✓]%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn() { printf '%s[!]%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
err()  { printf '%s[✗]%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }

# --- 优雅停止一个 PID：先 TERM，超时再 KILL ------------------------------
graceful_kill() {
    local pid="$1"
    kill -TERM "$pid" 2>/dev/null || return 1
    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 20 ]; do
        sleep 0.5
        waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill -KILL "$pid" 2>/dev/null
    fi
    return 0
}

# --- 收集本项目的服务进程（PID 文件 + 遗留进程兜底）-----------------------
# 返回去重后的 PID 列表（空格分隔）
collect_pids() {
    local list=""
    local p

    # ① start.sh 记录的实例
    if [ -f "$PID_FILE" ]; then
        p="$(cat "$PID_FILE" 2>/dev/null)"
        [ -n "$p" ] && list="$list $p"
    fi

    # ② 兜底：命令行里带 `run.py` 或 `backend.datanode` 的进程
    for p in $(pgrep -f 'python3 run\.py' 2>/dev/null) \
             $(pgrep -f 'backend\.datanode' 2>/dev/null); do
        list="$list $p"
    done

    # 去重
    printf '%s\n' $list | sort -un | tr '\n' ' '
}

# --- 端口是否仍有监听 ----------------------------------------------------
ports_busy() {
    local port
    for port in "${PORTS[@]}"; do
        if ss -tln 2>/dev/null | grep -qE "[:.]${port}[[:space:]]"; then
            return 0
        fi
    done
    return 1
}

# --- 关闭 ---------------------------------------------------------------
cmd_stop() {
    local pids
    pids="$(collect_pids)"
    local stopped=0

    if [ -z "$(echo "$pids" | tr -d ' ')" ]; then
        info "未发现正在运行的服务进程。"
    else
        for p in $pids; do
            if kill -0 "$p" 2>/dev/null; then
                info "停止进程 PID ${p} …"
                graceful_kill "$p"
                stopped=1
            fi
        done
        rm -f "$PID_FILE"
    fi

    # 校验端口释放情况
    if ports_busy; then
        warn "端口仍存在监听（8020-8024），可能存在未识别的进程。"
        info "  可手动排查：ss -tlnp | grep -E ':(8020|8021|8022|8023|8024)'"
        return 1
    fi

    if [ "$stopped" -eq 0 ]; then
        info "前后端原本就未运行。"
    else
        ok "前后端已全部关闭，端口已释放。"
    fi
    return 0
}

# --- 状态 ---------------------------------------------------------------
cmd_status() {
    local pids
    pids="$(collect_pids)"
    local alive=""
    local p
    for p in $pids; do
        if kill -0 "$p" 2>/dev/null; then
            alive="${alive:+$alive }$p"
        fi
    done

    if [ -n "$alive" ]; then
        warn "仍有服务进程存活：${alive}"
    else
        ok "无存活服务进程。"
    fi

    if ports_busy; then
        warn "端口仍被占用（8020-8024）："
        ss -tlnp 2>/dev/null | grep -E ':(8020|8021|8022|8023|8024)[[:space:]]' || true
    else
        ok "端口 8020-8024 均已释放。"
    fi
}

# --- 主入口 -------------------------------------------------------------
case "${1:-}" in
    status)
        cmd_status
        ;;
    -h|--help|help)
        cat <<'EOF'
用法：
  ./end.sh                      # 关闭前后端（停止 DFSVS 集群）
  ./end.sh status               # 查看是否还有残留进程 / 占用端口

说明：前端由后端 NameNode 进程托管，关闭后端即同时关闭前端。
EOF
        ;;
    ""|stop|end)
        cmd_stop
        ;;
    *)
        err "未知参数：$1（支持：status / --help）"
        exit 2
        ;;
esac
