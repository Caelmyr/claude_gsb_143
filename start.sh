#!/usr/bin/env bash
# -*- coding: utf-8 -*-
# ============================================================================
# start.sh — DFSVS 一键启动前后端
# ============================================================================
# 说明：
#   * 后端 = NameNode HTTP 服务(:8020) + 多个 DataNode(:8021-8023)，
#     由 `python3 run.py` 在同一进程内拉起整簇。
#   * 前端 = frontend/ 下的静态页面，由 NameNode 直接托管，
#     无需单独的 npm/dev server。
#   * 因此「一键打开前后端」= 启动后端集群 + 打开浏览器访问前端地址。
#
# 用法：
#   ./start.sh                    # 启动整簇，等待就绪后自动打开浏览器
#   ./start.sh --datanodes 4      # 4 个 DataNode
#   ./start.sh --reset            # 启动前清空 data/ 目录
#   ./start.sh --no-browser       # 只启动，不自动打开浏览器
#   ./start.sh stop               # 优雅停止集群
#   ./start.sh restart            # 重启
#   ./start.sh status             # 查看运行状态
#   ./start.sh url                # 只打印前端地址
# ----------------------------------------------------------------------------
set -u

# --- 路径 ---------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

HOST="127.0.0.1"
DEFAULT_PORT="8020"                       # NameNode 端口，前端也由它托管
PID_FILE="${SCRIPT_DIR}/.server.pid"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/server.log"

# --- 输出颜色（非 TTY 时关闭）------------------------------------------
if [ -t 1 ]; then
    C_RESET=$'\033[0m'
    C_GREEN=$'\033[1;32m'
    C_YELLOW=$'\033[1;33m'
    C_CYAN=$'\033[1;36m'
    C_RED=$'\033[1;31m'
else
    C_RESET=""; C_GREEN=""; C_YELLOW=""; C_CYAN=""; C_RED=""
fi

info()  { printf '%s\n' "$*"; }
ok()    { printf '%s[✓]%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
warn()  { printf '%s[!]%s %s\n' "$C_YELLOW" "$C_RESET" "$*"; }
err()   { printf '%s[✗]%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; }

# --- 从命令行参数里解析出 NameNode 端口 --------------------------------
detect_port() {
    local port="$DEFAULT_PORT"
    local args=("$@")
    local i
    for ((i = 0; i < ${#args[@]}; i++)); do
        if [ "${args[$i]}" = "--port" ] && [ $((i + 1)) -lt ${#args[@]} ]; then
            port="${args[$((i + 1))]}"
        fi
    done
    printf '%s' "$port"
}

frontend_url() {
    local port
    port="$(detect_port "$@")"
    printf 'http://%s:%s/index.html' "$HOST" "$port"
}

# --- 端口上是否有 HTTP 服务在响应（就绪 / 是否已被占用的统一判据）--------
is_up() {
    local port="$1"
    curl -s -o /dev/null --max-time 1 "http://${HOST}:${port}/" 2>/dev/null
}

# --- 等待服务就绪；若子进程提前退出则立即返回失败 ------------------------
# 返回码：0=就绪  1=超时  2=子进程提前退出（启动失败）
wait_ready() {
    local port="$1"
    local child_pid="${2:-}"
    local timeout="${3:-60}"
    local waited=0
    while [ "$waited" -lt "$timeout" ]; do
        if is_up "$port"; then
            return 0
        fi
        # 子进程若已退出（如端口被占用、启动异常），立刻失败而不是干等超时
        if [ -n "$child_pid" ] && ! kill -0 "$child_pid" 2>/dev/null; then
            return 2
        fi
        sleep 0.5
        waited=$((waited + 1))
    done
    return 1
}

# --- 读取 PID 文件里仍存活的进程（仅用于「本脚本启动的实例」）-----------
pid_from_file() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid="$(cat "$PID_FILE" 2>/dev/null)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            printf '%s' "$pid"
            return 0
        fi
    fi
    return 1
}

# --- 打开浏览器 ---------------------------------------------------------
open_browser() {
    local url="$1"
    if command -v xdg-open >/dev/null 2>&1; then
        (xdg-open "$url" >/dev/null 2>&1 &)
    elif command -v open >/dev/null 2>&1; then
        (open "$url" >/dev/null 2>&1 &)
    elif command -v sensible-browser >/dev/null 2>&1; then
        (sensible-browser "$url" >/dev/null 2>&1 &)
    else
        warn "未找到可用的浏览器打开命令，请手动访问：$url"
        return 1
    fi
    return 0
}

# --- 启动 ---------------------------------------------------------------
cmd_start() {
    local args=("$@")
    local no_browser=0

    # 解析 --no-browser 标记（不传给 run.py）
    local pass_args=()
    for a in "${args[@]}"; do
        if [ "$a" = "--no-browser" ]; then
            no_browser=1
        else
            pass_args+=("$a")
        fi
    done

    local port
    port="$(detect_port "${pass_args[@]}")"
    local url
    url="$(frontend_url "${pass_args[@]}")"

    # 端口已有人在服务 → 视为已运行，不重复启动（避免端口冲突误报）
    if is_up "$port"; then
        local owner
        owner="$(pid_from_file 2>/dev/null)"
        if [ -n "$owner" ]; then
            ok "服务已在运行（PID ${owner}）。"
        else
            warn "端口 ${port} 已有 HTTP 服务在响应（可能不是本脚本启动），跳过启动。"
        fi
        echo
        printf '  前端地址：%s%s%s\n' "${C_GREEN}" "$url" "${C_RESET}"
        if [ "$no_browser" -eq 0 ]; then
            info "正在打开浏览器…"
            open_browser "$url"
        fi
        return 0
    fi

    mkdir -p "$LOG_DIR"

    info "启动 DFSVS 集群（NameNode :${port} + DataNodes）…"
    nohup python3 run.py "${pass_args[@]}" >"$LOG_FILE" 2>&1 &
    local pid=$!
    printf '%s' "$pid" >"$PID_FILE"

    info "等待服务就绪…"
    local rc
    wait_ready "$port" "$pid" 60
    rc=$?
    if [ "$rc" -eq 0 ]; then
        ok "服务已就绪，PID=${pid}"
    elif [ "$rc" -eq 2 ]; then
        rm -f "$PID_FILE"
        err "启动失败：进程提前退出（常见原因：端口被占用、Python 版本过低）。"
        info "最近日志（${LOG_FILE}）："
        tail -n 15 "$LOG_FILE" 2>/dev/null | sed 's/^/    /'
        return 1
    else
        err "等待超时：服务未能就绪，请查看日志 tail -f ${LOG_FILE}"
    fi

    echo
    printf '%s\n' "${C_CYAN}────────────────────────────────────────────────${C_RESET}"
    printf '  前端地址：%s%s%s\n' "${C_GREEN}" "$url" "${C_RESET}"
    printf '  默认账号：admin / admin123\n'
    printf '  后端 API：%s%s%s%s%s\n' "${C_GREEN}" "http://${HOST}:${port}/api/" "${C_RESET}"
    printf '  停止服务：./start.sh stop\n'
    printf '  查看日志：tail -f %s\n' "$LOG_FILE"
    printf '%s\n' "${C_CYAN}────────────────────────────────────────────────${C_RESET}"
    echo

    if [ "$no_browser" -eq 0 ]; then
        info "正在打开浏览器…"
        open_browser "$url"
    else
        info "已跳过自动打开浏览器（--no-browser）。"
    fi
}

# --- 停止 ---------------------------------------------------------------
cmd_stop() {
    local pid
    pid="$(pid_from_file 2>/dev/null)"
    if [ -z "$pid" ]; then
        # 没有本脚本记录的存活 PID；但端口可能仍被别人占用
        if is_up "$(detect_port)"; then
            warn "未找到本脚本记录的 PID，但端口 $(detect_port) 仍有服务在响应。"
            warn "它可能不是本脚本启动的，请手动处理（或 kill 相关 python3 进程）。"
        else
            info "服务未在运行。"
        fi
        rm -f "$PID_FILE"
        return 0
    fi
    info "正在停止集群（PID ${pid}）…"
    kill -TERM "$pid" 2>/dev/null

    # 最多等待 10s 优雅退出
    local waited=0
    while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 20 ]; do
        sleep 0.5
        waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        warn "优雅退出超时，强制结束…"
        kill -KILL "$pid" 2>/dev/null
    fi
    rm -f "$PID_FILE"
    ok "已停止。"
}

# --- 状态 ---------------------------------------------------------------
cmd_status() {
    local pid
    pid="$(pid_from_file 2>/dev/null)"
    local port
    port="$(detect_port)"
    if [ -n "$pid" ]; then
        ok "运行中，PID=${pid}（本脚本启动）"
        info "前端地址：${C_CYAN}$(frontend_url)${C_RESET}"
    elif is_up "$port"; then
        warn "端口 ${port} 有服务在响应，但非本脚本启动（无 PID 文件）。"
        info "前端地址：${C_CYAN}$(frontend_url)${C_RESET}"
    else
        info "未运行。"
    fi
}

# --- 仅打印地址 ---------------------------------------------------------
cmd_url() {
    printf '%s\n' "$(frontend_url "$@")"
}

# --- 主入口 -------------------------------------------------------------
case "${1:-}" in
    stop)
        cmd_stop
        ;;
    restart)
        cmd_stop
        sleep 1
        shift
        cmd_start "$@"
        ;;
    status)
        cmd_status
        ;;
    url)
        shift
        cmd_url "$@"
        ;;
    ""|start)
        shift
        cmd_start "$@"
        ;;
    -h|--help|help)
        cat <<'EOF'
用法：
  ./start.sh                    # 启动整簇，等待就绪后自动打开浏览器
  ./start.sh --datanodes 4      # 4 个 DataNode
  ./start.sh --reset            # 启动前清空 data/ 目录
  ./start.sh --no-seed          # 不注入演示数据
  ./start.sh --port 9000        # 指定 NameNode 端口（前端地址随端口变化）
  ./start.sh --no-browser       # 只启动，不自动打开浏览器
  ./start.sh stop               # 优雅停止集群
  ./start.sh restart            # 重启
  ./start.sh status             # 查看运行状态
  ./start.sh url [--port N]     # 只打印前端地址

说明：后端 = NameNode HTTP 服务 + DataNodes，前端静态页面由 NameNode 托管，
因此一个进程即同时提供前后端。默认前端地址 http://127.0.0.1:8020/index.html
（默认账号 admin / admin123）。
EOF
        ;;
    *)
        # 兼容直接传 run.py 参数：如 ./start.sh --reset
        cmd_start "$@"
        ;;
esac
