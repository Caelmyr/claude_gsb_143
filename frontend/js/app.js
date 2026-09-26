/* ============================================================================
   app.js — DFSVS 控制台公共脚本
   提供：登录态管理 / API 封装 / 导航渲染 / 吐司 / 弹窗 / 格式化 / SVG 图表
   ========================================================================== */
"use strict";

const DFSVS = (() => {

  // -------------------------------------------------------------- 登录态
  const TOKEN_KEY = "dfsvs_token";
  const USER_KEY = "dfsvs_user";

  function getToken() { try { return localStorage.getItem(TOKEN_KEY) || ""; } catch (e) { return ""; } }
  function setToken(t) { try { t ? localStorage.setItem(TOKEN_KEY, t) : localStorage.removeItem(TOKEN_KEY); } catch (e) {} }
  function getUser() { try { return JSON.parse(localStorage.getItem(USER_KEY) || "null"); } catch (e) { return null; } }
  function setUser(u) { try { u ? localStorage.setItem(USER_KEY, JSON.stringify(u)) : localStorage.removeItem(USER_KEY); } catch (e) {} }

  // -------------------------------------------------------------- API 封装
  async function api(method, path, body, opts = {}) {
    const headers = {};
    let payload = undefined;
    if (body !== undefined && body !== null) {
      headers["Content-Type"] = "application/json";
      payload = JSON.stringify(body);
    }
    const tk = getToken();
    if (tk) headers["Authorization"] = "Bearer " + tk;
    const resp = await fetch(path, { method, headers, body: payload, ...opts.fetch });
    if (resp.status === 401 && !opts._retried && !opts.noAuth) {
      // 尝试用默认账号静默重登（演示系统）
      const ok = await tryAutoLogin();
      if (ok) return api(method, path, body, { ...opts, _retried: true });
      showLoginModal();
      throw new Error("未登录");
    }
    const ct = resp.headers.get("Content-Type") || "";
    let data = null;
    if (ct.includes("application/json")) data = await resp.json();
    else data = await resp.text();
    if (!resp.ok) {
      const msg = (data && data.error) || `HTTP ${resp.status}`;
      const err = new Error(msg);
      err.status = resp.status;
      err.data = data;
      throw err;
    }
    return data;
  }

  async function tryAutoLogin() {
    try {
      const r = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: "admin", password: "admin123" }),
      });
      if (!r.ok) return false;
      const d = await r.json();
      setToken(d.token); setUser(d.user);
      return true;
    } catch (e) { return false; }
  }

  function thumbUrl(path) {
    return `/api/thumbnail?path=${encodeURIComponent(path)}&token=${encodeURIComponent(getToken())}`;
  }

  function downloadUrl(path) {
    return `/api/download?path=${encodeURIComponent(path)}&token=${encodeURIComponent(getToken())}`;
  }

  // -------------------------------------------------------------- 吐司
  function toast(msg, type = "", ms = 3200) {
    let box = document.getElementById("toasts");
    if (!box) {
      box = document.createElement("div");
      box.id = "toasts";
      document.body.appendChild(box);
    }
    const el = document.createElement("div");
    el.className = "toast " + (type || "");
    el.textContent = msg;
    box.appendChild(el);
    setTimeout(() => {
      el.style.transition = "opacity .3s";
      el.style.opacity = "0";
      setTimeout(() => el.remove(), 320);
    }, ms);
  }

  // -------------------------------------------------------------- 弹窗
  function modal({ title, body, okText = "确定", cancelText = "取消", wide = false, onOk, hideCancel = false }) {
    const mask = document.createElement("div");
    mask.className = "modal-mask";
    mask.innerHTML = `
      <div class="modal ${wide ? "wide" : ""}">
        <div class="m-head"><span></span><span class="x" title="关闭">✕</span></div>
        <div class="m-body"></div>
        <div class="m-foot">
          ${hideCancel ? "" : `<button class="btn ghost m-cancel"></button>`}
          <button class="btn primary m-ok"></button>
        </div>
      </div>`;
    mask.querySelector(".m-head span").textContent = title || "";
    const bodyEl = mask.querySelector(".m-body");
    if (typeof body === "string") bodyEl.innerHTML = body;
    else if (body) bodyEl.appendChild(body);
    mask.querySelector(".m-ok").textContent = okText;
    const cancel = mask.querySelector(".m-cancel");
    if (cancel) cancel.textContent = cancelText;
    const close = () => mask.remove();
    mask.querySelector(".x").onclick = close;
    if (cancel) cancel.onclick = close;
    mask.addEventListener("mousedown", (e) => { if (e.target === mask) close(); });
    mask.querySelector(".m-ok").onclick = async () => {
      if (onOk) {
        const keep = await onOk(bodyEl, close);
        if (keep === false) return;
      }
      close();
    };
    document.body.appendChild(mask);
    return { el: mask, close };
  }

  function confirmDlg(msg, onOk, danger = false) {
    modal({
      title: "确认操作",
      body: `<div style="font-size:13.5px">${esc(msg)}</div>`,
      okText: danger ? "确认执行" : "确定",
      onOk: async () => { await onOk(); },
    });
    if (danger) {
      const okBtn = document.querySelector(".modal-mask .m-ok");
      if (okBtn) { okBtn.classList.remove("primary"); okBtn.classList.add("danger"); }
    }
  }

  function promptDlg(title, fields, onOk) {
    // fields: [{name, label, type?, value?, placeholder?, options?}]
    const html = fields.map(f => {
      if (f.type === "select") {
        return `<label class="fl" style="margin-bottom:10px">${esc(f.label)}
          <select data-f="${f.name}">${(f.options || []).map(o =>
            `<option value="${esc(o.value)}" ${o.value === f.value ? "selected" : ""}>${esc(o.label)}</option>`).join("")}
          </select></label>`;
      }
      if (f.type === "textarea") {
        return `<label class="fl" style="margin-bottom:10px">${esc(f.label)}
          <textarea data-f="${f.name}" rows="${f.rows || 3}" placeholder="${esc(f.placeholder || "")}">${esc(f.value || "")}</textarea></label>`;
      }
      return `<label class="fl" style="margin-bottom:10px">${esc(f.label)}
        <input type="${f.type || "text"}" data-f="${f.name}" value="${esc(f.value ?? "")}" placeholder="${esc(f.placeholder || "")}"></label>`;
    }).join("");
    modal({
      title, body: html, onOk: async (bodyEl) => {
        const values = {};
        bodyEl.querySelectorAll("[data-f]").forEach(el => values[el.dataset.f] = el.value);
        await onOk(values);
      }
    });
  }

  function showLoginModal() {
    const html = `
      <label class="fl" style="margin-bottom:10px">用户名
        <input type="text" id="lg-user" value="admin"></label>
      <label class="fl" style="margin-bottom:6px">口令
        <input type="password" id="lg-pass" value="admin123"></label>
      <div class="small muted">演示账号：admin/admin123 · alice/alice123 · bob/bob12345</div>`;
    modal({
      title: "登录 DFSVS 控制台", body: html, okText: "登录", hideCancel: true,
      onOk: async (bodyEl) => {
        const u = bodyEl.querySelector("#lg-user").value;
        const p = bodyEl.querySelector("#lg-pass").value;
        try {
          const d = await api("POST", "/api/auth/login", { username: u, password: p }, { noAuth: true });
          setToken(d.token); setUser(d.user);
          toast(`欢迎回来，${d.user.username}`, "ok");
          setTimeout(() => location.reload(), 400);
        } catch (e) {
          toast("登录失败：" + e.message, "bad");
          return false;
        }
      }
    });
  }

  async function logout() {
    try { await api("POST", "/api/auth/logout"); } catch (e) {}
    setToken(""); setUser(null);
    location.reload();
  }

  // -------------------------------------------------------------- 导航
  const NAV = [
    { group: "总览", items: [
      { href: "index.html", ico: "◈", name: "仪表盘" },
    ]},
    { group: "文件", items: [
      { href: "files.html", ico: "🗀", name: "文件浏览" },
      { href: "transfer.html", ico: "⇅", name: "上传下载" },
      { href: "recycle.html", ico: "🗑", name: "回收站" },
    ]},
    { group: "版本", items: [
      { href: "versions.html", ico: "⑂", name: "版本历史" },
      { href: "diff.html", ico: "±", name: "差异对比" },
    ]},
    { group: "集群", items: [
      { href: "nodes.html", ico: "🖶", name: "节点状态" },
      { href: "stats.html", ico: "📈", name: "存储统计" },
      { href: "logs.html", ico: "🗎", name: "系统日志" },
    ]},
    { group: "管理", items: [
      { href: "users.html", ico: "👤", name: "用户管理" },
      { href: "permissions.html", ico: "🔑", name: "权限设置" },
    ]},
  ];

  function renderNav(activeHref, pageTitle, pageSub) {
    const side = document.getElementById("sidebar");
    if (side) {
      side.innerHTML = `
        <div class="logo">
          <div class="mark">FS</div>
          <div>
            <div class="name">DFSVS</div>
            <div class="sub">分布式存储 · 版本控制</div>
          </div>
        </div>
        ${NAV.map(g => `
          <div class="nav-group">
            <div class="gt">${g.group}</div>
            ${g.items.map(it => `
              <a class="nav-item ${it.href === activeHref ? "active" : ""}" href="${it.href}">
                <span class="ico">${it.ico}</span><span>${it.name}</span>
              </a>`).join("")}
          </div>`).join("")}
        <div class="foot">
          NameNode <span class="mono" id="nn-clock">--:--:--</span><br>
          <span id="nn-cluster"></span>
        </div>`;
    }
    const tt = document.getElementById("page-title");
    if (tt && pageTitle) tt.textContent = pageTitle;
    const ts = document.getElementById("page-sub");
    if (ts && pageSub) ts.textContent = pageSub;
    renderUserChip();
    startClock();
  }

  function startClock() {
    const el = document.getElementById("nn-clock");
    const cl = document.getElementById("nn-cluster");
    const tick = async () => {
      if (el) el.textContent = new Date().toTimeString().slice(0, 8);
      if (cl) {
        try {
          const info = await fetch("/api/system/info").then(r => r.json());
          const vals = Object.values(info.nodes || {});
          const live = vals.filter(v => v === "LIVE").length;
          cl.innerHTML = `节点 <b style="color:${live === vals.length ? "var(--ok)" : "var(--warn)"}">${live}/${vals.length}</b>`;
        } catch (e) { cl.textContent = "集群离线"; }
      }
    };
    tick();
    setInterval(tick, 3000);
  }

  function renderUserChip() {
    const u = getUser();
    const chip = document.getElementById("userchip");
    if (!chip) return;
    chip.innerHTML = u
      ? `<span class="avatar">${esc(u.username[0].toUpperCase())}</span>
         <span>${esc(u.username)}</span><span class="role">${esc(u.role)}</span>`
      : `<span class="avatar">?</span><span>未登录</span>`;
    chip.onclick = () => {
      if (!u) return showLoginModal();
      modal({
        title: "账号", hideCancel: true, okText: "关闭",
        body: `<div class="kv">
          <span class="k">用户</span><span class="v">${esc(u.username)}</span>
          <span class="k">角色</span><span class="v">${esc(u.role)}</span>
          <span class="k">邮箱</span><span class="v">${esc(u.email || "-")}</span>
          <span class="k">最近登录</span><span class="v">${u.last_login ? fmtTs(u.last_login) : "-"}</span>
        </div>
        <hr class="hr">
        <div class="btn-group">
          <button class="btn" id="sw-user">切换用户</button>
          <button class="btn danger" id="lg-out">退出登录</button>
        </div>`,
      });
      document.getElementById("sw-user").onclick = () => { document.querySelector(".modal-mask")?.remove(); showLoginModal(); };
      document.getElementById("lg-out").onclick = logout;
    };
  }

  async function ensureLogin() {
    if (!getToken()) {
      const ok = await tryAutoLogin();
      if (!ok) { showLoginModal(); return false; }
      renderUserChip();
    } else {
      try {
        const me = await api("GET", "/api/auth/me");
        setUser(me.user);
        renderUserChip();
      } catch (e) { /* 401 会触发登录框 */ }
    }
    return true;
  }

  // -------------------------------------------------------------- 格式化
  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, c => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[c]));
  }

  function fmtBytes(n) {
    n = Number(n || 0);
    if (n < 1024) return n + " B";
    const units = ["KiB", "MiB", "GiB", "TiB"];
    let i = -1;
    do { n /= 1024; i++; } while (n >= 1024 && i < units.length - 1);
    return n.toFixed(n >= 100 ? 0 : 1) + " " + units[i];
  }

  function fmtTs(ts) {
    if (!ts) return "-";
    const d = new Date(ts * 1000);
    const p = x => String(x).padStart(2, "0");
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  }

  function fmtAgo(ts) {
    if (!ts) return "-";
    const s = Math.max(0, Date.now() / 1000 - ts);
    if (s < 5) return "刚刚";
    if (s < 60) return `${s | 0} 秒前`;
    if (s < 3600) return `${(s / 60) | 0} 分钟前`;
    if (s < 86400) return `${(s / 3600) | 0} 小时前`;
    return `${(s / 86400) | 0} 天前`;
  }

  function fmtDur(sec) {
    sec = Math.max(0, sec | 0);
    if (sec < 60) return sec + "s";
    if (sec < 3600) return `${(sec / 60) | 0}m ${sec % 60}s`;
    if (sec < 86400) return `${(sec / 3600) | 0}h ${((sec % 3600) / 60) | 0}m`;
    return `${(sec / 86400) | 0}d ${((sec % 86400) / 3600) | 0}h`;
  }

  function fileIcon(item) {
    if (item.type === "dir") return "🗀";
    const m = (item.mime || "") + " " + (item.name || "");
    if (/image\//.test(m)) return "🖼";
    if (/\.(py|js|css|html|sh)/.test(m)) return "🧩";
    if (/\.(json|ya?ml|ini|toml)/.test(m)) return "⚙";
    if (/\.(md|txt|log)/.test(m)) return "🗎";
    if (/\.(csv)/.test(m)) return "📊";
    if (/\.(bin|dat)/.test(m)) return "🧊";
    return "🗄";
  }

  // -------------------------------------------------------------- 轮询
  function poll(fn, ms) {
    let stopped = false;
    const tick = async () => {
      if (stopped) return;
      try { await fn(); } catch (e) { /* 忽略瞬时错误 */ }
      if (!stopped) setTimeout(tick, ms);
    };
    tick();
    return () => { stopped = true; };
  }

  // -------------------------------------------------------------- SHA-256
  async function sha256Hex(buf) {
    const hash = await crypto.subtle.digest("SHA-256", buf);
    return [...new Uint8Array(hash)].map(b => b.toString(16).padStart(2, "0")).join("");
  }

  // -------------------------------------------------------------- SVG 图表
  const PALETTE = ["#4f8cff", "#2fbf71", "#f0a538", "#e5534b", "#9b6cff",
                   "#39bcd0", "#f26ca7", "#a3b529", "#ff9f43", "#6c8eef"];

  function svgDonut(segments, size = 170, thickness = 26, centerText = "", centerSub = "") {
    // segments: [{label, value, color?}]
    const total = segments.reduce((s, x) => s + x.value, 0) || 1;
    const r = (size - thickness) / 2;
    const c = size / 2;
    const circ = 2 * Math.PI * r;
    let acc = 0;
    const arcs = segments.map((s, i) => {
      const frac = s.value / total;
      const dash = `${frac * circ} ${circ}`;
      const off = -acc * circ;
      acc += frac;
      return `<circle cx="${c}" cy="${c}" r="${r}" fill="none"
        stroke="${s.color || PALETTE[i % PALETTE.length]}" stroke-width="${thickness}"
        stroke-dasharray="${dash}" stroke-dashoffset="${off}"
        transform="rotate(-90 ${c} ${c})" stroke-linecap="butt"><title>${esc(s.label)}: ${esc(String(s.valueText ?? s.value))}</title></circle>`;
    }).join("");
    return `<svg class="chart" width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">
      <circle cx="${c}" cy="${c}" r="${r}" fill="none" stroke="#1c2436" stroke-width="${thickness}"/>
      ${arcs}
      <text x="${c}" y="${c - 2}" text-anchor="middle" style="font-size:17px;fill:var(--text);font-weight:700">${esc(centerText)}</text>
      <text x="${c}" y="${c + 16}" text-anchor="middle">${esc(centerSub)}</text>
    </svg>`;
  }

  function svgBars(items, { width = 560, height = 170, color = "#4f8cff", valueFmt = v => v, series = null } = {}) {
    // items: [{label, value}] 或 series: [{name,color,values:[]}]
    const pad = { l: 44, r: 8, t: 10, b: 26 };
    const iw = width - pad.l - pad.r;
    const ih = height - pad.t - pad.b;
    let maxV, n, get;
    if (series) {
      n = series[0].values.length;
      maxV = Math.max(1, ...series.flatMap(s => s.values));
      get = (i) => series.map(s => s.values[i] || 0);
    } else {
      n = items.length;
      maxV = Math.max(1, ...items.map(x => x.value));
      get = (i) => [items[i].value];
    }
    if (!n) return `<div class="empty">暂无数据</div>`;
    const bw = iw / n;
    let bars = "", labels = "";
    for (let i = 0; i < n; i++) {
      const vals = get(i);
      vals.forEach((v, si) => {
        const h = (v / maxV) * ih;
        const col = series ? series[si].color : color;
        const subW = series ? bw * 0.7 / vals.length : bw * 0.62;
        const x = pad.l + i * bw + (series ? bw * 0.15 + si * subW : bw * 0.19);
        bars += `<rect x="${x}" y="${pad.t + ih - h}" width="${subW}" height="${Math.max(h, v > 0 ? 1.5 : 0)}"
                  rx="2" fill="${col}" opacity="0.9"><title>${esc(series ? series[si].name : (items[i].label || ""))}: ${esc(valueFmt(v))}</title></rect>`;
      });
      const label = series ? (items[i] ? items[i].label : "") : (items[i].label || "");
      if (n <= 26 || i % Math.ceil(n / 13) === 0)
        labels += `<text x="${pad.l + i * bw + bw / 2}" y="${height - 8}" text-anchor="middle">${esc(label)}</text>`;
    }
    const grid = [0, 0.5, 1].map(f => {
      const y = pad.t + ih * (1 - f);
      return `<line class="grid-line" x1="${pad.l}" y1="${y}" x2="${width - pad.r}" y2="${y}"/>
              <text x="${pad.l - 6}" y="${y + 3.5}" text-anchor="end">${esc(valueFmt(maxV * f))}</text>`;
    }).join("");
    return `<svg class="chart" width="100%" viewBox="0 0 ${width} ${height}">${grid}${bars}${labels}</svg>`;
  }

  function svgLine(points, { width = 560, height = 170, color = "#4f8cff", valueFmt = v => Math.round(v), fill = true } = {}) {
    // points: [{label, value}]
    const pad = { l: 52, r: 10, t: 10, b: 24 };
    const iw = width - pad.l - pad.r, ih = height - pad.t - pad.b;
    if (points.length < 2) return `<div class="empty">采样点不足</div>`;
    const maxV = Math.max(1, ...points.map(p => p.value));
    const minV = Math.min(...points.map(p => p.value));
    const span = Math.max(1, maxV - minV);
    const xy = points.map((p, i) => [
      pad.l + (i / (points.length - 1)) * iw,
      pad.t + ih - ((p.value - minV) / span) * ih * 0.92 - ih * 0.04,
    ]);
    const path = xy.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
    const area = `${path} L${xy[xy.length - 1][0].toFixed(1)},${pad.t + ih} L${xy[0][0].toFixed(1)},${pad.t + ih} Z`;
    const grid = [0, 0.5, 1].map(f => {
      const y = pad.t + ih * (1 - f);
      const v = minV + span * f;
      return `<line class="grid-line" x1="${pad.l}" y1="${y}" x2="${width - pad.r}" y2="${y}"/>
              <text x="${pad.l - 6}" y="${y + 3.5}" text-anchor="end">${esc(valueFmt(v))}</text>`;
    }).join("");
    let labels = "";
    const step = Math.ceil(points.length / 7);
    points.forEach((p, i) => {
      if (i % step === 0)
        labels += `<text x="${xy[i][0]}" y="${height - 7}" text-anchor="middle">${esc(p.label)}</text>`;
    });
    return `<svg class="chart" width="100%" viewBox="0 0 ${width} ${height}">
      ${grid}
      ${fill ? `<path d="${area}" fill="${color}" opacity="0.12"/>` : ""}
      <path d="${path}" fill="none" stroke="${color}" stroke-width="2"/>
      ${labels}
    </svg>`;
  }

  function sparkline(values, { w = 90, h = 22, color = "#4f8cff" } = {}) {
    const max = Math.max(1, ...values);
    const bw = w / Math.max(1, values.length);
    const bars = values.map((v, i) => {
      const bh = Math.max(v > 0 ? 2 : 0.5, (v / max) * (h - 2));
      return `<rect x="${(i * bw + 0.5).toFixed(1)}" y="${(h - bh).toFixed(1)}" width="${(bw - 1).toFixed(1)}" height="${bh.toFixed(1)}" rx="1" fill="${color}" opacity="${0.45 + 0.55 * (v / max)}"/>`;
    }).join("");
    return `<svg class="spark" width="${w}" height="${h}">${bars}</svg>`;
  }

  function legend(items) {
    return `<div class="legend">${items.map((it, i) =>
      `<span class="li"><span class="sw" style="background:${it.color || PALETTE[i % PALETTE.length]}"></span>${esc(it.label)}${it.valueText ? ` · <b class="mono">${esc(it.valueText)}</b>` : ""}</span>`
    ).join("")}</div>`;
  }

  // -------------------------------------------------------------- 杂项
  function healthBadge(status) {
    if (status === "ok") return `<span class="badge ok">健康</span>`;
    if (status === "under") return `<span class="badge warn">副本不足</span>`;
    if (status === "missing") return `<span class="badge bad">副本丢失</span>`;
    if (status === "corrupt") return `<span class="badge bad">损坏</span>`;
    return `<span class="badge dim">${esc(status || "-")}</span>`;
  }

  function stateBadge(state) {
    if (state === "LIVE") return `<span class="badge ok">LIVE</span>`;
    if (state === "SUSPECT") return `<span class="badge warn">SUSPECT</span>`;
    if (state === "DEAD") return `<span class="badge bad">DEAD</span>`;
    return `<span class="badge dim">${esc(state)}</span>`;
  }

  function qs(name, def = "") {
    return new URLSearchParams(location.search).get(name) || def;
  }

  return {
    api, getToken, setToken, getUser, setUser, thumbUrl, downloadUrl,
    toast, modal, confirmDlg, promptDlg, showLoginModal, logout, ensureLogin,
    renderNav, renderUserChip, esc, fmtBytes, fmtTs, fmtAgo, fmtDur, fileIcon, poll,
    sha256Hex, svgDonut, svgBars, svgLine, sparkline, legend, PALETTE,
    healthBadge, stateBadge, qs,
  };
})();

// 页面通用初始化：渲染导航 + 保证登录
function initPage(activeHref, title, sub) {
  DFSVS.renderNav(activeHref, title, sub);
  return DFSVS.ensureLogin();
}
