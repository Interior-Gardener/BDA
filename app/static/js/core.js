/* ShopSense dashboard — core helpers (API, formatting, charts, router) */
'use strict';

const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

/* ------------------------------------------------------------------ API */
const _cache = new Map();
async function api(path, { cache = true } = {}) {
  if (cache && _cache.has(path)) return _cache.get(path);
  const p = fetch(path).then(async (r) => {
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      throw new Error(body.detail || `${r.status} ${r.statusText}`);
    }
    return r.json();
  });
  if (cache) _cache.set(path, p);
  p.catch(() => _cache.delete(path));
  return p;
}
async function post(path, body) {
  const r = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(`${r.status}`);
  return r.json();
}
const qs = (o) => Object.entries(o).filter(([, v]) => v !== undefined && v !== null && v !== '')
  .map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&');

/* ------------------------------------------------------------------ formatting (Indian system) */
const fmt = {
  inr(v, d = 2) {
    if (v === null || v === undefined || isNaN(v)) return '—';
    const a = Math.abs(v), s = v < 0 ? '-' : '';
    if (a >= 1e7) return `${s}₹${(a / 1e7).toFixed(d)} Cr`;
    if (a >= 1e5) return `${s}₹${(a / 1e5).toFixed(d)} L`;
    if (a >= 1e3) return `${s}₹${Math.round(a).toLocaleString('en-IN')}`;
    return `${s}₹${a.toFixed(0)}`;
  },
  inrAxis(v) {
    const a = Math.abs(v);
    if (a >= 1e7) return `₹${+(v / 1e7).toFixed(1)}Cr`;
    if (a >= 1e5) return `₹${+(v / 1e5).toFixed(1)}L`;
    if (a >= 1e3) return `₹${+(v / 1e3).toFixed(0)}K`;
    return `₹${v}`;
  },
  num(v, d = 0) { return v === null || v === undefined || isNaN(v) ? '—' : Number(v).toLocaleString('en-IN', { maximumFractionDigits: d, minimumFractionDigits: d }); },
  compact(v) {
    const a = Math.abs(v);
    if (a >= 1e7) return `${(v / 1e7).toFixed(1)}Cr`;
    if (a >= 1e5) return `${(v / 1e5).toFixed(1)}L`;
    if (a >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
    return `${Math.round(v)}`;
  },
  pct(v, d = 1) { return v === null || v === undefined || isNaN(v) ? '—' : `${Number(v).toFixed(d)}%`; },
  date(s) { if (!s) return '—'; const d = new Date(s.length === 10 ? s + 'T00:00:00' : s); return d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }); },
  month(s) { const [y, m] = s.split('-'); return new Date(+y, +m - 1, 1).toLocaleDateString('en-IN', { month: 'short', year: '2-digit' }); },
  secs(v) { const m = Math.floor(v / 60), s = Math.round(v % 60); return `${m}m ${s}s`; },
};
function delta(v, invert = false) {
  if (v === null || v === undefined) return '';
  const good = invert ? v < 0 : v >= 0;
  return `<span class="delta ${good ? 'up' : 'down'}">${v >= 0 ? '▲' : '▼'} ${Math.abs(v).toFixed(1)}%</span>`;
}

/* ------------------------------------------------------------------ theme & palette */
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const series = () => ['--s1', '--s2', '--s3', '--s4', '--s5', '--s6', '--s7', '--s8'].map(css);
const seq = () => ['--seq-1', '--seq-2', '--seq-3', '--seq-4', '--seq-5', '--seq-6', '--seq-7'].map(css);
const STATE = { categories: [], meta: null };

// colour follows the entity (never its rank)
function catColor(cat) {
  const i = STATE.categories.indexOf(cat);
  return series()[(i < 0 ? 0 : i) % 8];
}
const SEG_SLOT = {
  'Champions': 0, 'Loyal Customers': 2, 'Potential Loyalists': 6, 'New & Promising': 4, 'Needs Attention': 3,
  'At Risk': 1, 'About to Sleep': 3, "Can't Lose Them": 7, 'Hibernating': 7, 'Lost': -1, 'Prospects': -1,
};
function segColor(name) {
  const s = SEG_SLOT[name];
  return s === undefined || s < 0 ? css('--muted') : series()[s];
}
const riskColor = (r) => ({ High: css('--critical'), Medium: css('--warning'), Low: css('--good') }[r] || css('--muted'));
const riskBadge = (r) => r ? `<span class="badge ${r.toLowerCase()}">${r === 'High' ? '●' : r === 'Medium' ? '◐' : '○'} ${r}</span>` : '<span class="sub">—</span>';
const segBadge = (s) => s ? `<span class="badge"><span class="sw" style="background:${segColor(s)}"></span>${esc(s)}</span>` : '—';
const catBadge = (c) => `<span class="badge"><span class="sw" style="background:${catColor(c)}"></span>${esc(c)}</span>`;

/* ------------------------------------------------------------------ charts */
let CHARTS = [];
function chart(el, option) {
  if (typeof el === 'string') el = $(el);
  if (!el) return null;
  const c = echarts.init(el, null, { renderer: 'canvas' });
  c.setOption(Object.assign(baseOption(), option));
  CHARTS.push(c);
  return c;
}
function disposeCharts() { CHARTS.forEach((c) => c.dispose()); CHARTS = []; }
window.addEventListener('resize', () => CHARTS.forEach((c) => c.resize()));

function tooltipStyle() {
  return {
    backgroundColor: css('--surface'), borderColor: css('--border-strong'), borderWidth: 1, padding: [8, 12],
    textStyle: { color: css('--text'), fontSize: 12, fontFamily: 'Inter, system-ui, sans-serif' },
    extraCssText: 'border-radius:10px;box-shadow:0 12px 32px -12px rgba(0,0,0,.45);',
  };
}
function baseOption() {
  return {
    animationDuration: 600,
    textStyle: { fontFamily: 'Inter, system-ui, sans-serif', color: css('--text-2') },
    grid: { left: 8, right: 18, top: 28, bottom: 8, containLabel: true },
    tooltip: { trigger: 'axis', ...tooltipStyle(), axisPointer: { type: 'line', lineStyle: { color: css('--axis') } } },
    legend: { show: false, textStyle: { color: css('--text-2'), fontSize: 12 }, icon: 'roundRect', itemWidth: 10, itemHeight: 10 },
  };
}
function xAxis(data, extra = {}) {
  return {
    type: 'category', data, boundaryGap: extra.boundaryGap ?? true,
    axisLine: { lineStyle: { color: css('--axis') } }, axisTick: { show: false },
    axisLabel: { color: css('--muted'), fontSize: 11, hideOverlap: true, ...(extra.axisLabel || {}) },
    ...extra,
  };
}
function yAxis(formatter, extra = {}) {
  return {
    type: 'value', splitLine: { lineStyle: { color: css('--grid') } },
    axisLabel: { color: css('--muted'), fontSize: 11, formatter }, axisLine: { show: false }, ...extra,
  };
}
function lineSeries(name, data, color, extra = {}) {
  return {
    name, type: 'line', data, smooth: 0.25, showSymbol: false, symbolSize: 8,
    lineStyle: { width: 2, color }, itemStyle: { color },
    emphasis: { focus: 'series' },
    areaStyle: extra.area === false ? undefined : {
      color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{ offset: 0, color: hexA(color, .22) }, { offset: 1, color: hexA(color, 0) }]),
    },
    ...extra,
  };
}
function barSeries(name, data, color, extra = {}) {
  const horizontal = extra.horizontal;
  delete extra.horizontal;
  return {
    name, type: 'bar', data, barMaxWidth: 26,
    itemStyle: { color, borderRadius: horizontal ? [0, 4, 4, 0] : [4, 4, 0, 0] },
    emphasis: { itemStyle: { opacity: .85 } }, ...extra,
  };
}
function hexA(hex, a) {
  if (!hex || hex[0] !== '#') return hex;
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${n >> 16 & 255},${n >> 8 & 255},${n & 255},${a})`;
}
function sparkline(el, values, color) {
  return chart(el, {
    grid: { left: 0, right: 0, top: 4, bottom: 0 }, tooltip: { show: false },
    xAxis: { type: 'category', show: false, data: values.map((_, i) => i), boundaryGap: false },
    yAxis: { type: 'value', show: false, min: 'dataMin' },
    series: [lineSeries('', values, color, { smooth: .4, animation: false })],
  });
}

/* ------------------------------------------------------------------ UI building blocks */
const ICONS = {
  overview: '<path d="M3 13h8V3H3zM13 21h8V11h-8zM3 21h8v-6H3zM13 3v6h8V3z"/>',
  sales: '<path d="M3 3v18h18"/><path d="m7 15 4-4 3 3 5-6"/>',
  products: '<path d="M21 8 12 3 3 8v8l9 5 9-5z"/><path d="m3 8 9 5 9-5M12 13v8"/>',
  behaviour: '<path d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8z"/><path d="M4 21a8 8 0 0 1 16 0"/>',
  segments: '<circle cx="12" cy="12" r="9"/><path d="M12 3v9l6.4 6.4"/>',
  churn: '<path d="M12 9v4M12 17h.01"/><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/>',
  recs: '<path d="m12 2 3.1 6.3 6.9 1-5 4.9 1.2 6.8L12 17.8 5.8 21l1.2-6.8-5-4.9 6.9-1z"/>',
  basket: '<circle cx="9" cy="20" r="1.5"/><circle cx="18" cy="20" r="1.5"/><path d="M2 3h3l2.7 12.4a2 2 0 0 0 2 1.6h7.8a2 2 0 0 0 2-1.6L21 7H6"/>',
  forecast: '<path d="M3 17l6-6 4 4 8-8"/><path d="M14 7h7v7"/>',
  customer: '<rect x="3" y="4" width="18" height="16" rx="2"/><circle cx="9" cy="11" r="2.5"/><path d="M14 10h4M14 14h4M5.5 17a3.5 3.5 0 0 1 7 0"/>',
  pipeline: '<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/>',
  rupee: '<path d="M6 4h12M6 9h12M13 20 6 13h3a4.5 4.5 0 0 0 0-9"/>',
  cart: '<circle cx="9" cy="20" r="1.5"/><circle cx="18" cy="20" r="1.5"/><path d="M2 3h3l2.7 12.4a2 2 0 0 0 2 1.6h7.8a2 2 0 0 0 2-1.6L21 7H6"/>',
  users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.9M16 3.1a4 4 0 0 1 0 7.8"/>',
  receipt: '<path d="M4 2v20l3-2 3 2 3-2 3 2 3-2 1 1V2l-1 1-3-2-3 2-3-2-3 2-3-2z"/><path d="M8 8h8M8 12h8M8 16h5"/>',
  percent: '<path d="M19 5 5 19"/><circle cx="6.5" cy="6.5" r="2.5"/><circle cx="17.5" cy="17.5" r="2.5"/>',
  repeat: '<path d="m17 1 4 4-4 4"/><path d="M3 11V9a4 4 0 0 1 4-4h14M7 23l-4-4 4-4"/><path d="M21 13v2a4 4 0 0 1-4 4H3"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 3"/>',
  bolt: '<path d="M13 2 3 14h9l-1 8 10-12h-9z"/>',
  shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 16v-4M12 8h.01"/>',
  down: '<path d="M12 5v14M5 12l7 7 7-7"/>',
  up: '<path d="M12 19V5M5 12l7-7 7 7"/>',
  download: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  target: '<circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1"/>',
  star: '<path d="m12 2 3.1 6.3 6.9 1-5 4.9 1.2 6.8L12 17.8 5.8 21l1.2-6.8-5-4.9 6.9-1z"/>',
};
const icon = (n) => `<svg viewBox="0 0 24 24">${ICONS[n] || ''}</svg>`;

function kpi({ label, value, foot = '', ic = 'rupee', spark = null, id }) {
  return `<div class="card kpi fade-in">
    <div class="label"><i>${icon(ic)}</i>${label}</div>
    <div class="value">${value}</div>
    <div class="foot">${foot}</div>
    ${spark ? `<div class="spark" id="${id}"></div>` : ''}
  </div>`;
}
function card(title, sub, body, { right = '', cls = '' } = {}) {
  return `<section class="card fade-in ${cls}">
    <div class="card-head"><div><h3>${title}</h3>${sub ? `<p>${sub}</p>` : ''}</div><div class="spacer"></div>${right}</div>
    ${body}
  </section>`;
}
function table(cols, rows, { onRow = null, empty = 'No data' } = {}) {
  if (!rows.length) return `<div class="empty">${empty}</div>`;
  return `<div class="table-wrap"><table><thead><tr>${cols.map((c) => `<th class="${c.num ? 'num' : ''}">${c.label}</th>`).join('')}</tr></thead>
  <tbody>${rows.map((r, i) => `<tr class="${onRow ? 'click' : ''}" data-i="${i}">${cols.map((c) => `<td class="${c.num ? 'num' : ''} ${c.cls || ''}">${c.render ? c.render(r) : esc(r[c.key])}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}
function bindRows(container, rows, fn) {
  $$('tbody tr.click', container).forEach((tr) => tr.addEventListener('click', () => fn(rows[+tr.dataset.i])));
}
function segButtons(id, options, value) {
  return `<div class="seg" id="${id}">${options.map(([v, l]) => `<button data-v="${esc(v)}" class="${v === value ? 'on' : ''}">${l}</button>`).join('')}</div>`;
}
function onSeg(id, fn) {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener('click', (e) => {
    const b = e.target.closest('button');
    if (!b) return;
    $$('button', el).forEach((x) => x.classList.toggle('on', x === b));
    fn(b.dataset.v);
  });
}
function toast(msg) {
  const t = $('#toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2200);
}
function openModal(html) {
  $('#modalBody').innerHTML = html;
  $('#modal').classList.add('open');
}
function closeModal() { $('#modal').classList.remove('open'); }
$('#modal').addEventListener('click', (e) => { if (e.target.id === 'modal' || e.target.closest('[data-close]')) closeModal(); });
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModal(); });
function skeleton(n = 4) {
  return `<div class="grid g4">${Array.from({ length: n }, () => '<div class="skeleton" style="height:120px"></div>').join('')}</div>
          <div class="grid g-2-1"><div class="skeleton" style="height:340px"></div><div class="skeleton" style="height:340px"></div></div>`;
}
function initials(name) { return (name || '?').split(' ').map((w) => w[0]).slice(0, 2).join('').toUpperCase(); }

/* ------------------------------------------------------------------ router */
const ROUTES = {};
const NAV = [];
function route(key, def) { ROUTES[key] = def; }
function navGroup(label, items) { NAV.push({ label, items }); }

function renderNav(active) {
  $('#nav').innerHTML = NAV.map((g) => `<div class="nav-group">${g.label}</div>` + g.items.map((k) => {
    const r = ROUTES[k];
    return `<a href="#/${k}" class="${k === active ? 'active' : ''}">${icon(r.icon)}<span>${r.title}</span>${r.tag ? `<span class="tag">${r.tag}</span>` : ''}</a>`;
  }).join('')).join('');
}

let _renderToken = 0;
async function navigate() {
  const [key, ...args] = (location.hash.replace(/^#\/?/, '') || 'overview').split('/');
  const r = ROUTES[key] || ROUTES.overview;
  const token = ++_renderToken;
  disposeCharts();
  renderNav(ROUTES[key] ? key : 'overview');
  $('#pageTitle').textContent = r.title;
  $('#pageSub').textContent = r.sub || '';
  $('#sidebar').classList.remove('open');
  const view = $('#view');
  view.innerHTML = skeleton();
  window.scrollTo({ top: 0 });
  try {
    await r.render(view, args.map(decodeURIComponent), () => token === _renderToken);
  } catch (err) {
    console.error(err);
    view.innerHTML = `<div class="card empty"><h3>Could not load this page</h3><p>${esc(err.message)}</p>
      <p>Make sure MongoDB is running and the pipeline has been executed:<br><code>python run_pipeline.py</code></p></div>`;
  }
}

async function boot() {
  $('#themeBtn').addEventListener('click', () => {
    const t = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = t;
    try { localStorage.setItem('ss-theme', t); } catch (e) { /* storage unavailable */ }
    navigate();
  });
  $('#menuBtn').addEventListener('click', () => $('#sidebar').classList.toggle('open'));
  initSearch();
  try {
    STATE.meta = await api('/api/meta');
    STATE.categories = STATE.meta.categories || [];
    $('#asof').innerHTML = `Data as of <b>${fmt.date(STATE.meta.as_of)}</b><br>${fmt.date(STATE.meta.min_date)} → ${fmt.date(STATE.meta.max_date)}`;
  } catch (e) {
    $('#asof').textContent = 'No data — run the pipeline';
  }
  window.addEventListener('hashchange', navigate);
  navigate();
}

function initSearch() {
  const input = $('#searchInput'), box = $('#searchResults');
  let timer, items = [], sel = -1;
  const draw = () => {
    box.innerHTML = items.length ? items.map((c, i) => `<a href="#/customer/${c.customer_id}" class="${i === sel ? 'sel' : ''}">
      <div class="avatar" style="width:30px;height:30px;border-radius:9px;font-size:11px">${initials(c.full_name)}</div>
      <div style="flex:1;min-width:0"><div class="strong">${esc(c.full_name)}</div><div class="sub">${c.customer_id} · ${esc(c.city)}</div></div>
      ${segBadge(c.segment)}</a>`).join('') : '<div class="empty" style="padding:16px">No customers found</div>';
    box.classList.add('open');
  };
  input.addEventListener('input', () => {
    clearTimeout(timer);
    const q = input.value.trim();
    if (!q) { box.classList.remove('open'); return; }
    timer = setTimeout(async () => { items = await api(`/api/customers/search?q=${encodeURIComponent(q)}&limit=7`, { cache: false }); sel = -1; draw(); }, 180);
  });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') { sel = Math.min(sel + 1, items.length - 1); draw(); e.preventDefault(); }
    if (e.key === 'ArrowUp') { sel = Math.max(sel - 1, 0); draw(); e.preventDefault(); }
    if (e.key === 'Enter' && items.length) { location.hash = `#/customer/${items[Math.max(sel, 0)].customer_id}`; box.classList.remove('open'); input.value = ''; input.blur(); }
  });
  box.addEventListener('click', () => { box.classList.remove('open'); input.value = ''; });
  document.addEventListener('click', (e) => { if (!e.target.closest('#globalSearch')) box.classList.remove('open'); });
  document.addEventListener('keydown', (e) => { if (e.key === '/' && document.activeElement.tagName !== 'INPUT') { e.preventDefault(); input.focus(); } });
}
