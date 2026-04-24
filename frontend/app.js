/* Valve · app.js — 단일 페이지 SPA (Vanilla JS + SSE). flow 디자인 톤 일치. */

'use strict';

// ─────────────────────────────────────
// api helpers
// ─────────────────────────────────────
const api = {
  async get(path) {
    const r = await fetch(path);
    if (!r.ok) throw new Error(`${path} ${r.status}`);
    return r.json();
  },
  async post(path, body = {}) {
    const r = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const t = await r.text();
      throw new Error(`${path} ${r.status}: ${t}`);
    }
    return r.json();
  },
};

// ─────────────────────────────────────
// global state
// ─────────────────────────────────────
const STATE = {
  health: null,
  version: null,
  settings: null,
  settingsActive: null,
  products: null,
  productsDraft: null,
  schedule: null,
  plans: {},
  chunks: {},
  partitions: {},
  currentTab: 'monitor',
  es: null,
  logsFilter: { product: '', source: '', status: '', failed_only: false, kind: 'chunk', limit: 300 },
  logsItems: [],
  logsRefresh: null,
};

// ─────────────────────────────────────
// util
// ─────────────────────────────────────
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

function el(tag, attrs = {}, ...children) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') n.className = v;
    else if (k === 'style' && typeof v === 'object') Object.assign(n.style, v);
    else if (k.startsWith('on') && typeof v === 'function') n.addEventListener(k.slice(2).toLowerCase(), v);
    else if (k === 'html') n.innerHTML = v;
    else n.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c == null || c === false) continue;
    n.append(c.nodeType ? c : document.createTextNode(String(c)));
  }
  return n;
}

const fmt = {
  int(n) { return (n || 0).toLocaleString(); },
  pct(n) { return `${(n * 100).toFixed(1)}%`; },
  dur(sec) {
    if (sec == null) return '-';
    if (sec < 60) return `${sec.toFixed(1)}s`;
    const m = Math.floor(sec / 60); const s = Math.round(sec % 60);
    return `${m}:${String(s).padStart(2, '0')}`;
  },
  ago(ts) {
    if (!ts) return '-';
    const diff = Date.now() / 1000 - ts;
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
    return `${Math.floor(diff / 86400)}d ago`;
  },
  date(iso) { return iso ? iso.slice(5) : '-'; },  // MM-DD
  isoToday() { return new Date().toISOString().slice(0, 10); },
};

function recentDates(n) {
  const out = [];
  const base = new Date();
  for (let i = 0; i < n; i++) {
    const d = new Date(base); d.setDate(base.getDate() - i);
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

// ─────────────────────────────────────
// SSE
// ─────────────────────────────────────
function connectSSE() {
  if (STATE.es) STATE.es.close();
  const es = new EventSource('/api/jobs/stream');
  STATE.es = es;
  setSseStatus('connecting');

  es.addEventListener('snapshot', (e) => {
    try {
      const snap = JSON.parse(e.data);
      STATE.plans = snap.plans || {};
      STATE.chunks = snap.chunks || {};
      STATE.partitions = snap.partitions || {};
      setSseStatus('ok');
      renderCurrentTab();
    } catch (err) { console.warn('snapshot parse', err); }
  });

  es.addEventListener('update', (e) => {
    try {
      const evt = JSON.parse(e.data);
      applyEvent(evt);
      if (STATE.currentTab === 'monitor') renderMonitor();
    } catch (err) { console.warn('update parse', err); }
  });

  es.onerror = () => {
    setSseStatus('err');
    es.close();
    setTimeout(connectSSE, 3000);
  };
}

function setSseStatus(s) {
  const dot = $('#sseDot');
  const lbl = $('#sseLabel');
  dot.classList.remove('ok', 'err');
  if (s === 'ok') { dot.classList.add('ok'); lbl.textContent = '실시간'; }
  else if (s === 'err') { dot.classList.add('err'); lbl.textContent = '재연결 중'; }
  else { lbl.textContent = s === 'connecting' ? '연결 중' : s; }
}

function applyEvent(evt) {
  if (evt.kind === 'plan') {
    STATE.plans[evt.plan_id] = evt.plan;
    const p = evt.plan;
    const pkey = `${p.product}/${p.source}/${p.date}`;
    STATE.partitions[pkey] = {
      product: p.product, source: p.source, date: p.date,
      status: 'planned', total_chunks: (p.chunks || []).length, done_chunks: 0,
      last_ts: evt.ts,
    };
  } else if (evt.kind === 'chunk') {
    const prev = STATE.chunks[evt.chunk_id] || {};
    Object.assign(prev, evt.update || {});
    prev.chunk_id = evt.chunk_id;
    STATE.chunks[evt.chunk_id] = prev;
  } else if (evt.kind === 'partition') {
    const prev = STATE.partitions[evt.partition_key] || {};
    Object.assign(prev, evt.update || {});
    prev.last_ts = evt.ts;
    STATE.partitions[evt.partition_key] = prev;
  }
}

// ─────────────────────────────────────
// tab routing
// ─────────────────────────────────────
function route(tab) {
  STATE.currentTab = tab;
  $$('.tab').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
  renderCurrentTab();
}

function renderCurrentTab() {
  const map = {
    monitor: renderMonitor,
    products: renderProducts,
    logs: renderLogs,
    settings: renderSettings,
    browser: renderBrowser,
  };
  (map[STATE.currentTab] || renderMonitor)();
}

// ─────────────────────────────────────
// Monitor tab
// ─────────────────────────────────────
function renderMonitor() {
  const main = $('#main');
  main.innerHTML = '';

  main.append(
    el('div', { class: 'row', style: { marginBottom: '12px', gap: '8px' } },
      el('div', {},
        el('div', { class: 'section-title' }, '모니터'),
        el('div', { class: 'section-desc' }, '파티션 상태 · 현재 실행 chunk · 최근 실패. SSE 로 실시간 반영.'),
      ),
      el('div', { class: 'spacer' }),
      el('button', { class: 'btn primary', onclick: onEnqueueAll }, '▶ 전체 실행 (backfill 범위)'),
      el('button', { class: 'btn', onclick: onProbeInvalidateAll }, '↻ Probe 캐시 전체 무효화'),
    ),
    renderInProgressCard(),
    renderHeatmapCard(),
    renderFailuresCard(),
  );
}

function renderInProgressCard() {
  const running = Object.values(STATE.chunks).filter((c) => c.status === 'in_progress');
  const pending = Object.values(STATE.chunks).filter((c) => c.status === 'pending');

  const body = el('div', {});
  if (!running.length && !pending.length) {
    body.append(el('div', { class: 'empty' }, '대기/실행 중인 chunk 없음'));
  } else {
    running.forEach((c) => body.append(chunkRow(c, 'run')));
    pending.slice(0, 8).forEach((c) => body.append(chunkRow(c, 'pending')));
  }

  return el('div', { class: 'card' },
    el('div', { class: 'card-title' },
      '◉ 진행 중',
      el('span', { class: 'count' }, `${running.length} 실행 · ${pending.length} 대기`),
    ),
    body,
  );
}

function chunkRow(c, tone) {
  const cls = { run: 'run', pending: 'pending' }[tone] || 'pending';
  const started = c.started_at ? Math.round(Date.now() / 1000 - c.started_at) : 0;
  const widthPct = c.expected_rows && c.actual_rows ? Math.min(100, (c.actual_rows / c.expected_rows) * 100) : (tone === 'run' ? 30 : 0);
  return el('div', { class: 'chunk-row' },
    el('div', { class: 'chunk-id' }, c.chunk_id || '-'),
    el('span', { class: `pill ${cls}` }, c.status || tone),
    el('span', { class: 'mono' }, tone === 'run' ? `+${fmt.dur(started)}` : ''),
    el('div', { class: 'progress' }, el('div', { class: 'bar', style: { width: `${widthPct}%` } })),
    el('div', { class: 'mono', style: { color: 'var(--text-muted)' } }, `exp ${fmt.int(c.expected_rows)}`),
  );
}

function renderHeatmapCard() {
  const dates = recentDates(14).reverse();   // old → new
  const today = fmt.isoToday();
  const products = (STATE.products?.products || []);

  const thead = el('tr', {},
    el('th', { class: 'row-h' }, 'Product / Source'),
    ...dates.map((d) => el('th', { title: d }, fmt.date(d))),
  );

  const tbody = el('tbody', {});
  let totalConfigured = 0;

  products.forEach((p, pi) => {
    const configured = new Map(
      (p.sources || []).map((s) => [(s.name || '').toUpperCase(), s]),
    );
    totalConfigured += configured.size;

    // 제품 헤더 행 — 제품명 + 상태 배지 + backfill + configured/total 카운트
    const headMeta = [];
    if (p.enabled === false) headMeta.push(el('span', { class: 'pill pending' }, 'disabled'));
    if (p.priority != null) headMeta.push(el('span', { class: 'hint' }, `p${p.priority}`));
    if (p.backfill_days_override) headMeta.push(el('span', { class: 'hint backfill-pill' }, `backfill ${p.backfill_days_override}d`));

    tbody.append(el('tr', { class: 'prod-head-row' + (pi > 0 ? ' divider' : '') },
      el('td', { class: 'prod-head-cell', colspan: String(dates.length + 1) },
        el('div', { class: 'prod-head-inner' },
          el('span', { class: 'prod-head-name' }, p.product),
          ...headMeta,
          el('span', { class: 'spacer' }),
          el('span', { class: 'prod-head-count' },
            `${configured.size}/${CANONICAL_SOURCES.length} 소스 추출 중`),
        ),
      ),
    ));

    // 각 canonical 소스에 대해 고정 행
    CANONICAL_SOURCES.forEach((src) => {
      const cfg = configured.get(src);
      const on = !!cfg;
      const tr = el('tr', { class: 'src-row' + (on ? '' : ' src-off') },
        el('td', { class: 'row-label src-label' },
          el('span', { class: 'src-bullet' }, on ? '●' : '○'),
          src,
          on ? null : el('span', { class: 'hint', style: { marginLeft: '6px' } }, '(미추출)'),
        ),
      );
      dates.forEach((d) => {
        if (!on) {
          tr.append(el('td', { class: 'hm-cell s-off', title: `${p.product} · ${src} · ${d}\n(이 제품은 ${src} 를 뽑지 않음)` }));
          return;
        }
        const pkey = `${p.product}/${src}/${d}`;
        const part = STATE.partitions[pkey];
        const st = part?.status || 'idle';
        const cls = ({
          success: 's-success', running: 's-running', partial_failed: 's-partial',
          completeness_failed: 's-partial', upload_failed: 's-failed',
          failed: 's-failed', planned: 's-planned',
        })[st] || 's-idle';
        tr.append(el('td', {
          class: `hm-cell ${cls}`,
          title: `${p.product} · ${src} · ${d}\n${part ? JSON.stringify(part, null, 2) : '(미실행)'}`,
          onclick: () => onCellClick(p.product, src, d, part),
        }, d === today ? '●' : (st === 'idle' ? '' : '·')));
      });
      tbody.append(tr);
    });
  });

  const legend = el('div', { class: 'row', style: { marginTop: '12px', gap: '14px', fontSize: '11px', color: 'var(--text-muted)', flexWrap: 'wrap' } },
    legendItem('s-success', 'Success'),
    legendItem('s-running', 'Running'),
    legendItem('s-partial', 'Partial / Tolerance'),
    legendItem('s-failed',  'Failed / Upload err'),
    legendItem('s-planned', 'Planned'),
    legendItem('s-idle',    'Idle'),
    legendItem('s-off',     '미추출(해당 제품 설정 안됨)'),
  );

  return el('div', { class: 'card' },
    el('div', { class: 'card-title' },
      '📊 Partition heatmap',
      el('span', { class: 'count' },
        `${products.length} 제품 × ${CANONICAL_SOURCES.length} 소스 = ${totalConfigured} 추출 중 · ${dates.length}일`),
    ),
    products.length
      ? el('table', { class: 'heatmap' }, el('thead', {}, thead), tbody)
      : el('div', { class: 'empty' }, 'products.yaml 에 제품 없음'),
    legend,
  );
}

function legendItem(cls, text) {
  return el('span', { class: 'row', style: { gap: '4px' } },
    el('span', { class: `hm-cell ${cls}`, style: { width: '14px', height: '14px', display: 'inline-block', borderRadius: '3px' } }),
    text,
  );
}

function renderFailuresCard() {
  const fails = Object.values(STATE.chunks)
    .filter((c) => c.status === 'failed' || c.status === 'timeout_reshard')
    .sort((a, b) => (b.ended_at || 0) - (a.ended_at || 0))
    .slice(0, 8);

  const body = el('div', {});
  if (!fails.length) {
    body.append(el('div', { class: 'empty' }, '최근 실패 없음 ✓'));
  } else {
    fails.forEach((c) => {
      body.append(el('div', { class: 'chunk-row' },
        el('div', { class: 'chunk-id' }, c.chunk_id || '-'),
        el('span', { class: 'pill err' }, c.error_type || c.status),
        el('span', { class: 'mono', style: { color: 'var(--text-muted)' } }, fmt.ago(c.ended_at)),
        el('div', { class: 'mono', style: { color: '#991b1b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, title: c.error }, c.error || ''),
        el('button', { class: 'btn small', onclick: () => onRetry(c) }, '↻ retry'),
      ));
    });
  }

  return el('div', { class: 'card' },
    el('div', { class: 'card-title' }, '⚠ 최근 실패',
      el('span', { class: 'count' }, `${fails.length} 건`),
    ),
    body,
  );
}

async function onCellClick(product, source, date, existing) {
  const confirmMsg = existing
    ? `${product} · ${source} · ${date}\n재실행할까요? (현 파티션 overwrite)`
    : `${product} · ${source} · ${date}\n실행할까요?`;
  if (!confirm(confirmMsg)) return;
  try {
    await api.post('/api/jobs/enqueue', { product, source, date });
  } catch (e) { alert(`enqueue 실패: ${e.message}`); }
}

async function onRetry(c) {
  // chunk_id 에서 product/source/date 복원
  const parts = c.chunk_id.split('-');
  if (parts.length < 5) return alert('chunk_id 파싱 실패');
  const idx = parts.length - 4;
  const product = parts.slice(0, idx - 1).join('-') || parts[0];
  const source = parts[idx - 1];
  const date = `${parts[idx]}-${parts[idx + 1]}-${parts[idx + 2]}`;
  if (!confirm(`partition 재실행?\n${product} / ${source} / ${date}`)) return;
  try {
    await api.post('/api/jobs/retry-partition', { product, source, date });
  } catch (e) { alert(e.message); }
}

async function onEnqueueAll() {
  if (!confirm('backfill 창(3일) 전체 제품·소스 일괄 실행할까요?')) return;
  try {
    const r = await api.post('/api/jobs/enqueue-all', {});
    alert(`launched: ${r.launched}건 (backfill_days=${r.backfill_days})`);
  } catch (e) { alert(e.message); }
}

async function onProbeInvalidateAll() {
  if (!confirm('Probe 캐시 전체 무효화? (다음 실행 시 probe 다시 수행)')) return;
  try { await api.post('/api/jobs/probe-invalidate', {}); alert('cleared'); }
  catch (e) { alert(e.message); }
}

// ─────────────────────────────────────
// Products tab
// ─────────────────────────────────────
// 소스 타입 동적 레지스트리 (server: /api/schedule/source-types).
// 초기값은 built-in fallback, 실제 값은 init() 의 loadSourceTypes() 가 채움.
let SOURCE_TYPES = [
  { name: 'FAB',    columns: [], default_shard: [],                     accent: '#64748b', hint: '' },
  { name: 'INLINE', columns: [], default_shard: ['root_lot_id'],         accent: '#10b981', hint: 'INLINE 도 하루치가 크다 — `root_lot_id` probe 로 분포 스캔 후 shard 로 쪼개는 게 기본.' },
  { name: 'ET',     columns: [], default_shard: ['root_lot_id', 'item_id'], accent: '#f59e0b', hint: 'ET 는 `item_id` 필터 + `root_lot_id` 또는 `item_id` shard.' },
  { name: 'QTIME',  columns: [], default_shard: [],                     accent: '#06b6d4', hint: 'QTIME 은 `from_step_id`·`to_step_id` 쌍 필터.' },
  { name: 'EDS',    columns: [], default_shard: [],                     accent: '#8b5cf6', hint: 'EDS die-level — `test_item`·`pattern_id` 기준 축소.' },
  { name: 'VM',     columns: [], default_shard: [],                     accent: '#3b82f6', hint: 'VM — `residual` 지표.' },
];
let SOURCE_NAMES = SOURCE_TYPES.map(s => s.name);
let CANONICAL_SOURCES = [...SOURCE_NAMES];

async function loadSourceTypes() {
  try {
    const r = await api.get('/api/schedule/source-types');
    if (Array.isArray(r.source_types) && r.source_types.length) {
      SOURCE_TYPES = r.source_types.map(s => ({ ...s, name: (s.name || '').toUpperCase() }));
      SOURCE_NAMES = SOURCE_TYPES.map(s => s.name);
      CANONICAL_SOURCES = [...SOURCE_NAMES];
    }
  } catch (_) { /* keep fallback */ }
}

function getSourceType(name) {
  const key = (name || '').toUpperCase();
  return SOURCE_TYPES.find(s => s.name === key);
}

// `inline code` 표기 → children [text, <code>x</code>, text]
function renderInlineHintText(text) {
  if (!text) return [];
  const parts = [];
  const re = /`([^`]+)`/g;
  let last = 0, m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) parts.push(text.slice(last, m.index));
    parts.push(el('code', {}, m[1]));
    last = m.index + m[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts;
}

function renderHint(st) {
  if (!st || !st.hint) return null;
  const accent = st.accent || '#64748b';
  return el('div', { class: 'source-hint', style: { borderColor: accent, background: accent + '14' } },
    '💡 ', ...renderInlineHintText(st.hint));
}
const PARAM_OPS = ['eq', 'ne', 'in', 'lt', 'le', 'gt', 'ge', 'like', 'notLike'];
const _columnCache = {};

async function getSourceColumns(product, source) {
  const key = `${product}::${(source || '').toUpperCase()}`;
  if (_columnCache[key]) return _columnCache[key];
  try {
    const r = await api.get(`/api/schedule/columns?product=${encodeURIComponent(product || '')}&source=${encodeURIComponent(source || '')}`);
    _columnCache[key] = r.columns || [];
  } catch (_) { _columnCache[key] = []; }
  return _columnCache[key];
}

async function renderProducts() {
  const main = $('#main');
  main.innerHTML = '';
  if (!STATE.products) {
    try { STATE.products = await api.get('/api/schedule/products'); } catch (e) { return renderError(e); }
  }
  if (!STATE.productsDraft) STATE.productsDraft = structuredClone(STATE.products);
  const draft = STATE.productsDraft;

  const rerender = () => renderProducts();

  const saveAll = async () => {
    try {
      const r = await api.post('/api/schedule/products', draft);
      STATE.products = structuredClone(draft);
      for (const k of Object.keys(_columnCache)) delete _columnCache[k];
      alert(`저장됨 — ${r.count} 제품`);
    } catch (e) { alert(`저장 실패: ${e.message}`); }
  };
  const resetAll = () => {
    if (!confirm('편집 중 변경을 버리고 마지막 저장 상태로 되돌립니까?')) return;
    STATE.productsDraft = structuredClone(STATE.products);
    rerender();
  };
  const addProduct = () => {
    draft.products = draft.products || [];
    const usedNames = new Set(draft.products.map(p => p.product));
    let letter = 'A';
    while (usedNames.has(`PROD${letter}`)) letter = String.fromCharCode(letter.charCodeAt(0) + 1);
    const newName = `PROD${letter}`;
    draft.products.push({
      product: newName,
      enabled: true,
      priority: 50,
      sources: [{ name: 'FAB', table: 'RAW_FAB_DATA', shard_hierarchy: [], target_chunk_rows: 500000 }],
      params_template: { product_code: { op: 'eq', value: newName } },
      custom_col: ['lot_id', 'wafer_id', 'time', 'value'],
    });
    STATE.productsSelected = newName;
    rerender();
  };

  // 선택 상태 초기화
  const prodList = draft.products || [];
  if (prodList.length && !STATE.productsSelected) STATE.productsSelected = prodList[0].product;
  const selected = prodList.find(p => p.product === STATE.productsSelected) || prodList[0];
  if (selected && !STATE.productsSourceSelected) {
    STATE.productsSourceSelected = (selected.sources || [])[0]?.name || '';
  }

  const headerBar = el('div', { class: 'row' },
    el('div', {},
      el('div', { class: 'section-title' }, '제품 관리'),
      el('div', { class: 'section-desc' }, '제품 카드 클릭으로 선택 · 소스 탭으로 상세 drilldown · 저장 시 products.yaml 에 반영.'),
    ),
    el('div', { class: 'spacer' }),
    el('button', { class: 'btn ghost', onclick: resetAll }, '↺ 되돌리기'),
    el('button', { class: 'btn', onclick: addProduct }, '+ 제품 추가'),
    el('button', { class: 'btn primary', onclick: saveAll }, '💾 저장'),
  );

  main.append(headerBar);

  if (!prodList.length) {
    main.append(el('div', { class: 'alert info' }, '등록된 제품 없음. 우측 상단 "+ 제품 추가" 버튼으로 시작.'));
    return;
  }

  // 좌측 제품 목록 + 우측 상세
  const split = el('div', { class: 'products-split' },
    el('div', { class: 'products-list' },
      el('div', { class: 'list-title' }, `제품 (${prodList.length})`),
      ...prodList.map(p => productListItem(p, selected, rerender)),
    ),
    el('div', { class: 'product-detail' },
      selected ? productDetailView(selected, draft, rerender) : el('div', { class: 'empty' }, '제품 선택'),
    ),
  );
  main.append(split);
}

function productListItem(p, selected, rerender) {
  const active = selected && selected.product === p.product;
  const srcCount = (p.sources || []).length;
  const disabled = p.enabled === false;
  return el('div', {
    class: 'prod-item' + (active ? ' active' : '') + (disabled ? ' disabled' : ''),
    onclick: () => {
      STATE.productsSelected = p.product;
      STATE.productsSourceSelected = (p.sources || [])[0]?.name || '';
      rerender();
    },
  },
    el('div', { class: 'prod-item-top' },
      el('span', { class: 'prod-item-name' }, p.product),
      disabled
        ? el('span', { class: 'pill pending' }, 'off')
        : el('span', { class: 'pill brand' }, 'on'),
    ),
    el('div', { class: 'prod-item-sub' },
      `p${p.priority ?? 50} · 소스 ${srcCount}`,
    ),
    el('div', { class: 'prod-item-sources' },
      (p.sources || []).map(s => el('span', { class: 'prod-item-srcchip' }, s.name)).slice(0, 6),
    ),
  );
}

function productDetailView(p, draft, rerender) {
  const globalBf = STATE.settings?.schedule?.backfill_days ?? 3;
  const deleteProduct = () => {
    if (!confirm(`${p.product} 제품 삭제?`)) return;
    const idx = draft.products.findIndex(x => x.product === p.product);
    if (idx >= 0) draft.products.splice(idx, 1);
    STATE.productsSelected = (draft.products[0] || {}).product;
    rerender();
  };
  const addSource = () => {
    p.sources = p.sources || [];
    const existing = new Set(p.sources.map(s => (s.name || '').toUpperCase()));
    const next = SOURCE_NAMES.find(n => !existing.has(n)) || 'NEW';
    p.sources.push({ name: next, table: `RAW_${next}_DATA`, shard_hierarchy: [], target_chunk_rows: 500000 });
    STATE.productsSourceSelected = next;
    rerender();
  };

  const sources = p.sources || [];
  const selectedSrc = sources.find(s => s.name === STATE.productsSourceSelected) || sources[0];

  return el('div', {},
    // 헤더 행
    el('div', { class: 'product-head' },
      el('input', { type: 'text', class: 'prod-name', value: p.product || '',
        onchange: e => {
          const old = p.product; p.product = e.target.value;
          if (STATE.productsSelected === old) STATE.productsSelected = p.product;
        } }),
      el('label', { class: 'check' },
        el('input', { type: 'checkbox', ...(p.enabled !== false ? { checked: 'checked' } : {}),
          onchange: e => { p.enabled = e.target.checked; rerender(); } }),
        'enabled',
      ),
      el('span', { class: 'hint' }, 'priority'),
      el('input', { type: 'number', class: 'inline-input narrow', value: p.priority ?? 50,
        onchange: e => { p.priority = Number(e.target.value); } }),
      el('span', { class: 'hint' }, 'backfill'),
      el('input', { type: 'number', class: 'inline-input narrow', min: '0', max: '3650',
        value: p.backfill_days_override ?? '',
        placeholder: String(globalBf),
        title: `비우면 전역(${globalBf}) 사용. 신규 세팅 시 300·600 등 길게.`,
        onchange: e => {
          const v = e.target.value.trim();
          if (v === '' || Number(v) === globalBf) delete p.backfill_days_override;
          else p.backfill_days_override = Number(v);
        }}),
      el('span', { class: 'hint' }, `일${p.backfill_days_override ? ` (전역 ${globalBf})` : ''}`),
      el('div', { class: 'spacer' }),
      el('button', {
        class: 'btn small seed-btn',
        title: '이 제품만 backfill 기간 전체 일괄 추출.',
        onclick: async () => {
          const days = p.backfill_days_override || globalBf;
          if (!confirm(`${p.product} 의 ${days}일치를 ${sources.length}개 소스로 지금 일괄 추출합니다.\n계속?`)) return;
          try {
            const r = await api.post('/api/jobs/enqueue-product', { product: p.product });
            alert(`초기 시딩 시작 — ${r.launched} 개 chunk plan 투입 (${r.backfill_days}일 × ${r.source_count} 소스).`);
          } catch (e) { alert(`실패: ${e.message}`); }
        }
      }, '🚀 초기 시딩'),
      el('button', { class: 'btn ghost small', onclick: deleteProduct }, '🗑 제품 삭제'),
    ),

    // 제품 공통 기본 (process_id/line_id/product_code)
    productKeyFieldsEditor(p, rerender),

    // 소스 탭 + 선택된 소스만 상세 표시
    el('div', { class: 'subsection-title', style: { marginTop: '14px' } },
      '▤ 추출 소스',
      el('span', { class: 'hint' }, `${sources.length}/${SOURCE_NAMES.length} · 탭 클릭으로 소스 전환`),
    ),
    el('div', { class: 'source-tabs' },
      ...sources.map(s => el('span', {
        class: 'source-tab' + (s === selectedSrc ? ' active' : ''),
        onclick: () => { STATE.productsSourceSelected = s.name; rerender(); },
      },
        s.name,
        el('span', {
          class: 'source-tab-x',
          title: `${s.name} 소스 삭제`,
          onclick: (e) => {
            e.stopPropagation();
            if (!confirm(`${s.name} 소스를 삭제하시겠습니까?`)) return;
            const idx = sources.indexOf(s);
            sources.splice(idx, 1);
            if (STATE.productsSourceSelected === s.name) {
              STATE.productsSourceSelected = sources[0]?.name || '';
            }
            rerender();
          },
        }, '✕'),
      )),
      el('button', { class: 'source-tab add', onclick: addSource, title: '새 소스 추가' }, '+'),
    ),
    selectedSrc
      ? sourceCard(p, selectedSrc, sources.indexOf(selectedSrc), rerender)
      : el('div', { class: 'empty' }, '소스 없음'),

    // 공통 뽑을 컬럼 (접기)
    el('details', { class: 'subsection-collapsible' },
      el('summary', {},
        el('span', { class: 'subsection-title-inline' }, '📋 공통 뽑을 컬럼 (선택)'),
        el('span', { class: 'hint' }, `${(p.custom_col || []).length}개 · 소스별 override 없을 때 사용`),
      ),
      productDefaultsEditor(p, rerender),
    ),
  );
}

// productCard 는 더 이상 사용하지 않음 (productDetailView 로 대체). 호환성 보관.

// ─────────────────────────────────────────────────
// 신 포맷: params_template[column_name] = {op, value}. column 을 키로 직접 사용.
// process_id / line_id / product_code 는 1급 필드로 승격.
// ─────────────────────────────────────────────────
function productKeyFieldsEditor(p, rerender) {
  p.params_template = p.params_template || {};

  const getValue = (col) => {
    const entry = p.params_template[col];
    if (!entry) return '';
    return Array.isArray(entry.value) ? entry.value.join(', ') : String(entry.value ?? '');
  };

  const setValue = (col, rawValue, op = 'eq') => {
    const trimmed = (rawValue || '').trim();
    if (!trimmed) {
      delete p.params_template[col];
      return;
    }
    const value = trimmed.includes(',')
      ? trimmed.split(',').map(x => x.trim()).filter(Boolean)
      : trimmed;
    const usedOp = Array.isArray(value) ? 'in' : op;
    p.params_template[col] = { op: usedOp, value };
  };

  // 1급 필드 외의 추가 필터 (컬럼명 기준)
  const keyFieldCols = new Set(['process_id', 'line_id', 'product_code']);
  const extraCols = Object.keys(p.params_template).filter(col => !keyFieldCols.has(col));

  return el('div', { class: 'product-keyfields' },
    el('div', { class: 'subsection-title' },
      '⚙ 제품 공통 기본',
      el('span', { class: 'hint' }, `${p.product} 에 해당하는 모든 DB 쿼리의 WHERE 절에 자동 추가됨`),
    ),

    el('div', { class: 'keyfield-grid' },
      // process_id
      el('div', { class: 'keyfield' },
        el('label', { class: 'keyfield-label' }, 'process_id',
          el('span', { class: 'hint' }, '예: P4203 · 쉼표로 여러 개 (IN)')),
        el('input', {
          type: 'text', class: 'keyfield-input',
          value: getValue('process_id'),
          placeholder: '(없음 — 필터 안 함)',
          onchange: e => { setValue('process_id', e.target.value); rerender(); },
        }),
      ),
      // line_id
      el('div', { class: 'keyfield' },
        el('label', { class: 'keyfield-label' }, 'line_id',
          el('span', { class: 'hint' }, '예: L01, L02 · 쉼표로 여러 개 (IN)')),
        el('input', {
          type: 'text', class: 'keyfield-input',
          value: getValue('line_id'),
          placeholder: '(없음 — 필터 안 함)',
          onchange: e => { setValue('line_id', e.target.value); rerender(); },
        }),
      ),
      // product_code
      el('div', { class: 'keyfield' },
        el('label', { class: 'keyfield-label' }, 'product_code',
          el('span', { class: 'hint' }, '보통 제품명과 동일')),
        el('input', {
          type: 'text', class: 'keyfield-input',
          value: getValue('product_code'),
          placeholder: p.product || '(없음)',
          onchange: e => { setValue('product_code', e.target.value); rerender(); },
        }),
      ),
    ),

    // 추가 필터
    el('details', { class: 'subsection-collapsible extra-filters', ...(extraCols.length ? { open: '' } : {}) },
      el('summary', {},
        el('span', { class: 'subsection-title-inline' }, '⧗ 추가 필터'),
        el('span', { class: 'hint' }, `${extraCols.length}건 · 다른 컬럼에 대한 WHERE 조건`),
      ),
      paramsEditor(p, rerender, { skipColumns: ['process_id', 'line_id', 'product_code'] }),
    ),
  );
}

function sourceCard(p, s, si, rerender) {
  const deleteSource = () => {
    if (!confirm(`${s.name} 소스 삭제?`)) return;
    p.sources.splice(si, 1);
    rerender();
  };

  const srcKey = (s.name || '').toUpperCase();
  const st = getSourceType(srcKey);
  return el('div', { class: 'source-card' + (st?.hint ? ' source-hinted' : '') },
    renderHint(st),
    el('div', { class: 'source-head' },
      el('select', { class: 'inline-input', onchange: e => {
        s.name = e.target.value;
        if (!s.table || /^RAW_[A-Z]+_DATA$/.test(s.table)) s.table = `RAW_${s.name}_DATA`;
        rerender();
      }},
        ...SOURCE_NAMES.map(n => el('option', { value: n, ...(n === s.name ? { selected: 'selected' } : {}) }, n)),
        ...(!SOURCE_NAMES.includes(s.name) && s.name ? [el('option', { value: s.name, selected: 'selected' }, s.name)] : []),
      ),
      el('span', { class: 'hint' }, 'table'),
      el('input', { type: 'text', class: 'inline-input', style: { width: '170px' },
        value: s.table || '', onchange: e => { s.table = e.target.value; } }),
      el('span', { class: 'hint' }, 'chunk rows'),
      el('input', { type: 'number', class: 'inline-input', style: { width: '100px' },
        value: s.target_chunk_rows ?? 500000, onchange: e => { s.target_chunk_rows = Number(e.target.value); } }),
      el('span', { class: 'hint' }, 'shard'),
      el('input', { type: 'text', class: 'inline-input', style: { width: '180px' },
        value: (s.shard_hierarchy || []).join(', '), placeholder: 'root_lot_id, item_id',
        onchange: e => { s.shard_hierarchy = e.target.value.split(',').map(x => x.trim()).filter(Boolean); } }),
      el('label', { class: 'check', title: 'probe (사전 분포 스캔) 가 자꾸 실패·timeout 나는 소스는 probe 를 끄고 단일 chunk 로 바로 실행' },
        el('input', { type: 'checkbox', ...(s.probe_skip ? { checked: 'checked' } : {}),
          onchange: e => {
            if (e.target.checked) s.probe_skip = true;
            else delete s.probe_skip;
            rerender();
          }}),
        'probe skip',
      ),
      el('div', { class: 'spacer' }),
      el('button', { class: 'btn ghost small', onclick: deleteSource }, '🗑'),
    ),
    customColsEditor(p, s, rerender),
    queryPreview(p, s),
  );
}

// ─────────────────────────────────────────────────
// 소스별 최종 호출 미리보기 — 실제 사내 DataLake 함수 호출 형태 (Python).
// 백엔드 executor._build_params 가 조립하는 dict 를 그대로 시각화.
// ─────────────────────────────────────────────────
function queryPreview(p, s) {
  const cols = Array.isArray(s.custom_col) ? s.custom_col
             : Array.isArray(p.custom_col) ? p.custom_col : [];
  const table = s.table || `RAW_${s.name}_DATA`;

  // params_template — key 가 컬럼명, value 가 {op, value}. 빈 값은 제외.
  const paramEntries = [];
  for (const [col, e] of Object.entries(p.params_template || {})) {
    if (!e || typeof e !== 'object') continue;
    const isEmpty = e.value === '' || e.value == null
                 || (Array.isArray(e.value) && e.value.length === 0);
    if (isEmpty) continue;
    paramEntries.push([col, e]);
  }

  const pyVal = (v) => {
    if (Array.isArray(v)) return '[' + v.map(pyVal).join(', ') + ']';
    if (typeof v === 'number') return String(v);
    if (typeof v === 'boolean') return v ? 'True' : 'False';
    return `"${String(v ?? '').replace(/"/g, '\\"')}"`;
  };
  const pyDict = (entry) => {
    const parts = [];
    parts.push(`"op": "${entry.op || 'eq'}"`);
    parts.push(`"value": ${pyVal(entry.value)}`);
    return `{${parts.join(', ')}}`;
  };

  const paramLines = [
    `    "table": "${table}",`,
    `    "dateFrom": "{dateFrom}",          # YYYY-MM-DDT00:00:00`,
    `    "dateTo":   "{dateTo}",            # 다음 날 00:00:00`,
  ];
  for (const [col, e] of paramEntries) {
    paramLines.push(`    "${col}": ${pyDict(e)},`);
  }

  const shardKeys = s.shard_hierarchy || [];
  if (shardKeys.length) {
    paramLines.push(
      `    # planner 가 chunk 마다 shard 를 해당 컬럼명에 직접 주입:`,
      `    # "${shardKeys[0]}": {"op": "in", "value": ["R001", "R002", ...]}`,
    );
  }

  const colsPy = cols.length
    ? '[' + cols.map(c => `"${c}"`).join(', ') + ']'
    : '[]';

  const apiKeyLine = STATE.settings?.lake_api?.api_key
    ? `    api_key="{settings.lake_api.api_key}",  # **** (저장됨)`
    : `    # api_key=...  # Settings › Lake API 에서 등록 가능`;

  const snippet = [
    '# Valve 는 다음 호출로 DataLake 에서 데이터를 가져와 staging parquet 으로 저장.',
    '# (settings.lake_api.module 에서 로드한 query 함수; mock 모드면 내부 mock engine)',
    '',
    'df: pandas.DataFrame = query(',
    '    params={',
    ...paramLines,
    '    },',
    `    custom_col=${colsPy},`,
    `    user="{settings.lake_api.user}",`,
    apiKeyLine,
    ')',
  ].join('\n');

  return el('details', { class: 'query-preview', open: '' },
    el('summary', {}, '🔎 이 소스의 최종 호출 (Python)'),
    el('pre', { class: 'query-sql' }, snippet),
  );
}

function productDefaultsEditor(p, rerender) {
  p.custom_col = Array.isArray(p.custom_col) ? p.custom_col : [];
  const label = el('div', { class: 'form-label small' },
    '공통 뽑을 컬럼 (custom_col, product-level)',
    el('span', { class: 'hint' }, `${p.custom_col.length}개 · 소스별 override 없을 때 이 목록 사용`),
  );
  const chips = el('div', { class: 'chips-row' });
  (async () => {
    // 공통 풀 = 이 제품의 모든 소스 풀 합집합
    const union = new Set();
    for (const s of (p.sources || [])) {
      const pool = await getSourceColumns(p.product, s.name);
      pool.forEach(c => union.add(c));
    }
    // default 필수: lot_id, wafer_id, time
    ['lot_id', 'wafer_id', 'time', 'value', 'product_code'].forEach(c => union.add(c));
    p.custom_col.forEach((c, i) => {
      chips.append(el('span', { class: 'chip' }, c,
        el('span', { class: 'chip-x', onclick: () => { p.custom_col.splice(i, 1); rerender(); } }, '✕'),
      ));
    });
    const available = [...union].filter(c => !p.custom_col.includes(c));
    if (available.length) {
      chips.append(el('select', { class: 'chip-add', onchange: e => {
        const v = e.target.value;
        if (!v) return;
        p.custom_col.push(v);
        rerender();
      }},
        el('option', { value: '' }, '+ 공통 컬럼 추가'),
        ...available.map(c => el('option', { value: c }, c)),
      ));
    }
    chips.append(el('input', { type: 'text', class: 'inline-input chip-free', placeholder: '수동 + Enter',
      onkeydown: e => {
        if (e.key !== 'Enter') return;
        const v = e.target.value.trim();
        if (!v || p.custom_col.includes(v)) return;
        p.custom_col.push(v);
        rerender();
      }}));
  })();
  return el('div', { class: 'custom-cols product-defaults' }, label, chips);
}

function customColsEditor(p, s, rerender) {
  const label = el('div', { class: 'form-label small' },
    `${s.name} 뽑을 컬럼`,
    el('span', { class: 'hint' },
      Array.isArray(s.custom_col)
        ? `${s.custom_col.length}개 (소스 전용)`
        : '(product-level 기본값 상속 중 — 이 소스만 바꾸려면 컬럼 추가)'),
  );
  const chips = el('div', { class: 'chips-row' });

  (async () => {
    const pool = await getSourceColumns(p.product, s.name);
    const current = Array.isArray(s.custom_col) ? s.custom_col : null;

    // chips for current source-level custom_col
    if (current) {
      current.forEach((c, i) => {
        chips.append(el('span', { class: 'chip' }, c,
          el('span', { class: 'chip-x', onclick: () => { s.custom_col.splice(i, 1); rerender(); } }, '✕'),
        ));
      });
    } else if ((p.custom_col || []).length) {
      (p.custom_col || []).forEach(c => {
        chips.append(el('span', { class: 'chip chip-inherit', title: 'product-level 기본값' }, c));
      });
    }

    // available dropdown
    const used = new Set(current || p.custom_col || []);
    const available = pool.filter(c => !used.has(c));
    if (available.length) {
      chips.append(el('select', { class: 'chip-add', onchange: e => {
        const v = e.target.value;
        if (!v) return;
        if (!Array.isArray(s.custom_col)) s.custom_col = [...(p.custom_col || [])];
        s.custom_col.push(v);
        rerender();
      }},
        el('option', { value: '' }, '+ 컬럼 추가'),
        ...available.map(c => el('option', { value: c }, c)),
      ));
    }
    // manual input
    chips.append(el('input', { type: 'text', class: 'inline-input chip-free', placeholder: '수동 컬럼 + Enter',
      onkeydown: e => {
        if (e.key !== 'Enter') return;
        const v = e.target.value.trim();
        if (!v) return;
        if (!Array.isArray(s.custom_col)) s.custom_col = [...(p.custom_col || [])];
        if (!s.custom_col.includes(v)) s.custom_col.push(v);
        rerender();
      }}));
    // reset-to-inherit
    if (Array.isArray(s.custom_col)) {
      chips.append(el('button', { class: 'btn ghost small', onclick: () => {
        delete s.custom_col;
        rerender();
      }, title: 'product-level 기본값으로 돌아가기' }, '↺ 상속'));
    }
  })();

  return el('div', { class: 'custom-cols' }, label, chips);
}

function paramsEditor(p, rerender, opts = {}) {
  // 신 포맷: params_template[column_name] = {op, value}. 키=컬럼명 (사내 API 규약).
  // opts.skipColumns: 이 컬럼명은 제외 (1급 필드에서 편집).
  const skipCols = new Set((opts.skipColumns || []).map(c => c.toLowerCase()));
  p.params_template = p.params_template || {};
  const tbl = el('table', { class: 'tbl params-tbl' },
    el('thead', {}, el('tr', {},
      el('th', { style: { width: '30%' } }, 'Column (key)'),
      el('th', { style: { width: '100px' } }, 'Op'),
      el('th', {}, 'Value'),
      el('th', { style: { width: '40px' } }, ''),
    )),
    el('tbody', {}),
  );
  const tbody = tbl.querySelector('tbody');

  (async () => {
    const allCols = new Set();
    for (const s of (p.sources || [])) {
      const pool = await getSourceColumns(p.product, s.name);
      pool.forEach(c => allCols.add(c));
    }
    const colOptions = [...allCols];

    const cols = Object.keys(p.params_template).filter(c => !skipCols.has(c.toLowerCase()));
    cols.forEach((col) => {
      const entry = p.params_template[col] || {};
      const hasCurrent = colOptions.includes(col);

      // 컬럼명 변경: 기존 키 지우고 새 키로 이동
      const colSel = el('select', { class: 'inline-input', onchange: e => {
        const newCol = e.target.value;
        if (!newCol || newCol === col) return;
        p.params_template[newCol] = entry;
        delete p.params_template[col];
        rerender();
      }},
        el('option', { value: col }, col + (hasCurrent ? '' : ' (custom)')),
        ...colOptions.filter(c => c !== col).map(c => el('option', { value: c }, c)),
      );
      const opSel = el('select', { class: 'inline-input', onchange: e => {
        entry.op = e.target.value;
        // in/notLike 로 바뀌면 value 형태 자동 조정은 사용자가 값 재입력하게 둠
        rerender();
      }},
        ...PARAM_OPS.map(o => el('option', { value: o, ...(o === entry.op ? { selected: 'selected' } : {}) }, o)),
      );
      const valStr = Array.isArray(entry.value) ? entry.value.join(', ') : String(entry.value ?? '');
      const placeholder = entry.op === 'in' ? '쉼표 구분 (예: L1, L2)'
                       : (entry.op === 'like' || entry.op === 'notLike') ? "예: %AA% (SQL LIKE 패턴)"
                       : '';
      const valInp = el('input', { type: 'text', class: 'inline-input', value: valStr,
        placeholder,
        onchange: e => {
          const raw = e.target.value;
          entry.value = entry.op === 'in'
            ? raw.split(',').map(x => x.trim()).filter(Boolean)
            : raw;
          p.params_template[col] = entry;
        }});
      const delBtn = el('button', { class: 'btn ghost small', onclick: () => {
        delete p.params_template[col]; rerender();
      }, title: `${col} 제거` }, '🗑');
      tbody.append(el('tr', {},
        el('td', {}, colSel),
        el('td', {}, opSel),
        el('td', {}, valInp),
        el('td', {}, delBtn),
      ));
    });
    if (!cols.length) {
      tbody.append(el('tr', {}, el('td', { colspan: '4', class: 'hint', style: { textAlign: 'center', padding: '12px' } },
        '필터 없음 — 아래 버튼으로 추가')));
    }
  })();

  const addRow = () => {
    // 아직 사용하지 않은 컬럼 중 첫 번째, 없으면 'cata' 로 시작
    const used = new Set(Object.keys(p.params_template));
    const candidates = ['cata', 'catb', 'catc', 'catd', 'new_col'];
    const seed = candidates.find(c => !used.has(c)) || `col_${Object.keys(p.params_template).length}`;
    p.params_template[seed] = { op: 'eq', value: '' };
    rerender();
  };

  return el('div', {}, tbl,
    el('button', { class: 'btn ghost small', onclick: addRow, style: { marginTop: '6px' } }, '+ 필터 추가'),
  );
}

// ─────────────────────────────────────
// Logs tab — 시도 시간 / 결과 / 실패 사유
// ─────────────────────────────────────
const LOG_STATUS_META = {
  success:              { label: 'success',   cls: 'ok',   color: '#166534' },
  running:              { label: 'running',   cls: 'run',  color: '#1e40af' },
  in_progress:          { label: 'running',   cls: 'run',  color: '#1e40af' },
  pending:              { label: 'pending',   cls: 'pending', color: '#525252' },
  cancelled:            { label: 'cancelled', cls: 'pending', color: '#525252' },
  failed:               { label: 'failed',    cls: 'err',  color: '#991b1b' },
  timeout_reshard:      { label: 'timeout',   cls: 'err',  color: '#991b1b' },
  completeness_failed:  { label: 'incomplete', cls: 'warn', color: '#92400e' },
  upload_failed:        { label: 'upload err', cls: 'err', color: '#991b1b' },
};

async function renderLogs() {
  const main = $('#main');
  main.innerHTML = '';

  const f = STATE.logsFilter;

  const applyAndReload = () => loadLogs();
  const products = (STATE.products?.products || []).map(p => p.product);
  const allSources = [...new Set([...CANONICAL_SOURCES,
    ...(STATE.products?.products || []).flatMap(p => (p.sources || []).map(s => s.name))])];

  const filterBar = el('div', { class: 'logs-filter' },
    el('label', { class: 'hint' }, '제품'),
    el('select', { class: 'inline-input', onchange: e => { f.product = e.target.value; applyAndReload(); } },
      el('option', { value: '' }, '전체'),
      ...products.map(p => el('option', { value: p, ...(p === f.product ? { selected: 'selected' } : {}) }, p)),
    ),
    el('label', { class: 'hint' }, '소스'),
    el('select', { class: 'inline-input', onchange: e => { f.source = e.target.value; applyAndReload(); } },
      el('option', { value: '' }, '전체'),
      ...allSources.map(s => el('option', { value: s, ...(s === f.source ? { selected: 'selected' } : {}) }, s)),
    ),
    el('label', { class: 'hint' }, '상태'),
    el('select', { class: 'inline-input', onchange: e => { f.status = e.target.value; applyAndReload(); } },
      el('option', { value: '' }, '전체'),
      ...Object.keys(LOG_STATUS_META).map(k => el('option', { value: k, ...(k === f.status ? { selected: 'selected' } : {}) }, k)),
    ),
    el('label', { class: 'check' },
      el('input', { type: 'checkbox', ...(f.failed_only ? { checked: 'checked' } : {}),
        onchange: e => { f.failed_only = e.target.checked; applyAndReload(); } }),
      '실패만',
    ),
    el('label', { class: 'hint' }, '종류'),
    el('select', { class: 'inline-input', onchange: e => { f.kind = e.target.value; applyAndReload(); } },
      ...['chunk','plan','partition','all'].map(k => el('option', { value: k, ...(k === f.kind ? { selected: 'selected' } : {}) }, k)),
    ),
    el('label', { class: 'hint' }, 'N'),
    el('input', { type: 'number', class: 'inline-input narrow', min: '10', max: '5000',
      value: f.limit, onchange: e => { f.limit = Number(e.target.value) || 300; applyAndReload(); } }),
    el('div', { class: 'spacer' }),
    el('button', { class: 'btn ghost small', onclick: applyAndReload }, '↻ 새로고침'),
  );

  main.append(
    el('div', { class: 'row', style: { marginBottom: '12px', gap: '8px' } },
      el('div', {},
        el('div', { class: 'section-title' }, '실행 로그'),
        el('div', { class: 'section-desc' }, '각 chunk 의 마지막 시도 결과 — 언제 시도했고, 얼마 걸렸고, 왜 실패했는지.'),
      ),
    ),
    el('div', { class: 'card', id: 'logs-card' },
      el('div', { class: 'card-title' }, '📜 실행 이력', el('span', { class: 'count' }, '…')),
      filterBar,
      el('div', { id: 'logs-body' }, el('div', { class: 'empty' }, '로딩…')),
    ),
  );

  // auto-refresh every 15s while on Logs tab
  if (STATE.logsRefresh) { clearInterval(STATE.logsRefresh); STATE.logsRefresh = null; }
  STATE.logsRefresh = setInterval(() => {
    if (STATE.currentTab === 'logs') loadLogs();
    else { clearInterval(STATE.logsRefresh); STATE.logsRefresh = null; }
  }, 15000);

  loadLogs();
}

async function loadLogs() {
  const f = STATE.logsFilter;
  const q = new URLSearchParams();
  if (f.product) q.set('product', f.product);
  if (f.source) q.set('source', f.source);
  if (f.status) q.set('status', f.status);
  if (f.failed_only) q.set('failed_only', 'true');
  if (f.kind) q.set('kind', f.kind);
  q.set('limit', String(f.limit || 300));

  const body = $('#logs-body');
  const countEl = document.querySelector('#logs-card .card-title .count');
  try {
    const r = await api.get(`/api/jobs/history?${q.toString()}`);
    STATE.logsItems = r.items || [];
    if (countEl) countEl.textContent = `${STATE.logsItems.length} 건${r.log_exists ? '' : ' (로그 없음)'}`;
    if (!STATE.logsItems.length) {
      body.innerHTML = '';
      body.append(el('div', { class: 'empty' }, '조건에 맞는 이력 없음'));
      return;
    }
    body.innerHTML = '';
    body.append(renderLogsTable(STATE.logsItems));
  } catch (e) {
    body.innerHTML = '';
    body.append(el('div', { class: 'alert err' }, `로드 실패: ${e.message}`));
  }
}

function renderLogsTable(items) {
  const tbl = el('table', { class: 'tbl logs-tbl' },
    el('thead', {}, el('tr', {},
      el('th', {}, '시간'),
      el('th', {}, '종류'),
      el('th', {}, '제품'),
      el('th', {}, '소스'),
      el('th', {}, '날짜'),
      el('th', {}, '상태'),
      el('th', {}, 'duration'),
      el('th', {}, 'rows'),
      el('th', {}, '사유 / 메모'),
    )),
    el('tbody', {},
      items.map((it) => {
        const tsStr = it.ts ? new Date(it.ts * 1000).toLocaleString('sv').slice(5, 16) : '-';
        const tsAgo = it.ts ? fmt.ago(it.ts) : '';
        const meta = LOG_STATUS_META[it.status] || { label: it.status || '-', cls: 'pending' };
        let reason = '';
        if (it.kind === 'chunk') {
          if (it.error) reason = `${it.error_type || 'error'}: ${it.error}`;
          else if (it.actual_rows) reason = `expected ${fmt.int(it.expected_rows)} · actual ${fmt.int(it.actual_rows)}`;
        } else if (it.kind === 'plan') {
          const pm = it.probe_meta || {};
          if (pm.error) reason = `⚠ probe 실패 → 단일 chunk fallback: ${pm.error}`;
          else if (pm.skipped) reason = `probe skip (${pm.reason || 'manual'})`;
          else reason = `chunks=${it.chunks} · probe=${pm.strategy || '-'}${pm.estimated_rows != null ? ` · est ${fmt.int(pm.estimated_rows)}` : ''}${pm.shard_count ? ` · shards=${pm.shard_count}` : ''}`;
        } else {
          reason = JSON.stringify(it.update || {}).slice(0, 120);
        }
        const probeFailed = it.kind === 'plan' && it.probe_meta?.error;
        const rowCls = [
          it.status && ['failed','timeout_reshard','completeness_failed','upload_failed'].includes(it.status) ? 'row-err' : '',
          probeFailed ? 'row-warn' : '',
        ].filter(Boolean).join(' ');
        const rowsTxt = it.kind === 'chunk' && it.actual_rows != null
          ? `${fmt.int(it.actual_rows)}${it.expected_rows ? ` / ${fmt.int(it.expected_rows)}` : ''}`
          : '';
        const tr = el('tr', { class: rowCls, title: it.chunk_id || it.plan_id || it.partition_key || '' },
          el('td', { class: 'mono', style: { whiteSpace: 'nowrap' } }, tsStr, el('span', { class: 'hint', style: { marginLeft: '4px' } }, tsAgo)),
          el('td', {}, el('span', { class: 'pill' }, it.kind)),
          el('td', { class: 'mono' }, it.product || '-'),
          el('td', { class: 'mono' }, it.source || '-'),
          el('td', { class: 'mono' }, it.date || '-'),
          el('td', {},
            it.status ? el('span', { class: `pill ${meta.cls}` }, meta.label)
            : probeFailed ? el('span', { class: 'pill warn' }, 'probe fail')
            : (it.kind === 'plan' && it.probe_meta?.skipped) ? el('span', { class: 'pill pending' }, 'probe skip')
            : (it.kind === 'plan') ? el('span', { class: 'pill run' }, 'planned')
            : '-'),
          el('td', { class: 'mono' }, it.duration_sec != null ? fmt.dur(it.duration_sec) : '-'),
          el('td', { class: 'mono' }, rowsTxt),
          el('td', { class: 'logs-reason', title: reason }, reason || ''),
        );
        return tr;
      }),
    ),
  );
  return tbl;
}

// ─────────────────────────────────────
// Settings tab
// ─────────────────────────────────────
async function renderSettings() {
  const main = $('#main');
  main.innerHTML = '';
  if (!STATE.settings) {
    try { STATE.settings = await api.get('/api/settings'); } catch (e) { return renderError(e); }
  }

  const draft = structuredClone(STATE.settings);

  const sections = [
    { key: 'lake', label: '🔌 사내 Lake API', rows: [
      ['lake_api.mode',          'select', ['mock','real']],
      ['lake_api.module',        'text',   null,   'mycorp.datalake:query 형태 (real 모드에서만 의미)'],
      ['lake_api.user',          'text',   null,   '사내 query 함수 호출 시 user 파라미터로 전달'],
      ['lake_api.api_key',       'password', null, '사내 API 인증 키 (있는 경우). 저장 후 ****, 빈 값은 보존'],
      ['lake_api.timeout_sec',   'number', null,   '5분(300) 이하 권장. 기본 290'],
      ['lake_api.min_interval_sec', 'number'],
      ['lake_api.max_concurrent','number', null,   '동시 chunk 실행 수. 기본 3'],
      ['lake_api.retry.attempts','number'],
      ['lake_api.retry.backoff_sec', 'csv', null,  '쉼표 구분 int (예: 10,30,120)'],
      ['lake_api.retryable_errors', 'csv', null,   'HY000, TimeoutError 등'],
    ]},
    { key: 's3', label: '☁ S3 업로드', rows: [
      ['s3.endpoint_url', 'text', null, '비우면 AWS S3 / MinIO 는 http://host:9000'],
      ['s3.bucket', 'text'],
      ['s3.prefix', 'text'],
      ['s3.access_key', 'text'],
      ['s3.secret_key', 'password', null, '저장 후 ****. 그대로 두면 기존 값 유지'],
      ['s3.fake_local_path', 'text', null, 'endpoint_url 비어있고 이 값 있으면 개발 모드 (로컬 폴더)'],
    ]},
    { key: 'schedule', label: '📅 스케줄', rows: [
      ['schedule.backfill_days', 'number', null, '오늘 + 과거 N일 (권장 3~5). 제품별 override 는 제품 탭에서.'],
      ['schedule.interval_hours', 'number', null, '자동 스케줄 (v0.2 구현)'],
      ['schedule.force_overwrite', 'bool'],
      ['schedule.tolerance_pct', 'number', null, 'completeness 허용 %. 0.5 = 0.5%'],
    ]},
    { key: 'probe', label: '🔍 프로브', rows: [
      ['probe.strategy', 'select', ['sample_window','projection','none']],
      ['probe.window_hours', 'number'],
      ['probe.cache_days', 'number', null, 'probe 결과 N일 재사용. 기본 7'],
      ['probe.adaptive_correction', 'bool'],
      ['probe.fallback_on_timeout', 'bool'],
    ]},
    { key: 'types', label: '🧩 소스 타입', custom: renderSourceTypesManager },
  ];

  let active = STATE.settingsActive && sections.find(s => s.key === STATE.settingsActive)
    ? STATE.settingsActive : 'lake';

  const sectionEls = {};
  for (const s of sections) {
    sectionEls[s.key] = s.custom
      ? s.custom(draft)
      : settingsSection(s.label, s.rows, draft);
  }

  const switchTo = (key) => {
    active = key;
    STATE.settingsActive = key;
    for (const k in sectionEls) sectionEls[k].style.display = k === key ? '' : 'none';
    btnBar.querySelectorAll('button').forEach((b) => {
      const on = b.dataset.section === key;
      b.classList.toggle('primary', on);
    });
  };

  const btnBar = el('div', { class: 'settings-tabs' },
    ...sections.map((s) => el('button', {
      class: 'btn' + (s.key === active ? ' primary' : ''),
      'data-section': s.key,
      onclick: () => switchTo(s.key),
    }, s.label)),
  );

  for (const k in sectionEls) sectionEls[k].style.display = k === active ? '' : 'none';

  main.append(
    el('div', { class: 'row' },
      el('div', {},
        el('div', { class: 'section-title' }, '설정'),
        el('div', { class: 'section-desc' }, '저장 시 런타임 반영. secret 은 화면에서 ****.'),
      ),
      el('div', { class: 'spacer' }),
      el('button', { class: 'btn primary', onclick: () => onSaveSettings(draft) }, '💾 저장'),
    ),
    btnBar,
    ...Object.values(sectionEls),
  );
}

function renderSourceTypesManager(_draft) {
  // 독자적인 draft — settings 저장 버튼과 무관하게 별도 저장 버튼 노출.
  const draft = { source_types: SOURCE_TYPES.map(s => structuredClone(s)) };
  const card = el('div', { class: 'card' });
  const rerender = () => {
    card.innerHTML = '';
    buildUI();
  };

  const save = async () => {
    try {
      const r = await api.post('/api/schedule/source-types', draft);
      alert(`저장됨 — ${r.count} 개 타입`);
      await loadSourceTypes();   // 전역 레지스트리 즉시 갱신
      rerender();
    } catch (e) { alert(`저장 실패: ${e.message}`); }
  };
  const addType = () => {
    draft.source_types.push({
      name: 'NEW' + draft.source_types.length,
      table_template: 'RAW_{name}_DATA',
      columns: ['lot_id', 'wafer_id', 'time', 'value'],
      default_shard: [],
      accent: '#64748b',
      hint: '',
    });
    rerender();
  };

  function buildUI() {
    card.append(
      el('div', { class: 'card-title' }, '🧩 소스 타입 관리',
        el('span', { class: 'count' }, `${draft.source_types.length} 개`),
      ),
      el('div', { class: 'section-desc', style: { fontSize: '11px', marginBottom: '10px' } },
        '새 DB 추가 시 여기에 등록. 등록 후 제품 편집기의 소스 드롭다운·모니터 히트맵·컬럼 풀 모두에 반영.'),
      ...draft.source_types.map((st, i) => sourceTypeRow(st, i, draft, rerender)),
      el('div', { class: 'row', style: { marginTop: '10px', gap: '6px' } },
        el('button', { class: 'btn', onclick: addType }, '+ 타입 추가'),
        el('div', { class: 'spacer' }),
        el('button', { class: 'btn primary', onclick: save }, '💾 저장'),
      ),
    );
  }
  buildUI();
  return card;
}

function sourceTypeRow(st, idx, draft, rerender) {
  const del = () => {
    if (!confirm(`${st.name} 타입 삭제?`)) return;
    draft.source_types.splice(idx, 1);
    rerender();
  };
  const updateCols = (newVal) => {
    st.columns = newVal.split(',').map(x => x.trim()).filter(Boolean);
  };
  const updateShard = (newVal) => {
    st.default_shard = newVal.split(',').map(x => x.trim()).filter(Boolean);
  };
  return el('div', { class: 'source-type-row', style: { borderLeftColor: st.accent || '#64748b' } },
    el('div', { class: 'row', style: { gap: '6px', alignItems: 'center', marginBottom: '6px' } },
      el('input', { type: 'text', class: 'inline-input', value: st.name || '', style: { width: '100px', fontWeight: 700 },
        placeholder: 'NAME', onchange: e => { st.name = e.target.value.toUpperCase(); } }),
      el('span', { class: 'hint' }, 'color'),
      el('input', { type: 'color', class: 'inline-input', style: { width: '40px', padding: '0', height: '24px' },
        value: st.accent || '#64748b', onchange: e => { st.accent = e.target.value; rerender(); } }),
      el('span', { class: 'hint' }, 'table'),
      el('input', { type: 'text', class: 'inline-input', style: { width: '200px' },
        value: st.table_template || '', placeholder: 'RAW_{name}_DATA',
        onchange: e => { st.table_template = e.target.value; } }),
      el('div', { class: 'spacer' }),
      el('button', { class: 'btn ghost small', onclick: del }, '🗑'),
    ),
    el('div', { class: 'row', style: { gap: '6px', alignItems: 'center', marginBottom: '6px' } },
      el('span', { class: 'hint' }, 'columns'),
      el('input', { type: 'text', class: 'inline-input', style: { flex: 1 },
        value: (st.columns || []).join(', '), placeholder: 'lot_id, wafer_id, time, ...',
        onchange: e => updateCols(e.target.value) }),
      el('span', { class: 'hint' }, 'shard'),
      el('input', { type: 'text', class: 'inline-input', style: { width: '160px' },
        value: (st.default_shard || []).join(', '), placeholder: 'root_lot_id',
        onchange: e => updateShard(e.target.value) }),
    ),
    el('div', { class: 'row', style: { gap: '6px', alignItems: 'center' } },
      el('span', { class: 'hint' }, 'hint'),
      el('input', { type: 'text', class: 'inline-input', style: { flex: 1 },
        value: st.hint || '', placeholder: "가이드 문구 (inline `code` 지원)",
        onchange: e => { st.hint = e.target.value; } }),
    ),
  );
}

function settingsSection(title, rows, draft) {
  return el('div', { class: 'card' },
    el('div', { class: 'card-title' }, title),
    ...rows.map((row) => settingsRow(row, draft)),
  );
}

function settingsRow(def, draft) {
  const [path, type, options, hint] = def;
  const val = getByPath(draft, path);

  const label = el('div', { class: 'form-label' },
    path,
    hint ? el('span', { class: 'hint' }, hint) : '',
  );

  let input;
  if (type === 'select') {
    input = el('select', { onchange: (e) => setByPath(draft, path, e.target.value) },
      ...options.map((o) => el('option', { value: o, selected: o === val }, o)),
    );
  } else if (type === 'bool') {
    input = el('label', { class: 'check' },
      el('input', { type: 'checkbox', ...(val ? { checked: 'checked' } : {}),
        onchange: (e) => setByPath(draft, path, e.target.checked) }),
      String(val),
    );
  } else if (type === 'number') {
    input = el('input', { type: 'number', value: val == null ? '' : val,
      onchange: (e) => setByPath(draft, path, Number(e.target.value)) });
  } else if (type === 'password') {
    input = el('input', { type: 'password', placeholder: val === '****' ? '**** (저장된 값)' : '',
      onchange: (e) => setByPath(draft, path, e.target.value || val) });
  } else if (type === 'csv') {
    const str = Array.isArray(val) ? val.join(', ') : String(val || '');
    input = el('input', { type: 'text', value: str,
      onchange: (e) => setByPath(draft, path, e.target.value.split(',').map((s) => s.trim()).filter(Boolean).map((x) => isNaN(Number(x)) ? x : Number(x))) });
  } else {
    input = el('input', { type: 'text', value: val == null ? '' : String(val),
      onchange: (e) => setByPath(draft, path, e.target.value) });
  }

  return el('div', { class: 'form-row' }, label, input);
}

function getByPath(obj, path) {
  return path.split('.').reduce((a, k) => (a == null ? a : a[k]), obj);
}
function setByPath(obj, path, val) {
  const keys = path.split('.');
  const last = keys.pop();
  const parent = keys.reduce((a, k) => {
    if (a[k] == null || typeof a[k] !== 'object') a[k] = {};
    return a[k];
  }, obj);
  parent[last] = val;
}

async function onSaveSettings(draft) {
  try {
    const r = await api.post('/api/settings', draft);
    STATE.settings = r.settings;
    alert('저장됨 · 런타임 반영');
    renderSettings();
  } catch (e) { alert(`저장 실패: ${e.message}`); }
}

// ─────────────────────────────────────
// Browser tab
// ─────────────────────────────────────
let BR = { root: 'staging', path: '', selFile: '', sql: '' };

async function renderBrowser() {
  const main = $('#main');
  main.innerHTML = '';
  main.append(
    el('div', {},
      el('div', { class: 'section-title' }, '파일 탐색기'),
      el('div', { class: 'section-desc' }, 'staging · s3_local (fake S3) 탐색. parquet 선택 후 SQL 필터 가능.'),
    ),
    renderSqlGuide(),
    el('div', { class: 'split' },
      el('div', { class: 'pane' },
        el('div', { class: 'hdr' }, '📁 Roots'),
        el('div', { id: 'brTree' }, 'loading...'),
      ),
      el('div', { class: 'pane', style: { display: 'flex', flexDirection: 'column' } },
        el('div', { class: 'sql-bar' },
          el('input', { type: 'text', placeholder: "SQL filter (e.g. SELECT * FROM t WHERE root_lot_id = 'R001')",
            value: BR.sql, oninput: (e) => BR.sql = e.target.value,
            onkeydown: (e) => { if (e.key === 'Enter') reloadView(); } }),
          el('button', { class: 'btn primary', onclick: reloadView }, '▶ Run'),
          el('button', { class: 'btn', onclick: () => { BR.sql = ''; reloadView(); } }, 'Clear'),
        ),
        el('div', { id: 'brView', style: { flex: 1, overflow: 'auto' } },
          el('div', { class: 'empty' }, '좌측에서 parquet 파일 선택'),
        ),
      ),
    ),
  );
  await loadBrowserRoots();
}

function renderSqlGuide() {
  const applySnippet = (sql) => {
    BR.sql = sql;
    const inp = document.querySelector('.sql-bar input');
    if (inp) inp.value = sql;
    if (BR.selFile) reloadView();
  };
  const snippets = [
    ['전체 1000 행',                "SELECT * FROM t LIMIT 1000"],
    ['특정 lot 필터',               "SELECT * FROM t WHERE lot_id = 'L0042'"],
    ['root_lot 여러 개',            "SELECT * FROM t WHERE root_lot_id IN ('R001','R002','R003')"],
    ['wafer 번호 범위',             "SELECT * FROM t WHERE wafer_id BETWEEN 1 AND 12"],
    ['특정 item 상위 100',          "SELECT lot_id, wafer_id, time, value FROM t WHERE item_id = 'ITEM_042' ORDER BY time DESC LIMIT 100"],
    ['value 분위수 집계',           "SELECT item_id, COUNT(*) AS n, AVG(value) AS mean FROM t GROUP BY item_id ORDER BY n DESC"],
    ['실패 die 만 (EDS)',           "SELECT * FROM t WHERE pass_fail = 0"],
    ['ET 수율 outlier',             "SELECT * FROM t WHERE value > 5.0 OR value < -5.0"],
    ['시간 범위',                   "SELECT * FROM t WHERE time >= '2026-04-23T00:00:00' AND time < '2026-04-24T00:00:00'"],
    ['WHERE 만 간단히',             "wafer_id = 5"],
  ];
  const snipRow = (label, sql) => el('div', { class: 'sql-snip', onclick: () => applySnippet(sql) },
    el('span', { class: 'sql-snip-label' }, label),
    el('code', {}, sql),
  );
  return el('details', { class: 'sql-guide', open: undefined },
    el('summary', {}, '📘 SQL 사용 가이드 (polars SQL) — 클릭해서 펼치기'),
    el('div', { class: 'sql-guide-body' },
      el('div', { class: 'sql-rules' },
        el('div', { class: 'sql-rule-title' }, '규칙'),
        el('ul', {},
          el('li', {}, '선택한 parquet 의 테이블명은 항상 ', el('code', {}, 't'), ' — ', el('code', {}, 'SELECT * FROM t WHERE ...')),
          el('li', {}, el('code', {}, 'FROM'), ' 을 생략하고 조건만 쓰면 자동으로 ', el('code', {}, 'SELECT * FROM t WHERE ...'), ' 로 감쌈'),
          el('li', {}, '문자열은 ', el('code', {}, "'single-quote'"), " (backtick/double-quote 아님)"),
          el('li', {}, '지원 함수: 표준 SQL + polars 확장 (', el('code', {}, 'DATE_TRUNC'), ', ', el('code', {}, 'CAST'), ', ', el('code', {}, 'COALESCE'), ')'),
          el('li', {}, '최대 ', el('code', {}, '2000'), ' 행까지. 더 많으면 ', el('code', {}, 'LIMIT'), ' 로 명시'),
          el('li', {}, '날짜 비교는 ISO 문자열 또는 ', el('code', {}, "CAST(... AS TIMESTAMP)"), ' 사용'),
        ),
      ),
      el('div', { class: 'sql-snips' },
        el('div', { class: 'sql-rule-title' }, '예시 (클릭하면 바로 적용)'),
        ...snippets.map(([l, s]) => snipRow(l, s)),
      ),
    ),
  );
}

async function loadBrowserRoots() {
  try {
    const { roots } = await api.get('/api/browser/roots');
    const tree = $('#brTree');
    tree.innerHTML = '';
    for (const r of roots) {
      tree.append(el('div', { class: 'tree-item', style: { fontWeight: 800 }, onclick: () => loadBrowserDir(r.name, '') },
        el('span', { class: 'ic' }, '▸'),
        r.name,
        el('span', { class: 'sz' }, r.path),
      ));
    }
    loadBrowserDir(BR.root, BR.path);
  } catch (e) { $('#brTree').textContent = String(e); }
}

async function loadBrowserDir(root, path) {
  BR.root = root; BR.path = path;
  try {
    const r = await api.get(`/api/browser/list?root=${encodeURIComponent(root)}&path=${encodeURIComponent(path)}`);
    const tree = $('#brTree');
    tree.innerHTML = '';
    tree.append(el('div', { class: 'tree-item', style: { fontWeight: 800 } },
      el('span', { class: 'ic' }, '▾'),
      `${root}${path ? '/' + path : ''}`,
    ));
    if (path) {
      const parent = path.split('/').slice(0, -1).join('/');
      tree.append(el('div', { class: 'tree-item', onclick: () => loadBrowserDir(root, parent) },
        el('span', { class: 'ic' }, '↑'), '..'));
    }
    if (!r.entries.length) tree.append(el('div', { class: 'empty' }, '비어있음'));
    r.entries.forEach((e) => {
      const fullPath = path ? `${path}/${e.name}` : e.name;
      const icon = e.is_dir ? '📁' : (e.suffix === '.parquet' ? '📊' : '📄');
      const cls = BR.selFile === fullPath ? 'tree-item sel' : 'tree-item';
      tree.append(el('div', { class: cls, onclick: () => e.is_dir ? loadBrowserDir(root, fullPath) : selectFile(root, fullPath) },
        el('span', { class: 'ic' }, icon),
        e.name,
        el('span', { class: 'sz' }, e.is_dir ? '' : fmtBytes(e.size)),
      ));
    });
  } catch (e) { $('#brTree').textContent = String(e); }
}

function selectFile(root, path) {
  BR.selFile = path; BR.root = root;
  loadBrowserDir(root, path.split('/').slice(0, -1).join('/'));
  reloadView();
}

async function reloadView() {
  if (!BR.selFile) return;
  const view = $('#brView');
  view.innerHTML = '<div class="loading">Loading…</div>';
  try {
    const url = `/api/query/view?root=${encodeURIComponent(BR.root)}&file=${encodeURIComponent(BR.selFile)}&sql=${encodeURIComponent(BR.sql)}&rows=200`;
    const r = await api.get(url);
    view.innerHTML = '';
    view.append(
      el('div', { style: { padding: '10px 14px', fontSize: '12px', color: 'var(--text-secondary)', borderBottom: '1px solid var(--border)' } },
        el('span', { class: 'mono' }, BR.selFile), '  ·  ',
        `${r.n_rows} rows · ${r.columns.length} cols`,
      ),
      el('table', { class: 'tbl' },
        el('thead', {}, el('tr', {},
          ...r.columns.map((c) => el('th', { title: r.dtypes[c] }, c, el('div', { class: 'mono', style: { fontWeight: 400, fontSize: '10px', color: 'var(--text-muted)' } }, r.dtypes[c]))),
        )),
        el('tbody', {},
          r.rows.map((row) => el('tr', {},
            ...r.columns.map((c) => el('td', { class: 'mono' }, String(row[c] ?? ''))),
          )),
        ),
      ),
    );
  } catch (e) {
    view.innerHTML = `<div class="alert err">${e.message}</div>`;
  }
}

function fmtBytes(b) {
  if (!b) return '';
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${(b / 1024 / 1024).toFixed(2)} MB`;
}

// ─────────────────────────────────────
// error render
// ─────────────────────────────────────
function renderError(e) {
  $('#main').innerHTML = '';
  $('#main').append(el('div', { class: 'alert err' }, String(e?.message || e)));
}

// ─────────────────────────────────────
// init
// ─────────────────────────────────────
function applyTheme(mode) {
  const m = (mode === 'dark') ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', m);
  localStorage.setItem('valve_theme', m);
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = m === 'dark' ? '☀' : '☾';
}

(async function init() {
  // theme 초기값 (저장된 값 → 시스템 prefers-color-scheme → light)
  const savedTheme = localStorage.getItem('valve_theme');
  const sysDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  applyTheme(savedTheme || (sysDark ? 'dark' : 'light'));
  const tgl = document.getElementById('themeToggle');
  if (tgl) tgl.addEventListener('click', () => {
    const cur = document.documentElement.getAttribute('data-theme') || 'light';
    applyTheme(cur === 'dark' ? 'light' : 'dark');
  });

  // nav tab clicks
  $$('.tab').forEach((b) => b.addEventListener('click', () => route(b.dataset.tab)));

  try {
    STATE.health = await api.get('/api/health');
    $('#modeBadge').textContent = STATE.health.lake_mode;
  } catch (e) { console.warn('health', e); }
  try {
    STATE.version = await api.get('/api/version');
  } catch (e) { /* ignore */ }
  try { STATE.settings = await api.get('/api/settings'); } catch (e) { }
  try { STATE.products = await api.get('/api/schedule/products'); } catch (e) { }
  await loadSourceTypes();  // SOURCE_NAMES / SOURCE_HINTS / CANONICAL_SOURCES 갱신

  connectSSE();

  // initial tab from hash
  const initTab = (location.hash.slice(1) || 'monitor');
  route(['monitor','products','logs','settings','browser'].includes(initTab) ? initTab : 'monitor');

  window.addEventListener('hashchange', () => route(location.hash.slice(1) || 'monitor'));
})();
