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
  async put(path, body = {}) {
    const r = await fetch(path, {
      method: 'PUT',
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
  $$('.tab[data-tab]').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
  renderCurrentTab();
}

function renderCurrentTab() {
  const map = {
    monitor: renderMonitor,
    products: renderProducts,
    logs: renderLogs,
    settings: renderSettings,
    browser: renderBrowser,
    alerts: renderAlerts,
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
    renderDbHeatmapCard(),
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

// DB heatmap — db/ 단일 처리 현황. 셀 하나가 raw→event 단계 색으로:
//   남색 = raw query 만 완료(event 대기) · 초록 = event 완료 ·
//   노랑 = event 재처리 필요(matching 변경) · 빗금 = raw 없음.
// feature(db/3.FEATURE_STORE)는 소스·vehicle 단위 산출물 → 소스 행 배지로 표기.
function renderDbHeatmapCard() {
  const card = el('div', { class: 'card' },
    el('div', { class: 'card-title' }, '🗂 DB heatmap',
      el('span', { class: 'count' }, 'raw → event → feature · db/ 처리 현황')),
    el('div', { id: 'dbhmBody' }, el('div', { class: 'loading' }, 'Loading…')),
  );
  queueMicrotask(loadDbHeatmap);   // 카드가 DOM 에 붙은 뒤 로드 (최초 렌더 누락 방지)
  return card;
}

// 소스 → feature 카테고리 (FAB 는 fab/knob/mask 로 파생, INLINE·VM 은 동명 카테고리)
const FEATURE_CATS = { FAB: ['fab', 'knob', 'mask'], INLINE: ['inline'], VM: ['vm'] };

// DB heatmap 조회 기간 — 일 단위 vs 주/월 버킷. 재렌더에도 유지되도록 모듈 상태.
const DBHM_PERIODS = ['2주', '1달', '6개월', '2년'];
let DBHM_PERIOD = '1달';

// 선택 기간 → 컬럼 버킷 목록. 각 버킷은 [start, end] iso 범위 (일 단위면 start==end).
function dbhmBuckets(period) {
  const iso = (d) => d.toISOString().slice(0, 10);
  const addDays = (d, n) => { const x = new Date(d); x.setDate(x.getDate() + n); return x; };
  const today = new Date();
  const out = [];
  if (period === '2주' || period === '1달') {
    const days = period === '2주' ? 14 : 31;
    for (let i = days - 1; i >= 0; i--) { const d = iso(addDays(today, -i)); out.push({ label: d.slice(5), title: d, start: d, end: d }); }
  } else if (period === '6개월') {
    for (let i = 25; i >= 0; i--) {   // 26 주
      const e = addDays(today, -i * 7), s = addDays(e, -6);
      out.push({ label: iso(s).slice(5), title: `주간 ${iso(s)} ~ ${iso(e)}`, start: iso(s), end: iso(e) });
    }
  } else {   // 2년 — 24 개월
    const base = new Date(today.getFullYear(), today.getMonth(), 1);
    for (let i = 23; i >= 0; i--) {
      const s = new Date(base.getFullYear(), base.getMonth() - i, 1);
      const e = new Date(s.getFullYear(), s.getMonth() + 1, 0);
      out.push({ label: `${s.getFullYear()}-${String(s.getMonth() + 1).padStart(2, '0')}`,
                 title: `월간 ${iso(s)} ~ ${iso(e)}`, start: iso(s), end: iso(e) });
    }
  }
  return out;
}

async function loadDbHeatmap() {
  if (!$('#dbhmBody')) return;
  try {
    const status = await api.get('/api/pipeline/status');
    const body = $('#dbhmBody');   // fetch 중 재렌더로 노드 교체 가능 — 다시 조회
    if (!body) return;
    const vehicles = Object.keys(status);
    const anyRaw = vehicles.some((v) => Object.values(status[v].raw).some((a) => a.length));
    if (!anyRaw) {
      body.innerHTML = '<div class="empty">db raw 없음 — 알람 탭에서 파이프라인 실행</div>';
      return;
    }
    const buckets = dbhmBuckets(DBHM_PERIOD);
    const fmtTs = (ts) => ts ? new Date(ts * 1000).toLocaleString('ko-KR', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }) : '-';

    const rt = await api.get('/api/pipeline/runtime').catch(() => null);
    const runtimeBar = rt ? renderRuntimeBar(rt) : null;

    // 기간 선택 세그먼트
    const picker = el('div', { class: 'dbhm-period' },
      ...DBHM_PERIODS.map((p) => el('button', {
        class: 'seg' + (p === DBHM_PERIOD ? ' on' : ''),
        onclick: () => { DBHM_PERIOD = p; loadDbHeatmap(); },
      }, p)));

    const thead = el('tr', {},
      el('th', { class: 'row-h' }, 'Vehicle / Source'),
      ...buckets.map((b) => el('th', { title: b.title }, b.label)));
    const tbody = el('tbody', {});
    vehicles.forEach((v, vi) => {
      const st = status[v];
      const featTotal = Object.values(st.features || {}).reduce((a, n) => a + n, 0);
      tbody.append(el('tr', { class: 'prod-head-row' + (vi > 0 ? ' divider' : '') },
        el('td', { class: 'prod-head-cell', colspan: String(buckets.length + 1) },
          el('div', { class: 'prod-head-inner clickable', title: `클릭 → ${v} 파이프라인 재실행 (raw→event→feature)`,
            onclick: () => onRunVehicle(v) },
            el('span', { class: 'prod-head-name' }, v),
            el('span', { class: 'hint' }, st.product),
            el('span', { class: 'spacer' }),
            el('span', { class: 'prod-head-count' },
              `matching ${fmtTs(st.event.FAB?.applied_ts)} · ${st.matching.steps} step · feature ${featTotal}`)))));
      Object.keys(st.raw).forEach((src) => {
        const rawOnly = !(src in (st.event || {}));   // ET 등 raw 전용 소스 — event 없음
        const ev = st.event[src] || {};
        const evDates = new Set(ev.dates || []);
        const featN = (FEATURE_CATS[src] || []).reduce((a, c) => a + (st.features?.[c] || 0), 0);
        const tr = el('tr', { class: 'src-row' },
          el('td', { class: 'row-label src-label',
            title: rawOnly ? 'raw 전용 소스 — event DB 를 만들지 않음'
              : `매칭 파일: ${ev.matching_file || '-'}\n적용: ${fmtTs(ev.applied_ts)} · sha ${ev.matching_sha || '-'}` },
            el('span', { class: 'src-bullet' }, '●'), src,
            rawOnly ? el('span', { class: 'hint' }, 'raw 전용') : null,
            featN ? el('span', { class: 'feat-badge', title: 'feature store 산출물 수' }, `feat ${featN}`) : null,
            ev.stale ? el('span', { class: 'hint stale-tag' }, '재처리 필요') : null));
        buckets.forEach((b) => {
          // 이 버킷 [start,end] 안에 든 raw 날짜들의 단계 집계 (긴급도: 재처리>대기>완료)
          const inBucket = (st.raw[src] || []).filter((d) => d >= b.start && d <= b.end);
          let cls = 's-off', label = 'raw 없음';
          if (inBucket.length && rawOnly) {
            cls = 's-success'; label = `raw 완료 ${inBucket.length}일 (raw 전용)`;
          } else if (inBucket.length) {
            let nRaw = 0, nStale = 0, nDone = 0;
            inBucket.forEach((d) => { if (!evDates.has(d)) nRaw++; else if (ev.stale) nStale++; else nDone++; });
            if (nStale) { cls = 's-partial'; label = `event 재처리 필요 ${nStale}일`; }
            else if (nRaw) { cls = 's-raw'; label = `raw query 완료 · event 대기 ${nRaw}일`; }
            else { cls = 's-success'; label = `event 완료 ${nDone}일`; }
            label += ` · raw ${inBucket.length}일`;
          }
          tr.append(el('td', { class: `hm-cell ${cls}`, title: `${v} · ${src} · ${b.title}\n${label}` },
            cls === 's-off' ? '' : '·'));
        });
        tbody.append(tr);
      });
    });
    body.innerHTML = '';
    body.append(
      runtimeBar,
      el('div', { id: 'dbhmProg', class: 'dbhm-prog' }),
      picker,
      el('div', { class: 'hm-scroll' }, el('table', { class: 'heatmap' }, el('thead', {}, thead), tbody)),
      el('div', { class: 'row', style: { marginTop: '12px', gap: '14px', fontSize: '11px', color: 'var(--text-muted)', flexWrap: 'wrap' } },
        legendItem('s-raw', 'raw query 만 (event 대기)'),
        legendItem('s-success', 'event 완료'),
        legendItem('s-partial', 'event 재처리 필요 (matching 변경)'),
        legendItem('s-off', 'raw 없음'),
        el('span', { class: 'hint' }, '주/월 버킷은 가장 긴급한 단계 색 · feat N = 소스별 feature 수 · vehicle 헤더 클릭 = 재실행')));
    // 루프/스케줄 실행 중이거나 진행 중이면 진행상황 폴링 시작
    if (rt && (rt.loop_running || rt.scheduler_running)) { DBHM_LAST_RUNNING = true; startProgPoll(); }
    else if (!DBHM_PROG_TIMER) { const pr = await api.get('/api/pipeline/progress').catch(() => null); if (pr && pr.running) { DBHM_LAST_RUNNING = true; startProgPoll(); } }
  } catch (e) {
    const body = $('#dbhmBody');
    if (body) { body.innerHTML = ''; body.append(el('div', { class: 'empty' }, String(e.message || e))); }
  }
}

async function onRunVehicle(v) {
  if (!confirm(`${v} 파이프라인 재실행? (raw → event → feature)`)) return;
  try { await api.post(`/api/pipeline/run/${encodeURIComponent(v)}`, {}); await loadDbHeatmap(); }
  catch (e) { alert(e.message); }
}

// 워커 계획 + 전체 병렬 실행 + 스케줄/루프 토글 + 실시간 진행상황
function renderRuntimeBar(rt) {
  const p = rt.plan || {}, c = rt.config || {};
  const mem = p.total_mem_gb ? `${p.total_mem_gb}GB` : 'mem?';
  const info = el('span', { class: 'rt-info', title: `산정근거: ${p.reason || '-'} · ${p.sizing}` },
    `🖥 ${p.cpu_cores}코어 · ${mem} → `,
    el('span', { title: '전 vehicle 합친 동시 raw 쿼리 상한 (사내 API 풀 보호)' }, 'raw 동시 '),
    el('input', {
      type: 'number', class: 'rt-hours', min: '1', step: '1', value: String(p.raw_api_max ?? 3),
      title: 'raw_api_max — raw 쿼리 전역 동시 상한 (기본 3)',
      onchange: (e) => putRuntime({ raw_api_max: Math.max(1, Number(e.target.value)) }),
    }),
    ' · ',
    el('span', { title: '동시에 처리할 vehicle 수. 각 vehicle 내부는 raw→event→feature 순차 보장.' }, 'vehicle 병렬 '),
    el('input', {
      type: 'number', class: 'rt-hours', min: '1', step: '1', value: String(p.vehicle_workers ?? 1),
      title: 'vehicle_workers — 동시 처리 vehicle 수',
      onchange: (e) => putRuntime({ vehicle_workers: Math.max(1, Number(e.target.value)) }),
    }),
    ` · feature ${p.feature_workers}`,
    el('span', { class: 'hint', style: { marginLeft: '8px' } }, `(${c.raw_days || 5}일 · ${c.split_days || 1}일 분할 · vehicle별 raw→event→feature 순차 · feature=전체 event)`));

  const runBtn = el('button', { class: 'btn primary small', onclick: onRunAll }, '▶ 전체 병렬 실행');

  // 주기 스케줄 (interval_hours)
  const schedule = el('label', { class: 'rt-sched', title: '전 vehicle raw→event→feature 를 주기 실행' },
    el('input', {
      type: 'checkbox', ...(c.schedule_enabled ? { checked: 'checked' } : {}),
      onchange: (e) => putRuntime({ schedule_enabled: e.target.checked }),
    }),
    '⏱ 자동 ',
    el('input', {
      type: 'number', class: 'rt-hours', min: '0', step: '1', value: String(c.interval_hours ?? 0),
      title: 'interval_hours (0=끔)',
      onchange: (e) => putRuntime({ interval_hours: Number(e.target.value) }),
    }),
    'h');

  // 루프 실행 — 켜면 쉬지 않고 계속 반복 실행
  const loopBtn = el('button', {
    class: 'btn small rt-loop' + (c.loop_enabled ? ' on' : ''),
    title: '켜면 전 vehicle 파이프라인을 계속 반복 실행 (다시 누르면 정지)',
    onclick: () => putRuntime({ loop_enabled: !c.loop_enabled }),
  }, c.loop_enabled ? '🔁 루프 실행 중 (정지)' : '🔁 루프 실행');

  return el('div', { class: 'rt-bar' },
    info, el('span', { class: 'spacer' }), schedule, loopBtn, runBtn);
}

async function putRuntime(patch) {
  try { await api.put('/api/pipeline/runtime', patch); await loadDbHeatmap(); }
  catch (e) { alert(e.message); }
}

async function onRunAll() {
  if (!confirm('전 vehicle 을 병렬로 raw→event→feature 실행할까요?')) return;
  DBHM_LAST_RUNNING = true;
  startProgPoll();                         // 실행 중 단계 표시 → 완료 시 폴러가 heatmap 갱신
  try { await api.post('/api/pipeline/run-all', {}); }
  catch (e) { alert(e.message); }
}

// ── 실시간 진행상황 폴링 (raw/event/feature 단계 표시) ──
let DBHM_PROG_TIMER = null;
let DBHM_LAST_RUNNING = false;
const PROG_STAGE = { queued: '대기', raw: 'raw 쿼리', event: 'event DB화', feature: 'feature(전체 event)',
  wide: 'wide 병합', manual: '수동 실행', rebuild: '매칭 갱신 재생성', done: '완료', error: '오류' };

function stopProgPoll() { if (DBHM_PROG_TIMER) { clearInterval(DBHM_PROG_TIMER); DBHM_PROG_TIMER = null; } }

function startProgPoll() {
  stopProgPoll();
  DBHM_PROG_TIMER = setInterval(async () => {
    const box = $('#dbhmProg');
    if (!box) return stopProgPoll();
    let pr; try { pr = await api.get('/api/pipeline/progress'); } catch { return; }
    renderProg(box, pr);
    if (DBHM_LAST_RUNNING && !pr.running) { DBHM_LAST_RUNNING = false; loadDbHeatmap(); return; }
    if (pr.running) DBHM_LAST_RUNNING = true;
  }, 1200);
}

function renderProg(box, pr) {
  box.innerHTML = '';
  if (!pr || (!pr.running && !pr.mode)) return;
  const head = pr.running
    ? (pr.mode === 'loop' ? `🔁 루프 #${pr.loop_iter} 실행 중` : (pr.mode === 'schedule' ? '⏱ 자동 실행 중' : '⏳ 실행 중'))
    : '· 대기';
  const items = Object.entries(pr.vehicles || {}).map(([v, s]) => {
    let d = PROG_STAGE[s.stage] || s.stage || '-';
    if (s.stage === 'raw' && s.raw_total) d += ` ${s.raw_done || 0}/${s.raw_total}`;
    if (s.stage === 'feature' && s.event_dates) d += ` ${s.event_dates}일`;
    return el('span', { class: `prog-veh stage-${s.stage || ''}` }, `${v} · ${d}`);
  });
  box.append(el('span', { class: 'prog-head' }, head), ...items);
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
      ['s3.upload_mode', 'select', ['immediate','interval','manual'], 'immediate=chunk 직후 / interval=주기적 flush / manual=버튼 눌러야 업로드'],
      ['s3.upload_interval_sec', 'number', null, 'interval 모드에서만 의미. 기본 300초(5분). 최소 5초 보장'],
      ['s3.retry_failed_sec', 'number', null, '업로드 실패한 항목의 재시도 간격. 기본 120초'],
    ]},
    { key: 'schedule', label: '📅 스케줄', rows: [
      ['schedule.backfill_days', 'number', null, '오늘 + 과거 N일 (권장 3~5). 제품별 override 는 제품 탭에서.'],
      ['schedule.interval_hours', 'number', null, '자동 스케줄 (v0.2 구현)'],
      ['schedule.force_overwrite', 'bool'],
      ['schedule.tolerance_pct', 'number', null, 'completeness 허용 %. 0.5 = 0.5%'],
    ]},
    { key: 'alerts', label: '🔔 알림', rows: [
      ['alerts.enabled', 'bool', null, '전체 알람 마스터 스위치 (끄면 모든 채널 무시)'],
      ['alerts.min_severity', 'select', ['info','warn','error','critical'], '이 레벨 미만은 조용히 drop'],
      ['alerts.max_per_hour', 'number', null, '시간당 최대 알람 수. 0 이면 무제한'],
      ['alerts.dedupe_window_sec', 'number', null, '같은 (kind + chunk_id) 에 대해 이 시간 내 중복 억제. 0 이면 없음'],
      ['alerts.s3_enabled', 'bool', null, 'S3 에 알람 JSON 업로드 사용 여부'],
      ['alerts.s3_prefix', 'text', null, '알람 JSON 을 S3 에 누적할 prefix. 기본 valve-alerts'],
      ['alerts.s3_interval_min', 'number', null, '알람 S3 주기 발행 간격(분). 변경 있을 때만 업로드. 0 이면 파이프라인 실행 시에만 발행'],
      ['alerts.outbox_dir', 'text', null, '알람 업로드 폴더 (ROOT 기준). 이 폴더의 {s3_prefix} 하위만 S3 로 sync 하면 flow 매칭알람이 갱신된다. 비우면 미러링 안 함'],
      ['alerts.flow_enabled', 'bool', null, 'flow 앱에 알림 푸시 사용 여부'],
      ['alerts.flow_notify_url', 'text', null, 'flow 알림 엔드포인트 (예: http://flow/api/valve/alert)'],
      ['alerts.webhook_enabled', 'bool', null, '일반 webhook POST 사용 여부'],
      ['alerts.webhook_url', 'text', null, '범용 webhook URL'],
      ['alerts.config_prefix', 'text', null, 'S3 에서 settings/products/source_types 를 pull 해올 prefix. 기본 valve-config'],
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
let BR = { root: 'staging', path: '', selFile: '', sql: '', s3mode: 'sync', s3rules: null };

async function renderBrowser() {
  const main = $('#main');
  BR.s3rules = null;  // 탭 진입 시 전송 규칙 재조회 (편집 중에만 메모리 유지)
  main.innerHTML = '';
  main.append(
    el('div', {},
      el('div', { class: 'row', style: { alignItems: 'flex-start', gap: '10px' } },
        el('div', {},
          el('div', { class: 'section-title' }, '파일 탐색기'),
          el('div', { class: 'section-desc' }, 'staging · s3_local · config(설정파일 csv/yaml/json) · db(파이프라인 산출물) 탐색. parquet/csv 는 SQL 필터, yaml/json 은 텍스트로 열람.'),
        ),
        el('div', { class: 'spacer' }),
        el('button', { class: 'btn gear-btn', title: 'S3 업/다운로드 설정 — 항목별 key 지정 · 수동 실행/중지 · 주기',
          onclick: openS3Modal }, '⚙'),
      ),
    ),
    renderSqlGuide(),
    el('div', { class: 'split' },
      el('div', { class: 'pane' },
        el('div', { class: 'hdr' }, '📁 Roots'),
        el('div', { id: 'brS3Rules' }),
        el('div', { id: 'brConfigFiles' }),
        el('div', { id: 'brTree' }, 'loading...'),
        el('div', { id: 'brLegend' }),
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
          el('div', { class: 'empty' }, '좌측에서 파일 선택 (parquet/csv · yaml/json/txt)'),
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

// S3 연동 신호등 — 방향 배지(dir) + 상태 색점(state).
// 방향은 색·모양으로 확실히 갈라 놓는다: ↓받기(파랑) · ↑올리기(초록) · ↕양방향(보라) · 로컬(무표시)
const S3_DIR = {
  down: { arrow: '↓', label: 'S3 → Valve 다운로드' },
  up: { arrow: '↑', label: 'Valve → S3 업로드' },
  both: { arrow: '↕', label: 'S3 ↔ Valve 양방향' },
};

function syncBadge(sync) {
  if (!sync) return null;
  const d = S3_DIR[sync.dir];
  const dirTxt = d ? d.label : (sync.state === 'local' ? '로컬 전용 (S3 연동 없음)' : 'S3');
  return el('span', {
    class: `s3sig s3-${sync.state} s3dir-${sync.dir || 'none'}`,
    title: `${dirTxt}\n${sync.detail || ''}`,
  },
    el('span', { class: 'dot' }),
    d ? el('span', { class: 'arw' }, d.arrow) : null,
  );
}

// ─────────────────────────────────────
// S3 업/다운로드 항목 모달 (탐색기 ⚙)
//   항목 = 로컬 경로 ↔ S3 key 한 쌍. 방향/명령/주기를 각각 준다.
//   수동 ▶ 실행 · ■ 중지 (파일 경계에서 취소) · 5초 폴링으로 진행률
// ─────────────────────────────────────
const S3M = { open: false, tab: 'items', form: null, hist: [], timer: null, keyBrowse: null };

function openS3Modal() {
  S3M.open = true; S3M.tab = 'items'; S3M.form = null;
  renderS3Modal();
  if (S3M.timer) clearInterval(S3M.timer);
  // 항목/이력 탭만 자동 갱신 — 추가/수정 폼을 다시 그리면 입력 중인 값이 날아간다
  S3M.timer = setInterval(() => {
    if (!S3M.open) return closeS3Modal();
    if (S3M.tab !== 'add') renderS3Modal(true);
  }, 5000);
}

function closeS3Modal() {
  S3M.open = false;
  if (S3M.timer) { clearInterval(S3M.timer); S3M.timer = null; }
  document.getElementById('s3modal')?.remove();
  loadBrowserRoots();
}

async function renderS3Modal(quiet) {
  let data, dests;
  try {
    [data, dests] = await Promise.all([
      api.get('/api/s3/items'),
      api.get('/api/s3/destinations').catch(() => ({ destinations: { default: {} } })),
    ]);
  } catch (e) { if (!quiet) alert(e.message); return; }
  if (S3M.tab === 'history') {
    try { S3M.hist = (await api.get('/api/s3/history?limit=100')).history; } catch { S3M.hist = []; }
  }

  document.getElementById('s3modal')?.remove();
  const tabBtn = (id, label) => el('button', {
    class: 'seg' + (S3M.tab === id ? ' on' : ''),
    onclick: () => { S3M.tab = id; renderS3Modal(); },
  }, label);

  const body = S3M.tab === 'add' ? s3FormView(data, dests)
    : S3M.tab === 'history' ? s3HistoryView()
      : s3ItemsView(data);

  const modal = el('div', { id: 's3modal', class: 's3-modal-back',
    onclick: (e) => { if (e.target.id === 's3modal') closeS3Modal(); } },
    el('div', { class: 's3-modal' },
      el('div', { class: 'row', style: { gap: '8px', alignItems: 'center', marginBottom: '10px' } },
        el('span', { style: { fontWeight: 800 } }, '⚙ S3 업/다운로드'),
        tabBtn('items', `항목 ${data.items.length}`), tabBtn('add', '+ 추가'), tabBtn('history', '이력'),
        el('div', { class: 'spacer' }),
        el('button', { class: 'btn', onclick: closeS3Modal }, '닫기'),
      ),
      body,
    ));
  document.body.append(modal);
}

async function s3Post(path, body) {
  try { return await api.post(path, body); } catch (e) { alert(e.message); return null; }
}

function s3ItemsView(data) {
  const pill = (on, label, patch) => el('button', {
    class: 'seg' + (on ? ' on' : ''),
    title: '자동(주기) 실행 마스터 스위치 — 끄면 수동 실행만 된다',
    onclick: async () => { await s3Post('/api/s3/auto-sync', patch); renderS3Modal(); },
  }, `${label} ${on ? 'ON' : 'OFF'}`);

  const rows = data.items.map((it) => {
    const busy = it.is_running || it.is_queued;
    const p = it.progress;
    const st = it.status || {};
    const stateTxt = it.is_running ? (p ? `실행 중 ${p.done}/${p.total}` : '실행 중')
      : it.is_queued ? '대기'
        : ({ ok: '성공', error: '실패', cancelled: '중지됨', running: '중단?' }[st.last_status] || '—');
    const stateColor = it.is_running ? '#3b82f6' : it.is_queued ? '#f59e0b'
      : st.last_status === 'ok' ? '#30a46c' : st.last_status === 'error' ? '#e5484d' : 'var(--text-muted)';
    return el('tr', {},
      el('td', { class: 'mono', style: { color: it.direction === 'download' ? '#1d4ed8' : '#15803d', fontWeight: 700 } },
        it.direction === 'download' ? '↓ 받기' : '↑ 올리기'),
      el('td', { class: 'mono' }, it.id),
      el('td', { class: 'mono', style: { fontSize: '11px' } }, `${it.root}/${it.target || ''}`),
      el('td', { class: 'mono', style: { fontSize: '11px' } }, it.key),
      el('td', {}, it.mode),
      el('td', { class: 'mono' }, it.interval_min ? `${it.interval_min}분` : '수동'),
      el('td', { style: { color: stateColor, whiteSpace: 'nowrap' },
        title: p?.current ? `현재: ${p.current}` : (st.error || '') },
        stateTxt,
        st.last_status === 'ok' && !busy && st.moved !== undefined
          ? el('span', { class: 'hint' }, ` ${st.moved}건`) : null),
      el('td', { style: { display: 'flex', gap: '4px' } },
        busy
          ? el('button', { class: 'btn small', style: { color: '#e5484d' },
              onclick: async () => { await s3Post('/api/s3/stop', { id: it.id }); renderS3Modal(); } }, '■ 중지')
          : el('button', { class: 'btn small primary',
              onclick: async () => { await s3Post('/api/s3/run', { id: it.id }); renderS3Modal(); } }, '▶ 실행'),
        el('button', { class: 'btn small', onclick: () => { S3M.form = { ...it }; S3M.tab = 'add'; renderS3Modal(); } }, '수정'),
        el('button', { class: 'btn small', onclick: async () => {
          if (!confirm(`${it.id} 삭제?`)) return;
          await s3Post('/api/s3/delete', { id: it.id }); renderS3Modal();
        } }, '✕'),
      ),
    );
  });

  return el('div', {},
    el('div', { class: 'row', style: { gap: '8px', marginBottom: '8px', fontSize: '11px' } },
      pill(data.auto_download_enabled, '⬇ 자동 다운로드', { auto_download_enabled: !data.auto_download_enabled }),
      pill(data.auto_upload_enabled, '⬆ 자동 업로드', { auto_upload_enabled: !data.auto_upload_enabled }),
      el('span', { class: 'hint' }, '주기가 0(수동)이면 마스터가 켜져 있어도 자동 실행되지 않는다'),
    ),
    el('div', { style: { maxHeight: '52vh', overflow: 'auto' } },
      alTable(['방향', 'id', '로컬', 'S3 key', '명령', '주기', '상태', '동작'], rows)),
  );
}

function s3FormView(data, dests) {
  const f = S3M.form || { id: '', direction: 'download', root: 'config', target: '',
    dest: 'default', key: '', mode: 'sync', interval_min: 0, enabled: true, note: '' };
  const set = (k, v) => { f[k] = v; S3M.form = f; };
  const inp = (k, ph, w) => el('input', { type: 'text', class: 'mono', value: f[k] ?? '',
    placeholder: ph, style: { width: w || '260px', fontSize: '11px' },
    oninput: (e) => set(k, e.target.value) });
  const row = (label, ...kids) => el('div', { class: 'cfg-row', style: { gap: '8px', alignItems: 'center' } },
    el('span', { style: { flex: '0 0 92px', fontSize: '11px', color: 'var(--text-muted)' } }, label), ...kids);
  const seg = (k, vals) => el('span', { class: 'cfg-mode' }, ...vals.map(([v, lab]) =>
    el('button', { class: 'seg' + (f[k] === v ? ' on' : ''),
      onclick: () => { set(k, v); renderS3Modal(); } }, lab)));

  // S3 key 고르기 — prefix 를 훑어 실제 존재하는 key 를 눌러서 선택
  const keyBox = el('div', { class: 'mono', style: { fontSize: '11px', maxHeight: '150px',
    overflow: 'auto', border: '1px solid var(--border)', borderRadius: '4px', padding: '4px',
    display: S3M.keyBrowse ? 'block' : 'none' } });
  const renderKeys = (b) => {
    keyBox.innerHTML = '';
    keyBox.style.display = 'block';
    keyBox.append(el('div', { class: 'hint' }, `prefix: ${b.prefix || '(루트)'} · ${b.keys.length}개`));
    if (b.prefix) keyBox.append(el('div', { class: 'clickable', style: { color: 'var(--text-muted)' },
      onclick: () => browseKeys(b.prefix.replace(/\/?[^/]+\/?$/, '')) }, '⬆ 상위'));
    b.folders.forEach((d) => keyBox.append(el('div', { class: 'clickable', style: { color: '#3b82f6' },
      onclick: () => browseKeys(d) }, `📁 ${d}/`)));
    b.keys.filter((k) => !b.folders.some((d) => k.startsWith(d + '/'))).forEach((k) =>
      keyBox.append(el('div', { class: 'clickable',
        onclick: () => { set('key', k); renderS3Modal(); } }, `📄 ${k}`)));
  };
  const browseKeys = async (prefix) => {
    try {
      const b = await api.get(`/api/s3/browse-keys?dest=${encodeURIComponent(f.dest)}&prefix=${encodeURIComponent(prefix || '')}`);
      S3M.keyBrowse = b; renderKeys(b);
    } catch (e) { alert(e.message); }
  };
  // key 를 골라 폼이 다시 그려진 뒤에도 탐색 목록이 그대로 남아야 한다
  if (S3M.keyBrowse) renderKeys(S3M.keyBrowse);

  return el('div', {},
    row('방향', seg('direction', [['download', '⬇ 받기 (S3→Valve)'], ['upload', '⬆ 올리기 (Valve→S3)']])),
    row('id', inp('id', '영문/숫자/_/- 64자', '200px'),
      el('span', { class: 'hint' }, '항목 식별자 — 중복 불가')),
    row('로컬 root', seg('root', (data.roots || ['config']).map((r) => [r, r]))),
    row('로컬 경로', inp('target', 'root 기준 상대경로 (비우면 root 전체)', '320px'),
      el('span', { class: 'hint' }, '파일 또는 폴더')),
    row('S3 연결', el('select', { style: { fontSize: '11px' }, onchange: (e) => set('dest', e.target.value) },
      ...Object.keys(dests.destinations || { default: {} }).map((n) =>
        el('option', n === f.dest ? { value: n, selected: '' } : { value: n }, n)))),
    row('S3 key', inp('key', 'prefix/파일.csv', '320px'),
      el('button', { class: 'btn small', onclick: () => browseKeys(f.key.split('/').slice(0, -1).join('/')) }, '🔍 S3 에서 고르기')),
    el('div', { style: { paddingLeft: '100px' } }, keyBox),
    row('명령', seg('mode', [['sync', 'sync (변경분만)'], ['cp', 'cp (항상 덮어쓰기)']])),
    row('주기(분)', el('input', { type: 'number', min: '0', value: String(f.interval_min ?? 0),
      style: { width: '70px', fontSize: '11px' }, onchange: (e) => set('interval_min', Number(e.target.value)) }),
      el('span', { class: 'hint' }, '0 = 수동 전용')),
    row('활성화', el('input', Object.assign({ type: 'checkbox', onchange: (e) => set('enabled', e.target.checked) },
      f.enabled ? { checked: '' } : {}))),
    row('메모', inp('note', '(선택)', '320px')),
    el('div', { class: 'mono', style: { fontSize: '11px', color: 'var(--text-muted)', margin: '8px 0 0 100px' } },
      f.direction === 'download'
        ? `s3://<${f.dest}>/${f.key}  →  ${f.root}/${f.target || ''}`
        : `${f.root}/${f.target || ''}  →  s3://<${f.dest}>/${f.key}`),
    el('div', { class: 'row', style: { gap: '6px', marginTop: '10px', paddingLeft: '100px' } },
      el('button', { class: 'btn primary', onclick: async () => {
        const r = await s3Post('/api/s3/save', f);
        if (r) { S3M.form = null; S3M.keyBrowse = null; S3M.tab = 'items'; renderS3Modal(); }
      } }, '💾 저장'),
      el('button', { class: 'btn', onclick: () => { S3M.form = null; S3M.keyBrowse = null; S3M.tab = 'items'; renderS3Modal(); } }, '취소')),
  );
}

function s3HistoryView() {
  const rows = (S3M.hist || []).map((h) => el('tr', {},
    el('td', { class: 'mono' }, fmtRunTs(h.ts)),
    el('td', { class: 'mono' }, h.id),
    el('td', { style: { color: h.status === 'ok' ? '#30a46c' : h.status === 'cancelled' ? '#f5a524' : '#e5484d' } },
      ({ ok: '성공', error: '실패', cancelled: '중지됨' }[h.status] || h.status)),
    el('td', { class: 'mono' }, `${h.duration_sec ?? '-'}s`),
    el('td', { class: 'mono' }, h.direction || '-'),
    el('td', { class: 'mono' }, `옮김 ${h.moved ?? 0} · 생략 ${h.skipped ?? 0} · 실패 ${h.failed ?? 0}`),
    el('td', { style: { color: '#e5484d', fontSize: '11px' } }, h.error || ''),
  ));
  return el('div', { style: { maxHeight: '56vh', overflow: 'auto' } },
    alTable(['시각', 'id', '결과', '소요', '방향', '건수', '오류'], rows));
}

// 탐색기 하단 범례 — 화살표가 무슨 뜻인지 화면에서 바로 읽히게
function syncLegend() {
  const item = (dir, state, text) => el('span', { class: 'row', style: { gap: '4px', alignItems: 'center' } },
    syncBadge({ dir, state, detail: '' }), text);
  return el('div', { class: 'row', style: { marginTop: '10px', gap: '14px', fontSize: '11px',
    color: 'var(--text-muted)', flexWrap: 'wrap' } },
    item('down', 'ok', '받기 — flow 가 관리 (Vehicle_matching · ppid_knob · inline_matching …)'),
    item('up', 'ok', '올리기 — Valve 산출물 (staging · 알람 outbox)'),
    item('both', 'ok', '양방향 — settings · products · source_types'),
    item(null, 'local', '로컬 전용 — S3 자동 동기화 없음'),
    el('span', { class: 'hint' }, '점 색 = 상태 (초록 정상 · 주황 대기/미수신 · 빨강 실패)'),
  );
}

async function loadBrowserRoots() {
  try {
    const { roots } = await api.get('/api/browser/roots');
    const tree = $('#brTree');
    tree.innerHTML = '';
    for (const r of roots) {
      const sig = r.dir ? { dir: r.dir, state: r.dir === 'down' ? 'ok' : 'ok', detail: r.detail } : null;
      tree.append(el('div', { class: 'tree-item', style: { fontWeight: 800 }, onclick: () => loadBrowserDir(r.name, '') },
        syncBadge(sig),
        el('span', { class: 'ic' }, '▸'),
        r.name,
        el('span', { class: 'sz' }, r.path),
      ));
    }
    loadBrowserDir(BR.root, BR.path);
  } catch (e) { $('#brTree').textContent = String(e); }
  const lg = $('#brLegend');
  if (lg) { lg.innerHTML = ''; lg.append(syncLegend()); }
  loadS3RulesSection();
  loadConfigFilesSection();
}

// S3 전송 규칙 — root 별 mode(cp/sync) + 타겟(S3 연결 × prefix 이름, 2개 이상 가능) 편집.
// 설정파일은 보통 cp(항상 덮어쓰기), DB 산출물은 sync(변경분만).
async function loadS3RulesSection() {
  const box = $('#brS3Rules');
  if (!box) return;
  if (!BR.s3rules) {
    try { BR.s3rules = await api.get('/api/browser/s3-transfer/config'); } catch { return; }
    if (BR.s3rules.rules?.config) BR.s3mode = BR.s3rules.rules.config.mode;  // 설정파일 빠른 목록 기본 모드
  }
  box.innerHTML = '';
  const { rules, destinations } = BR.s3rules;
  const destNames = () => Object.keys(destinations);
  const inp = (val, ph, on, grow) => el('input', { type: 'text', class: 'mono', value: val ?? '',
    placeholder: ph, style: { flex: grow ? 1 : '0 0 90px', minWidth: 0, fontSize: '11px' },
    oninput: (e) => on(e.target.value) });

  // ── S3 연결 (destinations) — default 는 settings.json 고정, 나머지는 key 별 추가/삭제 ──
  const destRow = (name) => {
    const d = destinations[name];
    if (d.builtin) return el('div', { class: 'cfg-row' },
      el('span', { class: 'cfg-name', style: { flex: '0 0 90px', fontWeight: 700 } }, name),
      el('span', { style: { fontSize: '11px', color: 'var(--text-muted)' } }, 'settings.json 의 S3 (기본 연결)'));
    return el('div', { class: 'cfg-row', style: { flexWrap: 'wrap' } },
      inp(name, '이름', (v) => { if (v && v !== name) { destinations[v] = d; delete destinations[name]; loadS3RulesSection(); } }),
      inp(d.bucket, 'bucket', (v) => { d.bucket = v; }),
      inp(d.endpoint_url, 'endpoint_url', (v) => { d.endpoint_url = v; }, true),
      inp(d.access_key, 'access key', (v) => { d.access_key = v; }),
      inp(d.secret_key, 'secret key', (v) => { d.secret_key = v; }),
      el('button', { class: 'btn ghost xsmall', title: '연결 삭제', onclick: () => {
        delete destinations[name];
        Object.values(rules).forEach((r) => { r.targets = r.targets.filter((t) => t.dest !== name); });
        loadS3RulesSection();
      } }, '✕'));
  };

  // ── 규칙 — root 별 mode + 타겟 목록 (연결 선택 × prefix 이름) ──
  const modeSeg = (rule, m) => el('button', {
    class: 'seg' + (rule.mode === m ? ' on' : ''),
    title: m === 'sync' ? '변경분만 업로드 (텍스트=내용·바이너리=크기 비교)' : '항상 덮어쓰기 업로드',
    onclick: () => { rule.mode = m; loadS3RulesSection(); },
  }, m);
  const targetRow = (rule, t, i) => el('div', { class: 'cfg-row', style: { paddingLeft: '58px' } },
    el('select', { class: 'mono', style: { flex: '0 0 90px', fontSize: '11px' },
      onchange: (e) => { t.dest = e.target.value; } },
      ...destNames().map((n) => el('option', { value: n, selected: t.dest === n ? 'selected' : undefined }, n))),
    inp(t.prefix, 'S3 prefix (이름)', (v) => { t.prefix = v; }, true),
    rule.targets.length > 1 ? el('button', { class: 'btn ghost xsmall', title: '타겟 삭제',
      onclick: () => { rule.targets.splice(i, 1); loadS3RulesSection(); } }, '✕') : null);
  const ruleRows = (root) => {
    const rule = rules[root];
    return [
      el('div', { class: 'cfg-row' },
        el('span', { class: 'cfg-name', style: { flex: '0 0 52px', fontWeight: 700 } }, root),
        el('span', { class: 'cfg-mode' }, modeSeg(rule, 'cp'), modeSeg(rule, 'sync')),
        el('span', { class: 'spacer' }),
        el('button', { class: 'btn ghost xsmall', title: '전송 대상(S3 연결×이름) 추가',
          onclick: () => { rule.targets.push({ dest: 'default', prefix: '' }); loadS3RulesSection(); } }, '＋ 대상'),
        el('button', { class: 'btn ghost xsmall', title: `${root} 전체를 ${rule.mode} 로 전 타겟에 전송`,
          onclick: async (e) => {
            const btn = e.target;
            btn.disabled = true; btn.textContent = '전송 중…';
            try {
              const r = await api.post('/api/browser/s3-transfer', { root, path: '', mode: rule.mode });
              btn.textContent = `↑${r.uploaded} =${r.unchanged}${r.errors ? ` ✗${r.errors}` : ''}`;
              if (r.errors) alert(`${root}: ${r.errors}건 전송 실패`);
            } catch (err) {
              btn.textContent = '⇧ 전송'; alert(err.message);
            } finally {
              btn.disabled = false;
              setTimeout(() => { btn.textContent = '⇧ 전송'; }, 4000);
            }
          } }, '⇧ 전송')),
      ...rule.targets.map((t, i) => targetRow(rule, t, i)),
    ];
  };

  box.append(el('details', { class: 'cfg-sec' },
    el('summary', {},
      el('span', {}, '☁ S3 전송 규칙'),
      el('span', { class: 'spacer' }),
      el('button', { class: 'btn ghost xsmall', onclick: async (e) => {
        e.stopPropagation(); e.preventDefault();
        const btn = e.target;
        try {
          const r = await api.put('/api/browser/s3-transfer/config', { rules, destinations });
          BR.s3rules = { rules: r.rules, destinations: r.destinations };
          btn.textContent = '✓ 저장됨';
          setTimeout(() => { btn.textContent = '💾 저장'; loadS3RulesSection(); }, 1200);
        } catch (err) { alert(err.message); }
      } }, '💾 저장')),
    el('div', { class: 'cfg-list' },
      el('div', { style: { fontSize: '11px', color: 'var(--text-muted)', padding: '2px 0' } },
        'S3 연결 (key 여러 개 등록 가능)'),
      ...destNames().map(destRow),
      el('div', { class: 'cfg-row' },
        el('button', { class: 'btn ghost xsmall', onclick: () => {
          let n = 2; while (destinations[`s3_${n}`]) n++;
          destinations[`s3_${n}`] = { bucket: '', endpoint_url: '', access_key: '', secret_key: '' };
          loadS3RulesSection();
        } }, '＋ S3 연결 추가')),
      el('div', { style: { fontSize: '11px', color: 'var(--text-muted)', padding: '6px 0 2px' } },
        '전송 규칙 (root 별 — 타겟 여러 개면 전부 전송)'),
      ...Object.keys(rules).flatMap(ruleRows)),
  ));
}

// 최상위 "설정파일 → S3" 빠른 목록 — 각 파일 개별 전송 (sync/cp 선택)
async function loadConfigFilesSection() {
  const box = $('#brConfigFiles');
  if (!box) return;
  let files = [];
  try { files = (await api.get('/api/browser/config-files')).files || []; } catch { }
  box.innerHTML = '';
  if (!files.length) return;
  const modeBtn = (m) => el('button', {
    class: 'seg' + (BR.s3mode === m ? ' on' : ''),
    title: m === 'sync' ? '내용 다를 때만 업로드' : '항상 덮어쓰기 업로드',
    onclick: () => { BR.s3mode = m; loadConfigFilesSection(); },
  }, m);
  const list = el('div', { class: 'cfg-list' },
    ...files.map((f) => el('div', { class: 'cfg-row' },
      syncBadge(f.sync),
      el('span', { class: 'cfg-name', title: `→ s3://${f.s3_key}`,
        onclick: () => { BR.root = 'config'; selectFile('config', f.rel); } }, f.rel),
      el('button', { class: 'btn ghost xsmall', title: `S3 전송 (${BR.s3mode}) → ${f.s3_key}`,
        onclick: () => onS3Transfer('config', f.rel) }, '⇧ S3'),
    )));
  box.append(el('details', { class: 'cfg-sec', open: 'open' },
    el('summary', {},
      el('span', {}, '⚙ 설정파일 → S3'),
      el('span', { class: 'spacer' }),
      el('span', { class: 'cfg-mode', onclick: (e) => e.stopPropagation() }, modeBtn('sync'), modeBtn('cp'))),
    list));
}

async function onS3Transfer(root, path) {
  try {
    const r = await api.post('/api/browser/s3-transfer', { root, path, mode: BR.s3mode });
    await loadConfigFilesSection();  // 신호등 갱신
    console.log(`s3-transfer ${path} [${r.mode}] → ${r.status} (${r.s3_key})`);
    if (r.status === 'error') alert(`전송 실패: ${path}`);
  } catch (e) { alert(e.message); }
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
      const icon = e.is_dir ? '📁' : (e.suffix === '.parquet' ? '📊' : (e.suffix === '.csv' ? '🧾'
        : (['.yaml', '.yml', '.json'].includes(e.suffix) ? '⚙️' : '📄')));
      const cls = BR.selFile === fullPath ? 'tree-item sel' : 'tree-item';
      // db/staging 은 파일·폴더 단위 S3 전송 지원 (폴더=재귀, 모드는 전송 규칙 기본)
      const canSend = (root === 'db' || root === 'staging');
      tree.append(el('div', { class: cls, onclick: () => e.is_dir ? loadBrowserDir(root, fullPath) : selectFile(root, fullPath) },
        syncBadge(e.sync),
        el('span', { class: 'ic' }, icon),
        e.name,
        el('span', { class: 'sz' }, e.is_dir ? '' : fmtBytes(e.size)),
        canSend ? el('button', { class: 'btn ghost xsmall', title: `S3 전송 (규칙 기본 모드)`,
          onclick: async (ev) => {
            ev.stopPropagation();
            const btn = ev.target;
            btn.disabled = true; btn.textContent = '…';
            try {
              const r = await api.post('/api/browser/s3-transfer', { root, path: fullPath });
              btn.textContent = r.files ? `↑${r.uploaded} =${r.unchanged}` : '✓';
              if (r.errors) alert(`${fullPath}: ${r.errors}건 전송 실패`);
            } catch (err) { btn.textContent = '✗'; alert(err.message); }
            finally { btn.disabled = false; setTimeout(() => { btn.textContent = '⇧ S3'; }, 4000); }
          } }, '⇧ S3') : null,
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
    // yaml/json/txt/md 등 설정파일은 텍스트로 표시
    if (r.kind === 'text') {
      view.append(
        el('div', { style: { padding: '10px 14px', fontSize: '12px', color: 'var(--text-secondary)', borderBottom: '1px solid var(--border)' } },
          el('span', { class: 'mono' }, BR.selFile),
          r.truncated ? el('span', { style: { color: 'var(--text-muted)', marginLeft: '8px' } }, '· 일부만 표시') : null,
        ),
        el('pre', { class: 'text-view' }, r.text),
      );
      return;
    }
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
// alerts tab — 파이프라인 리포트 (미매칭 step · KNOB RO · event 처리 현황)
// ─────────────────────────────────────
const AL_HAIR = '1px solid var(--border)';

const alSub = (t, d) => el('div', { style: { margin: '12px 0 4px' } },
  el('span', { style: { fontWeight: 700, fontSize: '12px' } }, t),
  d ? el('span', { style: { color: 'var(--text-muted)', fontSize: '11px', marginLeft: '8px' } }, d) : null);

function alTable(headers, rows) {
  return el('table', { class: 'tbl' },
    el('thead', {}, el('tr', {}, ...headers.map((h) => el('th', {}, h)))),
    el('tbody', {}, rows.length ? rows
      : el('tr', {}, el('td', { colspan: String(headers.length), style: { color: 'var(--text-muted)' } }, '없음'))),
  );
}

async function renderAlerts() {
  const main = $('#main');
  main.innerHTML = '';
  main.append(
    el('div', {},
      el('div', { class: 'section-title' }, '알람'),
      el('div', { class: 'section-desc' },
        'FAB 미매칭 step · KNOB 미변환(RO raw ppid) · event 처리 현황. '
        + '설정파일(config/step_matching · feature_rules · pipeline.yaml)은 탐색기 config 루트에서 열람 가능.'),
    ),
    el('div', { id: 'alWrap' }, el('div', { class: 'loading' }, 'Loading…')),
  );
  await loadAlerts();
}

async function loadAlerts() {
  const wrap = $('#alWrap');
  if (!wrap) return;
  try {
    const [status, alerts, cfg, csvInfo, outbox, sched, runs] = await Promise.all([
      api.get('/api/pipeline/status'),
      api.get('/api/pipeline/alerts'),
      api.get('/api/pipeline/config'),
      api.get('/api/pipeline/csv-sync'),
      api.get('/api/pipeline/alerts/outbox').catch(() => null),
      api.get('/api/pipeline/schedule').catch(() => null),
      api.get('/api/pipeline/runs?limit=60').catch(() => null),
    ]);
    wrap.innerHTML = '';

    // ── 처리 현황 (vehicle 별 한 줄 — 주기 조절 포함)
    wrap.append(alSub('파이프라인 처리 현황',
      'raw → event → feature · vehicle_matching 변경 시 재처리 필요 표시 · 제품별 실행 주기 조절'));
    if (sched && !sched.master_enabled) {
      wrap.append(el('div', { class: 'alert warn', style: { fontSize: '11px', margin: '4px 0' } },
        '자동 실행 마스터 스위치가 꺼져 있습니다 — 제품 주기와 무관하게 스케줄이 돌지 않습니다 (모니터 탭 ⏱).'));
    }
    Object.keys(status).forEach((v) => wrap.append(
      alStatusLine(v, status[v], sched?.vehicles?.[v], sched?.summary?.[v])));
    if (runs) wrap.append(alRunLog(runs));

    // ── 통합 알람 리스트
    const toggle = el('label', { style: { fontSize: '12px', color: 'var(--text-muted)', display: 'flex', gap: '5px', alignItems: 'center', marginLeft: 'auto' } },
      el('input', Object.assign({ type: 'checkbox', onchange: (e) => { AL_SHOW_SUPPRESSED = e.target.checked; loadAlerts(); } },
        AL_SHOW_SUPPRESSED ? { checked: '' } : {})),
      '억제된 알람 포함');
    wrap.append(el('div', { style: { display: 'flex', alignItems: 'baseline', gap: '10px', borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
      el('span', { style: { fontWeight: 700, fontSize: '12px' } }, '알람'),
      el('span', { style: { fontSize: '12px', color: 'var(--text-muted)' } },
        `활성 ${alerts.active} · 억제 ${alerts.suppressed} — 상태 변경은 S3 ack.json 으로 flow 와 공유`),
      toggle,
    ));
    wrap.append(alAlertTable(alerts));

    if (outbox) wrap.append(alOutbox(outbox));
    wrap.append(alExcludeEditor(cfg));
    wrap.append(alCsvSync(csvInfo));
  } catch (e) {
    wrap.innerHTML = '';
    wrap.append(el('div', { class: 'alert err' }, String(e.message || e)));
  }
}

// vehicle 처리 현황 한 줄 (raw → event → feature · stale 감지 · 실행 버튼)
function alStatusLine(v, st, sc, sum) {
  const line = el('div', { style: { display: 'flex', alignItems: 'center', gap: '10px', padding: '5px 0', borderBottom: '1px solid var(--border-weak, rgba(128,128,128,.15))', fontSize: '12px' } });
  line.append(
    el('span', { style: { fontWeight: 800, minWidth: '90px' } }, v),
    el('span', { class: 'mono', style: { color: 'var(--text-muted)', minWidth: '54px' } }, st?.product || ''),
  );
  if (st) {
    const ev = st.event;
    const srcs = Object.keys(st.raw);
    const evSrcs = srcs.filter((s) => s in (ev || {}));      // raw 전용 소스(ET)는 event 현황 제외
    const rawOnlySrcs = srcs.filter((s) => !(s in (ev || {})));
    const evTxt = evSrcs.map((s) => `${s} ${(ev[s]?.dates || []).length}/${(st.raw[s] || []).length}`).join(' · ');
    const staleSrcs = evSrcs.filter((s) => ev[s]?.stale);
    const pendingSrcs = evSrcs.filter((s) => (ev[s]?.pending || []).length);
    line.append(el('span', {}, `event ${evTxt}`));
    if (rawOnlySrcs.length) line.append(el('span', { style: { color: 'var(--text-muted)' } },
      rawOnlySrcs.map((s) => `${s} raw ${(st.raw[s] || []).length}일 (raw 전용)`).join(' · ')));
    if (pendingSrcs.length) line.append(el('span', { style: { color: '#e5484d' } }, `미처리 ${pendingSrcs.join(', ')}`));
    if (staleSrcs.length) line.append(el('span', { style: { color: '#e5484d' } }, `매칭 변경 — 재처리 필요 (${staleSrcs.join(', ')})`));
    if (!pendingSrcs.length && !staleSrcs.length && evSrcs.some((s) => (ev[s]?.dates || []).length)) {
      line.append(el('span', { style: { color: '#30a46c' } }, '최신'));
    }
    line.append(el('span', { style: { color: 'var(--text-muted)' } }, '|'));
    line.append(el('span', { style: { color: 'var(--text-secondary)' } },
      `feature ${Object.entries(st.features).map(([k, n]) => `${k} ${n}`).join(' · ')}`));
  }
  line.append(el('span', { style: { marginLeft: 'auto' } }), alSchedCell(v, sc, sum));
  line.append(el('button', { class: 'btn', onclick: async (ev) => {
    const b = ev.target; b.disabled = true; b.textContent = '실행 중…';
    try { await api.post(`/api/pipeline/run/${encodeURIComponent(v)}`); }
    catch (e) { alert(e.message); }
    loadAlerts();
  } }, '▶ 실행'));
  return line;
}

// 제품별 실행 주기 — 하루 N회. 비우면 전역 interval_hours 를 따르고 0 이면 자동 실행 제외.
function alSchedCell(v, sc, sum) {
  if (!sc) return el('span', {});
  const inp = el('input', {
    type: 'number', min: '0', max: '48', step: '1',
    value: sc.source === 'vehicle' ? String(sc.runs_per_day) : '',
    placeholder: String(sc.runs_per_day || 0),
    style: { width: '46px', fontSize: '11px' },
    title: '하루 실행 횟수 (3=8시간 간격 · 6=4시간 간격 · 0=자동 실행 안 함)\n'
         + '비우면 전역 interval_hours 를 따른다',
    onchange: async (e) => {
      try {
        await api.put(`/api/pipeline/schedule/${encodeURIComponent(v)}`,
          { runs_per_day: e.target.value === '' ? null : Number(e.target.value) });
      } catch (err) { alert(err.message); }
      loadAlerts();
    },
  });
  const period = sc.interval_sec > 0 ? `${sc.interval_hours}h 간격` : '자동 실행 안 함';
  const next = !sc.enabled ? '—'
    : (sc.due ? '곧 실행' : new Date(sc.next_ts * 1000).toLocaleString('ko-KR',
        { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }));
  const bits = [el('span', { style: { color: 'var(--text-muted)' } }, '일'), inp,
    el('span', { style: { color: 'var(--text-muted)' } }, `회 · ${period}`),
    el('span', { title: '다음 자동 실행 예정', style: { color: sc.due ? '#f5a524' : 'var(--text-muted)' } },
      `→ ${next}`)];
  if (sc.source === 'global') {
    bits.push(el('span', { class: 'hint', title: '제품 개별 설정 없음 — 전역 주기를 따름' }, '전역'));
  }
  if (sum && sum.failed) {
    bits.push(el('span', { style: { color: '#e5484d' }, title: sum.last_error || '' },
      `최근 ${sum.runs}회 중 실패 ${sum.failed}`));
  }
  return el('span', { style: { display: 'flex', gap: '5px', alignItems: 'center', fontSize: '11px' } }, ...bits);
}

// 통합 알람 테이블 — 한 행 = 한 알람. 유형은 색으로 구분 (미매칭 step 빨강 · RO ppid 주황)
const AL_TYPE = {
  unmatched_step: { label: '미매칭 step', color: '#e5484d' },
  ro_ppid: { label: 'RO ppid', color: '#f5a524' },
};
let AL_SHOW_SUPPRESSED = false;

function alAlertTable(data) {
  const rows = data.alerts
    .filter((a) => AL_SHOW_SUPPRESSED || a.status === 'active')
    .map((a) => {
      const t = AL_TYPE[a.type] || { label: a.type, color: 'inherit' };
      const sel = el('select', { style: { fontSize: '11px' }, onchange: async (ev) => {
        await api.put('/api/pipeline/alerts/ack', { id: a.id, status: ev.target.value });
        loadAlerts();
      } }, ...['active', '미확인예정', '반영불필요'].map((s) =>
        el('option', s === a.status ? { value: s, selected: '' } : { value: s }, s)));
      return el('tr', { style: a.status === 'active' ? {} : { opacity: 0.45 } },
        el('td', { style: { color: t.color, fontWeight: 700, whiteSpace: 'nowrap' } }, t.label),
        el('td', { class: 'mono' }, a.vehicle),
        el('td', { class: 'mono' }, a.product),
        el('td', { class: 'mono', style: { color: t.color } }, a.step_id),
        el('td', {}, a.step_desc || ''),
        el('td', { class: 'mono', style: a.ppid ? { color: t.color, fontWeight: 700 } : {} }, a.ppid || '-'),
        el('td', { class: 'mono', style: { fontSize: '11px', color: 'var(--text-muted)' } }, a.split || '-'),
        el('td', { class: 'mono' }, a.eqp_id || '-'),
        el('td', { class: 'mono' }, a.eqp_model || '-'),
        el('td', { class: 'mono' }, String(a.n_lots || '')),
        el('td', { class: 'mono' }, String(a.rows || '')),
        el('td', {}, sel),
      );
    });
  return alTable(
    ['유형', 'vehicle', 'product', 'step_id', 'step_desc', 'ppid', 'split', 'eqp_id', 'eqp_model', 'lots', 'rows', '상태'],
    rows,
  );
}

// ── 실행 로그 — 제품별 1회 실행 = 1행, 펼치면 단계(raw/event/feature/wide)별 상세
let AL_RUN_FILTER = '';       // vehicle 필터 ('' = 전체)
let AL_RUN_FAILED = false;

const RUN_STAGE_LABEL = { raw: 'raw', event: 'event', feature: 'feature', wide: 'wide' };
const fmtRunTs = (ts) => ts ? new Date(ts * 1000).toLocaleString('ko-KR',
  { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '-';
const RUN_MODE = { schedule: '자동', manual: '수동', loop: '루프', rebuild: '매칭갱신' };

function alRunLog(data) {
  const runs = (data.runs || [])
    .filter((r) => (!AL_RUN_FILTER || r.vehicle === AL_RUN_FILTER))
    .filter((r) => (!AL_RUN_FAILED || !r.ok));
  const vehicles = [...new Set((data.runs || []).map((r) => r.vehicle))];

  const sel = el('select', { style: { fontSize: '11px' },
    onchange: (e) => { AL_RUN_FILTER = e.target.value; loadAlerts(); } },
    el('option', AL_RUN_FILTER === '' ? { value: '', selected: '' } : { value: '' }, '전체 제품'),
    ...vehicles.map((v) => el('option', v === AL_RUN_FILTER ? { value: v, selected: '' } : { value: v }, v)));
  const failChk = el('label', { style: { display: 'flex', gap: '4px', alignItems: 'center', fontSize: '11px' } },
    el('input', Object.assign({ type: 'checkbox', onchange: (e) => { AL_RUN_FAILED = e.target.checked; loadAlerts(); } },
      AL_RUN_FAILED ? { checked: '' } : {})), '실패만');

  const rows = runs.map((r) => {
    const st = r.stages || {};
    const bar = ['raw', 'event', 'feature', 'wide'].map((k) => {
      const s = st[k];
      if (!s) return el('span', { class: 'hint', title: `${k} 미도달` }, `${RUN_STAGE_LABEL[k]} –`);
      return el('span', { class: 'mono', title: runStageTip(k, s) },
        `${RUN_STAGE_LABEL[k]} ${s.sec}s`);
    });
    const detail = el('div', { class: 'mono', style: { display: 'none', fontSize: '11px',
      whiteSpace: 'pre-wrap', color: 'var(--text-secondary)', padding: '6px 0 8px 12px',
      borderLeft: '2px solid var(--border)' } }, runDetailText(r));
    const head = el('tr', { style: { cursor: 'pointer' },
      onclick: () => { detail.style.display = detail.style.display === 'none' ? 'block' : 'none'; } },
      el('td', { class: 'mono' }, fmtRunTs(r.ts)),
      el('td', { class: 'mono', style: { fontWeight: 700 } }, r.vehicle || '-'),
      el('td', {}, RUN_MODE[r.mode] || r.mode || '-'),
      el('td', { style: { color: r.ok ? '#30a46c' : '#e5484d', fontWeight: 700 } }, r.ok ? '성공' : '실패'),
      el('td', { class: 'mono' }, `${r.elapsed_sec ?? '-'}s`),
      el('td', { style: { display: 'flex', gap: '10px', flexWrap: 'wrap' } }, ...bar),
    );
    const detailRow = el('tr', {}, el('td', { colspan: '6', style: { padding: 0 } }, detail));
    return [head, detailRow];
  }).flat();

  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('실행 로그', '제품 × 1회 실행 = 1행 — 행을 클릭하면 단계별 상세 (logs/pipeline_runs.jsonl)'),
    el('div', { style: { display: 'flex', gap: '10px', alignItems: 'center', margin: '6px 0' } },
      sel, failChk,
      el('span', { class: 'hint' }, `${runs.length}건 표시`)),
    alTable(['시각', 'vehicle', '트리거', '결과', '소요', '단계별 (클릭 → 상세)'], rows),
  );
}

function runStageTip(k, s) {
  if (k === 'raw') return `유닛 ${s.units} · rows ${JSON.stringify(s.rows || {})}`
    + (s.errors?.length ? `\n실패 ${s.errors.length}건` : '');
  if (k === 'event') return Object.entries(s.sources || {})
    .map(([src, e]) => `${src}: raw ${e.raw_rows} → event ${e.event_rows} (${e.partitions} 파티션)${e.rebuilt ? ' [전체 재생성]' : ''}`)
    .join('\n') || '변경 없음';
  if (k === 'feature') return `${Object.entries(s.counts || {}).map(([c, n]) => `${c} ${n}`).join(' · ')}`
    + `\nevent ${s.event_dates}일 대상 · skip ${s.skipped?.length || 0} · knob miss ${s.knob_miss || 0}`;
  if (k === 'wide') return `rows ${s.rows} · feature ${s.features} → ${s.path || ''}`;
  return '';
}

function runDetailText(r) {
  const st = r.stages || {};
  const L = [];
  if (r.error) L.push(`✗ 실행 중단: ${r.error}`);
  if (st.raw) {
    L.push(`[raw] ${st.raw.sec}s · 유닛 ${st.raw.units}`);
    Object.entries(st.raw.rows || {}).forEach(([s, n]) => L.push(`    ${s}: ${n} rows`));
    (st.raw.errors || []).forEach((e) => L.push(`    ✗ ${e.source} ${e.date}: ${e.error}`));
  }
  if (st.event) {
    L.push(`[event] ${st.event.sec}s${st.event.rebuilt?.length ? ` · 전체 재생성: ${st.event.rebuilt.join(', ')}` : ''}`);
    Object.entries(st.event.sources || {}).forEach(([s, e]) =>
      L.push(`    ${s}: raw ${e.raw_rows} → event ${e.event_rows} · ${e.partitions} 파티션${e.rebuilt ? ' (재생성)' : ''}`));
  }
  if (st.feature) {
    L.push(`[feature] ${st.feature.sec}s · event ${st.feature.event_dates}일 전체 대상`);
    L.push(`    ${Object.entries(st.feature.counts || {}).map(([c, n]) => `${c} ${n}`).join(' · ')}`);
    if (st.feature.knob_miss) L.push(`    knob 미변환(RO) ${st.feature.knob_miss}건 · skip 판정 ${st.feature.knob_skip}건`);
    (st.feature.agg_overrides || []).forEach((o) =>
      L.push(`    ⓘ ${o.feature}: 룰북 agg '${o.csv_agg}' 무시 → 고정 규칙 '${o.applied}'`));
    (st.feature.skipped || []).forEach((s) => L.push(`    ⚠ ${s.feature}: ${s.reason}`));
  }
  if (st.wide) L.push(`[wide] ${st.wide.sec}s · rows ${st.wide.rows} · feature ${st.wide.features} → ${st.wide.path || ''}`);
  return L.join('\n') || '기록된 단계 없음';
}

// 알람 업로드 폴더 (Valve → S3 → flow 매칭알람). 이 폴더 하나만 sync 하면 된다.
function alOutbox(ob) {
  const box = el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('S3 업로드 폴더', 'flow 매칭알람이 읽는 파일 — 이 폴더만 S3 로 sync 하면 된다 (트리 = S3 key)'));
  if (!ob.enabled) {
    box.append(el('div', { class: 'alert warn', style: { fontSize: '12px' } },
      '미설정 — 설정 탭 alerts.outbox_dir 을 채우면 발행 시 이 폴더로 미러링된다.'));
    return box;
  }
  const cmd = `aws s3 sync "${ob.sync_dir}" s3://<bucket>/${ob.s3_prefix} --exclude "*.tmp"`;
  box.append(el('div', { style: { fontSize: '12px', display: 'flex', gap: '14px', flexWrap: 'wrap', alignItems: 'center', margin: '6px 0' } },
    el('span', { style: { color: 'var(--text-muted)' } }, '폴더'),
    el('span', { class: 'mono' }, ob.sync_dir),
    el('span', { style: { color: 'var(--text-muted)' } }, '→ s3 prefix'),
    el('span', { class: 'mono' }, ob.s3_prefix),
    el('span', { style: { color: 'var(--text-muted)' } }, `발행 주기 ${ob.interval_min || 0}분`),
  ));
  box.append(el('div', { class: 'mono', style: { fontSize: '11px', color: 'var(--text-muted)', margin: '2px 0 8px', wordBreak: 'break-all' } }, cmd));
  box.append(alTable(['key', '크기', '갱신'], (ob.files || []).map((f) => el('tr', {},
    el('td', { class: 'mono' }, f.key),
    el('td', { class: 'mono' }, `${f.size} B`),
    el('td', { class: 'mono', style: { color: 'var(--text-muted)' } }, new Date(f.mtime * 1000).toLocaleString()),
  ))));
  box.append(el('div', { style: { fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px' } },
    'ack.json 은 flow 도 쓰는 양방향 파일이라 이 폴더에 없다 — 폴더 sync 로 덮으면 판정이 유실된다.'));
  return box;
}

// csv 설정파일 S3 동기화 관리 (flow → Valve)
function alCsvSync(info) {
  const cfg = info.config;
  const status = info.status || {};
  const enabled = el('input', Object.assign({ type: 'checkbox' }, cfg.enabled ? { checked: '' } : {}));
  const interval = el('input', { type: 'number', value: String(cfg.interval_min), style: { width: '54px' } });
  const prefix = el('input', { type: 'text', value: cfg.s3_prefix || '', style: { width: '180px' } });
  const fileRows = [];

  const mkRow = (f) => {
    const key = el('input', { type: 'text', value: f.key || '', style: { width: '240px' } });
    const dest = el('input', { type: 'text', value: f.dest || '', style: { width: '300px' } });
    const st = status[f.key] || {};
    const stTxt = st.status
      ? `${st.status}${st.ts ? ' · ' + new Date(st.ts * 1000).toLocaleString() : ''}`
      : '-';
    const row = el('tr', {},
      el('td', {}, key),
      el('td', {}, dest),
      el('td', { class: 'mono', style: { fontSize: '11px', color: st.status === 'error' || st.status === 'missing' ? '#e5484d' : 'var(--text-muted)' } }, stTxt),
      el('td', {}, el('button', { class: 'btn', onclick: () => { row.remove(); fileRows.splice(fileRows.indexOf(entry), 1); } }, '✕')),
    );
    const entry = { key, dest, row };
    fileRows.push(entry);
    return row;
  };

  const tbl = el('table', { class: 'tbl' },
    el('thead', {}, el('tr', {}, ...['S3 key (prefix 이하)', '로컬 경로 (dest)', '마지막 동기화', ''].map((h) => el('th', {}, h)))),
    el('tbody', {}, (cfg.files || []).map(mkRow)),
  );

  const save = async () => {
    await api.put('/api/pipeline/csv-sync/config', {
      enabled: enabled.checked,
      interval_min: Number(interval.value) || 30,
      s3_prefix: prefix.value,
      files: fileRows.map((r) => ({ key: r.key.value, dest: r.dest.value })),
    });
    loadAlerts();
  };

  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('CSV 설정파일 S3 동기화', 'flow 가 S3 에 올린 matching csv 를 주기적으로 다운로드 — config/csv_sync.yaml'),
    el('div', { style: { fontSize: '12px', display: 'flex', gap: '14px', alignItems: 'center', flexWrap: 'wrap', margin: '6px 0' } },
      el('label', { style: { display: 'flex', gap: '5px', alignItems: 'center' } }, enabled, '주기 동기화'),
      el('span', {}, '주기(분)'), interval,
      el('span', {}, 'S3 prefix'), prefix,
      el('button', { class: 'btn', onclick: save }, '저장'),
      el('button', { class: 'btn primary', onclick: async (ev) => {
        ev.target.disabled = true; ev.target.textContent = '동기화 중…';
        try { await api.post('/api/pipeline/csv-sync/run'); } catch (e) { alert(e.message); }
        loadAlerts();
      } }, '↓ 지금 동기화'),
    ),
    tbl,
    el('button', { class: 'btn', style: { marginTop: '6px' }, onclick: () => {
      tbl.querySelector('tbody').append(mkRow({ key: '', dest: '' }));
    } }, '+ 파일 추가'),
  );
}

function alExcludeEditor(cfg) {
  const ex = (cfg.unmatched_scan || {}).exclude || {};
  const eqp = el('input', { type: 'text', value: (ex.eqp_id || []).join(', '), style: { width: '300px' } });
  const model = el('input', { type: 'text', value: (ex.eqp_model || []).join(', '), style: { width: '300px' } });
  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('미매칭 제외 규칙 (전역)', 'fnmatch 패턴 · 쉼표 구분 — config/pipeline.yaml · unmatched_scan.exclude'),
    el('div', { style: { fontSize: '12px', display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' } },
      'eqp_id', eqp, 'eqp_model', model,
      el('button', { class: 'btn', onclick: async () => {
        await api.put('/api/pipeline/config/exclude', {
          eqp_id: eqp.value.split(',').map((s) => s.trim()).filter(Boolean),
          eqp_model: model.value.split(',').map((s) => s.trim()).filter(Boolean),
        });
        loadAlerts();
      } }, '저장 + 재스캔'),
    ),
  );
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
  $$('.tab[data-tab]').forEach((b) => b.addEventListener('click', () => route(b.dataset.tab)));

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
