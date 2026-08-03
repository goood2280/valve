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
  logsFilter: { product: '', source: '', status: '', severity: '', failed_only: false, kind: 'all', limit: 300 },
  logsItems: [],
  logsRefresh: null,
  logsOpen: new Set(),   // 펼쳐 둔 로그 행 (15초 자동 갱신 후에도 유지)
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
      // 재연결 때마다 오는 snapshot 으로 탭을 통째로 다시 그리지 않는다 (heatmap 깜빡임)
      if (STATE.currentTab === 'monitor' && $('#monRunning')) refreshMonitorLive();
      else renderCurrentTab();
    } catch (err) { console.warn('snapshot parse', err); }
  });

  es.addEventListener('update', (e) => {
    try {
      const evt = JSON.parse(e.data);
      applyEvent(evt);
      if (STATE.currentTab === 'monitor') refreshMonitorLive();
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
    diagnose: renderDiagnose,
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
      el('div', { class: 'spacer' }),
      el('button', { class: 'btn primary', onclick: onEnqueueAll }, '▶ 전체 실행 (backfill 범위)'),
      el('button', { class: 'btn', onclick: onProbeInvalidateAll }, '↻ Probe 캐시 전체 무효화'),
    ),
    el('div', { id: 'monRunning' }, renderInProgressCard()),
    renderDbHeatmapCard(),
    el('div', { id: 'monFails' }, renderFailuresCard()),
  );
}

// SSE 로 갱신되는 카드(진행 chunk · 최근 실패)만 부분 렌더.
// 예전엔 이벤트마다 renderMonitor() 로 탭 전체를 다시 그렸는데, 그러면 DB heatmap
// 카드까지 통째로 교체되어 실시간 진행 표시가 매 이벤트마다 사라졌다 다시 나타났다
// (= 화면이 깜빡임). heatmap 은 자체 폴링으로만 갱신한다.
function refreshMonitorLive() {
  const run = $('#monRunning'), fail = $('#monFails');
  if (!run || !fail) { renderMonitor(); return; }
  run.replaceChildren(renderInProgressCard());
  fail.replaceChildren(renderFailuresCard());
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

// feature 가 담고 있는 구간 — 소스별 커버(feature_cov)를 vehicle 하나로 합친 값.
// feature parquet 자체엔 날짜가 없어서, 산출 시점의 event 파티션 범위가 근거다.
function featureSpan(st) {
  const covs = Object.values(st.feature_cov || {}).filter((c) => c && c.days);
  if (!covs.length) return null;
  return {
    days: Math.max(...covs.map((c) => c.days)),
    start: covs.map((c) => c.start).sort()[0],
    end: covs.map((c) => c.end).sort().slice(-1)[0],
    approx: covs.some((c) => c.approx),
  };
}

function featureTitle(st, featTotal, fmtTs) {
  const lines = [`feature store 산출물(컬럼) ${featTotal}개`,
    Object.entries(st.features || {}).map(([k, n]) => `${k} ${n}`).join(' · ')];
  const covs = Object.entries(st.feature_cov || {}).filter(([, c]) => c && c.days);
  if (covs.length) {
    lines.push('', '대상 event 구간 (소스별):');
    covs.forEach(([s, c]) => lines.push(`  ${s}: ${c.days}일 · ${c.start} ~ ${c.end}${c.approx ? ' (추정)' : ''}`));
    lines.push(`산출 ${fmtTs(st.feature_ts)}`);
  }
  return lines.join('\n');
}

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

    // 실행 관련(전역 주기 · 금지 시간대 · 제품별 주기 · 수동 실행)은 이 패널 한 곳에 모은다
    const [rt, sched] = await Promise.all([
      api.get('/api/pipeline/runtime').catch(() => null),
      api.get('/api/pipeline/schedule').catch(() => null),
    ]);
    const runtimeBar = rt ? renderRuntimeBar(rt, sched) : null;

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
      const cov = featureSpan(st);
      tbody.append(el('tr', { class: 'prod-head-row' + (vi > 0 ? ' divider' : '') },
        el('td', { class: 'prod-head-cell', colspan: String(buckets.length + 1) },
          el('div', { class: 'prod-head-inner clickable', title: `클릭 → ${v} 파이프라인 재실행 (raw→event→feature)`,
            onclick: () => onRunVehicle(v) },
            el('span', { class: 'prod-head-name' }, v),
            el('span', { class: 'hint' }, st.product),
            el('span', { class: 'spacer' }),
            el('span', { class: 'prod-head-count', title: featureTitle(st, featTotal, fmtTs) },
              `matching ${fmtTs(st.event.FAB?.applied_ts)} · ${st.matching.steps} step`
              + ` · feature ${featTotal}${cov ? ` · ${cov.days}일 (${cov.start}~${cov.end})` : ''}`)))));
      Object.keys(st.raw).forEach((src) => {
        const rawOnly = !(src in (st.event || {}));   // ET 등 raw 전용 소스 — event 없음
        const ev = st.event[src] || {};
        const evDates = new Set(ev.dates || []);
        const featN = (FEATURE_CATS[src] || []).reduce((a, c) => a + (st.features?.[c] || 0), 0);
        const fc = (st.feature_cov || {})[src];
        const cats = (FEATURE_CATS[src] || []).map((c) => `${c} ${st.features?.[c] || 0}`).join(' · ');
        const tr = el('tr', { class: 'src-row' },
          el('td', { class: 'row-label src-label',
            title: rawOnly ? 'raw 전용 소스 — event DB 를 만들지 않음'
              : `매칭 파일: ${ev.matching_file || '-'}\n적용: ${fmtTs(ev.applied_ts)} · sha ${ev.matching_sha || '-'}` },
            el('span', { class: 'src-bullet' }, '●'), src,
            rawOnly ? el('span', { class: 'hint' }, 'raw 전용') : null,
            featN ? el('span', { class: 'feat-badge', title:
              `feature store 산출물(컬럼) ${featN}개 — ${cats}\n`
              + (fc ? `대상 event ${fc.days}일치 · ${fc.start} ~ ${fc.end}`
                      + (fc.approx ? '\n(산출 기록 없음 — 현재 event 기준 추정. 다음 실행 후 정확해집니다)' : '')
                      + `\n산출 ${fmtTs(st.feature_ts)}`
                    : '대상 구간 기록 없음 — 파이프라인을 한 번 실행하면 기록됩니다') },
              `feat ${featN}${fc ? ` · ${fc.days}일 (${fc.start.slice(5)}~${fc.end.slice(5)})` : ''}`) : null,
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
        el('span', { class: 'hint' }, '주/월 버킷은 가장 긴급한 단계 색 · '
          + 'feat N · D일 (시작~끝) = 소스별 feature 컬럼 수와 그 feature 가 담은 event 구간 · '
          + 'vehicle 헤더 클릭 = 재실행')));
    DBHM_LAST_RELOAD = Date.now();
    // 카드를 새로 그렸으므로 진행 표시부터 즉시 복원한다 — 비워둔 채 다음 폴링(최대 1.2초)을
    // 기다리면 실행 중에도 표시가 사라졌다 나타나 깜빡인다.
    if (DBHM_PROG_TIMER) {
      renderProg($('#dbhmProg'), DBHM_LAST_PROG);
    } else {
      const pr = await api.get('/api/pipeline/progress').catch(() => null);
      if (pr) {
        renderProg($('#dbhmProg'), pr);
        DBHM_LAST_RUNNING = !!pr.running;   // running 전이는 실제 running 으로만 판단
        if (pr.running || pr.loop_enabled || pr.schedule_enabled || pr.quiet?.now) startProgPoll();
      }
    }
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

// ⚙ 실행 관리 — 실행에 관한 것은 전부 여기 한 곳에서 본다/바꾼다.
//   1줄: 자원·워커 계획
//   2줄: 전역 자동 주기 · 실행 금지 시간대 · 루프 · 수동 전체 실행
//   3줄: 제품별 주기(하루 N회)와 다음 실행 예정  ← 예전엔 알람 탭에 흩어져 있던 것
function renderRuntimeBar(rt, sched) {
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

  return el('div', { class: 'rt-panel' },
    el('div', { class: 'rt-line' }, info),
    el('div', { class: 'rt-line' },
      el('span', { class: 'rt-cap' }, '⚙ 실행 관리'),
      schedule, renderQuietControl(rt), el('span', { class: 'spacer' }), loopBtn, runBtn),
    renderVehicleSchedLine(sched),
  );
}

// 실행 금지 시간대 — 이 구간엔 자동 실행(스케줄·루프)이 뜨지 않는다. 시각은 조절 가능.
function renderQuietControl(rt) {
  const q = rt.quiet || {};
  const timeInput = (key, val) => el('input', {
    type: 'time', class: 'rt-time', value: val || '',
    title: `${key} — 서버 로컬 시각 (HH:MM)`,
    onchange: (e) => putRuntime({ [key]: e.target.value }),
  });
  const on = !!q.enabled;
  const blocking = !!q.now;
  const bits = [
    el('input', {
      type: 'checkbox', ...(on ? { checked: 'checked' } : {}),
      onchange: (e) => putRuntime({ quiet_enabled: e.target.checked }),
    }),
    '🌙 금지 ',
    timeInput('quiet_start', q.start),
    '~',
    timeInput('quiet_end', q.end),
  ];
  if (on && !q.valid) {
    bits.push(el('span', { class: 'hint', style: { color: 'var(--danger)' } }, '시각 확인 필요'));
  } else if (blocking) {
    bits.push(el('span', { class: 'rt-quiet-now' },
      `지금 금지 중 → ${fmtClock(q.until)} 해제`));
  }
  return el('label', {
    class: 'rt-quiet' + (blocking ? ' on' : ''),
    title: '이 시간대에는 자동 실행(주기·루프)을 띄우지 않습니다.\n'
         + '· 시작 > 종료 면 자정을 넘는 구간 (예 23:00~02:00)\n'
         + '· 사람이 누르는 수동 실행은 막지 않습니다\n'
         + '· 이미 돌고 있는 실행은 중단하지 않습니다',
  }, ...bits);
}

// 제품별 주기 — 하루 N회. 비우면 전역 주기를 따르고 0 이면 자동 실행 제외.
function renderVehicleSchedLine(sched) {
  const vehs = Object.entries(sched?.vehicles || {});
  if (!vehs.length) return null;
  const chips = vehs.map(([v, sc]) => {
    const inp = el('input', {
      type: 'number', class: 'rt-hours', min: '0', max: '48', step: '1',
      value: sc.source === 'vehicle' ? String(sc.runs_per_day) : '',
      placeholder: String(sc.runs_per_day || 0),
      title: '하루 실행 횟수 (3=8시간 간격 · 6=4시간 간격 · 0=자동 실행 안 함)\n'
           + '비우면 전역 주기를 따른다',
      onchange: async (e) => {
        try {
          await api.put(`/api/pipeline/schedule/${encodeURIComponent(v)}`,
            { runs_per_day: e.target.value === '' ? null : Number(e.target.value) });
        } catch (err) { alert(err.message); }
        loadDbHeatmap();
      },
    });
    const next = !sc.enabled ? '자동 안 함'
      : sc.quiet_blocked ? `금지 해제 후 ${fmtClock(sc.next_ts)}`
      : sc.due ? '곧 실행' : fmtClock(sc.next_ts);
    return el('span', {
      class: 'rt-veh' + (sc.enabled ? '' : ' off') + (sc.due && sc.enabled ? ' due' : ''),
      title: `${v} · ${sc.source === 'global' ? '전역 주기 사용' : '제품 개별 주기'}`,
    },
      el('b', {}, v), '일', inp, '회',
      el('span', { class: 'hint' },
        sc.interval_sec > 0 ? `${sc.interval_hours}h 간격 → ${next}` : '자동 실행 안 함'));
  });
  return el('div', { class: 'rt-line rt-veh-line' },
    el('span', { class: 'rt-cap' }, '제품별 주기'), ...chips,
    sched.master_enabled ? null
      : el('span', { class: 'hint', style: { color: 'var(--warn)' } }, '자동 실행 꺼짐 — 위 ⏱ 자동 체크'));
}

const fmtClock = (ts) => (ts ? new Date(ts * 1000).toLocaleString('ko-KR',
  { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }) : '-');

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
//
// 표시는 "상태가 바뀔 때만" 바뀌어야 한다. 예전 구현이 깜빡였던 이유 세 가지:
//   (a) 폴링마다 box 를 통째로 지우고 다시 만들었다 (.dbhm-prog:empty 는 display:none —
//       지우는 순간 줄이 접혔다가 다시 펴져 화면이 튄다)
//   (b) 루프/스케줄은 회차 사이에 running 이 잠깐 false 가 된다 → "실행 중"과 "대기"가
//       1초 간격으로 번갈아 떴다. 이제 loop_enabled/schedule_enabled/busy 로 그 공백을 메운다
//   (c) 회차가 끝날 때마다 heatmap 을 통째로 다시 로드했다 → 카드 전체가 교체됐다
let DBHM_PROG_TIMER = null;
let DBHM_LAST_RUNNING = false;
let DBHM_LAST_PROG = null;      // 카드 재렌더 시 진행 표시를 즉시 복원하기 위한 마지막 스냅샷
let DBHM_LAST_RELOAD = 0;       // heatmap 전체 재로드 시각 (루프 회차마다 재로드하지 않도록)
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
    // 실행 → 종료 전이에서만 heatmap 갱신. 루프면 회차마다 전이가 생기므로 최소 간격을 둔다.
    if (DBHM_LAST_RUNNING && !pr.running) {
      DBHM_LAST_RUNNING = false;
      if (Date.now() - DBHM_LAST_RELOAD > 10000) { loadDbHeatmap(); return; }
    }
    if (pr.running) DBHM_LAST_RUNNING = true;
    // 완전히 놀고 있으면(루프·스케줄도 꺼짐) 폴링 중단 — 마지막 상태 그대로 멈춰 있는다
    if (!pr.running && !pr.loop_enabled && !pr.schedule_enabled && !pr.busy
        && !pr.quiet?.now) stopProgPoll();
  }, 1200);
}

// 실행 상태 한 줄. running 이 아니어도 루프/스케줄이 켜져 있으면 계속 "실행 중"으로 —
// 회차 사이 공백에 "대기"로 떨어졌다 올라오지 않게 한다.
function progHeadText(pr) {
  if (pr.running || pr.busy) {
    if (pr.mode === 'loop') return `🔁 루프 #${pr.loop_iter} 실행 중`;
    if (pr.mode === 'schedule') return '⏱ 자동 실행 중';
    return '⏳ 실행 중';
  }
  // 금지 시간대에는 루프/스케줄이 켜져 있어도 새 실행이 뜨지 않는다 — 그대로 표시
  if (pr.quiet?.now) return `🌙 실행 금지 시간대 (${fmtClock(pr.quiet.until)} 해제)`;
  if (pr.loop_enabled) return '🔁 루프 실행 중 (다음 회차 준비)';
  if (pr.schedule_enabled) return '⏱ 자동 실행 대기';
  return '· 대기';
}

// 노드를 새로 만들지 않고 텍스트/클래스만 갱신 (재생성하면 매 폴링마다 화면이 튄다)
function setText(node, text) { if (node.textContent !== text) node.textContent = text; }

function renderProg(box, pr) {
  if (!box) return;
  DBHM_LAST_PROG = pr || null;
  const show = pr && (pr.running || pr.busy || pr.mode || pr.loop_enabled
                      || pr.schedule_enabled || pr.quiet?.now);
  if (!show) { if (box.firstChild) box.replaceChildren(); return; }

  let head = box.querySelector('.prog-head');
  if (!head) { head = el('span', { class: 'prog-head' }); box.append(head); }
  setText(head, progHeadText(pr));

  const seen = new Set();
  Object.entries(pr.vehicles || {}).forEach(([v, s]) => {
    seen.add(v);
    let d = PROG_STAGE[s.stage] || s.stage || '-';
    if (s.stage === 'raw' && s.raw_total) d += ` ${s.raw_done || 0}/${s.raw_total}`;
    if (s.stage === 'feature' && s.event_dates) d += ` ${s.event_dates}일`;
    let chip = [...box.querySelectorAll('[data-veh]')].find((n) => n.dataset.veh === v);
    if (!chip) { chip = el('span', { 'data-veh': v }); box.append(chip); }
    const cls = `prog-veh stage-${s.stage || ''}`;
    if (chip.className !== cls) chip.className = cls;
    setText(chip, `${v} · ${d}`);
  });
  box.querySelectorAll('[data-veh]').forEach((n) => { if (!seen.has(n.dataset.veh)) n.remove(); });
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
        el('div', { class: 'mono failure-full', title: c.error }, c.error || ''),
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

// 소스(DB 종류) 배지 — 색은 source_types.yaml 의 accent 그대로다
// (설정 › Source types 에서 바꾸면 제품 탭 힌트 박스와 함께 여기 색도 같이 바뀐다).
// 화면마다 다른 색을 쓰면 색이 정보가 아니라 장식이 된다 — 한 곳에서만 만든다.
function srcBadge(name) {
  const key = String(name || '').toUpperCase();
  const accent = getSourceType(key)?.accent || '#64748b';
  return el('span', {
    class: 'src-badge', title: `${key} DB`,
    style: { color: accent, background: `${accent}22`, borderColor: `${accent}66` },
  }, key);
}

// 문장 안의 소스명만 배지로 바꿔 children 배열로 돌려준다 (나머지는 그대로 글자).
// 진단 검사명은 "FAB raw 파티션" 처럼 앞에, SEND_FORM 은 "1.FAB (FAB+MASK)" 처럼
// 중간에 섞여 오므로 위치를 가리지 않고 훑는다. 앞뒤가 영숫자면 배지로 보지 않는다
// (ETC·FABRIC 의 ET/FAB 까지 물들이지 않기 위해).
function withSourceBadges(text) {
  const s = String(text ?? '');
  const names = SOURCE_NAMES.filter(Boolean).slice().sort((a, b) => b.length - a.length);
  if (!s || !names.length) return [s];
  const re = new RegExp(`(${names.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`, 'g');
  const isWord = (ch) => !!ch && /[A-Za-z0-9_]/.test(ch);
  const out = [];
  let last = 0, m;
  while ((m = re.exec(s)) !== null) {
    if (isWord(s[m.index - 1]) || isWord(s[m.index + m[0].length])) continue;
    if (m.index > last) out.push(s.slice(last, m.index));
    out.push(srcBadge(m[0]));
    last = m.index + m[0].length;
  }
  if (!out.length) return [s];
  if (last < s.length) out.push(s.slice(last));
  return out;
}

// 소스 타입의 table_template({name} 치환) — 제품 source.table 기본값.
// 서버 _render_table 과 같은 규칙이어야 한다 (backend/routers/schedule.py).
function sourceTypeTable(name) {
  const key = (name || '').toUpperCase();
  const tpl = (getSourceType(key)?.table_template || '').trim();
  return tpl ? tpl.replace(/\{name\}/g, key) : `RAW_${key}_DATA`;
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
      sources: [{
        name: SOURCE_NAMES[0] || 'FAB',
        table: sourceTypeTable(SOURCE_NAMES[0] || 'FAB'),
        shard_hierarchy: [...(getSourceType(SOURCE_NAMES[0] || 'FAB')?.default_shard || [])],
        target_chunk_rows: 500000,
      }],
      params_template: {},
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
    p.sources.push({
      name: next,
      table: sourceTypeTable(next),
      shard_hierarchy: [...(getSourceType(next)?.default_shard || [])],
      target_chunk_rows: 500000,
    });
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

    // 사내 API 공통 필터 (process_id / line_id)
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
// 사내 API 포맷: params_template[column_name] = value.
// 여러 값은 배열이며 op 래퍼와 product_code는 사용하지 않는다.
// ─────────────────────────────────────────────────
function productKeyFieldsEditor(p, rerender) {
  p.params_template = p.params_template || {};

  const getValue = (col) => {
    const entry = p.params_template[col];
    if (entry == null) return '';
    const value = entry && typeof entry === 'object' && !Array.isArray(entry)
      ? entry.value : entry;
    return Array.isArray(value) ? value.join(', ') : String(value ?? '');
  };

  const setValue = (col, rawValue) => {
    const trimmed = (rawValue || '').trim();
    if (!trimmed) {
      delete p.params_template[col];
      return;
    }
    const value = trimmed.includes(',')
      ? trimmed.split(',').map(x => x.trim()).filter(Boolean)
      : trimmed;
    p.params_template[col] = value;
  };

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
        const prev = s.name;
        s.name = e.target.value;
        // 직접 지정한 table 이 아니면(이전 타입의 템플릿 값 그대로면) 새 타입 템플릿으로 교체
        if (!s.table || s.table === sourceTypeTable(prev)) s.table = sourceTypeTable(s.name);
        rerender();
      }},
        ...SOURCE_NAMES.map(n => el('option', { value: n, ...(n === s.name ? { selected: 'selected' } : {}) }, n)),
        ...(!SOURCE_NAMES.includes(s.name) && s.name ? [el('option', { value: s.name, selected: 'selected' }, s.name)] : []),
      ),
      el('span', { class: 'hint' }, 'table'),
      el('input', { type: 'text', class: 'inline-input', style: { width: '170px' },
        value: s.table || '', placeholder: sourceTypeTable(srcKey),
        title: `소스 타입 기본값: ${sourceTypeTable(srcKey)}\n`
             + '다르게 적으면 이 제품 전용 값이 되고, 소스 타입을 저장해도 자동으로 안 바뀝니다.',
        onchange: e => { s.table = e.target.value; } }),
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
  const table = s.table || sourceTypeTable(s.name);

  // 사내 API는 op 래퍼 없이 필터 값을 직접 받는다. product_code는 전달하지 않는다.
  const paramEntries = [];
  for (const col of ['process_id', 'line_id']) {
    const entry = (p.params_template || {})[col];
    const value = entry && typeof entry === 'object' && !Array.isArray(entry)
      ? entry.value : entry;
    const isEmpty = value === '' || value == null
                 || (Array.isArray(value) && value.length === 0);
    if (isEmpty) continue;
    paramEntries.push([col, value]);
  }

  const pyVal = (v) => {
    if (Array.isArray(v)) return '[' + v.map(pyVal).join(', ') + ']';
    if (typeof v === 'number') return String(v);
    if (typeof v === 'boolean') return v ? 'True' : 'False';
    return `"${String(v ?? '').replace(/"/g, '\\"')}"`;
  };
  const paramLines = [
    `    "table_name": "${table}",`,
    `    "dateFrom": "{dateFrom}",          # YYYY-MM-DDT00:00:00`,
    `    "dateTo":   "{dateTo}",            # 다음 날 00:00:00`,
  ];
  for (const [col, value] of paramEntries) {
    paramLines.push(`    "${col}": ${pyVal(value)},`);
  }

  const shardKeys = s.shard_hierarchy || [];
  if (shardKeys.length) {
    paramLines.push(
      `    # planner 가 chunk 마다 shard 를 해당 컬럼명에 직접 주입:`,
      `    # "${shardKeys[0]}": ["R001", "R002", ...]`,
    );
  }

  const colsPy = cols.length
    ? '[' + cols.map(c => `"${c}"`).join(', ') + ']'
    : '[]';

  const queryLines = shardKeys[0] === 'root_lot_id'
    ? [
        'root_lots = getData(params, custom_columns=["root_lot_id"], user_name="{settings.lake_api.user}")',
        'root_lot_ids = root_lots["root_lot_id"].dropna().unique().tolist()',
        `Query = getData({**params, "root_lot_id": root_lot_ids}, custom_columns=${colsPy}, user_name="{settings.lake_api.user}") if root_lot_ids else root_lots.iloc[0:0]`,
      ]
    : [`Query = getData(params, custom_columns=${colsPy}, user_name="{settings.lake_api.user}")`];

  const snippet = [
    'from bigdataquery import *',
    '',
    'params = {',
    ...paramLines,
    '}',
    '',
    ...queryLines,
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
    ['lot_id', 'wafer_id', 'time', 'value'].forEach(c => union.add(c));
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
  partial_failed:       { label: 'partial',    cls: 'warn', color: '#92400e' },
  retry_wait:           { label: 'retry',      cls: 'warn', color: '#92400e' },
};

const LOG_SEVERITY_META = {
  info:     { label: 'INFO',     cls: 'sev-info' },
  warning:  { label: 'WARNING',  cls: 'sev-warning' },
  critical: { label: 'CRITICAL', cls: 'sev-critical' },
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
    el('label', { class: 'hint' }, 'Severity'),
    el('select', { class: 'inline-input', onchange: e => { f.severity = e.target.value; applyAndReload(); } },
      el('option', { value: '' }, 'ALL'),
      ...Object.keys(LOG_SEVERITY_META).map(k => el('option', {
        value: k, ...(k === f.severity ? { selected: 'selected' } : {})
      }, LOG_SEVERITY_META[k].label)),
    ),
    el('label', { class: 'check' },
      el('input', { type: 'checkbox', ...(f.failed_only ? { checked: 'checked' } : {}),
        onchange: e => { f.failed_only = e.target.checked; applyAndReload(); } }),
      '실패만',
    ),
    el('label', { class: 'hint' }, '종류'),
    el('select', { class: 'inline-input', onchange: e => { f.kind = e.target.value; applyAndReload(); } },
      ...['all','pipeline','chunk','plan','partition'].map(k => el('option', { value: k, ...(k === f.kind ? { selected: 'selected' } : {}) }, k)),
    ),
    el('label', { class: 'hint' }, 'N'),
    el('input', { type: 'number', class: 'inline-input narrow', min: '10', max: '5000',
      value: f.limit, onchange: e => { f.limit = Number(e.target.value) || 300; applyAndReload(); } }),
    el('div', { class: 'spacer' }),
    el('button', { class: 'btn ghost small', onclick: applyAndReload }, '↻ 새로고침'),
  );

  main.append(
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
  if (f.severity) q.set('severity', f.severity);
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

// 한 줄 요약 — 표는 "한 행 = 한 줄"이 원칙이다. 전문은 행을 눌러 펼친 상세에서 본다.
const firstLine = (s) => String(s ?? '').split('\n')[0].trim();

function logSummary(it) {
  if (it.kind === 'pipeline') {
    if (it.error) return firstLine(it.error);
    const errs = it.raw_errors || [];
    if (errs.length) {
      const head = errs[0];
      const causes = new Set(errs.map(e => e.error || ''));
      const blocked = errs.filter(e => e.blocked).length;
      return `raw 실패 ${errs.length}건 · ${head.source || 'RAW'} ${head.date || ''} ${firstLine(head.error) || 'retry scheduled'}`
        + (causes.size > 1 ? ` (원인 ${causes.size}종)` : '')
        + (blocked ? ` · 재시도 중단 ${blocked}` : '');
    }
    return `pipeline ${it.status || ''}${it.mode ? ` · ${it.mode}` : ''}`.trim();
  }
  if (it.kind === 'chunk') {
    if (it.error) return `${it.error_type || 'error'}: ${firstLine(it.error)}`;
    if (it.actual_rows != null) return `expected ${fmt.int(it.expected_rows)} · actual ${fmt.int(it.actual_rows)}`;
    return it.status || '';
  }
  if (it.kind === 'plan') {
    const pm = it.probe_meta || {};
    if (pm.error) return `⚠ probe 실패 → 단일 chunk fallback: ${firstLine(pm.error)}`;
    if (pm.skipped) return `probe skip (${pm.reason || 'manual'})`;
    return `chunks=${it.chunks} · probe=${pm.strategy || '-'}`
      + (pm.estimated_rows != null ? ` · est ${fmt.int(pm.estimated_rows)}` : '')
      + (pm.shard_count ? ` · shards=${pm.shard_count}` : '');
  }
  const u = it.update || {};
  if (Object.keys(u).length) {
    const bits = [];
    if (u.total_rows != null) bits.push(`rows ${fmt.int(u.total_rows)}`);
    const c = u.completeness || {};
    if (c.diff_pct != null) bits.push(`diff ${c.diff_pct}%`);
    if (u.error) bits.push(firstLine(u.error));
    if (u.s3_key) bits.push(`s3 ${u.s3_key}`);
    if (bits.length) return bits.join(' · ');
  }
  return firstLine(JSON.stringify(u));
}

// 자동 갱신(15초)으로 표를 다시 그려도 펼친 행이 닫히지 않도록 키로 기억한다.
const logKey = (it) =>
  `${it.kind}|${it.ts}|${it.chunk_id || it.plan_id || it.partition_key || it.product || it.vehicle || ''}`;

function renderLogsTable(items) {
  const rows = [];
  items.forEach((it) => {
    const tsStr = it.ts ? new Date(it.ts * 1000).toLocaleString('sv').slice(5, 16) : '-';
    const tsAgo = it.ts ? fmt.ago(it.ts) : '';
    const meta = LOG_STATUS_META[it.status] || { label: it.status || '-', cls: 'pending' };
    const summary = logSummary(it);
    const sev = LOG_SEVERITY_META[it.severity] || LOG_SEVERITY_META.info;
    const probeFailed = it.kind === 'plan' && it.probe_meta?.error;
    const rowCls = [
      it.severity === 'critical' ? 'row-err' : '',
      it.severity === 'warning' || probeFailed ? 'row-warn' : '',
    ].filter(Boolean).join(' ');
    const rowsTxt = it.kind === 'chunk' && it.actual_rows != null
      ? `${fmt.int(it.actual_rows)}${it.expected_rows ? ` / ${fmt.int(it.expected_rows)}` : ''}`
      : '';

    const key = logKey(it);
    const open = STATE.logsOpen.has(key);
    const detailTr = el('tr', { class: 'logs-detail-row', ...(open ? {} : { hidden: '' }) },
      el('td', { colspan: '10' }, renderLogDetail(it)));
    const tr = el('tr', {
      class: `logs-row ${rowCls}${open ? ' open' : ''}`.trim(),
      tabindex: '0',
      title: it.chunk_id || it.plan_id || it.partition_key || '클릭하면 상세',
      onclick: () => toggleLogRow(key, tr, detailTr),
      onkeydown: (e) => {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleLogRow(key, tr, detailTr); }
      },
    },
      el('td', { class: 'logs-caret-cell' }, el('span', { class: 'logs-caret' }, '▸')),
      el('td', { class: 'mono logs-ts' }, tsStr, el('span', { class: 'hint' }, tsAgo)),
      el('td', {}, el('span', { class: 'pill' }, it.kind)),
      el('td', { class: 'mono' }, it.product || '-'),
      el('td', {}, it.source ? srcBadge(it.source) : '-'),
      el('td', { class: 'mono' }, it.date || '-'),
      el('td', {},
        it.status ? el('span', { class: `pill ${meta.cls}` }, meta.label)
        : probeFailed ? el('span', { class: 'pill warn' }, 'probe fail')
        : (it.kind === 'plan' && it.probe_meta?.skipped) ? el('span', { class: 'pill pending' }, 'probe skip')
        : (it.kind === 'plan') ? el('span', { class: 'pill run' }, 'planned')
        : '-'),
      el('td', {}, el('span', { class: `severity-pill ${sev.cls}` }, sev.label)),
      el('td', { class: 'mono' }, it.duration_sec != null ? fmt.dur(it.duration_sec) : '-'),
      el('td', { class: 'logs-reason', title: summary }, summary || ''),
    );
    rows.push(tr, detailTr);
  });

  return el('table', { class: 'tbl logs-tbl' },
    el('thead', {}, el('tr', {},
      el('th', { class: 'logs-caret-cell' }, ''),
      el('th', {}, '시간'),
      el('th', {}, '종류'),
      el('th', {}, '제품'),
      el('th', {}, '소스'),
      el('th', {}, '날짜'),
      el('th', {}, '상태'),
      el('th', {}, 'Severity'),
      el('th', {}, 'duration'),
      el('th', {}, '사유 / 메모'),
    )),
    el('tbody', {}, rows),
  );
}

function toggleLogRow(key, tr, detailTr) {
  const open = !STATE.logsOpen.has(key);
  if (open) STATE.logsOpen.add(key); else STATE.logsOpen.delete(key);
  tr.classList.toggle('open', open);
  detailTr.toggleAttribute('hidden', !open);
}

// 펼친 상세 — 한 줄 요약에서 잘린 것을 여기서 전부 본다.
function renderLogDetail(it) {
  const kv = [];
  const add = (k, v) => { if (v != null && v !== '') kv.push(el('dt', {}, k), el('dd', {}, v)); };
  const clock = (ts) => (ts ? new Date(ts * 1000).toLocaleString('sv') : null);

  add('시각', it.ts ? `${clock(it.ts)} (${fmt.ago(it.ts)})` : null);
  add('ID', it.chunk_id || it.plan_id || it.partition_key || null);
  add('제품', [it.product, it.vehicle].filter(Boolean).join(' · ') || null);
  add('소스 · 날짜', [it.source, it.date].filter(Boolean).join(' · ') || null);
  add('상태', [it.status, (LOG_SEVERITY_META[it.severity] || {}).label].filter(Boolean).join(' · ') || null);
  add('실행 모드', it.mode || null);
  add('소요', it.duration_sec != null ? fmt.dur(it.duration_sec) : null);
  if (it.started_at || it.ended_at) add('구간', `${clock(it.started_at) || '?'} → ${clock(it.ended_at) || '?'}`);
  if (it.expected_rows != null || it.actual_rows != null) {
    add('rows', `actual ${fmt.int(it.actual_rows)} / expected ${fmt.int(it.expected_rows)}`);
  }
  if (it.chunks != null) add('chunks', String(it.chunks));

  const u = it.update || {};
  if (u.total_rows != null) add('total rows', fmt.int(u.total_rows));
  const comp = u.completeness || {};
  if (comp.actual != null || comp.expected != null) {
    add('completeness', `actual ${fmt.int(comp.actual)} / expected ${fmt.int(comp.expected)}`
      + (comp.diff_pct != null ? ` · diff ${comp.diff_pct}% (tol ${comp.tolerance_pct ?? '-'}%)` : ''));
  }
  if (u.s3_key) add('s3 key', u.s3_key);

  const blocks = [el('dl', { class: 'kv log-detail-kv' }, ...kv)];

  if (it.error) {
    blocks.push(logDetailBlock('오류',
      el('pre', { class: 'log-detail-pre' }, `${it.error_type ? `${it.error_type}: ` : ''}${it.error}`)));
  }

  const errs = it.raw_errors || [];
  if (errs.length) {
    blocks.push(logDetailBlock(`raw 실패 ${errs.length}건`,
      el('div', { class: 'log-detail-scroll' },
        el('table', { class: 'tbl log-detail-tbl' },
          el('thead', {}, el('tr', {},
            ['소스', '날짜', '오류', '시도', '재시도', '다음'].map(h => el('th', {}, h)))),
          el('tbody', {}, errs.map(e => el('tr', {},
            el('td', {}, e.source ? srcBadge(e.source) : '-'),
            el('td', { class: 'mono' }, e.date || '-'),
            el('td', {}, e.error || '-'),
            el('td', { class: 'mono' }, e.attempts != null ? String(e.attempts) : '-'),
            el('td', {}, e.blocked
              ? el('span', { class: 'pill err' }, '중단')
              : el('span', { class: 'pill warn' }, '대기')),
            el('td', { class: 'mono' }, e.next_retry_at ? fmtClock(e.next_retry_at) : '-'),
          )))))));
  }

  const rt = it.retry;
  if (rt && (rt.pending || rt.due || rt.max_attempts)) {
    blocks.push(logDetailBlock('재시도 큐', el('div', { class: 'log-detail-line' },
      `대기 ${rt.pending ?? 0}건 · 지금 실행 대상 ${rt.due ?? 0}건 · 최대 ${rt.max_attempts ?? '-'}회`
      + (rt.oldest_age_sec ? ` · 가장 오래된 건 ${Math.round(rt.oldest_age_sec / 3600)}h 경과` : '')
      + (rt.next_retry_at ? ` · 다음 ${fmtClock(rt.next_retry_at)}` : ''))));
  }

  const pm = it.probe_meta;
  if (pm && Object.keys(pm).length) {
    const bits = [
      pm.strategy ? `strategy=${pm.strategy}` : null,
      pm.estimated_rows != null ? `est ${fmt.int(pm.estimated_rows)} rows` : null,
      pm.sample_rows != null ? `sample ${fmt.int(pm.sample_rows)} rows${pm.sample_hours ? ` / ${pm.sample_hours}h` : ''}` : null,
      pm.shard_count != null ? `shards ${pm.shard_count}` : null,
      pm._from_cache ? `캐시 사용 (${Math.round((pm._cache_age_sec || 0) / 3600)}h 경과)` : null,
      pm.skipped ? `skipped (${pm.reason || 'manual'})` : null,
      pm.error ? `error: ${pm.error}` : null,
    ].filter(Boolean);
    blocks.push(logDetailBlock('probe', el('div', {},
      el('div', { class: 'log-detail-line' }, bits.join(' · ')),
      pm.shards?.length ? logDetailChips(`shard ${pm.shards.length}개`, pm.shards) : null)));
  }

  const sf = it.shard_filters;
  if (sf && Object.keys(sf).length) {
    blocks.push(logDetailBlock('shard filter', el('div', {},
      Object.entries(sf).map(([k, v]) => Array.isArray(v)
        ? logDetailChips(`${k} · ${v.length}개`, v)
        : el('div', { class: 'log-detail-line' }, `${k} = ${JSON.stringify(v)}`)))));
  }

  blocks.push(el('details', { class: 'log-detail-raw' },
    el('summary', {}, '원본 JSON'),
    el('pre', { class: 'text-view' }, JSON.stringify(it, null, 2))));

  return el('div', { class: 'log-detail' }, ...blocks);
}

function logDetailBlock(title, node) {
  return el('div', { class: 'log-detail-block' },
    el('div', { class: 'log-detail-title' }, title), node);
}

function logDetailChips(label, values) {
  return el('details', { class: 'log-detail-chips' },
    el('summary', {}, label),
    el('div', {}, values.map(v => el('span', { class: 'pill' }, String(v)))));
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
      ['lake_api.module',        'text',   null,   '실 API 함수 — "패키지.모듈:함수". 기본값 backend.core.real_lake_adapter:query 는 사내 getData(params, custom_columns=custom_col, user_name=user)를 호출한다'],
      ['lake_api.user',          'text',   null,   '사내 getData 의 user_name — 이 API 의 인증 정보는 이것 하나뿐이다 (키/토큰 없음)'],
      ['lake_api.timeout_sec',   'number', null,   '5분(300) 이하 권장. 기본 290'],
      ['lake_api.min_interval_sec', 'number'],
      ['lake_api.max_concurrent','number', null,   '동시 chunk 실행 수. 기본 3'],
      ['lake_api.retry.attempts','number'],
      ['lake_api.retry.backoff_sec', 'csv', null,  '쉼표 구분 int (예: 10,30,120)'],
      ['lake_api.retryable_errors', 'csv', null,   'HY000, TimeoutError 등'],
    ]},
    // ☁ S3 는 탐색기 ⚙ 한 곳에서만 편집한다 — 연결·전송 규칙·업로드 항목이 한 화면에
    // 있어야 "어디로 무엇이 올라가는지" 가 읽힌다. 여기엔 자리도 두지 않는다.
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
    { key: 'ai', label: '🤖 AI', custom: renderAiSettings, rows: [
      ['llm.enabled',   'bool',   null, 'AI 보조 사용 여부. 꺼 두면 진단 요약이 규칙 요약으로만 나온다 (기능은 그대로 동작)'],
      ['llm.api_url',   'text',   null, '사내 LLM 엔드포인트. OpenAI 호환이면 ".../v1" 까지만 적어도 된다'],
      ['llm.model',     'text',   null, '예: gpt-oss-120b'],
      ['llm.auth_mode', 'select', ['bearer','dep_ticket','none'], 'bearer → Authorization, dep_ticket → x-dep-ticket 헤더'],
      ['llm.token',     'password', null, '저장 후에는 **** 로만 보인다 (빈 칸으로 저장하면 기존 값 유지)'],
      ['llm.format',    'select', ['openai','raw'], 'openai = messages[] · raw = {"prompt": …}'],
      ['llm.timeout_s', 'number', null, '초. 기본 20 — 한 번 실패하면 60초 동안은 호출을 건너뛴다'],
    ]},
    { key: 'types', label: '🧩 소스 타입', custom: renderSourceTypesManager },
  ];

  let active = STATE.settingsActive && sections.find(s => s.key === STATE.settingsActive)
    ? STATE.settingsActive : 'lake';

  const sectionEls = {};
  for (const s of sections) {
    sectionEls[s.key] = s.custom
      ? s.custom(draft, s)          // custom 이 rows 를 직접 그릴 수 있게 섹션째 넘긴다
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
      el('div', { class: 'spacer' }),
      el('button', { class: 'btn primary', onclick: () => onSaveSettings(draft) }, '💾 저장'),
    ),
    btnBar,
    ...Object.values(sectionEls),
  );
}

// 🤖 AI — 사내 LLM 은 **있으면 돕고 없으면 없는 대로** 돈다. 그래서 이 화면의
// 기본값은 '꺼짐' 이고, 상태 줄이 "지금 어느 쪽으로 동작 중인지" 를 먼저 말한다.
function renderAiSettings(draft, section) {
  const statusEl = el('div', { class: 'section-desc' }, '상태 확인 중…');
  const testOut = el('div', { class: 'hint', style: { marginTop: '6px' } }, '');

  const paint = (st) => {
    if (!st) { statusEl.replaceChildren('상태를 읽지 못했습니다.'); return; }
    const on = st.available;
    // replaceChildren 는 el() 과 달리 null 을 "null" 로 찍는다 — 넣기 전에 걸러 낸다
    statusEl.replaceChildren(...[
      el('span', { class: `pill ${on ? 'ok' : 'pending'}` }, on ? 'AI 사용 중' : 'AI 꺼짐'),
      ' ',
      on ? `${st.model || '(모델 미지정)'} · ${st.host || 'url 없음'}`
         : '진단 요약은 규칙 요약으로 나옵니다 — 켜면 같은 내용을 문장으로 정리해 줍니다.',
      st.breaker_open
        ? el('span', { class: 'hint' }, ` · 최근 실패로 ${st.cooldown_s}초간 호출 중단: ${st.last_error}`)
        : null,
    ].filter((c) => c != null));
  };
  api.get('/api/settings/ai').then(paint).catch(() => paint(null));

  const onTest = async (btn) => {
    btn.disabled = true;
    testOut.textContent = '연결 테스트 중…';
    try {
      const r = await api.post('/api/settings/ai/test', {});
      testOut.textContent = r.ok
        ? `✅ 응답 받음 (${r.status?.last_latency_ms ?? '-'}ms): ${r.text || '(빈 응답)'}`
        : `❌ ${r.error || '실패'}`;
      paint(r.status);
    } catch (e) {
      testOut.textContent = `❌ ${e.message}`;
    } finally { btn.disabled = false; }
  };

  return el('div', { class: 'card' },
    el('div', { class: 'card-title' }, '🤖 AI (사내 LLM · 선택)'),
    statusEl,
    el('div', { class: 'section-desc', style: { marginTop: '4px' } },
      'AI 는 판정을 만들지 않습니다. 진단의 ok/warn/fail 과 조치 문구는 언제나 규칙 코드가 만들고, '
      + 'AI 는 그 결과를 문장으로 옮겨 적을 뿐입니다 — 연결이 끊겨도 진단은 그대로 동작합니다.'),
    ...(section?.rows || []).map((row) => settingsRow(row, draft)),
    el('div', { class: 'row', style: { marginTop: '10px', gap: '8px' } },
      el('button', { class: 'btn', onclick: (e) => onTest(e.target) }, '🔌 연결 테스트'),
      el('div', { class: 'hint' }, '※ 위 💾 저장을 먼저 눌러야 바뀐 값으로 테스트합니다.')),
    testOut,
  );
}

function renderSourceTypesManager(_draft) {
  // 독자적인 draft — settings 저장 버튼과 무관하게 별도 저장 버튼 노출.
  // prev_name 은 rename 추적용(서버가 제품 소스명까지 함께 바꾸고 yaml 엔 안 남김).
  const draft = { source_types: SOURCE_TYPES.map(s => ({ ...structuredClone(s), prev_name: s.name })) };
  const card = el('div', { class: 'card' });
  const rerender = () => {
    card.innerHTML = '';
    buildUI();
  };

  const save = async (force = false) => {
    try {
      const r = await api.post('/api/schedule/source-types',
        { ...draft, apply_to_products: true, ...(force ? { force_table: true } : {}) });
      await loadSourceTypes();   // 전역 레지스트리 즉시 갱신
      // prev_name 재기준 — 저장 후 현재 이름이 곧 이전 이름
      for (const st of draft.source_types) st.prev_name = st.name;
      const applied = r.products || {};
      await afterSourceTypesSaved(applied);
      rerender();
      alert(`저장됨 — ${r.count} 개 타입\n${summarizeProductPropagation(applied)}`);
      const conflicts = applied.conflicts || [];
      if (conflicts.length && !force) {
        const list = conflicts.slice(0, 8)
          .map(c => `· ${c.product}/${c.source}: ${c.current} → ${c.template}`).join('\n');
        if (confirm(`제품에서 직접 지정한 table ${conflicts.length}건은 유지했습니다.\n`
                  + `${list}${conflicts.length > 8 ? '\n…' : ''}\n\n이것도 템플릿 값으로 덮어쓸까요?`)) {
          await save(true);
        }
      }
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
        '새 DB 추가 시 여기에 등록. 등록 후 제품 편집기의 소스 드롭다운·모니터 히트맵·컬럼 풀 모두에 반영. '
        + '저장하면 이 소스를 쓰는 제품의 table·소스명(이름 변경 시)·shard 기본값도 함께 갱신됩니다 '
        + '(제품에서 직접 바꾼 table 은 물어본 뒤에만 덮어씀).'),
      ...draft.source_types.map((st, i) => sourceTypeRow(st, i, draft, rerender)),
      el('div', { class: 'row', style: { marginTop: '10px', gap: '6px' } },
        el('button', { class: 'btn', onclick: addType }, '+ 타입 추가'),
        el('div', { class: 'spacer' }),
        el('button', { class: 'btn primary', onclick: () => save(false) }, '💾 저장'),
      ),
    );
  }
  buildUI();
  return card;
}

// source type 저장 결과(products 전파) 를 사람이 읽는 한 줄로.
function summarizeProductPropagation(applied) {
  const ch = applied.changes || [], cf = applied.conflicts || [], or = applied.orphans || [];
  if (!ch.length && !cf.length && !or.length) return '제품 반영: 변경 없음';
  const byField = {};
  for (const c of ch) byField[c.field] = (byField[c.field] || 0) + 1;
  const parts = Object.entries(byField).map(([f, n]) => `${f} ${n}건`);
  const lines = [`제품 반영: ${parts.join(' · ') || '없음'}`];
  if (cf.length) lines.push(`직접 지정한 table 유지: ${cf.length}건`);
  if (or.length) {
    const names = [...new Set(or.map(o => o.source))].join(', ');
    lines.push(`⚠ 레지스트리에 없는 소스를 아직 쓰는 제품: ${or.length}건 (${names})`);
  }
  return lines.join('\n');
}

// products.yaml 이 서버에서 바뀌었으니 화면 상태도 맞춘다.
// 제품 탭에 미저장 편집이 있으면 덮어쓰지 않고 알린다 (그대로 저장하면 전파분이 되돌아감).
async function afterSourceTypesSaved(applied) {
  if (!(applied.changes || []).length) return;
  let fresh;
  try { fresh = await api.get('/api/schedule/products'); } catch (_) { return; }
  const dirty = STATE.productsDraft && STATE.products
    && JSON.stringify(STATE.productsDraft) !== JSON.stringify(STATE.products);
  STATE.products = fresh;
  if (dirty) {
    alert('제품 탭에 저장하지 않은 편집이 있어 화면 값은 그대로 뒀습니다.\n'
        + '그 상태로 저장하면 방금 전파된 table 이 되돌아갑니다 — 제품 탭에서 "되돌리기" 후 확인하세요.');
  } else {
    STATE.productsDraft = structuredClone(fresh);
  }
  for (const k of Object.keys(_columnCache)) delete _columnCache[k];
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
// Diagnose tab — 사내 반입 시 "어디서 막혔는지" 를 단계별로 하나씩 확인한다.
//   raw 가 된다 → event 가 정확하다 → feature/매칭테이블이 S3 에서 온다
// 검사마다 백엔드가 열어볼 parquet(view)을 같이 주므로, 결과를 숫자로만 보지 않고
// 탐색기(파일 뷰어)에서 눈으로 확인할 수 있다.
// ─────────────────────────────────────
const DG = { vehicle: '', data: null, busy: false };
const DG_TONE = {
  ok: { ico: '●', color: 'var(--ok)', label: '정상' },
  warn: { ico: '▲', color: 'var(--warn)', label: '확인' },
  fail: { ico: '✕', color: 'var(--err)', label: '실패' },
  skip: { ico: '–', color: 'var(--text-muted)', label: '해당없음' },
};

async function renderDiagnose() {
  const main = $('#main');
  main.innerHTML = '';
  main.append(
    el('div', { class: 'row', style: { marginBottom: '12px', gap: '8px', alignItems: 'flex-start' } },
      el('div', { class: 'spacer' }),
      el('div', { id: 'dgPicker' }),
    ),
    el('div', { id: 'dgBody' }, el('div', { class: 'loading' }, 'Loading…')),
  );
  try {
    const vehicles = await api.get('/api/pipeline/vehicles');
    const names = Object.keys(vehicles || {});
    if (!DG.vehicle || !names.includes(DG.vehicle)) DG.vehicle = names[0] || '';
    $('#dgPicker').replaceChildren(
      el('select', { style: { fontSize: '12px' },
        onchange: (e) => { DG.vehicle = e.target.value; DG.data = null; loadDiagnose(); } },
        ...names.map((n) => el('option', n === DG.vehicle ? { value: n, selected: '' } : { value: n }, n))),
      el('button', { class: 'btn', style: { marginLeft: '6px' }, onclick: () => loadDiagnose() }, '↻ 다시 검사'),
    );
  } catch (e) {
    $('#dgPicker').replaceChildren(el('span', { class: 'hint' }, String(e.message || e)));
  }
  await loadDiagnose();
}

async function loadDiagnose() {
  const body = $('#dgBody');
  if (!body) return;
  if (!DG.vehicle) {
    body.replaceChildren(el('div', { class: 'empty' }, '제품이 없습니다 — 제품 탭에서 먼저 등록하세요.'));
    return;
  }
  body.replaceChildren(el('div', { class: 'loading' }, `${DG.vehicle} 검사 중…`));
  try {
    DG.data = await api.get(`/api/pipeline/diagnose/${encodeURIComponent(DG.vehicle)}`);
  } catch (e) {
    body.replaceChildren(el('div', { class: 'alert err' }, String(e.message || e)));
    return;
  }
  const d = DG.data;
  body.replaceChildren(
    dgSummary(d),
    dgAiCard(d),
    ...d.stages.map((s, i) => dgStageCard(s, i, d)),
  );
}

function dgSummary(d) {
  const blocked = d.blocked_at && d.stages.find((s) => s.key === d.blocked_at);
  return el('div', { class: `alert ${d.status === 'ok' ? 'ok' : d.status === 'warn' ? 'warn' : 'err'}`,
    style: { marginBottom: '12px' } },
    el('div', { style: { fontWeight: 700 } },
      d.status === 'ok' ? `${d.vehicle} — 세 단계 모두 통과`
        : blocked ? `${d.vehicle} — 「${blocked.title}」 에서 막혔습니다`
        : `${d.vehicle} — 확인이 필요한 항목이 있습니다`),
    el('div', { class: 'mono', style: { fontSize: '11px', marginTop: '3px' } },
      `product ${d.product} · db ${d.db_root} · ${fmtClock(d.ts)}`),
  );
}

// 요약 카드 — 아래 표를 한 덩어리로 줄여 준다. 사내 LLM 이 연결돼 있으면 문장으로,
// 아니면 같은 사실을 규칙이 옮겨 적는다(source). **AI 가 없어도 버튼은 동작한다** —
// 없을 때 사라지는 기능이 되면 사람은 이 화면을 신뢰하지 않는다.
function dgAiCard(d) {
  const out = el('div', { class: 'hint' }, '아래 검사 결과를 한 덩어리로 줄여 줍니다.');
  const btn = el('button', { class: 'btn', onclick: () => run() }, '📝 요약 만들기');

  async function run() {
    btn.disabled = true;
    out.replaceChildren(el('span', { class: 'loading' }, '요약 중…'));
    try {
      const r = await api.post(`/api/pipeline/diagnose/${encodeURIComponent(d.vehicle)}/summary`, {});
      out.replaceChildren(
        el('div', { class: 'row', style: { gap: '6px', marginBottom: '4px' } },
          el('span', { class: `pill ${r.source === 'ai' ? 'brand' : 'pending'}` },
            r.source === 'ai' ? `AI · ${r.model || '모델'}` : '규칙 요약'),
          r.error ? el('span', { class: 'hint' }, r.error) : null),
        el('pre', { class: 'log-detail-pre', style: { whiteSpace: 'pre-wrap', margin: 0 } },
          r.text || ''),
      );
    } catch (e) {
      out.replaceChildren(el('div', { class: 'alert err' }, String(e.message || e)));
    } finally { btn.disabled = false; }
  }

  return el('div', { class: 'card', style: { marginBottom: '14px' } },
    el('div', { class: 'row', style: { gap: '8px', alignItems: 'center', marginBottom: '6px' } },
      el('span', { style: { fontWeight: 700 } }, '📝 요약'),
      el('div', { class: 'spacer' }), btn),
    out,
  );
}

function dgStageCard(stage, idx, d) {
  const tone = DG_TONE[stage.status] || DG_TONE.skip;
  const rows = stage.checks.map((c) => {
    const t = DG_TONE[c.status] || DG_TONE.skip;
    return el('tr', { style: c.status === 'skip' ? { opacity: 0.5 } : {} },
      el('td', { style: { color: t.color, fontWeight: 700, whiteSpace: 'nowrap' } }, `${t.ico} ${t.label}`),
      el('td', { style: { fontWeight: 600, whiteSpace: 'nowrap' } }, ...withSourceBadges(c.name)),
      el('td', {},
        el('div', {}, c.detail || ''),
        c.fix ? el('div', { class: 'hint', style: { marginTop: '2px' } }, `→ ${c.fix}`) : null),
      el('td', { style: { whiteSpace: 'nowrap' } },
        c.view ? el('button', { class: 'btn ghost xsmall',
          title: `${c.view.root}/${c.view.file}`,
          onclick: () => openInViewer(c.view) }, `📊 ${c.view.label}`) : null),
    );
  });
  // 앞 단계가 실패면 뒤 단계 결과는 그 여파일 수 있다 — 순서대로 보라고 알려 준다
  const prevFailed = d.stages.slice(0, idx).some((s) => s.status === 'fail');
  return el('div', { class: 'card', style: { marginBottom: '14px' } },
    el('div', { style: { display: 'flex', alignItems: 'baseline', gap: '10px', marginBottom: '4px' } },
      el('span', { style: { color: tone.color, fontWeight: 800 } }, tone.ico),
      el('span', { style: { fontWeight: 700 } }, stage.title),
      el('span', { class: 'hint' }, stage.desc),
      el('span', { class: 'spacer' }),
      el('span', { style: { fontSize: '12px', color: stage.failed ? 'var(--err)' : 'var(--text-muted)' } },
        `실패 ${stage.failed} · 확인 ${stage.warned} · 전체 ${stage.checks.length}`),
    ),
    prevFailed && stage.status !== 'ok'
      ? el('div', { class: 'hint', style: { marginBottom: '4px' } },
        '앞 단계가 막혀 있어 이 단계의 결과는 그 여파일 수 있습니다 — 위부터 해결하세요.')
      : null,
    el('table', { class: 'tbl' },
      el('thead', {}, el('tr', {},
        el('th', {}, '판정'), el('th', {}, '검사'), el('th', {}, '결과'), el('th', {}, '확인'))),
      el('tbody', {}, rows.length ? rows
        : el('tr', {}, el('td', { colspan: '4', style: { color: 'var(--text-muted)' } }, '검사 항목 없음'))),
    ),
  );
}

// 검사 결과 → 탐색기(parquet 뷰어) 로 이동해 그 파일을 바로 연다.
function openInViewer(view) {
  BR.sql = view.sql || '';
  route('browser');
  // renderBrowser 는 DOM 을 동기로 만든 뒤 루트 목록만 await 한다 —
  // 다음 tick 이면 #brTree/#brView 가 이미 있다.
  setTimeout(() => selectFile(view.root, view.file), 0);
}

// ─────────────────────────────────────
// Browser tab
// ─────────────────────────────────────
// 화면 구성은 flow 파일탐색기와 같은 규격이다 (좌측 260px 사이드바 = 범위 칩 → 루트 → 현재 폴더,
// 우측 = SQL 바 + 가이드 + 결과). S3 전송 설정은 전부 ⚙ 모달로 모았다 — 사이드바에 편집기를
// 끼워 넣으면 "지금 보고 있는 폴더" 가 무엇인지 읽히지 않는다.
let BR = { root: 'staging', path: '', selFile: '', sql: '', s3mode: 'sync', s3rules: null,
  scope: 'data', roots: [], guide: false };

// 루트를 두 범위로 가른다 — 데이터(db 아래: db·staging) / 설정·연동(config·outbox·s3_local)
const BR_SCOPES = [
  { key: 'data', icon: '💾', label: '데이터', desc: '파이프라인 산출물 (db · staging)' },
  { key: 'files', icon: '📁', label: '설정·연동', desc: '설정파일 · 알람 outbox · S3 로컬 저장소' },
];

function brScopeOf(r, dbPath) {
  const path = (r.path || '').replace(/\\/g, '/').toLowerCase();
  const isDbArea = r.name === 'db' || (dbPath && (path === dbPath || path.startsWith(dbPath + '/')));
  return isDbArea ? 'data' : 'files';
}

function brRootsInScope(scope) {
  const dbRoot = BR.roots.find((r) => r.name === 'db');
  const dbPath = (dbRoot?.path || '').replace(/\\/g, '/').replace(/\/$/, '').toLowerCase();
  return BR.roots.filter((r) => brScopeOf(r, dbPath) === scope);
}

async function renderBrowser() {
  const main = $('#main');
  BR.s3rules = null;  // 탭 진입 시 전송 규칙 재조회 (편집 중에만 메모리 유지)
  main.innerHTML = '';
  main.append(
    el('div', { class: 'fb-shell' },
      el('aside', { class: 'fb-side' },
        el('div', { class: 'fb-side-head' },
          el('span', {}, '파일 탐색기'),
          el('span', { class: 'fb-count', id: 'brCount' }, ''),
          // 톱니는 flow 의 공용 톱니(PageGearButton) 와 같은 규격 — 40×40 원형 ⚙️.
          // 두 앱을 나란히 쓰는 화면이라 "설정은 이 동그란 톱니" 로 같이 읽혀야 한다.
          el('button', { class: 'gear-btn', type: 'button', 'aria-label': 'S3 설정',
            title: 'S3 설정 — 연결(key)·전송 규칙·업/다운로드 항목·이력',
            onclick: openS3Modal }, '⚙️'),
        ),
        el('div', { class: 'fb-scope', id: 'brScope' }),
        el('div', { class: 'fb-side-body' },
          el('div', { class: 'fb-sec-title' }, '루트'),
          el('div', { id: 'brRootList' }, el('div', { class: 'fb-empty' }, 'loading…')),
          el('div', { class: 'fb-sec-title' }, '현재 폴더',
            el('span', { class: 'fb-crumb', id: 'brCrumb' }, '')),
          el('div', { id: 'brTree' }),
        ),
        el('div', { class: 'fb-side-foot', id: 'brLegend' }),
      ),
      el('section', { class: 'fb-main' },
        el('div', { class: 'fb-sqlbar' },
          el('span', { class: 'fb-sqlbar-label' }, 'SQL:'),
          el('input', { type: 'text', placeholder: "예: SELECT lot_id, wafer_id FROM t WHERE root_lot_id = 'R001'",
            value: BR.sql, oninput: (e) => BR.sql = e.target.value,
            onkeydown: (e) => { if (e.key === 'Enter') reloadView(); } }),
          el('button', { class: 'btn primary', onclick: reloadView }, '실행'),
          el('button', { class: 'btn', onclick: () => { BR.sql = ''; renderBrowser(); } }, '초기화'),
          el('button', { class: 'btn', title: 'DB의 WIDE FORM을 합쳐 FAB·INLINE·VM 컬럼을 한 번에 조회',
            onclick: reloadCombinedView }, '통합 보기'),
          el('span', { class: 'hint' }, '표시는 200행까지'),
        ),
        renderSqlGuide(),
        el('div', { id: 'brView', class: 'fb-content' },
          el('div', { class: 'fb-empty' }, '왼쪽에서 파일을 고르세요 — parquet/csv 는 SQL 로, yaml/json/txt 는 텍스트로 엽니다.'),
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
  return el('details', { class: 'sql-guide', ...(BR.guide ? { open: '' } : {}),
    ontoggle: (e) => { BR.guide = e.target.open; } },
    el('summary', {}, 'SQL 가이드(예시) — polars SQL'),
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
//
// 화면 규격은 flow 의 톱니 패널과 같다 (flow 파일탐색기 ⚙ · 캐시 예산 ⚙):
//   머리(제목 + 닫기) → 탭 줄(선택 탭은 accent-glow) → 본문.
//   본문에서 무언가를 조절하는 단위는 항상 **카드 한 장 = 제목 + 설명 + 입력**이다.
//   설명 없이 입력만 있는 줄은 두지 않는다 — "이 값이 뭘 바꾸는지" 를 같은 카드에서 읽어야 한다.
// ─────────────────────────────────────
const S3M = { open: false, tab: 'items', form: null, hist: [], timer: null, keyBrowse: null,
  settings: null, onKey: null };

// 자동 갱신해도 되는 탭 = 입력 폼이 없는 탭. 나머지는 다시 그리면 입력 중인 값이 날아간다.
const S3M_LIVE_TABS = ['items', 'history'];

function openS3Modal() {
  S3M.open = true; S3M.tab = 'items'; S3M.form = null; S3M.settings = null;
  renderS3Modal();
  if (S3M.timer) clearInterval(S3M.timer);
  S3M.timer = setInterval(() => {
    if (!S3M.open) return closeS3Modal();
    if (S3M_LIVE_TABS.includes(S3M.tab)) renderS3Modal(true);
  }, 5000);
  // Esc 로 닫기 — flow 톱니 패널과 같은 동작
  S3M.onKey = (e) => { if (e.key === 'Escape') closeS3Modal(); };
  window.addEventListener('keydown', S3M.onKey);
}

function closeS3Modal() {
  S3M.open = false;
  if (S3M.timer) { clearInterval(S3M.timer); S3M.timer = null; }
  if (S3M.onKey) { window.removeEventListener('keydown', S3M.onKey); S3M.onKey = null; }
  document.getElementById('s3modal')?.remove();
  loadBrowserRoots();
}

// 조절 단위 한 장 — flow 톱니 패널의 카드와 같은 구성(제목 → 설명 → 입력).
function s3Card(title, desc, ...kids) {
  return el('div', { class: 's3-card' },
    el('div', { class: 's3-card-title' }, title),
    desc ? el('div', { class: 's3-card-desc' }, desc) : null,
    ...kids,
  );
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
  const tabBtn = ([id, label]) => el('button', {
    class: 's3-tab' + (S3M.tab === id ? ' on' : ''), type: 'button',
    title: S3M_TAB[id].desc,
    onclick: () => { S3M.tab = id; renderS3Modal(); },
  }, label, id === 'items' ? el('span', { class: 's3-tab-count' }, String(data.items.length)) : null);

  const body = S3M.tab === 'add' ? s3FormView(data, dests)
    : S3M.tab === 'history' ? s3HistoryView()
      : S3M.tab === 'conn' ? s3ConnView()
        : S3M.tab === 'rules' ? s3Card(S3M_TAB.rules.title, S3M_TAB.rules.desc, el('div', { id: 's3RulesBox' }))
          : S3M.tab === 'files' ? s3Card(S3M_TAB.files.title, S3M_TAB.files.desc, el('div', { id: 's3FilesBox' }))
            : s3ItemsView(data);

  const modal = el('div', { id: 's3modal', class: 's3-modal-back',
    onclick: (e) => { if (e.target.id === 's3modal') closeS3Modal(); } },
    el('div', { class: 's3-modal' },
      el('div', { class: 's3-modal-head' },
        el('span', { class: 's3-modal-title' }, '⚙️ S3 설정'),
        el('span', { class: 's3-modal-sub' }, S3M_TAB[S3M.tab]?.title || ''),
        el('span', { class: 'spacer' }),
        el('button', { class: 's3-modal-x', type: 'button', title: '닫기 (Esc)',
          'aria-label': '닫기', onclick: closeS3Modal }, '✕'),
      ),
      el('div', { class: 's3-modal-tabs' }, ...Object.entries(S3M_TAB).map(([id, t]) => tabBtn([id, t.label]))),
      el('div', { class: 's3-modal-body' }, body),
    ));
  document.body.append(modal);
  if (S3M.tab === 'rules') loadS3RulesSection('rules');
  if (S3M.tab === 'files') loadConfigFilesSection();
}

// 탭 = 조절 대상 하나. title/desc 는 본문 카드의 제목·설명으로 그대로 쓴다
// (flow 톱니처럼 "지금 무엇을 조절하는 중인지" 를 본문 안에서 읽게 한다).
const S3M_TAB = {
  conn: { label: '연결', title: 'S3 접속 정보',
    desc: '기본 연결(default)은 파이프라인 업로드에도 그대로 쓰인다. 다른 버킷/계정이 필요하면 아래에 연결을 더 등록한다.' },
  rules: { label: '전송 규칙', title: 'root 별 전송 규칙',
    desc: 'root 별 기본 전송 방식(cp/sync)과 올릴 위치. 탐색기 목록의 ⇧ S3 버튼이 이 규칙을 따른다.' },
  items: { label: '항목', title: '업/다운로드 항목',
    desc: '주기·수동으로 도는 업/다운로드 항목. ▶ 실행 / ■ 중지 는 안전 지점(파일 경계)에서만 끊는다.' },
  add: { label: '+ 추가', title: '항목 추가·수정',
    desc: '로컬 경로 ↔ S3 key 한 쌍. 방향·명령·주기를 각각 정한다.' },
  files: { label: '설정파일', title: '설정파일 → S3 개별 전송',
    desc: '설정파일을 S3 로 하나씩 전송한다. 이름을 누르면 탐색기에서 그 파일이 열린다.' },
  history: { label: '이력', title: '최근 실행 이력',
    desc: '최근 100건. 실패한 항목은 오류 열에 이유가 남는다.' },
};

// 연결 탭 — settings.json 의 s3.* (구 설정 탭 ☁ 섹션) + 추가 S3 연결(destinations).
function s3ConnView() {
  const box = el('div', {});
  const build = (draft) => {
    box.innerHTML = '';
    const rows = [
      ['s3.enabled', 'bool', null, '끄면 파이프라인은 로컬 DB 저장까지만 하고 S3 작업을 전부 건너뜀'],
      ['s3.endpoint_url', 'text', null, '비우면 AWS S3 / MinIO 는 http://host:9000'],
      ['s3.bucket', 'text'],
      ['s3.prefix', 'text'],
      ['s3.access_key', 'text'],
      ['s3.secret_key', 'password', null, '저장 후 ****. 그대로 두면 기존 값 유지'],
      ['s3.fake_local_path', 'text', null, 'endpoint_url 비어있고 이 값 있으면 개발 모드 (로컬 폴더)'],
      ['s3.upload_mode', 'select', ['immediate', 'interval', 'manual'], 'immediate=chunk 직후 / interval=주기적 flush / manual=버튼'],
      ['s3.upload_interval_sec', 'number', null, 'interval 모드에서만 의미. 기본 300초(5분). 최소 5초'],
      ['s3.retry_failed_sec', 'number', null, '업로드 실패 항목 재시도 간격. 기본 120초'],
    ];
    box.append(
      s3Card('기본 연결 (default)', S3M_TAB.conn.desc,
        ...rows.map((row) => settingsRow(row, draft)),
        el('div', { class: 's3-card-foot' },
          el('button', { class: 'btn primary small', onclick: async (e) => {
            const btn = e.target;
            try {
              await api.post('/api/settings', { s3: draft.s3 });
              STATE.settings = null;   // 설정 탭 재조회
              S3M.settings = null;
              btn.textContent = '✓ 저장됨';
              setTimeout(() => { btn.textContent = '💾 연결 저장'; }, 1500);
            } catch (err) { alert(`저장 실패: ${err.message}`); }
          } }, '💾 연결 저장'))),
      s3Card('추가 S3 연결', '기본 연결 외에 다른 버킷·계정을 쓸 때 등록한다. 전송 규칙에서 이 이름으로 고른다.',
        el('div', { id: 's3ConnBox' })),
    );
    loadS3RulesSection('conn');
  };
  if (S3M.settings) build(S3M.settings);
  else {
    box.append(el('div', { class: 'loading' }, 'Loading…'));
    api.get('/api/settings')
      .then((s) => { S3M.settings = structuredClone(s); build(S3M.settings); })
      .catch((e) => { box.innerHTML = ''; box.append(el('div', { class: 'alert err' }, e.message)); });
  }
  return box;
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
      : st.last_status === 'ok' ? 'var(--ok)' : st.last_status === 'error' ? 'var(--danger)' : 'var(--text-muted)';
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
          ? el('button', { class: 'btn small', style: { color: 'var(--danger)' },
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

  return s3Card(S3M_TAB.items.title, S3M_TAB.items.desc,
    el('div', { class: 'row', style: { gap: '8px', flexWrap: 'wrap' } },
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

  return s3Card(S3M.form?.id ? `항목 수정 — ${S3M.form.id}` : S3M_TAB.add.title, S3M_TAB.add.desc,
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
    el('td', { style: { color: h.status === 'ok' ? 'var(--ok)' : h.status === 'cancelled' ? 'var(--warn)' : 'var(--danger)' } },
      ({ ok: '성공', error: '실패', cancelled: '중지됨' }[h.status] || h.status)),
    el('td', { class: 'mono' }, `${h.duration_sec ?? '-'}s`),
    el('td', { class: 'mono' }, h.direction || '-'),
    el('td', { class: 'mono' }, `옮김 ${h.moved ?? 0} · 생략 ${h.skipped ?? 0} · 실패 ${h.failed ?? 0}`),
    el('td', { style: { color: 'var(--danger)', fontSize: '11px' } }, h.error || ''),
  ));
  return s3Card(S3M_TAB.history.title, S3M_TAB.history.desc,
    el('div', { style: { maxHeight: '56vh', overflow: 'auto' } },
      alTable(['시각', 'id', '결과', '소요', '방향', '건수', '오류'], rows)));
}

// 탐색기 하단 범례 — 화살표가 무슨 뜻인지 화면에서 바로 읽히게
function syncLegend() {
  const item = (dir, state, text) => el('span', { class: 'fb-legend-item' },
    syncBadge({ dir, state, detail: '' }), text);
  return el('div', { class: 'fb-legend' },
    item('down', 'ok', '받기 (flow 관리)'),
    item('up', 'ok', '올리기 (Valve 산출)'),
    item('both', 'ok', '양방향'),
    item(null, 'local', '로컬 전용'),
    el('span', { class: 'hint' }, '점 = 상태 · 초록 정상 / 주황 대기 / 빨강 실패'),
  );
}

// 사이드바 한 줄 — flow 파일탐색기와 같은 2단 구성(이름 + 메타줄).
function fbRow({ sel, sig, icon, name, title, badge, meta, onclick, actions }) {
  return el('div', { class: 'fb-row' + (sel ? ' sel' : ''), title: title || name, onclick },
    sig !== undefined ? syncBadge(sig) : null,
    el('span', { class: 'fb-ic' }, icon),
    el('span', { class: 'fb-stack' },
      el('span', { class: 'fb-name' }, name),
      badge || meta ? el('span', { class: 'fb-meta' },
        badge ? el('span', { class: 'fb-ext' }, badge) : null,
        meta ? el('span', { class: 'fb-size' }, meta) : null,
      ) : null,
    ),
    ...(actions || []),
  );
}

function renderScopeChips() {
  const box = $('#brScope');
  if (!box) return;
  box.innerHTML = '';
  for (const s of BR_SCOPES) {
    const roots = brRootsInScope(s.key);
    box.append(el('span', {
      class: 'fb-chip' + (BR.scope === s.key ? ' on' : '') + (roots.length ? '' : ' off'),
      title: `${s.desc}${roots.length ? '' : '\n(표시할 루트 없음)'}`,
      onclick: () => {
        if (!roots.length || BR.scope === s.key) return;
        BR.scope = s.key;
        renderScopeChips();
        renderRootList();
        loadBrowserDir(roots[0].name, '');
      },
    }, `${s.icon} ${s.label}`));
  }
}

function renderRootList() {
  const box = $('#brRootList');
  if (!box) return;
  box.innerHTML = '';
  const roots = brRootsInScope(BR.scope);
  if (!roots.length) { box.append(el('div', { class: 'fb-empty' }, '이 범위에 표시할 루트가 없습니다.')); return; }
  for (const r of roots) {
    box.append(fbRow({
      sel: BR.root === r.name,
      sig: r.dir ? { dir: r.dir, state: 'ok', detail: r.detail } : null,
      icon: '📂',
      name: r.name === 'config' ? '설정파일' : r.name,
      title: `${r.path}\n${r.detail || ''}`,
      meta: r.path,
      onclick: () => loadBrowserDir(r.name, ''),
    }));
  }
  const count = $('#brCount');
  if (count) count.textContent = `${roots.length} roots`;
}

async function loadBrowserRoots() {
  try {
    const { roots } = await api.get('/api/browser/roots');
    BR.roots = roots || [];
    const cur = BR.roots.find((r) => r.name === BR.root);
    const dbRoot = BR.roots.find((r) => r.name === 'db');
    const dbPath = (dbRoot?.path || '').replace(/\\/g, '/').replace(/\/$/, '').toLowerCase();
    if (cur) BR.scope = brScopeOf(cur, dbPath);
    renderScopeChips();
    renderRootList();
    loadBrowserDir(BR.root, BR.path);
  } catch (e) { $('#brTree').textContent = String(e); }
  const lg = $('#brLegend');
  if (lg) { lg.innerHTML = ''; lg.append(syncLegend()); }
}

// S3 전송 규칙 — root 별 mode(cp/sync) + 타겟(S3 연결 × prefix 이름, 2개 이상 가능) 편집.
// 설정파일은 보통 cp(항상 덮어쓰기), DB 산출물은 sync(변경분만).
// v0.3.12: 탐색기 사이드바에서 ⚙ 모달의 '연결'·'전송 규칙' 탭으로 옮겼다.
//   part='conn'  → S3 연결(key) 목록만
//   part='rules' → root 별 전송 규칙만
async function loadS3RulesSection(part) {
  const box = $(part === 'conn' ? '#s3ConnBox' : '#s3RulesBox');
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
      el('span', { style: { fontSize: '11px', color: 'var(--text-muted)' } }, '아래 기본 연결(위 폼) 을 그대로 사용'));
    return el('div', { class: 'cfg-row', style: { flexWrap: 'wrap' } },
      inp(name, '이름', (v) => { if (v && v !== name) { destinations[v] = d; delete destinations[name]; loadS3RulesSection('conn'); } }),
      inp(d.bucket, 'bucket', (v) => { d.bucket = v; }),
      inp(d.endpoint_url, 'endpoint_url', (v) => { d.endpoint_url = v; }, true),
      inp(d.access_key, 'access key', (v) => { d.access_key = v; }),
      inp(d.secret_key, 'secret key', (v) => { d.secret_key = v; }),
      el('button', { class: 'btn ghost xsmall', title: '연결 삭제', onclick: () => {
        delete destinations[name];
        Object.values(rules).forEach((r) => { r.targets = r.targets.filter((t) => t.dest !== name); });
        loadS3RulesSection('conn');
      } }, '✕'));
  };

  // ── 규칙 — root 별 mode + 타겟 목록 (연결 선택 × prefix 이름) ──
  const modeSeg = (rule, m) => el('button', {
    class: 'seg' + (rule.mode === m ? ' on' : ''),
    title: m === 'sync' ? '변경분만 업로드 (텍스트=내용·바이너리=크기 비교)' : '항상 덮어쓰기 업로드',
    onclick: () => { rule.mode = m; loadS3RulesSection('rules'); },
  }, m);
  const targetRow = (rule, t, i) => el('div', { class: 'cfg-row', style: { paddingLeft: '58px' } },
    el('select', { class: 'mono', style: { flex: '0 0 90px', fontSize: '11px' },
      onchange: (e) => { t.dest = e.target.value; } },
      ...destNames().map((n) => el('option', { value: n, selected: t.dest === n ? 'selected' : undefined }, n))),
    inp(t.prefix, 'S3 prefix (이름)', (v) => { t.prefix = v; }, true),
    rule.targets.length > 1 ? el('button', { class: 'btn ghost xsmall', title: '타겟 삭제',
      onclick: () => { rule.targets.splice(i, 1); loadS3RulesSection('rules'); } }, '✕') : null);
  const ruleRows = (root) => {
    const rule = rules[root];
    return [
      el('div', { class: 'cfg-row' },
        el('span', { class: 'cfg-name', style: { flex: '0 0 52px', fontWeight: 700 } }, root),
        el('span', { class: 'cfg-mode' }, modeSeg(rule, 'cp'), modeSeg(rule, 'sync')),
        el('span', { class: 'spacer' }),
        el('button', { class: 'btn ghost xsmall', title: '전송 대상(S3 연결×이름) 추가',
          onclick: () => { rule.targets.push({ dest: 'default', prefix: '' }); loadS3RulesSection('rules'); } }, '＋ 대상'),
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

  const saveBtn = (label) => el('button', { class: 'btn primary small', onclick: async (e) => {
    const btn = e.target;
    try {
      const r = await api.put('/api/browser/s3-transfer/config', { rules, destinations });
      BR.s3rules = { rules: r.rules, destinations: r.destinations };
      btn.textContent = '✓ 저장됨';
      setTimeout(() => { btn.textContent = label; loadS3RulesSection(part); }, 1200);
    } catch (err) { alert(err.message); }
  } }, label);

  if (part === 'conn') {
    box.append(
      el('div', { class: 'cfg-list', style: { maxHeight: 'none' } },
        ...destNames().map(destRow),
        el('div', { class: 'cfg-row' },
          el('button', { class: 'btn ghost xsmall', onclick: () => {
            let n = 2; while (destinations[`s3_${n}`]) n++;
            destinations[`s3_${n}`] = { bucket: '', endpoint_url: '', access_key: '', secret_key: '' };
            loadS3RulesSection('conn');
          } }, '＋ S3 연결 추가'),
          el('span', { class: 'spacer' }),
          saveBtn('💾 연결 저장'))),
    );
    return;
  }
  box.append(
    el('div', { class: 'cfg-list', style: { maxHeight: 'none' } },
      ...Object.keys(rules).flatMap(ruleRows),
      el('div', { class: 'cfg-row' }, el('span', { class: 'spacer' }), saveBtn('💾 규칙 저장'))),
  );
}

// "설정파일 → S3" 빠른 목록 — 각 파일 개별 전송 (sync/cp 선택). ⚙ 모달의 '설정파일' 탭.
async function loadConfigFilesSection() {
  const box = $('#s3FilesBox');
  if (!box) return;
  let files = [];
  try { files = (await api.get('/api/browser/config-files')).files || []; } catch { }
  box.innerHTML = '';
  if (!files.length) { box.append(el('div', { class: 'fb-empty' }, '설정파일이 없습니다.')); return; }
  const modeBtn = (m) => el('button', {
    class: 'seg' + (BR.s3mode === m ? ' on' : ''),
    title: m === 'sync' ? '내용 다를 때만 업로드' : '항상 덮어쓰기 업로드',
    onclick: () => { BR.s3mode = m; loadConfigFilesSection(); },
  }, m);
  const configRoot = BR.roots.find((r) => r.name === 'config')?.path || 'config';
  box.append(
    el('div', { class: 'cfg-row' },
      el('span', { class: 'config-root-path', title: configRoot }, configRoot),
      el('span', { class: 'spacer' }),
      el('span', { class: 'cfg-mode' }, modeBtn('sync'), modeBtn('cp'))),
    el('div', { class: 'cfg-list', style: { maxHeight: '48vh' } },
      ...files.map((f) => el('div', { class: 'cfg-row' },
        syncBadge(f.sync),
        el('span', { class: 'cfg-name', title: `→ s3://${f.s3_key}\n(클릭하면 탐색기에서 이 파일을 엽니다)`,
          onclick: () => { closeS3Modal(); BR.root = 'config'; selectFile('config', f.rel); } }, f.rel),
        el('button', { class: 'btn ghost xsmall', title: `S3 전송 (${BR.s3mode}) → ${f.s3_key}`,
          onclick: () => onS3Transfer('config', f.rel) }, '⇧ S3'),
      ))),
  );
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
  renderRootList();
  const crumb = $('#brCrumb');
  const tree = $('#brTree');
  if (!tree) return;
  if (crumb) {
    // breadcrumb — 세그먼트를 눌러 위 폴더로. flow 와 같이 섹션 제목 옆에 둔다.
    crumb.innerHTML = '';
    const parts = String(path || '').split('/').filter(Boolean);
    crumb.append(el('span', { class: 'fb-crumb-seg', onclick: () => loadBrowserDir(root, '') }, root));
    parts.forEach((seg, i) => {
      const upto = parts.slice(0, i + 1).join('/');
      crumb.append(el('span', { class: 'fb-crumb-sep' }, '/'),
        el('span', { class: 'fb-crumb-seg', onclick: () => loadBrowserDir(root, upto) }, seg));
    });
  }
  try {
    const r = await api.get(`/api/browser/list?root=${encodeURIComponent(root)}&path=${encodeURIComponent(path)}`);
    tree.innerHTML = '';
    if (path) {
      const parent = path.split('/').slice(0, -1).join('/');
      tree.append(fbRow({ icon: '↩', name: '상위 폴더', title: `${root}/${parent}`,
        onclick: () => loadBrowserDir(root, parent) }));
    }
    if (!r.entries.length) tree.append(el('div', { class: 'fb-empty' }, '비어있음'));
    r.entries.forEach((e) => {
      const fullPath = path ? `${path}/${e.name}` : e.name;
      const icon = e.is_dir ? '📂' : (e.suffix === '.parquet' ? '📊' : (e.suffix === '.csv' ? '📋'
        : (['.yaml', '.yml', '.json'].includes(e.suffix) ? '⚙️' : '📄')));
      // db/staging 은 파일·폴더 단위 S3 전송 지원 (폴더=재귀, 모드는 전송 규칙 기본)
      const canSend = (root === 'db' || root === 'staging');
      const sendBtn = el('button', { class: 'btn ghost xsmall', title: 'S3 전송 (규칙 기본 모드)',
        onclick: async (ev) => {
          ev.stopPropagation();
          const btn = ev.target;
          btn.disabled = true; btn.textContent = '…';
          try {
            const res = await api.post('/api/browser/s3-transfer', { root, path: fullPath });
            btn.textContent = res.files ? `↑${res.uploaded} =${res.unchanged}` : '✓';
            if (res.errors) alert(`${fullPath}: ${res.errors}건 전송 실패`);
          } catch (err) { btn.textContent = '✗'; alert(err.message); }
          finally { btn.disabled = false; setTimeout(() => { btn.textContent = '⇧ S3'; }, 4000); }
        } }, '⇧ S3');
      tree.append(fbRow({
        sel: BR.selFile === fullPath,
        sig: e.sync,
        icon,
        name: e.name,
        title: fullPath,
        badge: e.is_dir ? 'DIR' : (e.suffix || '').replace('.', '').toUpperCase(),
        meta: e.is_dir ? '' : fmtBytes(e.size),
        onclick: () => e.is_dir ? loadBrowserDir(root, fullPath) : selectFile(root, fullPath),
        actions: canSend ? [sendBtn] : [],
      }));
    });
  } catch (e) { tree.textContent = String(e); }
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

async function reloadCombinedView() {
  const view = $('#brView');
  view.innerHTML = '<div class="loading">FAB · INLINE · VM 통합 로딩…</div>';
  try {
    const url = `/api/query/combined?root=db&sql=${encodeURIComponent(BR.sql)}&rows=200`;
    const r = await api.get(url);
    view.innerHTML = '';
    const headCells = r.columns.map((c) => el('th', { title: r.dtypes[c] }, c));
    const bodyRows = r.rows.map((row) => el('tr', {},
      ...r.columns.map((c) => el('td', { class: 'mono' }, String(row[c] ?? '')))));
    view.append(
      el('div', { style: { padding: '10px 14px', fontSize: '12px', color: 'var(--text-secondary)', borderBottom: '1px solid var(--border)' } },
        el('span', { class: 'mono' }, '4.WIDE_FORM · FAB + INLINE + VM 통합'), '  ·  ',
        `${r.n_rows} rows · ${r.columns.length} cols · ${r.files.length} files`),
      el('table', { class: 'tbl' },
        el('thead', {}, el('tr', {}, ...headCells)),
        el('tbody', {}, ...bodyRows)));
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
    el('div', { id: 'alWrap' }, el('div', { class: 'loading' }, 'Loading…')),
  );
  await loadAlerts();
}

async function loadAlerts() {
  const wrap = $('#alWrap');
  if (!wrap) return;
  try {
    const [status, alerts, cfg, csvInfo, outbox, sched, runs, s3items, sources, health] = await Promise.all([
      api.get('/api/pipeline/status'),
      api.get('/api/pipeline/alerts'),
      api.get('/api/pipeline/config'),
      api.get('/api/pipeline/csv-sync'),
      api.get('/api/pipeline/alerts/outbox').catch(() => null),
      api.get('/api/pipeline/schedule').catch(() => null),
      api.get('/api/pipeline/runs?limit=60').catch(() => null),
      api.get('/api/s3/items').catch(() => null),
      api.get('/api/pipeline/sources').catch(() => null),
      api.get('/api/pipeline/health').catch(() => null),
    ]);
    wrap.innerHTML = '';

    // ── 작업 큐 — 지금 도는 것 · 락을 기다리는 것 · 예정된 것 (취소 가능)
    const qBox = el('div', { id: 'alQueue' });
    wrap.append(qBox);
    api.get('/api/pipeline/queue').then((q) => renderQueue(qBox, q)).catch(() => {});
    startQueuePoll();
    // ── DB 사용량 — 제품별 임계(기본 40GB) 초과만 경고 (보존 정책은 사람이 판단)
    api.get('/api/pipeline/db-usage').then((u) => {
      const node = alDbUsage(u);
      if (node) qBox.after(node);
    }).catch(() => {});

    // ── 처리 현황 (vehicle 별 한 줄 — 주기 조절 포함)
    wrap.append(alSub('파이프라인 처리 현황',
      'raw → event → feature · vehicle_matching 변경 시 재처리 필요 표시 · '
      + '실행 주기/금지 시간대 조절은 모니터 탭 DB heatmap 의 ⚙ 실행 관리'));
    if (sched && !sched.master_enabled) {
      wrap.append(el('div', { class: 'alert warn', style: { fontSize: '11px', margin: '4px 0' } },
        '자동 실행 마스터 스위치가 꺼져 있습니다 — 제품 주기와 무관하게 스케줄이 돌지 않습니다 '
        + '(모니터 탭 → DB heatmap → ⚙ 실행 관리 → ⏱ 자동).'));
    }
    if (sched?.quiet?.now) {
      wrap.append(el('div', { class: 'alert warn', style: { fontSize: '11px', margin: '4px 0' } },
        `🌙 실행 금지 시간대 (${sched.quiet.start}~${sched.quiet.end}) — `
        + `${fmtClock(sched.quiet.until)} 까지 자동 실행이 뜨지 않습니다. 수동 ▶ 실행은 가능합니다.`));
    }
    Object.keys(status).forEach((v) => wrap.append(
      alStatusLine(v, status[v], sched?.vehicles?.[v], sched?.summary?.[v])));
    if (runs) wrap.append(alRunLog(runs));

    // ── 단계 정체 (제품별 raw→event→feature→wide 가 며칠째 멈췄는지)
    if (health) wrap.append(alStallSection(health, alerts));

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

    if (outbox) wrap.append(alOutbox(outbox, s3items));
    wrap.append(alAlertColsEditor(cfg, alerts));
    wrap.append(alAlertHintEditor(cfg));
    if (health) wrap.append(alStallEditor(health));
    wrap.append(alSourcesEditor(sources));
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
    if (pendingSrcs.length) line.append(el('span', { style: { color: 'var(--danger)' } }, `미처리 ${pendingSrcs.join(', ')}`));
    if (staleSrcs.length) line.append(el('span', { style: { color: 'var(--danger)' } }, `매칭 변경 — 재처리 필요 (${staleSrcs.join(', ')})`));
    if (!pendingSrcs.length && !staleSrcs.length && evSrcs.some((s) => (ev[s]?.dates || []).length)) {
      line.append(el('span', { style: { color: 'var(--ok)' } }, '최신'));
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

// 제품별 실행 주기 — 여기서는 "읽기 전용" 표시만 한다.
// 실행에 관한 설정(전역 주기 · 금지 시간대 · 제품별 주기)은 모니터 탭 DB heatmap 의
// ⚙ 실행 관리 한 곳에 모았다 — 두 곳에서 같은 값을 바꾸면 어느 쪽이 최신인지 헷갈린다.
function alSchedCell(v, sc, sum) {
  if (!sc) return el('span', {});
  const period = sc.interval_sec > 0 ? `${sc.interval_hours}h 간격` : '자동 실행 안 함';
  const next = !sc.enabled ? '—'
    : sc.quiet_blocked ? `금지 해제 후 ${fmtClock(sc.next_ts)}`
    : (sc.due ? '곧 실행' : fmtClock(sc.next_ts));
  const bits = [el('span', {
    title: '실행 주기 조절 → 모니터 탭 DB heatmap 의 ⚙ 실행 관리',
    style: { color: 'var(--text-muted)' },
  }, `자동 일 ${sc.runs_per_day}회 · ${period}`),
    el('span', { title: '다음 자동 실행 예정', style: { color: sc.due ? 'var(--warn)' : 'var(--text-muted)' } },
      `→ ${next}`)];
  if (sc.source === 'global') {
    bits.push(el('span', { class: 'hint', title: '제품 개별 설정 없음 — 전역 주기를 따름' }, '전역'));
  }
  if (sum && sum.failed) {
    bits.push(el('span', { style: { color: 'var(--danger)' }, title: sum.last_error || '' },
      `최근 ${sum.runs}회 중 실패 ${sum.failed}`));
  }
  if (sc.retry_blocked) {
    bits.push(el('span', { style: { color: 'var(--danger)', fontWeight: 800 },
      title: sc.retry_hint || '연속 실패로 자동 재시도를 멈췄습니다 — 작업 큐에서 재개',
    }, `⛔ 재시도 중단 ${sc.retry_blocked}`));
  }
  if (sc.retry_pending - (sc.retry_blocked || 0) > 0) {
    bits.push(el('span', {
      style: { color: sc.retry_oldest_age_sec > 43200 ? 'var(--danger)' : 'var(--warn)', fontWeight: 700 },
      title: '실패한 (source×날짜) 유닛 — 롤링 윈도우를 벗어나도 재시도 대상으로 남는다'
        + (sc.enabled ? '' : ' (자동 실행이 꺼져 있어 수동 실행 때 처리된다)'),
    }, `놓친 유닛 ${sc.retry_pending - (sc.retry_blocked || 0)}`
      + (sc.retry_next_ts ? ` · 재시도 ${fmtClock(sc.retry_next_ts)}` : '')
      + (sc.enabled ? '' : ' · 자동 실행 꺼짐')));
  }
  return el('span', { style: { display: 'flex', gap: '5px', alignItems: 'center', fontSize: '11px' } }, ...bits);
}

// ─── 작업 큐 ───────────────────────────────────────────────
// 파이프라인의 쓰기 작업은 실행 락 하나를 공유한다. 예전에는 그 락을 기다리는 작업
// (특히 매칭 갱신 재생성)이 화면에 안 보여서 "왜 아무 일도 안 하지" 로 보였다.
// 여기서 실행 중 · 대기 · 예정을 한눈에 보고 취소한다.
let AL_QUEUE_TIMER = null;
let AL_QUEUE_SIG = '';

const QUEUE_KIND = { run: '실행', rebuild: '매칭 갱신 재생성', wide: 'wide 병합',
  send: 'send form', job: '작업' };
const QUEUE_STATE = { done: '완료', error: '실패', cancelled: '취소됨', skipped: '건너뜀' };

function stopQueuePoll() { if (AL_QUEUE_TIMER) { clearInterval(AL_QUEUE_TIMER); AL_QUEUE_TIMER = null; } }

function startQueuePoll() {
  stopQueuePoll();
  AL_QUEUE_SIG = '';
  AL_QUEUE_TIMER = setInterval(async () => {
    const box = $('#alQueue');
    if (!box) return stopQueuePoll();      // 탭을 떠나면 스스로 멈춘다
    let q; try { q = await api.get('/api/pipeline/queue'); } catch { return; }
    renderQueue(box, q);
  }, 3000);
}

const fmtSecs = (s) => (s >= 3600 ? `${Math.floor(s / 3600)}시간 ${Math.floor((s % 3600) / 60)}분`
  : s >= 60 ? `${Math.floor(s / 60)}분 ${Math.round(s % 60)}초` : `${Math.round(s)}초`);

async function cancelQueueTask(id, btn) {
  btn.disabled = true;
  btn.textContent = '취소 중…';
  try {
    const r = await api.post('/api/pipeline/queue/cancel', { id });
    btn.textContent = r.state === 'cancelled' ? '취소됨' : '중단 대기…';
  } catch (e) {
    alert(e.message);
    btn.disabled = false;
    btn.textContent = '취소';
  }
  AL_QUEUE_SIG = '';       // 다음 폴링에서 무조건 다시 그린다
}

async function resumeRetries(btn) {
  btn.disabled = true;
  try { await api.post('/api/pipeline/retries/resume', {}); } catch (e) { alert(e.message); }
  AL_QUEUE_SIG = '';
  loadAlerts();
}

function renderQueue(box, q) {
  const now = q.ts || (Date.now() / 1000);
  const running = q.running || [];
  const waiting = q.waiting || [];
  const recent = q.recent || [];
  const upcoming = q.upcoming || [];
  const retry = q.retry || {};
  // 변경이 없으면 다시 그리지 않는다 (3초마다 노드를 갈아치우면 화면이 튀고 클릭이 씹힌다).
  // 실행 중 경과만 5초 단위로 갱신되게 서명에 넣는다.
  const sig = JSON.stringify([
    running.map((t) => [t.id, t.state, t.cancel, Math.round((now - (t.started_ts || now)) / 5)]),
    waiting.map((t) => [t.id, t.cancel]),
    recent.map((t) => [t.id, t.state]),
    upcoming.map((u) => [u.vehicle, Math.round((u.next_ts || 0) / 30), u.due]),
    [retry.pending, retry.blocked, q.busy],
  ]);
  if (sig === AL_QUEUE_SIG) return;
  AL_QUEUE_SIG = sig;

  box.innerHTML = '';
  box.append(alSub('작업 큐',
    '지금 도는 작업 · 실행 락을 기다리는 작업 · 다음 예정. '
    + '취소는 안전 지점(raw 유닛/단계 경계)에서 멈춘다 — 쓰는 중인 event/feature 를 끊지 않는다'));

  const line = (bits, style) => el('div', {
    style: Object.assign({ display: 'flex', gap: '10px', alignItems: 'center',
      fontSize: '12px', padding: '4px 0',
      borderBottom: '1px solid var(--border-weak, rgba(128,128,128,.15))' }, style || {}),
  }, ...bits);

  const cancelBtn = (t) => (t.cancellable && !t.cancel
    ? el('button', { class: 'btn', style: { marginLeft: 'auto' },
      onclick: (ev) => cancelQueueTask(t.id, ev.target) }, '취소')
    : el('span', { style: { marginLeft: 'auto', color: 'var(--text-muted)', fontSize: '11px' } },
      t.cancel ? '중단 대기…' : '취소 불가'));

  running.forEach((t) => box.append(line([
    el('span', { style: { color: 'var(--ok)', fontWeight: 800 } }, '▶ 실행 중'),
    el('span', { style: { fontWeight: 700 } }, t.label),
    el('span', { class: 'mono', style: { color: 'var(--text-muted)' } }, (t.vehicles || []).join(', ')),
    el('span', { style: { color: 'var(--text-muted)' } }, fmtSecs(now - (t.started_ts || now))),
    cancelBtn(t),
  ])));
  waiting.forEach((t) => box.append(line([
    el('span', { style: { color: 'var(--warn)', fontWeight: 800 } }, '⏸ 대기'),
    el('span', { style: { fontWeight: 700 } }, t.label),
    el('span', { style: { color: 'var(--text-muted)' } },
      `${fmtSecs(now - (t.enqueued_ts || now))} 째 실행 락 대기`),
    cancelBtn(t),
  ])));
  if (!running.length && !waiting.length) {
    box.append(line([el('span', { style: { color: 'var(--text-muted)' } },
      q.busy ? '· 다른 작업이 락을 잡고 있음' : '· 지금 도는 작업 없음')]));
  }

  // 재시도 큐 — blocked 는 자동 재시도가 멈춘 상태라 사람이 봐야 한다
  if (retry.blocked) {
    box.append(el('div', { class: 'alert err', style: { fontSize: '12px', margin: '8px 0' } },
      el('div', { style: { fontWeight: 700 } },
        `⛔ 재시도 중단 ${retry.blocked}건 — ${retry.hint || '연속 실패로 자동 재시도를 멈췄습니다'}`),
      ...(retry.blocked_errors || []).slice(0, 2).map((e) =>
        el('div', { class: 'mono', style: { fontSize: '11px', marginTop: '2px' } }, e)),
      el('button', { class: 'btn', style: { marginTop: '6px' },
        onclick: (ev) => resumeRetries(ev.target) }, '↻ 재시도 재개'),
    ));
  } else if (retry.pending) {
    box.append(line([
      el('span', { style: { color: 'var(--warn)', fontWeight: 700 } }, `↻ 재시도 대기 ${retry.pending}건`),
      el('span', { style: { color: 'var(--text-muted)' } },
        retry.next_retry_at ? `다음 ${fmtClock(retry.next_retry_at)}` : ''),
    ]));
  }

  if (upcoming.length) {
    box.append(el('div', { style: { fontSize: '11px', color: 'var(--text-muted)', margin: '8px 0 2px' } },
      '예정 (자동 실행)'));
    upcoming.slice(0, 6).forEach((u) => box.append(line([
      el('span', { class: 'mono', style: { fontWeight: 700, minWidth: '90px' } }, u.vehicle),
      el('span', { style: { color: u.due ? 'var(--warn)' : 'var(--text-muted)' } },
        u.due ? '곧 실행' : fmtClock(u.next_ts)),
      ...(u.quiet_blocked ? [el('span', { class: 'hint' }, '🌙 금지 시간대 해제 후')] : []),
      ...(u.retry_pending ? [el('span', { style: { color: 'var(--warn)' } },
        `놓친 유닛 ${u.retry_pending}`)] : []),
      ...(u.retry_blocked ? [el('span', { style: { color: 'var(--danger)', fontWeight: 700 } },
        `재시도 중단 ${u.retry_blocked}`)] : []),
    ], { borderBottom: 'none', padding: '2px 0' })));
  }

  if (recent.length) {
    box.append(el('div', { style: { fontSize: '11px', color: 'var(--text-muted)', marginTop: '8px' } },
      '최근: ' + recent.slice(0, 5).map((t) =>
        `${t.label} ${QUEUE_STATE[t.state] || t.state}${t.result ? ` (${t.result})` : ''}`).join(' · ')));
  }
}

// DB 사용량 — 제품 × 소스(FAB/INLINE/VM/ET) 단위로 raw·event 를 나눠 본다.
// "어느 DB 가 커졌는지" 를 봐야 무엇부터 지울지 정할 수 있다. 자동 삭제는 하지 않는다.
// 크기 표기 — GB 고정이면 개발/초기 데이터가 전부 0.00GB 로 보인다. 단위를 맞춰 준다.
const GBs = (b) => (!b ? '0'
  : b >= 1073741824 ? `${(b / 1073741824).toFixed(2)}GB`
  : b >= 1048576 ? `${(b / 1048576).toFixed(1)}MB`
  : `${Math.max(1, Math.round(b / 1024))}KB`);

function alDbUsage(u) {
  if (!u || !u.vehicles || !Object.keys(u.vehicles).length) return null;
  const box = el('div', { style: { margin: '10px 0' } });
  if ((u.warn_vehicles || []).length) {
    box.append(el('div', { class: 'alert warn', style: { fontSize: '12px' } },
      `⚠ DB 사용량 ${u.warn_gb}GB 초과: ${u.warn_vehicles.join(', ')} — `
      + '아래에서 큰 소스의 오래된 date= 파티션부터 정리하세요 (자동 삭제는 하지 않습니다)'));
  }

  const rows = [];
  Object.entries(u.vehicles)
    .sort((a, b) => b[1].bytes - a[1].bytes)
    .forEach(([v, x]) => {
      Object.entries(x.sources || {})
        .sort((a, b) => b[1].bytes - a[1].bytes)
        .forEach(([s, d], i) => rows.push(el('tr', {},
          el('td', { class: 'mono', style: { fontWeight: 700 } }, i === 0 ? v : ''),
          el('td', { class: 'mono', style: { fontWeight: 700 } }, s),
          el('td', { class: 'mono', style: { fontSize: '11px', color: 'var(--text-muted)' },
            title: 'raw 를 date= 파티션으로 나누는 기준 열' }, d.time_col || '(없음)'),
          el('td', { class: 'mono' }, GBs(d.raw)),
          el('td', { class: 'mono', style: d.event_enabled ? {} : { color: 'var(--text-muted)' } },
            d.event_enabled ? GBs(d.event) : '– raw 전용'),
          el('td', { class: 'mono', style: { fontWeight: 700 } }, GBs(d.bytes)),
        )));
      rows.push(el('tr', { style: { borderTop: '1px solid var(--border)' } },
        el('td', { class: 'mono', style: { fontWeight: 800 } }, `${v} 합계`),
        el('td', { colspan: '3', style: { fontSize: '11px', color: 'var(--text-muted)' } },
          `feature ${GBs(x.parts.feature)} · reports ${GBs(x.parts.reports)} · 파일 ${x.files}개`),
        el('td', {}, ''),
        el('td', { class: 'mono', style: { fontWeight: 800, color: x.warn ? 'var(--danger)' : '' },
          title: x.warn ? `임계 ${u.warn_gb}GB 초과` : '' }, GBs(x.bytes)),
      ));
    });

  box.append(el('details', { style: { marginTop: '4px' } },
    el('summary', { style: { fontSize: '12px', cursor: 'pointer' } },
      `DB 사용량 — 소스별 (${Object.entries(u.by_source || {})
        .sort((a, b) => b[1].bytes - a[1].bytes)
        .map(([s, a]) => `${s} ${GBs(a.bytes)}`).join(' · ')})`),
    alTable(['제품', '소스', '기준 열', 'raw', 'event', '소계'], rows),
    el('div', { style: { fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px' } },
      `공용 wide ${GBs((u.shared?.wide?.bytes) || 0)} · send ${GBs((u.shared?.send?.bytes) || 0)}`
      + ` · 임계 ${u.warn_gb}GB · ${u.db_root || ''}`),
  ));
  return box;
}

// 통합 알람 테이블 — 한 행 = 한 알람. 유형은 색으로 구분 (미매칭 step 빨강 · RO ppid 주황)
const AL_TYPE = {
  unmatched_step: { label: '미매칭 step', color: 'var(--danger)' },
  ro_ppid: { label: 'RO ppid', color: 'var(--warn)' },
  stage_stall: { label: '단계 정체', color: 'var(--danger)' },
};
let AL_SHOW_SUPPRESSED = false;
let AL_HEALTH_ALL = false;   // 정체 아닌 단계까지 펼쳐 보기

// ── 단계 정체 — 제품별 raw/event/feature/wide 가 며칠째 안 늘고 있는지.
// 같은 내용이 매칭알람 payload(health 블록 + stage_stall 행)로 flow 에도 나간다.
const STAGE_ORDER = ['raw', 'event', 'feature', 'wide', 'flow', 'send', 's3'];

function alStallSection(health, alerts) {
  const cfg = health?.config || {};
  const byVehicle = health?.vehicles || {};
  const ackOf = {};
  (alerts?.alerts || []).forEach((a) => {
    if (a.type === 'stage_stall') ackOf[a.id] = a.status;
  });

  const rows = [];
  let stalledTotal = 0;
  let rootTotal = 0;
  const seenGlobal = new Set();
  Object.keys(byVehicle).sort().forEach((v) => {
    const h = byVehicle[v] || {};
    const stages = (h.stages || []).slice().sort(
      (a, b) => STAGE_ORDER.indexOf(a.stage) - STAGE_ORDER.indexOf(b.stage)
        || String(a.source).localeCompare(String(b.source)));
    stages.forEach((r) => {
      // SEND_FORM 은 전 제품을 합쳐 만든다 — 제품별 현황에 똑같이 들어 있으므로
      // 표에는 한 줄만 그린다 (vehicle 칸은 '전 제품').
      const global = r.scope === 'global';
      if (global) {
        const key = `${r.stage}|${r.source}`;
        if (seenGlobal.has(key)) return;
        seenGlobal.add(key);
      }
      if (r.stalled) stalledTotal += 1;
      if (r.stalled && !r.cascade) rootTotal += 1;
      if (!r.stalled && !AL_HEALTH_ALL) return;
      const id = `stall|${global ? '-' : v}|${r.stage}|${r.source}`;
      const status = ackOf[id] || 'active';
      const tone = !r.stalled ? 'var(--ok)' : r.cascade ? 'var(--warn)' : 'var(--danger)';
      // 앞 단계가 밀린 여파(cascade)는 알람으로 나가지 않으므로 억제 선택도 없다 —
      // 원인 단계의 알람을 처리하면 같이 사라진다.
      const sel = (r.stalled && !r.cascade)
        ? el('select', { style: { fontSize: '11px' }, onchange: async (ev) => {
          await api.put('/api/pipeline/alerts/ack', { id, status: ev.target.value });
          loadAlerts();
        } }, ...['active', '미확인예정', '반영불필요'].map((s) =>
          el('option', s === status ? { value: s, selected: '' } : { value: s }, s)))
        : el('span', { class: 'hint' }, r.stalled ? '앞 단계 여파' : '정상');
      rows.push(el('tr', { style: (r.stalled && !r.cascade && status === 'active') ? {} : { opacity: 0.5 } },
        el('td', { class: 'mono', style: { fontWeight: 700 } }, global ? '전 제품' : v),
        el('td', { class: 'mono' }, global ? '' : (h.product || '')),
        el('td', { style: { fontWeight: 700, color: tone, whiteSpace: 'nowrap' } }, r.label),
        el('td', { class: 'mono' }, r.source || '-'),
        el('td', { class: 'mono' }, r.latest_date || '-'),
        el('td', { class: 'mono', style: { color: (r.lag_days ?? 0) > (cfg.threshold_days ?? 1) ? tone : 'inherit' } },
          r.lag_days === null || r.lag_days === undefined ? '-' : `${r.lag_days}일`),
        el('td', { class: 'mono' }, r.behind_days === null || r.behind_days === undefined
          ? '-' : `${r.behind_of} −${r.behind_days}일`),
        el('td', { class: 'mono', style: { fontSize: '11px' } }, fmtClock(r.last_write_ts)),
        el('td', { style: { color: r.stalled ? tone : 'var(--text-muted)' } }, r.reason || '정상'),
        el('td', {}, sel),
      ));
    });
  });

  const toggle = el('label', { style: { fontSize: '12px', color: 'var(--text-muted)', display: 'flex', gap: '5px', alignItems: 'center', marginLeft: 'auto' } },
    el('input', Object.assign({ type: 'checkbox', onchange: (e) => { AL_HEALTH_ALL = e.target.checked; loadAlerts(); } },
      AL_HEALTH_ALL ? { checked: '' } : {})),
    '정상 단계도 표시');

  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    el('div', { style: { display: 'flex', alignItems: 'baseline', gap: '10px' } },
      el('span', { style: { fontWeight: 700, fontSize: '12px' } }, '단계 정체'),
      el('span', { style: { fontSize: '12px', color: stalledTotal ? 'var(--danger)' : 'var(--text-muted)' } },
        stalledTotal
          ? `${stalledTotal}개 단계 정체 — 원인 ${rootTotal}건 (${cfg.threshold_days || 1}일 임계)`
          : `전 제품 정상 (임계 ${cfg.threshold_days || 1}일)`),
      toggle,
    ),
    el('div', { class: 'hint', style: { margin: '4px 0 6px' } },
      'raw → event → feature → ML_TABLE(내부) → flow 발행본 → SEND_FORM(prefix 분리) → S3 전송 순. '
      + 'raw·event 는 데이터 날짜, 그 뒤는 마지막 산출 시각, S3 는 마지막 전송 성공 시각이 기준입니다. '
      + '앞 단계가 밀린 여파는 알람으로 보내지 않고 여기에만 표시합니다 — '
      + '원인 단계를 고치면 같이 풀립니다. 같은 내용이 매칭알람으로 flow 에도 전달됩니다.'),
    alTable(['vehicle', 'product', '단계', '소스', '최신 데이터', '지연', '앞 단계 대비',
      '마지막 산출', '사유', '상태'], rows),
  );
}

// ⚙ 단계 정체 알람 — 임계 일수와 감시 대상 단계 (pipeline.yaml stall_alert)
function alStallEditor(health) {
  const cfg = health?.config || {};
  const on = el('input', Object.assign({ type: 'checkbox' },
    cfg.enabled === false ? {} : { checked: '' }));
  const thr = el('input', { type: 'number', value: String(cfg.threshold_days || 1),
    min: '1', max: '60', style: { width: '54px' } });
  const boxes = {};
  const picks = STAGE_ORDER.map((s) => {
    const cb = el('input', Object.assign({ type: 'checkbox' },
      (cfg.stages || STAGE_ORDER).includes(s) ? { checked: '' } : {}));
    boxes[s] = cb;
    return el('label', { style: { display: 'flex', gap: '4px', alignItems: 'center' } }, cb, s);
  });
  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('⚙ 단계 정체 알람',
      '오늘 파티션은 하루가 지나야 차므로 임계 1일 = "어제까지는 정상, 그저께가 최신이면 알람". '
      + 'flow 매칭알람 화면에도 같은 기준으로 표시됩니다'),
    el('div', { style: { fontSize: '12px', display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' } },
      el('label', { style: { display: 'flex', gap: '4px', alignItems: 'center' } }, on, '사용'),
      '임계', thr, '일', '감시 단계', ...picks,
      el('button', { class: 'btn', onclick: async (ev) => {
        ev.target.disabled = true;
        try {
          await api.put('/api/pipeline/config/stall', {
            enabled: on.checked,
            threshold_days: Number(thr.value) || 1,
            stages: STAGE_ORDER.filter((s) => boxes[s].checked),
          });
        } catch (e) { alert(e.message); }
        loadAlerts();
      } }, '저장'),
    ),
  );
}

function alAlertTable(data) {
  // 전송 열은 ⚙ 설정(unmatched_scan.alert_cols)을 그대로 따른다 — flow 화면과 동일 구성
  const extraCols = (data.alert_cols && data.alert_cols.length) ? data.alert_cols : ['eqp_id', 'eqp_model'];
  const exText = (a) => (a.examples || [])
    .map((e) => [e.root_lot_id, e.wafer_id].filter(Boolean).join('·')).join(', ');
  const rows = data.alerts
    // 단계 정체는 열 구성이 전혀 달라 위쪽 '단계 정체' 표가 따로 그린다
    .filter((a) => a.type !== 'stage_stall')
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
        ...extraCols.map((c) => el('td', { class: 'mono' }, a[c] || '-')),
        el('td', { class: 'mono', style: { fontSize: '11px' }, title: exText(a) }, exText(a) || '-'),
        el('td', { class: 'mono' }, String(a.n_lots || '')),
        el('td', { class: 'mono' }, String(a.rows || '')),
        el('td', {}, sel),
      );
    });
  return alTable(
    ['유형', 'vehicle', 'product', 'step_id', 'step_desc', 'ppid', 'split',
      ...extraCols, '예시 lot·wafer', 'lots', 'rows', '상태'],
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
      // 성공 / 취소 / 부분 실패(raw 유닛 일부 실패 — 재시도 큐에 남음) / 중단(단계가 못 돎)
      el('td', { style: { color: r.ok ? 'var(--ok)' : (r.cancelled ? 'var(--text-muted)'
        : (r.error ? 'var(--danger)' : 'var(--warn)')), fontWeight: 700 }, title: r.error || '' },
        r.ok ? '성공' : (r.cancelled ? '취소' : (r.error ? '중단' : '부분 실패'))),
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
    (st.raw.errors || []).forEach((e) => L.push(
      `    ✗ ${e.source} ${e.date}: ${e.error}`
      + (e.blocked ? ` — ${e.attempts}회 연속 실패로 자동 재시도 중단 (작업 큐에서 재개)`
        : e.attempts ? ` — 시도 ${e.attempts}회 · 재시도 예정 ${fmtRunTs(e.next_retry_at)}` : '')));
    if (st.raw.cancelled_units) L.push(`    ⊘ 취소로 실행하지 않은 유닛 ${st.raw.cancelled_units}개`);
    (st.raw.recovered || []).forEach((e) => L.push(
      `    ↻ ${e.source} ${e.date}: 재시도 성공${e.attempts ? ` (실패 ${e.attempts}회 후 복구)` : ''}`));
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

// 알람 업로드 폴더 (Valve → S3 → flow 매칭알람). 이 폴더 하나가 폴더 단위로 전송된다.
function alOutbox(ob, s3items) {
  const box = el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('S3 업로드 폴더', 'flow 매칭알람이 읽는 파일 — 이 폴더가 통째로 전송 항목을 타고 올라간다 (트리 = S3 key)'));
  if (!ob.enabled) {
    box.append(el('div', { class: 'alert warn', style: { fontSize: '12px' } },
      '미설정 — 설정 탭 alerts.outbox_dir 을 채우면 발행 시 이 폴더로 미러링된다.'));
    return box;
  }
  box.append(el('div', { style: { fontSize: '12px', display: 'flex', gap: '14px', flexWrap: 'wrap', alignItems: 'center', margin: '6px 0' } },
    el('span', { style: { color: 'var(--text-muted)' } }, '폴더'),
    el('span', { class: 'mono' }, ob.sync_dir),
    el('span', { style: { color: 'var(--text-muted)' } }, '→ s3 prefix'),
    el('span', { class: 'mono' }, ob.s3_prefix),
    el('span', { style: { color: 'var(--text-muted)' } }, `발행 주기 ${ob.interval_min || 0}분`),
  ));
  // 이 폴더를 올리는 전송 항목 (탐색기 ⚙ 과 같은 엔진 — db 폴더 전송과 동일)
  const item = ((s3items || {}).items || []).find((i) => i.direction === 'upload' && i.root === 'outbox');
  if (item) {
    const st = item.status || {};
    const stTxt = item.is_running ? '전송 중…'
      : st.last_status ? `최근 ${st.last_status}${st.last_end ? ' · ' + new Date(st.last_end * 1000).toLocaleString() : ''}` : '실행 이력 없음';
    box.append(el('div', { style: { fontSize: '12px', display: 'flex', gap: '10px', flexWrap: 'wrap', alignItems: 'center', margin: '2px 0 8px' } },
      el('span', { style: { color: 'var(--text-muted)' } }, '전송 항목'),
      el('span', { class: 'mono' }, item.id),
      el('span', {}, item.enabled && item.interval_min > 0 ? `자동 · ${item.interval_min}분 주기` : '수동 전용'),
      el('span', { style: { color: 'var(--text-muted)' } }, stTxt),
      el('button', { class: 'btn', disabled: item.is_running ? '' : undefined, onclick: async (ev) => {
        ev.target.disabled = true; ev.target.textContent = '전송 중…';
        try { await api.post('/api/s3/run', { id: item.id }); } catch (e) { alert(e.message); }
        setTimeout(loadAlerts, 1200);
      } }, '▶ 지금 업로드'),
      el('span', { class: 'hint' }, '주기/연결 변경은 탐색기 ⚙'),
    ));
  } else {
    const cmd = `aws s3 sync "${ob.sync_dir}" s3://<bucket>/${ob.s3_prefix} --exclude "*.tmp"`;
    box.append(el('div', { class: 'mono', style: { fontSize: '11px', color: 'var(--text-muted)', margin: '2px 0 8px', wordBreak: 'break-all' } }, cmd));
  }
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
      el('td', { class: 'mono', style: { fontSize: '11px', color: st.status === 'error' || st.status === 'missing' ? 'var(--danger)' : 'var(--text-muted)' } }, stTxt),
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

// ⚙ 매칭알람 전송 열 — flow 로 보내는 알람에 실을 raw 열 + 예시 lot/wafer 개수.
// 저장은 pipeline.yaml unmatched_scan.alert_cols / example_limit.
function alAlertColsEditor(cfg, alerts) {
  const us = cfg.unmatched_scan || {};
  const cur = (us.alert_cols && Array.isArray(us.alert_cols)) ? us.alert_cols
    : (alerts.alert_cols || ['eqp_id', 'eqp_model']);
  const cols = el('input', { type: 'text', value: cur.join(', '), style: { width: '300px' } });
  const limit = el('input', { type: 'number', value: String(us.example_limit || 3), min: '1', max: '20', style: { width: '54px' } });
  const fabCols = (((cfg.sources || {}).FAB || {}).columns || []).join(', ');
  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('⚙ 매칭알람 전송 열',
      'flow 매칭알람(→ function step 추천)에 같이 실을 raw 열 — 쉼표 구분, raw 에 없는 열은 자동 제외. '
      + '예시 (root_lot_id·wafer_id) 쌍은 항상 포함'),
    el('div', { style: { fontSize: '12px', display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' } },
      '전송 열', cols, '예시 개수', limit,
      el('button', { class: 'btn', onclick: async (ev) => {
        ev.target.disabled = true;
        try {
          await api.put('/api/pipeline/config/alert-cols', {
            cols: cols.value.split(',').map((s) => s.trim()).filter(Boolean),
            example_limit: Number(limit.value) || 3,
          });
        } catch (e) { alert(e.message); }
        loadAlerts();
      } }, '저장'),
      el('span', { class: 'hint' }, '다음 파이프라인 실행/발행부터 반영'),
    ),
    el('div', { class: 'hint', style: { marginTop: '4px' } }, `FAB raw 열: ${fabCols}`),
  );
}

// ⚙ function step 추천 컨텍스트 — 신규 step 의 앞뒤 이웃 step 이 최근 며칠간
// 어떤 ppid/eqp 로 돌았는지를 알람에 실어 보낸다 (flow 가 이걸로 추천).
// 저장은 pipeline.yaml unmatched_scan.hint.
function alAlertHintEditor(cfg) {
  const h = (cfg.unmatched_scan || {}).hint || {};
  const on = el('input', Object.assign({ type: 'checkbox' },
    h.enabled === false ? {} : { checked: '' }));
  const days = el('input', { type: 'number', value: String(h.days || 7), min: '1', max: '90', style: { width: '54px' } });
  const nb = el('input', { type: 'number', value: String(h.neighbors || 3), min: '1', max: '10', style: { width: '48px' } });
  const cols = el('input', { type: 'text', style: { width: '260px' },
    value: (Array.isArray(h.cols) && h.cols.length ? h.cols : ['ppid', 'eqp_id', 'eqp_model', 'area']).join(', ') });
  const lim = el('input', { type: 'number', value: String(h.value_limit || 12), min: '1', max: '50', style: { width: '48px' } });
  const fabCols = (((cfg.sources || {}).FAB || {}).columns || []);
  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('⚙ function step 추천 컨텍스트',
      'AA100002 의 앞뒤(AA100000·AA100006) 이웃 step 이 최근 며칠간 쓴 ppid·설비를 같이 실어 보낸다 '
      + '— flow 가 ppid 가 같은 step 을 1순위, 새 ppid 면 eqp_id·eqp_model·area 가 겹치는 step 을 후보로 추천'),
    el('div', { style: { fontSize: '12px', display: 'flex', gap: '10px', alignItems: 'center', flexWrap: 'wrap' } },
      el('label', { style: { display: 'flex', gap: '4px', alignItems: 'center' } }, on, '사용'),
      '최근', days, '일', '앞뒤 각', nb, '개',
      '비교 열', cols, '값 상한', lim,
      el('button', { class: 'btn', onclick: async (ev) => {
        ev.target.disabled = true;
        try {
          await api.put('/api/pipeline/config/alert-hint', {
            enabled: on.checked,
            days: Number(days.value) || 7,
            neighbors: Number(nb.value) || 3,
            cols: cols.value.split(',').map((s) => s.trim()).filter(Boolean),
            value_limit: Number(lim.value) || 12,
          });
        } catch (e) { alert(e.message); }
        loadAlerts();
      } }, '저장'),
      el('span', { class: 'hint' }, '다음 파이프라인 실행/발행부터 반영'),
    ),
    !fabCols.includes('area') ? el('div', { class: 'hint', style: { marginTop: '4px' } },
      'FAB 조회 컬럼에 area 가 없습니다 — area 로 비교하려면 위 ⚙ 조회 컬럼에 먼저 추가하세요 '
      + '(없는 열은 조용히 빠집니다).') : null,
  );
}

// ⚙ 소스별 조회 컬럼 · 파티션 기준 열 — raw 를 date= 로 나누는 기준을 웹에서 바꾼다.
// 전 소스를 tkout_time(공정 진행 시각)으로 통일해 두면 소스가 달라도 같은 날짜 축이 된다.
function alSourcesEditor(sources) {
  if (!sources || !Object.keys(sources).length) return el('div', {});
  const draft = {};
  const rows = [];
  Object.entries(sources).forEach(([name, s]) => {
    draft[name] = { table: s.table, columns: (s.columns || []).join(', '),
      time_col: s.time_col || '' };
    const sel = el('select', { style: { fontSize: '11px', minWidth: '120px' },
      onchange: (e) => { draft[name].time_col = e.target.value; } },
      el('option', draft[name].time_col ? { value: '' } : { value: '', selected: '' },
        `(자동: ${s.resolved_time_col || '없음'})`),
      ...(s.columns || []).map((c) => el('option',
        c === draft[name].time_col ? { value: c, selected: '' } : { value: c }, c)));
    rows.push(el('tr', {},
      el('td', { class: 'mono', style: { fontWeight: 700 } }, name),
      el('td', {}, el('input', { type: 'text', value: draft[name].table,
        style: { width: '150px' }, onchange: (e) => { draft[name].table = e.target.value; } })),
      el('td', {}, el('input', { type: 'text', value: draft[name].columns,
        style: { width: '100%', minWidth: '280px' },
        onchange: (e) => { draft[name].columns = e.target.value; } })),
      el('td', {}, sel),
      el('td', { class: 'mono', style: { fontSize: '11px',
        color: s.error ? 'var(--danger)' : 'var(--text-muted)' } },
        s.error ? '설정 오류' : (s.resolved_time_col || '-')),
    ));
  });

  const save = async (ev) => {
    const btn = ev.target;
    btn.disabled = true;
    const payload = {};
    Object.entries(draft).forEach(([name, d]) => {
      payload[name] = { table: d.table,
        columns: d.columns.split(',').map((x) => x.trim()).filter(Boolean),
        time_col: d.time_col };
    });
    try {
      await api.put('/api/pipeline/config/sources', payload);
    } catch (e) {
      alert(e.message);
    }
    btn.disabled = false;
    loadAlerts();
  };

  return el('div', { style: { borderTop: AL_HAIR, marginTop: '20px', paddingTop: '12px' } },
    alSub('⚙ 조회 컬럼 · 파티션 기준 열',
      'raw 를 date= 파티션으로 나누는 기준 열 — 전 소스를 tkout_time(공정 진행 시각)으로 '
      + '맞추면 소스가 달라도 같은 날짜 축이 된다. 기준 열은 조회 컬럼에 있어야 하며, '
      + '바꾼 뒤에는 해당 소스 raw 를 다시 받아야 파티션이 새 기준으로 정리된다'),
    el('div', { style: { overflowX: 'auto' } },
      alTable(['소스', 'table', '조회 컬럼 (쉼표 구분)', '기준 열', '적용 중'], rows)),
    el('div', { style: { marginTop: '6px', display: 'flex', gap: '8px', alignItems: 'center' } },
      el('button', { class: 'btn', onclick: save }, '저장'),
      el('span', { class: 'hint' }, '저장 즉시 다음 raw 실행부터 적용 (기존 파티션은 그대로)'),
    ),
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
    $('#modeBadge').textContent = 'REAL';
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
