# Valve v0.1.1 — 점검 & scorecard (2026-04-24 — v0.2 이월 해소 회차)

v0.1 배포 후 이월됐던 운영성/품질 항목 해소. 41/41 pytest · 41/41 smoke PASS.

## 요약

| 축 | 이전 | 이번 | 증감 | 한 줄 |
|---|---:|---:|:-:|---|
| 도메인 모델 | 7.5 | 7.5 | = | 변동 없음 (이전 대비) |
| 백엔드 | 7.0 | 8.0 | ▲+1.0 | rotate + snapshot replay + /metrics + webhook + agent 라우터 추가 |
| 프론트엔드 | 6.5 | 7.0 | ▲+0.5 | 다크모드 토글 (system-prefers 반영) |
| 운영성 | 6.0 | **7.8** | ▲+1.8 | /api/metrics(JSON+Prom) + failure webhook cooldown + agent audit log |
| 확장성 | 6.5 | 6.5 | = | 변동 없음 |
| **테스트** | 6.0 | **8.5** | ▲+2.5 | pytest 41건(planner/state/api/rotate/ops/agent) + pytest-asyncio |
| 보안 | 4.5 | 5.0 | ▲+0.5 | agent 화이트리스트 + cooldown + 감사 로그 |
| 문서 | 5.5 | 6.8 | ▲+1.3 | agent_design.md + scorecard 갱신 |
| **종합** | **6.3** | **7.1** | ▲+0.8 | **사내 배포 직전 (인증만 붙이면 ready)** |

## 이번 회차 반영 (v0.2 이월 8건 중 5건 해소)

### ✅ [1] jobs.jsonl rotate
- [backend/core/state.py](Valve/backend/core/state.py) — `max_bytes`(기본 50 MB) 초과 시 회전.
- `.1 → .2 → ... → .keep` 순환, 가장 오래된 것 삭제.
- 새 파일 첫 줄에 현재 메모리 snapshot 기록 → 재기동 시 1줄 읽기로 즉시 복원.
- **사이드이펙트 fix**: `_apply` 를 `_append` 보다 먼저 호출하도록 변경 (rotate snapshot 이 최신 이벤트까지 포함하게).
- tests: `test_rotate_creates_numbered_backup`, `test_rotate_snapshot_allows_replay`, `test_rotate_keep_limit`.

### ✅ [2] 다크모드 토글
- [frontend/style.css](Valve/frontend/style.css) — `html[data-theme="dark"]` 로 CSS 변수 전체 다크 팔레트 오버라이드.
- [frontend/app.js](Valve/frontend/app.js) — 초기화 시 localStorage → `prefers-color-scheme` 순으로 결정. nav 우측 ☾/☀ 버튼 클릭 토글.
- 검증 결과: `bg=rgb(26,26,26)`, `navBg=rgb(38,38,38)` — flow 다크 톤 일치.

### ✅ [3] Prometheus metrics + JSON metrics
- [backend/routers/ops.py](Valve/backend/routers/ops.py)
  - `GET /api/metrics` (JSON) — chunk/partition status count, p50/p95/max duration, running 등 9종 지표.
  - `GET /api/metrics/prom` (text/plain) — `valve_total_chunks`, `valve_chunk_duration_p95_seconds`, `valve_chunk_status_count{status="..."}` 등 prometheus text format.
- 외부 의존성 0 (stdlib urllib + Counter).
- tests: `test_metrics_json_basic_shape`, `test_metrics_prom_format`.

### ✅ [4] Failure webhook
- `POST /api/alerts/test` — 연결 테스트 (URL 직접 지정 또는 settings.alerts.webhook_url).
- `emit_failure_webhook()` — [executor.py](Valve/backend/core/executor.py) chunk 실패 시 best-effort 호출.
- 60초 cooldown per chunk_id — 폭주 방지.
- tests: `test_webhook_test_no_url_returns_error`, `test_webhook_test_hits_given_url` (http.server 로 로컬 캡처).

### ✅ [5] 에이전트 설계 + 스캐폴딩
- [docs/agent_design.md](Valve/docs/agent_design.md) — 3-tier 역할 (Watcher/Suggester/Executor), 8종 액션 카탈로그(safety LOW/MEDIUM/HIGH), dry_run 우선 원칙, cooldown/suspend 규칙.
- [backend/routers/agent.py](Valve/backend/routers/agent.py) — `GET /api/agent/diagnose`, `POST /api/agent/suggest-fix`, `POST /api/agent/apply-fix`, `GET /api/agent/audit`, `GET /api/agent/actions`.
- **오픈소스 모델 친화 설계**: LLM 은 "읽고 고르고 호출" 만. 액션 선택은 서버 규칙 기반. free-form SQL 금지.
- **안전장치 5중**: 화이트리스트 × 인자 검증 × dry_run 우선 × 60s cooldown × 3연속 실패 suspend.
- HIGH safety 액션(enqueue_product_seed)은 `confirm_high_risk=true` 명시 필요.
- 감사 로그: `logs/agent_audit.jsonl` append-only.
- tests: 10건 (actions/diagnose/apply-fix unknown/missing arg/dry-run/real/cooldown/HIGH confirm/suggest rules/audit).

## 이월 여전 (v0.2 계속 이월)

| 항목 | 이월 사유 | 예상 공수 |
|---|---|---|
| 인증 (X-Agent-Key 또는 session token) | 별도 미들웨어 + 키 관리 UI 필요 | 1일 |
| ET cross-source pre-filter | planner 에 source 간 의존성 스케줄링 + 테스트용 mock 데이터 설계 | 2일 |
| 30~60GB/일 perf 측정 + chunk auto-tune | 실 데이터 또는 대규모 mock + 프로파일링 세션 | 2일 |
| 멀티 워커 대응 (uvicorn --workers >1) | state singleton → 공유 구조(redis?) 또는 단일 writer pattern | 2~3일 |

## 현 수준

| 차원 | 상태 |
|---|---|
| 사내 pilot (단일 운영자, 5~10 제품) | **즉시 가능** |
| 사내 배포 (여러 팀, 10~30 제품) | **인증만 붙이면 가능** |
| 외부 SaaS | perf 검증·멀티 테넌시·감사 강화 필요 (3~4주) |

## 엔드포인트 맵 (v0.1.1)

```
GET  /api/health                        헬스
GET  /api/version                       버전
GET  /api/settings                      설정 조회 (secret 마스킹)
POST /api/settings                      설정 저장
GET  /api/settings/schema               스키마

GET  /api/schedule                      스케줄 items
GET  /api/schedule/products             products 조회
POST /api/schedule/products             products 저장
GET  /api/schedule/source-types         소스 타입 레지스트리
POST /api/schedule/source-types         소스 타입 저장
GET  /api/schedule/columns              컬럼 풀 (product/source)

GET  /api/jobs/state                    현재 snapshot
GET  /api/jobs/stream                   SSE
POST /api/jobs/enqueue                  단일 (제품,소스,날짜) 실행
POST /api/jobs/enqueue-all              전체 일괄
POST /api/jobs/enqueue-product          제품 단위 초기 시딩 ★
POST /api/jobs/cancel                   chunk 취소
POST /api/jobs/retry-partition          (제품,소스,날짜) 재실행
POST /api/jobs/probe-invalidate         probe 캐시 무효화
GET  /api/jobs/history                  실행 이력 (필터·페이징)

GET  /api/browser/roots                 staging·s3_local 목록
GET  /api/browser/list                  디렉토리
GET  /api/query/view                    parquet + SQL (polars)

GET  /api/probe-preview/*               probe 미리보기 (개발용)

GET  /api/metrics                       JSON 메트릭 ★
GET  /api/metrics/prom                  Prometheus text ★
POST /api/alerts/test                   webhook 연결 테스트 ★

GET  /api/agent/actions                 액션 카탈로그 ★
GET  /api/agent/diagnose                이상 목록 ★
POST /api/agent/suggest-fix             룰 기반 제안 ★
POST /api/agent/apply-fix               액션 실행 (dry_run 우선) ★
GET  /api/agent/audit                   감사 로그 ★
```

★ = 이번 회차 신규/개선

## 테스트 커버리지

```
tests/
├── conftest.py          pytest-asyncio, FakeLakeAPI, TestClient 픽스처
├── test_api.py          12건 — FastAPI 전 라우터 엔드투엔드
├── test_planner.py      6건 — probe 3 전략 + probe_skip + 실패 캐시 방지
├── test_state.py        5건 — append + crash recovery + partition 상태 계산
├── test_rotate.py       3건 — 사이즈 초과 rotate + snapshot replay + keep limit
├── test_ops.py          4건 — metrics JSON/Prom + webhook 연결
└── test_agent.py        10건 — diagnose/suggest/apply-fix/cooldown/audit/HIGH

────────────────────────────────────────
41 passed in 2.67s
```

## 이번 회차 커밋 포인트

1. `backend/core/state.py` — rotate + snapshot replay, `_apply` 선행으로 변경.
2. `backend/core/executor.py` — chunk 실패 시 webhook emit.
3. `backend/routers/ops.py` — /metrics + /alerts (신규).
4. `backend/routers/agent.py` — /api/agent/* (신규).
5. `app.py` — ops/agent 라우터 mount + `VALVE_ROOT` env 지원 (테스트 격리).
6. `frontend/style.css` — dark theme CSS 변수.
7. `frontend/app.js` — theme toggle.
8. `tests/` — pytest 전체 (41건).
9. `pytest.ini` — asyncio_mode=auto.
10. `docs/agent_design.md`, `docs/valve_v011_scorecard.md`.
