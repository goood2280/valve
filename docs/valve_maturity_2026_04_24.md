# Valve v0.1 — maturity scorecard (2026-04-24)

한 날치기 세션(2026-04-24)에서 Valve 가 도달한 수준을 도메인/기술/UX 축으로 점수화. 점수는 **사내 pilot 적합도** 를 기준으로 한 10점 만점(1=뼈대만, 5=pilot 가능, 7=사내 배포, 9=외부 SaaS).

## 요약

| 축 | 점수 | 한 줄 |
|---|---:|---|
| 도메인 모델 (반도체 데이터 적재) | **7.5/10** | FAB/INLINE/ET/QTIME/EDS/VM + custom DB 추가까지 동적. item_id·root_lot_id shard 계층 지원. |
| 백엔드 (FastAPI + polars) | **7.0/10** | probe/executor/state/staging 완결. jobs.jsonl append-only + 크래시 복구. SSE. |
| 프론트엔드 (Vanilla JS SPA) | **6.5/10** | flow 디자인 DNA 일치(Pretendard·오렌지 accent·icon 탭). 제품/모니터/로그/설정/탐색기 5탭. |
| 운영성 (logs/monitor/alert) | **6.0/10** | 실행 이력 뒤에서부터 tail, probe fail/skip 배지, 제품×소스 히트맵 14일. 알림 hooks 없음. |
| 확장성 (신규 DB·규모) | **6.5/10** | source_types 레지스트리로 신규 DB 1줄 추가. 30~60 GB/일 perf 검증은 이월. |
| 테스트 | **6.0/10** | stdlib-only smoke 31/31 PASS (<0.5s). 단위/통합 테스트 미보유. |
| 보안 | **4.5/10** | CORS `*`, 인증 없음, secret 은 settings.json 평문. pilot 망 내 전제. |
| 문서 | **5.5/10** | README + products.yaml 주석 + source_types.yaml 주석. API 레퍼런스 부족. |
| **종합** | **6.3/10** | **사내 pilot 즉시 적합**, 보안/관측성 보강 후 사내 배포. |

## 축별 상세

### 1) 도메인 모델 — 7.5/10

**강점**
- 소스 6종 내장(FAB/INLINE/ET/QTIME/EDS/VM) + `config/source_types.yaml` 로 런타임 확장.
- 각 소스별 columns 풀 · default_shard · accent · hint 메타 보유 → 제품 편집기 드롭다운/모니터 히트맵/소스 카드 힌트에 자동 반영.
- 제품 단위
  - `custom_col` 공통 기본 + 소스별 override (chip-inherit UI 로 가시화)
  - `params_template` (cat_a…cat_j 슬롯) — process_id·line_id 같은 고정 필터 슬롯.
  - `backfill_days_override` — 신규 세팅 시 300·600 일 길게 주고 초기 시딩 끝나면 비우는 운영 패턴.
  - 소스별 `probe_skip` — probe 상습 실패 소스에 대해 skip 허용.
- Shard 2단 계층 (예: INLINE `[root_lot_id, lot_id]`) 지원 → 한 root 에 lot 이 많아도 자동 세분화.

**감점**
- ET reformatter raw item 목록을 외부에서 read 해서 자동 주입하는 연동 (cross-source pre-filter) 미구현. 현재는 `params_template` 에 `item_id in [...]` 를 사람이 붙여야 함.
- 크로스 소스 의존 스케줄링 없음(예: FAB 완료 → INLINE lot list 로 축소).

### 2) 백엔드 — 7.0/10

**강점**
- `lake_api.py` retry + timeout + rate-limit + mock/real dual. HY000/Timeout/ConnectionError 재시도 토큰 설정.
- `planner.py` 3 전략(sample_window/projection/none) + probe 캐시(일 TTL) + 실패 결과는 **캐시하지 않음** (반복 실패 고착 방지).
- `executor.py` 동시성 제한 세마포어, timeout_reshard 상태, staging parquet 저장, S3 업로드.
- `state.py` append-only jobs.jsonl → 메모리 snapshot 복원 + `in_progress → pending` 크래시 복구 + SSE broadcast.
- 라우터 깔끔 분리: `jobs / settings / schedule / browser / query / probe_preview`.

**감점**
- 실 모드에서 사내 query 함수 임포트 실패 시 startup abort (graceful degrade 없음).
- 큰 로그 파일(>10만 라인) 시 `/api/jobs/history` 가 `readlines()` 전체 로딩 → 스트리밍 tail 최적화 필요.
- jobs.jsonl rotate 없음.

### 3) 프론트엔드 — 6.5/10

**강점**
- Vanilla JS SPA + SSE 실시간 — 의존성 제로, 300~600 ms 로드.
- flow 디자인 DNA: Pretendard, JetBrains Mono, `#FF5E00` accent, icon 탭, Modal/Pill/Chip 컴포넌트 일관.
- 탭 5종 전부 한글 라벨. nav 중립 bg + accent-glow active (flow 식).
- **제품 편집기**: 공통 기본(⚙) + 소스별 override(▤) + 공통 필터(⧗) 3섹션. 소스 힌트 카드(hint 필드 `inline code` 렌더).
- **모니터 히트맵**: 제품별 그룹 헤더 + 6 canonical source 고정 행 + 미추출 빗금 셀 + 레전드.
- **로그**: 제품/소스/상태/실패만/종류 필터 + 15s auto-refresh. probe fail/skip 배지 + warn 행 하이라이트.
- **탐색기**: SQL 가이드 `<details>` 접기 + 10개 snippet 클릭-적용.
- **설정**: 5개 버튼 탭(Lake/S3/Schedule/Probe/Source types). 탭 전환 시 draft 유지.

**감점**
- 번들러 없음 — 파일 1개(app.js 1,400+ lines)로 커짐. 모듈화 부채.
- 다크모드 토글 없음(flow 와 일치시키려면).
- 접근성(aria/label) 최소.

### 4) 운영성 — 6.0/10

**강점**
- `/api/jobs/history` + Logs 탭: 뒤에서부터 tail + 필터. "언제 시도, 얼마 걸림, 왜 실패" 즉시 확인.
- probe 실패 → plan entry 에 warn 배지 + 실패 이유 문자열.
- 제품×소스 히트맵 14일 × 6소스 × N제품 → 한 화면 overview.
- SSE 로 chunk 상태 실시간 반영.

**감점**
- 알림 채널(slack/email/webhook) 없음.
- 메트릭 export(prometheus) 없음.
- 전역 health dashboard(실패율·p95 latency 등) 없음.

### 5) 확장성 — 6.5/10

**강점**
- 소스 타입: YAML 한 줄 추가 → FE/BE 자동 반영. 색상/가이드/컬럼 풀 커스터마이즈.
- 초기 시딩 버튼: 신규 제품 세팅 시 300·600일 일괄 수동 실행(`/api/jobs/enqueue-product`).
- probe_skip + adaptive_correction + fallback_on_timeout → 변동성 큰 소스에 대한 자동 조정.
- max_concurrent 세마포어로 사내 API 부하 제어.

**감점**
- 30~60 GB/일 볼륨 perf 측정 · chunk size 자동 튜닝 미검증(이월).
- 멀티 워커(uvicorn `--workers > 1`) 시 state singleton 충돌 (현재 단일 프로세스 전제).

### 6) 테스트 — 6.0/10

**강점**
- `scripts/smoke_test.py` stdlib only, 31 항목, <0.5s, 전부 URL 체크 + 스키마 검증.
- 외부 의존성 제로 — 사내 PC에서도 바로 실행.

**감점**
- 단위 테스트(pytest) 0건 — planner/executor/state 핵심 로직 커버리지 없음.
- 실모드 회귀 테스트 없음(mock 모드만).
- FE 테스트(playwright) 없음.

### 7) 보안 — 4.5/10

**강점**
- secret_key 는 GET /api/settings 응답에서 `****` 로 마스킹.
- SQL 인젝션은 polars SQLContext 가 차단(사용자 SQL 은 read-only).
- 경로 이탈 방지: browser 가 `target.relative_to(base.resolve())` 로 escape 검사.

**감점**
- 인증 없음 (CORS `*`, 모든 라우트 open).
- settings.json 평문 저장(secret_key 포함).
- 감사 로그(auth event) 없음.
- 실모드에서 `importlib.import_module(settings.lake_api.module)` — settings 주입으로 임의 모듈 로딩 가능(트러스트 필요).

### 8) 문서 — 5.5/10

**강점**
- `README.md` — 개요/설치/실행 커맨드.
- `products.yaml`·`source_types.yaml` 주석 풍부.
- `config/settings.json` 주석 없이도 `/api/settings/schema` 로 자기 설명.

**감점**
- API 레퍼런스(OpenAPI) 자동 생성만 있고 큐레이티드 문서 없음.
- flow 연동 가이드 없음.
- 운영 플레이북(초기 세팅 → pilot 전환) 없음.

## 즉각 개선한 항목 (이 세션)

1. **flow 무한 리렌더**: `visibleCharts` 레퍼런스 변동으로 knob_lineage_summary 무한 루프 → primitive deps 로 fix.
2. **Valve v0.1.0 뱃지 제거** + **Nav flow 스타일 통일**(흰 배경 + accent-glow 활성).
3. **Settings 버튼 탭 전환** (한 페이지에 쌓이던 4 섹션 → 5 섹션).
4. **Products 편집기 전부 새로** (제품/소스/custom_col/params 추가·수정·삭제, 저장 성공).
5. **Monitor 히트맵 제품 그룹 + 6 canonical 소스 고정 행**.
6. **소스 타입 확장** (FAB/INLINE/ET + QTIME/EDS/VM) + 소스별 힌트 카드.
7. **소스 타입 동적 레지스트리** (`config/source_types.yaml` + `/api/schedule/source-types` + Settings 관리 UI).
8. **로그 탭 신규** (`/api/jobs/history` + 필터 + 15s auto-refresh + probe fail/skip 배지).
9. **제품 공통 기본 섹션 분리** (공통 custom_col + 공통 filter 명시).
10. **probe_skip 토글** (per-source) — probe 상습 실패 시 스킵.
11. **Browser SQL 가이드** (`<details>` 접기 + 10 snippet).
12. **초기 시딩 버튼** (제품별 600일 수동 일괄 실행).
13. **UI 한글화** (5개 탭 + 섹션명 + 상태바 + 주요 버튼).
14. **smoke test** 31/31 PASS.

## 다음 우선순위 (v0.2 이월)

1. 인증 (token/session) — CORS 제한 + /api/auth/* 추가.
2. prometheus metrics + 실패율 dashboard.
3. ET cross-source pre-filter (FAB lot list → INLINE/ET 자동 축소).
4. 30~60 GB/일 perf 측정 + chunk size auto-tune.
5. pytest 단위테스트 (planner/executor/state 커버리지 60%+).
6. jobs.jsonl rotate + `/history` streaming tail.
7. 다크모드 토글 + 번들러(esbuild) 도입.
8. 알림(slack/email) webhook.
