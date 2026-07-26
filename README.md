# Valve

> **DataLake 의 수도꼭지.** 사내 DataLake 에서 데이터를 뽑아 parquet 으로 정리 → S3 로 흘려보내 **flow** 에 공급한다. flow 와 별개의 독립 프로세스로 동작하며, flow 는 Valve 가 채운 S3 를 소비하기만 한다.

```
                       [사내 DataLake]
                              │
                              │  query(params, custom_col, user)
                              │  (rate limit · 5분 제한 · HY000 간헐)
                              ▼
                      ╔════════════════╗
                      ║     Valve      ║   ← 운영 대시보드
                      ║  · Probe       ║     (Monitor · Products
                      ║  · Plan        ║      · Settings · Browser)
                      ║  · Execute ×3  ║
                      ║  · Merge       ║
                      ║  · Upload      ║
                      ╚════════════════╝
                              │
                              │  hive partition parquet
                              │  date=YYYY-MM-DD/part-0.parquet
                              ▼
                         [S3 bucket]
                              │
                              ▼
                           [flow]
```

## 핵심 설계

- **Probe-First Two-Stage** — 하루치 쿼리 전에 1시간 샘플로 row 수 추정 → chunk plan 생성. 결과는 **7일 캐시** (한 번 측정하면 일주일 재사용).
- **Adaptive fallback** — chunk 가 timeout 나면 root_lot_id → item_id 로 자동 재분할.
- **1일 단위 Hive Partition** — `date=2026-04-24/part-0.parquet` 한 파일로 머지.
- **Rolling Backfill** — 기본 3일 창(오늘·어제·그제) 1일 단위 replace. `backfill_days` 로 3~5 조정.
- **Idempotent Overwrite + Completeness Check** — probe 예상 row 수 vs 실제 row 수 비교, 허용치(기본 0.5%) 초과 시 S3 업로드 보류 + 재큐잉.
- **max_concurrent: 3** — 사내 API 부담 최소.
- **HY000 / Timeout / 5xx 자동 재시도** — exponential backoff 10s → 30s → 2min, 3회까지.

## 실행

```bash
cd Valve
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8090 --reload
```

기본값은 **Mock 모드** (가짜 데이터 · HY000 5% 확률 · 1% 확률 6분 timeout 주입). 웹에서 Settings → `lake_api.mode: real` 로 바꾸고 `module: mycorp.datalake:query` 같은 실제 경로를 넣으면 전환.

## 폴더 구조

```
Valve/
├── app.py                    FastAPI entry
├── config/
│   ├── settings.json         사내 API · S3 · schedule · probe 설정 (웹 CRUD)
│   ├── products.yaml         제품별 params 템플릿
│   └── probe_cache.json      probe 결과 7일 캐시 (자동 생성)
├── backend/
│   ├── core/
│   │   ├── lake_api.py       Mock + Real 어댑터 · retry · timeout
│   │   ├── planner.py        probe + chunk plan + 7일 cache
│   │   ├── executor.py       asyncio worker(3 concurrent) + merge + completeness
│   │   ├── s3_up.py          atomic put + fake_local 모드
│   │   └── state.py          jobs.jsonl + SSE broadcast + crash recovery
│   └── routers/
│       ├── jobs.py           /api/jobs — state · stream · enqueue · cancel · retry
│       ├── settings.py       /api/settings — GET/POST (secret 마스킹)
│       ├── schedule.py       /api/schedule — 예정 목록 · products CRUD
│       ├── browser.py        /api/browser — staging · s3_local 탐색
│       ├── query.py          /api/query — parquet + polars SQL 필터
│       └── probe_preview.py  /api/probe-preview — probe dry-run
├── staging/                  임시 parquet (자동 정리)
├── s3_outbox/valve-alerts/   ★ S3 업로드 폴더 — flow 매칭알람이 읽는 파일 (아래 참조)
├── logs/jobs.jsonl           append-only 이벤트 로그
├── logs/pipeline_runs.jsonl  파이프라인 실행 로그 (제품 × 1회 = 1행)
├── logs/s3_jobs_*.json(l)    S3 전송 상태 · 이력
├── s3_local/                 fake_local_path (개발용 가짜 S3)
├── frontend/index.html       단일 페이지 (v0.2)
└── scripts/smoke_test.py     stdlib 만으로 핵심 라우트 검증 (v0.2)
```

## S3 업/다운로드 — 탐색기 ⚙

전송은 **항목(item)** 단위다. "로컬 어디" ↔ "S3 어느 key" 를 짝지어 두고 방향·명령·주기를 각각 준다.
탐색기 우상단 **⚙** 에서 관리한다 (flow 의 s3_ingest 와 같은 사용 모델).

```yaml
# config/s3_jobs.yaml — ⚙ 에서 편집하면 여기에 쓰인다
items:
  - id: dl_feature_rules_ppid_knob
    direction: download          # download(S3→Valve) | upload(Valve→S3)
    root: config                 # config | staging | db | outbox
    target: feature_rules/ppid_knob.csv
    dest: default                # S3 연결 (s3_transfer.yaml 의 destinations)
    key: flow/artifacts/matching/ppid_knob.csv
    mode: sync                   # sync(변경분만) | cp(항상 덮어쓰기)
    interval_min: 30             # 0 = 수동 전용
    enabled: true
auto_download_enabled: true      # 주기 실행 마스터 (방향별)
auto_upload_enabled: true
```

- **S3 key 는 눌러서 고른다** — 폼의 `🔍 S3 에서 고르기` 가 버킷을 계단식으로 훑어 실제 존재하는 key 를 보여준다.
- **▶ 실행 / ■ 중지** — 실행은 단일 워커 큐(동시 1개, 나머지 대기). 중지는 **파일 경계**에서 끊으므로
  이미 옮긴 파일은 남고 반쪽 파일은 생기지 않는다. 상태는 `cancelled` 로 기록된다.
- 진행률은 5초 폴링 (`{done, total, current}`). 이력은 `logs/s3_jobs_history.jsonl` (최근 500).
- 기존 `csv_sync.yaml`(다운로드) · `s3_transfer.yaml`(업로드) 은 **최초 기동 때 항목으로 자동 이관**된다.
  업로드 항목은 사고 방지를 위해 `enabled: false` (수동 전용) 로 시작한다.

### 탐색기 신호등 — 받는 파일과 올리는 파일

| 표시 | 뜻 | 예 |
|---|---|---|
| **↓** 파랑 | S3 → Valve **받기**. flow 가 소유하므로 올리면 다음 sync 에 덮인다 | `Vehicle_matching.csv` · `ppid_knob.csv` · `inline.csv` · `mask.csv` · `vm.csv` · `scan_ignore_*.json` |
| **↑** 초록 | Valve → S3 **올리기** | `staging/` · 알람 `outbox/` |
| **↕** 보라 | 양방향 — 기동 시 pull, 탐색기에서 push(seed) 가능 | `settings.json` · `products.yaml` · `source_types.yaml` |
| 화살표 없음 | 로컬 전용 (S3 자동 동기화 없음) | `pipeline.yaml` · `vehicles.yaml` · `feature_funcs.py` … |

점 색은 **상태** — 초록 정상 · 주황 대기/미수신 · 빨강 실패.

## 제품별 실행 주기 · 실행 로그

파이프라인(raw→event→feature→wide)은 **제품마다 다른 주기**로 돈다. 중요한 제품만 자주 돌릴 수 있다.

```yaml
# config/vehicles.yaml
VH_PRODA:
  runs_per_day: 6     # 4시간 간격
VH_PRODB:
  runs_per_day: 3     # 8시간 간격
VH_PRODC: {}          # 생략 → 전역 runtime.interval_hours 를 따름
```

| 값 | 뜻 |
|---|---|
| `runs_per_day: N` | 하루 N회 = `24/N` 시간 간격 |
| `0` | 자동 실행 제외 (수동 ▶ 실행은 가능) |
| 생략 | `pipeline.yaml` 의 `runtime.interval_hours` 를 따름 |

- 스케줄러는 `sched_tick_sec`(기본 60초)마다 **돌 때가 된 제품만** 골라 실행한다. 주기 자체가 아니라 "확인 간격"이다.
- 마지막 실행 시각의 근거는 `logs/pipeline_runs.jsonl` — **재기동해도 주기가 유지된다.**
- `runtime.schedule_enabled: false` 면 제품 설정과 무관하게 전부 정지한다 (마스터 스위치).
- 알람 탭 › 파이프라인 처리 현황에서 제품별로 웹 편집 가능. `PUT /api/pipeline/schedule/{vehicle}`.

**실행 로그** — 제품 × 1회 실행 = 1레코드 (`logs/pipeline_runs.jsonl`).
알람 탭 › 실행 로그에서 행을 클릭하면 단계별 상세가 펼쳐진다.

```
[raw] 1.74s · 유닛 24 / FAB 1050 · INLINE 576 · VM 384 · ET 960 rows
[event] 1.37s / FAB: raw 0 → event 0 · 0 파티션        ← 이미 처리된 날짜는 건너뜀
[feature] 1.34s · event 19일 전체 대상 / fab 4 · knob 2 · mask 1 · inline 2 · vm 2
    knob 미변환(RO) 7건 · skip 판정 17건
    ⚠ KNOB_CONTACT_ETCH_ppid: auto skip 보류 — 뒤쪽 step 판별 불가
[wide] 0.21s · rows 705 · feature 11 → db/4.WIDE_FORM/ML_TABLE_VH_PRODA.parquet
```

트리거(자동/수동/루프)와 성공·실패가 함께 남고, 실패는 어느 단계까지 갔는지와 사유가 남는다.
파일이 4MB 를 넘으면 최신 절반만 남긴다.

## flow 매칭알람 — S3 로 올릴 폴더 하나

flow 의 **매칭알람** 탭은 `s3://{bucket}/valve-alerts/pipeline/*.json` 만 읽는다.
Valve 는 알람을 발행할 때 같은 트리를 로컬에도 미러링하므로, **이 폴더 하나만 sync 하면 끝**이다.

```
D:/semi all/Valve/s3_outbox/valve-alerts/     ← sync 대상 (= s3://{bucket}/valve-alerts)
└── pipeline/
    ├── VH_PRODA.json                          제품별 알람 (미매칭 step · RO ppid)
    └── VH_PRODB.json
```

```bash
aws s3 sync "D:/semi all/Valve/s3_outbox/valve-alerts" s3://<bucket>/valve-alerts --exclude "*.tmp"
```

| 항목 | 값 | 어디서 |
|---|---|---|
| 로컬 폴더 | `alerts.outbox_dir` (기본 `s3_outbox`) + `alerts.s3_prefix` | 설정 탭 |
| 발행 주기 | `alerts.s3_interval_min` (기본 10분 — 5~15분 권장) | 설정 탭 |
| flow 폴링 주기 | `poll_seconds` (기본 300초) | flow `data/flow-data/valve_alerts.json` |
| 현황 확인 | 알람 탭 › **S3 업로드 폴더** · `GET /api/pipeline/alerts/outbox` | Valve UI |

- 파일은 **내용이 바뀔 때만** 갱신된다 (mtime 도 안 건드림) → `aws s3 sync` 가 헛돌지 않는다.
- 폴더가 통째로 지워져도 다음 발행 사이클에 자동 복구된다.
- **`ack.json` 은 이 폴더에 없다.** flow 도 쓰는 양방향 파일이라, 폴더 sync 로 덮으면
  flow 의 판정(보류/반영불필요)이 유실된다. ack 는 Valve/flow 가 각자 S3 에 직접 읽고 쓴다.
- 반대 방향(flow → Valve)의 matching csv 는 `config/csv_sync.yaml` 이 담당한다 (별개 경로).

## 현재 범위

- **v0.1** (2026-04-24) — 백엔드 완성 · Mock 으로 end-to-end 돌아감 · API 로 enqueue/조회 가능
- **v0.2** — frontend 단일 페이지 (Monitor 캘린더 히트맵 · Products · Settings · Browser 4탭) · smoke_test · 실행 검증
- **v0.3+** — 실사내 API 연결 · 자동 스케줄러 (interval_hours) · 알림 연동

## API 요약

| Method | Path | 설명 |
|---|---|---|
| GET  | `/api/health` | 서버/모드 확인 |
| GET  | `/api/version` | VERSION.json |
| GET  | `/api/jobs/state` | plans · chunks · partitions snapshot |
| GET  | `/api/jobs/stream` | SSE 실시간 |
| POST | `/api/jobs/enqueue` | `{product, source, date}` 단건 |
| POST | `/api/jobs/enqueue-all` | backfill 창 전체 일괄 |
| POST | `/api/jobs/cancel` | `{chunk_id}` |
| POST | `/api/jobs/retry-partition` | `{product, source, date}` 재실행 |
| POST | `/api/jobs/probe-invalidate` | probe 캐시 무효화 |
| GET  | `/api/schedule` | 예정 (제품 × 소스 × 날짜) |
| GET  | `/api/schedule/products` | products.yaml |
| POST | `/api/schedule/products` | products.yaml 저장 |
| GET  | `/api/settings` | 현재 설정 (secret 마스킹) |
| POST | `/api/settings` | 설정 업데이트 (런타임 반영) |
| GET  | `/api/settings/schema` | UI 폼 스키마 힌트 |
| POST | `/api/probe-preview` | probe dry-run + chunk plan 미리보기 |
| GET  | `/api/s3/items` | 전송 항목 + 상태 + 진행률 (⚙ 모달 폴링) |
| POST | `/api/s3/save` · `/delete` | 항목 추가·수정 / 삭제 |
| POST | `/api/s3/run` · `/stop` | 수동 실행 / 중지 (파일 경계 취소) |
| GET  | `/api/s3/browse-keys` | S3 key 고르기 (`?dest=&prefix=`) |
| GET  | `/api/s3/history` | 전송 이력 |
| GET  | `/api/pipeline/schedule` | 제품별 실행 주기 + 다음 실행 예정 + 최근 실행 요약 |
| PUT  | `/api/pipeline/schedule/{vehicle}` | `{runs_per_day}` 저장 (0=제외 · null=전역 추종) |
| GET  | `/api/pipeline/runs` | 실행 로그 (`?vehicle=&limit=&failed_only=`) |
| GET  | `/api/pipeline/alerts/outbox` | S3 업로드 폴더 현황 (경로 · 파일 · 발행 주기) |
| GET  | `/api/browser/roots` · `/list` | 파일탐색기 |
| GET  | `/api/query/view` | parquet + SQL 필터 |
