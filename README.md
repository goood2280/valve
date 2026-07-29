# Valve

> **DataLake 의 수도꼭지.** 사내 DataLake 에서 데이터를 뽑아 parquet 으로 정리 → S3 로 흘려보내 **flow** 에 공급한다. flow 와 별개의 독립 프로세스로 동작하며, flow 는 Valve 가 채운 S3 를 소비하기만 한다.

```
                       [사내 DataLake]
                              │
                              │  query(params, custom_col, user)
                              │  (rate limit · 5분 제한 · HY000 간헐)
                              ▼
                      ╔════════════════════╗
                      ║        Valve       ║   ← 운영 대시보드 6탭
                      ║  · Probe → Plan    ║     (모니터 · 제품 · 로그
                      ║  · Execute ×3      ║      · 설정 · 탐색기 · 알람)
                      ║  · raw → event     ║
                      ║    → feature       ║
                      ║    → wide → send   ║
                      ║  · Upload · 알람   ║
                      ╚════════════════════╝
                              │
                              │  hive partition parquet
                              │  date=YYYY-MM-DD/data.parquet
                              ▼
                         [S3 bucket]
                              │
                              ▼
                           [flow]
```

## 핵심 설계

- **Probe-First Two-Stage** — 하루치 쿼리 전에 1시간 샘플로 row 수 추정 → chunk plan 생성. 결과는 **7일 캐시** (한 번 측정하면 일주일 재사용).
- **Adaptive fallback** — chunk 가 timeout 나면 root_lot_id → item_id 로 자동 재분할.
- **1일 단위 Hive Partition** — `date=2026-04-24/data.parquet` 한 파일로 머지. 파티션 날짜는
  **데이터의 시간 컬럼**(`tkout_time`) 기준이지 쿼리 날짜가 아니다 (auto report 와 동일 규약).
- **Rolling Backfill** — 기본 3일 창(오늘·어제·그제) 1일 단위 replace. `backfill_days` 로 3~5 조정.
- **Idempotent Overwrite + Completeness Check** — probe 예상 row 수 vs 실제 row 수 비교, 허용치(기본 0.5%) 초과 시 S3 업로드 보류 + 재큐잉.
- **max_concurrent: 3** — 사내 API 부담 최소.
- **HY000 / Timeout / 5xx 자동 재시도** — exponential backoff 10s → 30s → 2min, 3회까지.

## 설치 · 실행

**요구사항: Python 3.10 이상.** (backend 코드가 3.10+ 타입 문법을 쓴다 — 3.9 이하에서는
서버 기동 시 `TypeError: unsupported operand type(s) for |` 로 죽는다. 여러 버전이 깔린
PC 라면 `py -3.11` 처럼 버전을 지정해서 실행할 것.)

### 설치본(setup.py)으로 — 사내 배포 표준

```bash
python setup.py        # 소스 추출 + requirements.txt 전체 pip 설치까지 자동
uvicorn app:app --host 0.0.0.0 --port 8090
```

이미 추출된 상태에서 의존성만 다시 깔려면 `python setup.py install-deps`.

### 저장소에서 직접

```bash
cd Valve
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8090 --reload
```

### 안 뜰 때 체크리스트

| 증상 | 원인 · 해결 |
|---|---|
| `ModuleNotFoundError: sse_starlette` (또는 polars·pyarrow…) | `pip install fastapi` 만으로는 부족 — **`pip install -r requirements.txt` 로 전체 설치** |
| `TypeError: unsupported operand type(s) for \|` | Python 3.9 이하 — 3.10+ 로 실행 (`python --version` 확인) |
| pip 이 아주 낮은 버전을 골라 설치 | 파이썬이 낡아 최신 wheel 을 못 받는 것 — 3.10+ 환경에서 다시 설치 |
| 사내망이라 pip 이 외부를 못 봄 | 외부 PC 에서 `pip download -r requirements.txt -d wheels/` 후 반입 → `pip install --no-index --find-links wheels/ -r requirements.txt` |

### Mock → 사내 실 API 전환

기본값은 **Mock 모드** (가짜 데이터 · HY000 5% 확률 · 1% 확률 6분 timeout 주입).
웹 **설정 탭**에서 `lake_api.mode` 를 `real` 로 바꾸면 전환된다. 이때 `lake_api.module` 이
실제로 호출할 함수를 가리켜야 한다.

- 형식은 **`패키지.모듈:함수`** (`importlib` 로 동적 로드). 기본값 `valve.mock:query` 는 자리표시자다.
- Valve 가 기대하는 시그니처는 **`query(params, custom_col, user)`** — 반환은 pandas/polars DataFrame.
- **인증 정보는 `user_name` 하나뿐이다.** 사내 DataLake query API 는 키/토큰을 받지 않는다 —
  설정 탭의 `lake_api.user` 가 그대로 `getData(user_name=…)` 로 간다. (v0.3.9 에서 쓰지도 않던
  `api_key` 필드를 없앴다.)
- 사내 `getData` 는 keyword 시그니처(`getData(params, custom_columns=~, user_name=~)`)라 **그대로 꽂을 수 없다.**
  `PYTHONPATH` 에 잡히는 곳에 얇은 어댑터를 하나 두고 그 경로를 `module` 에 적는다:

```python
# mycorp/valve_adapter.py   →   설정 탭에 module: mycorp.valve_adapter:query
from bigdataquery import getData

def query(params, custom_col, user):
    if custom_col:                      # 빈 리스트면 전체 컬럼 (인자 자체를 뺀다)
        return getData(params, custom_columns=custom_col, user_name=user)
    return getData(params, user_name=user)
```

`user` 는 `lake_api.user` 기본값이 넘어오고, 호출 측에서 `api.query(params, cols, user="다른계정")`
처럼 건당 덮어쓸 수 있다. `params` 키(`table_name`·`dateFrom`·`dateTo`·shard 컬럼)는
제품 탭의 params 템플릿이 만들어 준다 — `reference/Ref_raw_query.py` 와 같은 모양이다.

## 파일 구조

```
Valve/
├── app.py                       FastAPI entry (라우터 등록 · startup 백그라운드 루프)
├── setup.py                     self-extracting 설치본 (_build_setup.py 로 생성)
├── _build_setup.py              설치본 빌더 — 코드는 항상 교체, config 는 seed-only
├── VERSION.json                 버전 · changelog (설치본 메타의 원본)
├── requirements.txt             의존성
│
├── config/                      ★ 설정·룰북 (아래 표 참조) — 설치본이 덮지 않는다
├── backend/                     서버 코드
├── frontend/                    단일 페이지 UI (index.html · app.js · style.css · favicon.svg)
│
├── db/                          ★ 파이프라인 산출물 (번들 대상 아님)
│   ├── 1.RAWDATA_DB/{SOURCE}/{vehicle}/date=YYYY-MM-DD/data.parquet
│   ├── 2.EVENT_DB/{vehicle}/{SOURCE}/date=YYYY-MM-DD/data.parquet  (+ _meta.json)
│   ├── 3.FEATURE_STORE/{vehicle}/{FEATURE}.parquet                 (+ _meta.json)
│   ├── 4.WIDE_FORM/ML_TABLE_{vehicle}.parquet
│   ├── 5.SEND_FORM/{0.KNOB,1.FAB,2.VM,3.INLINE}/*.parquet · *.csv
│   └── REPORTS/{vehicle}/       knob_miss · knob_skip · unmatched · scan_result ·
│                                alerts_published (알람 탭 원본)
│
├── staging/{product}/{SOURCE}/  임시 parquet (머지 후 정리)
├── s3_outbox/valve-alerts/      ★ S3 업로드 폴더 — flow 매칭알람이 읽는다 (아래 참조)
├── s3_local/                    fake_local_path 기본값 (개발용 가짜 S3 · 실제 운영엔 없음)
├── logs/                        운영 로그 (아래 표)
├── docs/                        설계 문서 · 매뉴얼 PDF
├── scripts/smoke_test.py        stdlib 만으로 핵심 라우트 검증
├── scripts/gen_manual_pdf.py    docs/valve_manual.pdf 생성
└── tests/                       pytest (187개 · `python -m pytest tests -q` · 약 3분)
```

### `config/` — 있어야 도는 파일들

**필수** 은 없으면 해당 기능이 죽는다는 뜻이다. *누가 쓰나* 는 그 파일을 **덮어쓰는 주체** —
같은 파일을 다른 데서 고치면 다음 동기화/저장에 날아간다.

| 파일 | 필수 | 누가 쓰나 | 내용 |
|---|---|---|---|
| `pipeline.yaml` | ✅ 전체 | 웹(모니터 ⚙ 실행 관리 · 알람 탭) | `db_root` · `runtime`(워커·주기·**실행 금지 시간대**·재시도) · `sources`(테이블/컬럼/**파티션 기준 열**) · 룰북 경로 · `unmatched_scan`(제외 규칙 · 알람 전송 열 · **function step 추천 컨텍스트**) · `knob_skip` |
| `vehicles.yaml` | ✅ 전체 | 웹(제품별 주기) | vehicle 정의 — `product` `process_id` `line_id` `QueryTimeSpan` `SplitTimeSpan` `event_days_back` `event_lot_startwith` `runs_per_day` |
| `settings.json` | ✅ 전체 | 웹(설정 탭) | `lake_api`(mode·module·**user_name**·timeout·retry — 인증은 user 하나뿐) · `s3`(bucket·key) · `schedule` · `probe` · `alerts` |
| `products.yaml` | ✅ 모니터 백필 | 웹(제품 탭) | 제품 × 소스 테이블 · `shard_hierarchy` · `target_chunk_rows` · `params_template` |
| `source_types.yaml` | ✅ 제품 편집기 | 웹(설정 › Source types) | 소스 메타 — 컬럼 풀 · 기본 shard · 색 · 힌트 |
| `step_matching/vehicle_matching.csv` | ✅ event·feature | **flow → S3 ↓** | `vehicle,product,step_id,step_desc` — 이게 없으면 event DB 가 비어 feature 도 안 나온다 |
| `feature_rules/fab.csv` | FAB feature | **flow → S3 ↓** | `step_desc,feature_name,agg` |
| `feature_rules/ppid_knob.csv` | KNOB feature | **flow → S3 ↓** | 사내 룰 형식 `feature_name,function_step,rule_order,operator,value,category[,use]`. flow 판정 결과가 반영되는 마스터 룰북 |
| `feature_rules/mask.csv` | MASK feature | **flow → S3 ↓** | `step_desc,agg` (photo step 의 `part\|reticle`) |
| `feature_rules/inline.csv` | INLINE feature | **flow → S3 ↓** | `item_id,agg` |
| `feature_rules/vm.csv` | VM feature | **flow → S3 ↓** | `sensor_id,agg` (residual 집계) |
| `feature_rules/knob_ppid.csv` | 선택 | 사람 | legacy **직접 매핑** 형식 `vehicle,step_id,step_desc,ppid,knob[,agg]` 예시. `pipeline.yaml` 의 `feature_rules.knob` 을 이 파일로 바꾸면 그대로 쓴다 (기본값은 `ppid_knob.csv`) |
| `reformatter/{vehicle}_reformatter.csv` | ET raw | 사람 / auto report 공유 | auto report 와 같은 포맷. `CATEGORY=REAL` 행의 `ITEMID` 만 ET 쿼리 대상. **파일 없는 vehicle 은 ET 를 건너뛴다** |
| `fab_scan/{vehicle}/scan_config.yaml` | FAB 스캔 | 웹(스캔 탭) | 스캔 대상 기간·step 범위 |
| `fab_scan/{vehicle}/scan_ignore.json` | FAB 스캔 | **flow → S3 ↓** | 무시할 step/ppid (flow 에서 "반영불필요" 판정한 것) |
| `feature_funcs.py` | 선택 | 사람 | 커스텀 feature 함수. `def <이름>` → `fab.csv` 의 `feature_name`, `def agg_<이름>` → `agg` 컬럼. **재시작 없이** 실행 시점마다 다시 읽는다 |
| `s3_jobs.yaml` | 자동 생성 | 탐색기 ⚙ | S3 전송 항목(방향·key·주기). 최초 기동 때 아래 두 파일에서 이관된다 |
| `csv_sync.yaml` | legacy | — | 구 다운로드 설정. `s3_jobs.yaml` 로 이관된 뒤에는 참고용 |
| `s3_transfer.yaml` | legacy | — | 구 업로드 규칙 + `destinations`(S3 접속 정보). destinations 는 지금도 쓴다 |
| `probe_cache.json` | 자동 생성 | planner | probe 결과 7일 캐시. 지워도 다시 만들어진다 (다음 실행이 느려질 뿐) |
| `scan/` | ❌ 미사용 | — | `fab_scan/` 으로 대체된 옛 경로. 코드가 참조하지 않는다 |

### 설정 파일은 언제 덮이나 — 3가지 경로

같은 `config/` 를 건드리는 주체가 셋이라, "내가 고친 게 왜 사라졌지" 는 대부분 여기서 갈린다.

| 경로 | 무엇을 덮나 | 안전장치 |
|---|---|---|
| **설치본 재실행** (`python setup.py`) | 코드(`backend/` `frontend/` `app.py` …)는 **항상 교체**. `config/` 는 **이미 있으면 절대 안 덮는다**(seed-only) | 추출 직전 `~/.valve_backups/` 로 스냅샷 → `python setup.py restore latest` |
| **S3 동기화** (flow → Valve) | 위 표의 **flow → S3 ↓** 행 — 룰북 csv 와 `scan_ignore.json`. 로컬에서 고쳐도 다음 sync 에 덮인다 | 탐색기 신호등(↓)으로 소유자 표시. 룰북은 flow 판정 페이지에서 고칠 것 |
| **웹에서 저장** | 그 파일 하나를 통째로 다시 쓴다 (`pipeline.yaml` `vehicles.yaml` `settings.json` …) | 값은 그대로. 단 yaml **주석**은 파일 맨 앞 블록만 살아남는다 — 아래 참조 |

> **주석 유실 주의.** yaml 저장은 `yaml.safe_dump` 라 주석을 전부 버리고, 복원되는 건
> **파일 첫 줄부터 이어지는 주석 블록**뿐이다. 그래서 `pipeline.yaml` · `vehicles.yaml` 은
> 설명을 전부 파일 맨 위에 모아 뒀다. **키 옆에 주석을 달면 웹에서 한 번 저장하는 순간 지워진다** —
> 설명을 추가할 땐 맨 위 블록에 넣을 것. (**설정 값 자체가 사라지는 일은 없다.**)

### `logs/` — 운영 기록

| 파일 | 내용 |
|---|---|
| `pipeline_runs.jsonl` | 제품 × 1회 실행 = 1행. 단계별 소요·산출·실패 사유. **스케줄 주기의 근거**(재기동해도 유지) |
| `jobs.jsonl` | 모니터 탭 plan/chunk/partition 이벤트 (append-only · crash recovery) |
| `s3_jobs_status.json` · `s3_jobs_history.jsonl` | S3 전송 진행 상태 · 이력(최근 500) |
| `csv_sync.json` | legacy csv 동기화 상태 |
| `alerts_ack.json` | 알람 확인 상태 로컬 캐시 (원본은 S3 `ack.json`) |
| `agent_audit.jsonl` | 에이전트 호출 감사 로그 |

`db/` · `logs/` · `staging/` · `s3_local/` 은 **설치본에 들어가지도, 건드려지지도 않는다**
(`_build_setup.py` 의 제외 목록 + `setup.py` 쓰기 가드 이중 방어).

### `backend/` — 코드 지도

| `core/` | 역할 |
|---|---|
| `feature_pipeline.py` | **파이프라인 본체** — raw → event → feature → wide → send 전 단계 + 룰북 로딩 + 리포트 |
| `pipeline_runner.py` | 병렬 오케스트레이터 + 진행상황(stage) 추적 + 제품별 주기/루프 스케줄러 + **실행 금지 시간대** + 실행 락 + 작업 큐(실행·대기·예정 · 안전 지점 취소) |
| `pipeline_retry.py` | raw 실패 유닛 영구 재시도 큐 (severity 3단계 · 상한 도달 시 blocked) |
| `run_log.py` | 실행 이력 (vehicle 1회 = 1레코드, append-only JSONL) |
| `runtime_env.py` | 호스트 코어/메모리 → 워커 수 산정 (`auto` 값의 근거) |
| `lake_api.py` | 사내 DataLake `query()` 어댑터 — Mock/Real · rate limit · retry · timeout |
| `planner.py` | probe → chunk plan (7일 캐시) |
| `executor.py` | asyncio chunk worker (`max_concurrent=3`) + 머지 + completeness |
| `state.py` | plan/chunk/partition 상태 + SSE broadcast + crash recovery |
| `alert_store.py` | 알람 통합 리스트 + flow 와의 S3 순환(ack) + 업로드 폴더 미러 · 미매칭 step 에 `match_hint` 동봉 |
| `fab_scanner.py` | FAB DB 스캔 — 미매칭 step · 미등록 PPID |
| `s3_jobs.py` | S3 전송 항목 엔진 (단일 워커 큐 · 파일 경계 중지 · 이력) |
| `s3_up.py` · `s3_queue.py` · `s3_link.py` | atomic put · 지연 큐 · 탐색기 신호등(방향 판정) |
| `csv_sync.py` · `config_sync.py` | legacy csv 동기화 · 다중 인스턴스 config 공유 |
| `fab_scan.py` | 스텁 — `fab_scanner.py` 로 통합됨 |

| `routers/` | 경로 |
|---|---|
| `pipeline.py` | `/api/pipeline` — 실행 · 상태 · 주기 · 진행률 · 알람 · 룰북 설정 (알람 탭 + DB heatmap) |
| `jobs.py` | `/api/jobs` — plan/chunk 상태 · SSE · enqueue · cancel · retry |
| `scanner.py` | `/api/scanner` — FAB 스캔 |
| `s3_jobs.py` | `/api/s3` — 전송 항목 CRUD · 실행/중지 · 진행률 · 이력 |
| `browser.py` · `query.py` | 파일탐색기 · parquet + polars SQL 조회 |
| `settings.py` · `schedule.py` · `probe_preview.py` | 설정 CRUD(secret 마스킹) · 예정 목록 · probe dry-run |
| `ops.py` · `agent.py` · `aipd_bridge.py` | Prometheus 메트릭 + webhook · 에이전트 스캐폴딩 · aipd 연결 |
| `fab_scan.py` | 스텁 — `scanner.py` 로 통합됨 |

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

## DB heatmap — 처리 현황과 `feat` 배지

모니터 탭의 DB heatmap 은 vehicle × 소스 × 날짜로 `db/` 처리 단계를 보여준다.
소스 행의 배지가 **feature store 산출물**이다.

```
● FAB   feat 7 · 20일 (07-04~07-27)
        └ 7 = feature 컬럼(parquet) 수 — fab 4 · knob 2 · mask 1
          20일 (07-04~07-27) = 그 feature 가 대상으로 삼은 event 구간
```

feature 는 특정 기간이 아니라 **그때 event DB 에 있던 전체**를 대상으로 산출된다.
feature parquet 은 `root_lot_id`·`wafer_id` 키만 있고 날짜 컬럼이 없어서,
무엇을 며칠치 담았는지는 산출 시점에 `db/3.FEATURE_STORE/{vehicle}/_meta.json` 으로 남긴다.
(그 기록이 없는 예전 산출물은 현재 event 기준 **추정**으로 표시하고 툴팁에 그렇게 적는다 —
한 번 실행하면 정확해진다.)

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
- 실행에 관한 설정은 **모니터 탭 › DB heatmap › ⚙ 실행 관리** 한 곳에 모여 있다 —
  전역 주기 · 실행 금지 시간대 · 루프 · 수동 전체 실행 · 제품별 주기(일 N회).
  알람 탭에는 같은 내용이 **읽기 전용**으로 표시된다. `PUT /api/pipeline/schedule/{vehicle}`.

### 실행 금지 시간대 (quiet window)

야간 백업·사내 API 점검처럼 파이프라인이 돌면 안 되는 구간을 비워둔다.

```yaml
# config/pipeline.yaml
runtime:
  quiet_enabled: true
  quiet_start: '00:00'    # 시작 > 종료 면 자정을 넘는 구간 (예 '23:00'~'02:00')
  quiet_end: '02:00'
```

- 대상은 **자동 실행(주기·루프)뿐** — 사람이 누르는 ▶ 실행은 그대로 된다.
- **이미 돌고 있는 실행은 중단하지 않는다** (중간에 끊으면 event 파티션이 반만 남는다).
- 예정 시각은 건너뛰는 게 아니라 **해제 시각으로 밀린다** — 02:00 에 밀렸던 제품이 돈다.
- 키가 없는 기존 설치도 코드 기본값(`00:00~02:00`)이 적용된다. 끄려면 `quiet_enabled: false`.

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

### 미매칭 step 알람에 실리는 것 — function step 추천 근거

flow 는 신규 step 이 **어느 function step 인지**를 GPT OSS 120B 로 추천한다. 그런데
flow 서버에는 FAB raw DB 가 없다 — 근거는 Valve 가 알람에 실어 보내야 한다.

`unmatched_step` 알람에는 `match_hint` 가 함께 실린다: step_id 는 같은 prefix·자릿수
안에서 번호가 공정 순서를 뜻하므로(`AA100002` 는 `AA100000` 과 `AA100006` 사이),
**번호가 가장 가까운 앞뒤 "매칭된" step** 을 고른 뒤, 최근 며칠치 FAB raw 에서 각각이
실제로 쓴 `ppid · eqp_id · eqp_model · area` unique 를 뽑아 같이 보낸다.

```jsonc
{ "id": "um|VH_PRODA|CC955200", "type": "unmatched_step", "step_id": "CC955200",
  "match_hint": {
    "prefix": "CC", "number": 955200, "days": 7,
    "window": {"from": "2026-07-20", "to": "2026-07-26", "dates": 7},
    "cols": ["ppid", "eqp_id", "eqp_model", "area"],
    "values": {"ppid": ["PP_SPAC_STD"], "eqp_id": ["CVD_02"]},      // 신규 step
    "neighbors": [                                                   // 앞뒤 매칭 step
      {"step_id": "CC955100", "step_desc": "SPACER_CVD", "direction": "prev", "gap": 100,
       "values": {"ppid": ["PP_SPAC_STD"], "eqp_id": ["CVD_02"]}}
    ] } }
```

판단은 flow 몫이다 — ppid 가 같은 이웃이 1순위, 새 ppid 면 eqp_id·eqp_model·area 가
겹치는 이웃이 후보다. **한 step 은 1회만 검사하고 flow 가 결과를 기록한다.**

| 항목 | 값 | 어디서 |
|---|---|---|
| 사용 여부 · 기간 · 이웃 수 · 비교 열 | `unmatched_scan.hint` | 알람 탭 ⚙ *function step 추천 컨텍스트* |
| 만드는 곳 | `feature_pipeline._step_match_hints` → `alert_store.build` | — |

- `cols` 의 열이 FAB raw 에 없으면 조용히 빠진다. **`area` 를 쓰려면 `sources.FAB.columns`
  에 먼저 추가**해야 한다 (알람 탭 ⚙ *조회 컬럼*) — 사내 테이블에 없는 열을 조회에 넣으면
  FAB raw 쿼리가 통째로 실패하므로 기본 조회 컬럼에는 넣지 않았다.
- 이웃은 **이미 매칭된 step** 만이다. 매칭 테이블에 아직 없는 step 은 추천 근거가 못 된다.
- 발행 payload 에는 `schema` 버전이 붙는다. Valve 를 올리면 알람 구성이 그대로여도
  한 번은 다시 발행된다 — 새 필드가 flow 에 전달되도록.

## 현재 범위

- **v0.1** (2026-04-24) — 백엔드 완성 · Mock 으로 end-to-end 돌아감 · API 로 enqueue/조회 가능
- **v0.2** — frontend 단일 페이지 (Monitor 캘린더 히트맵 · Products · Settings · Browser 4탭) · smoke_test · 실행 검증
- **v0.3** (2026-07-26) — 파이프라인 3단계(raw→event→feature)+wide/send · 제품별 실행 주기 · 실행 로그 · S3 전송 항목 엔진(⚙) · 알람 S3 순환 · 실행 락
- **v0.3.1** (2026-07-27) — `feat` 배지에 커버 구간 · **실행 금지 시간대** · ⚙ 실행 관리로 실행 조작 통합 · 진행 표시 깜빡임 제거
- **v0.3.2~0.3.4** (2026-07-27~28) — Python 3.10 미만 기동 차단 안내 · Windows 설치 실패 수정 · **매칭알람 전송 개편**(예시 lot/wafer · 전송 열 ⚙ · 업로드 폴더) · DB 루트 분리(`db_root` 절대경로 · `VALVE_DB_ROOT`)
- **v0.3.5~0.3.6** (2026-07-28) — raw 실패 유닛 **영구 재시도 큐**(severity 3단계) · event skip 버그 수정 · **작업 큐**(실행·대기·예정 + 안전 지점 취소) · 재시도 5회 상한 · DB 40GB 경고
- **v0.3.7~0.3.8** (2026-07-29) — DB 사용량을 소스별 raw·event 로 분해 · 전 소스 **파티션 기준 열을 `tkout_time` 으로 통일** + 웹(⚙)에서 조회 컬럼·기준 열 편집
- **v0.3.9** (2026-07-29) — 매칭알람에 **function step 추천 근거**(앞뒤 이웃 step 컨텍스트 `match_hint`) 동봉 + 알람 탭 ⚙ 설정 · 사내 Lake API 설정에서 쓰지 않는 `api_key` 제거(인증은 `user_name` 뿐)
- **다음** — 실사내 API 연결 (`lake_api.mode: real`) · 알림 연동

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
| GET  | `/api/pipeline/queue` · POST `/queue/cancel` | 작업 큐(실행·락대기·예정) · 안전 지점 취소 |
| GET  | `/api/pipeline/retries` · POST `/retries/resume` | raw 재시도 큐 · blocked 재개 |
| GET  | `/api/pipeline/db-usage` | 제품 × 소스 raw·event DB 사용량 |
| GET  | `/api/pipeline/sources` · PUT `/config/sources` | 소스별 table·조회 컬럼·파티션 기준 열 |
| GET  | `/api/pipeline/alerts/outbox` | S3 업로드 폴더 현황 (경로 · 파일 · 발행 주기) |
| PUT  | `/api/pipeline/config/alert-cols` | 매칭알람 전송 열 · 예시 개수 |
| PUT  | `/api/pipeline/config/alert-hint` | function step 추천 컨텍스트 (기간 · 이웃 수 · 비교 열) |
| GET  | `/api/browser/roots` · `/list` | 파일탐색기 |
| GET  | `/api/query/view` | parquet + SQL 필터 |
