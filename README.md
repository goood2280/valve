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
├── logs/jobs.jsonl           append-only 이벤트 로그
├── s3_local/                 fake_local_path (개발용 가짜 S3)
├── frontend/index.html       단일 페이지 (v0.2)
└── scripts/smoke_test.py     stdlib 만으로 핵심 라우트 검증 (v0.2)
```

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
| GET  | `/api/browser/roots` · `/list` | 파일탐색기 |
| GET  | `/api/query/view` | parquet + SQL 필터 |
