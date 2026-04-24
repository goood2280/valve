# Valve ↔ flow 연결성 평가 (2026-04-24)

**결론: 90% 는 그대로 맞물림. 남은 10%는 1줄 설정 또는 prefix 하나로 정렬 가능.**

## 1. 데이터 경로 비교

### Valve 가 쓰는 곳

| 대상 | 경로 |
|---|---|
| 로컬 staging | `Valve/staging/{product}/{source}/date=YYYY-MM-DD/{chunk_id}.parquet` + `_merged.parquet` |
| S3 업로드 | `s3://{bucket}/{prefix}/{source}/{product}/date=YYYY-MM-DD/part-0.parquet` |

구현 출처: [executor.py:196](Valve/backend/core/executor.py:196) — `f"{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"`.

### flow 가 읽는 곳

[long_pivot.py:32-34](flow/backend/core/long_pivot.py:32)

| 대상 | 경로 |
|---|---|
| FAB | `{db_root}/1.RAWDATA_DB_FAB/{product}/date=YYYY-MM-DD/*.parquet` (hive) |
| INLINE | `{db_root}/1.RAWDATA_DB_INLINE/{product}/date=YYYY-MM-DD/*.parquet` (hive) |
| ET | `{db_root}/1.RAWDATA_DB_ET/{product}/{product}_YYYYMMDD.parquet` (flat) **또는** hive |

## 2. 정렬 매트릭스

| 항목 | Valve | flow | 호환 |
|---|---|---|:-:|
| 포맷 | parquet (polars write) | parquet (polars scan) | ✅ |
| 파티셔닝 | hive `date=YYYY-MM-DD` | hive `date=YYYY-MM-DD` | ✅ |
| 폴더 계층 | `SOURCE/PRODUCT/date=.../` | `1.RAWDATA_DB_SOURCE/PRODUCT/date=.../` | ⚠ prefix 만 다름 |
| 파일명 | `part-0.parquet` | `*.parquet` glob | ✅ |
| 스키마 (FAB/INLINE) | long: `lot_id·wafer_id·time·item_id·value` | long: `lot_id·wafer_id·tkout_time·item_id·value` | ✅ (flow 어댑터가 자동 rename) |
| 스키마 (ET) | `lot_id·wafer_id·root_lot_id·item_id·time·value·pattern_id·die_x·die_y` | long + die 정보 | ✅ |
| 컬럼 풀 커스터마이즈 | source_types.yaml | x (읽기만) | ✅ |
| 멀티 chunk 파일 | `_merged.parquet` 로 합병 후 업로드 | glob 으로 자동 unload | ✅ |
| 날짜 외 필터 | process_id/line_id 등 params_template | 클라이언트측 | ✅ |
| 업로드 원자성 | `.tmp` → copy → delete (rename 효과) | 읽는 중 반쪽 파일 감지 불가 | ✅ 실제로 안전 |

## 3. 마지막 1마일 — 하나만 맞추면 끝

### Option A (가장 단순): Valve 의 `s3.prefix` 를 활용

**Settings › S3**:
- `bucket`: `flow-datalake` (또는 공용 버킷)
- `prefix`: 공란 (또는 환경 접두사)

**executor 출력 규칙 변경**: [executor.py:196](Valve/backend/core/executor.py:196) 한 줄만 flow 규약에 맞춤.

```python
# 현재
s3_key = f"{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"
# ↓ flow 규약
s3_key = f"1.RAWDATA_DB_{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"
```

→ 결과: `s3://flow-datalake/1.RAWDATA_DB_FAB/PRODA/date=2026-04-24/part-0.parquet` — flow 가 **그대로** 읽음.

### Option B (환경 격리): 설정으로 제어

settings.json 에 `s3.flow_layout: "canonical"` 플래그 추가, executor 가 `1.RAWDATA_DB_{SOURCE}/...` 로 내보냄. 기존 레이아웃 유지 옵션도 남김.

## 4. flow 쪽에서 Valve 를 바라보는 시나리오

### A. 사내 DataLake 가 온라인 (real mode)

```
[사내 DB] ─Valve(real)─▶ S3 (public bucket)
                              │
                              ▼
          [shared workspace 동기화 스크립트 또는 S3 mount]
                              │
                              ▼
                   flow 의 db_root ─ flow UI
```

**권장 연동**:
1. Valve 는 S3 에 `1.RAWDATA_DB_{source}` prefix 로 적재.
2. flow 쪽 [s3_ingest](flow/backend/s3_ingest.py) 가 같은 버킷을 down-sync 해 `{db_root}/1.RAWDATA_DB_{source}` 로 푼다.
3. flow UI 에서는 일반 DB 로 보임. 사용자는 Valve 의 존재를 몰라도 됨.

### B. 개발/mock 모드

Valve 는 `staging/` 로컬만 쓰고 S3 는 `fake_local_path` (디스크) 로 대체.
flow 는 별도의 `db_root` 를 사용. 두 앱이 파일시스템을 공유하지 않아도 문제 없음 (각자 데모 가능).

### C. 소규모 배포 — 공유 워크스페이스 직접 쓰기

`s3.endpoint_url=""`, `fake_local_path` 를 flow 의 `db_root` 로 지정:
```json
"s3": {
    "endpoint_url": "",
    "bucket": "db",
    "fake_local_path": "/config/work/sharedworkspace/DB",
    "prefix": ""
}
```
→ Valve 는 `/config/work/sharedworkspace/DB/db/1.RAWDATA_DB_FAB/...` 에 쓰고, flow 의 db_root 를 `/config/work/sharedworkspace/DB/db` 로 맞추면 **중간 동기화 없이 바로 연결**.

## 5. 스키마 드리프트 대응

| 시나리오 | 영향 | 대응 |
|---|---|---|
| Valve 에 custom_col 추가 (예: `eqp_id`) | flow 에 새 컬럼 등장 | flow 는 read-time schema 감지 → SplitTable/Dashboard 에서 자동 노출. 영향 없음. |
| Valve 에 custom_col 삭제 | flow 가 None-fill | flow 의 coalesce 로직(예: fab_lot_id) 이 이미 대비. |
| 소스 타입 신설 (예: `CUSTOMDB1`) | flow 가 모름 | flow 의 adapter 에 새 source 이름 등록 필요 (수동). |
| 컬럼 이름 변경 (예: `time` → `tkout_time`) | flow FAB 어댑터는 이미 둘 다 처리 | ✅ |

## 6. 타이밍·원자성

- Valve `put_atomic` — `.tmp` 업로드 → `copy_object` → `delete tmp`. 반쪽 쓰인 파일을 flow 가 읽을 가능성 **0**.
- flow 스캐너는 glob 시점 기준. Valve 가 `date=YYYY-MM-DD` 폴더 자체를 반쯤 만들고 있는 동안 flow 가 읽으면 빈 파일 세트. 이게 문제면 Valve 가 `_SUCCESS` 마커 쓰고 flow 쪽에서 체크하도록 보강 필요. **현재는 둘 다 마커 없음 — 정합성 보강 여지**.

## 7. 에이전트 연동까지 확장

[docs/agent_design.md](Valve/docs/agent_design.md) 의 `/api/agent/diagnose` → orchestrator 가 받음 → 동일 orchestrator 가 flow 의 `/api/admin/my-notifications` 에도 푸시 → **사용자 한 유리창에서 "flow 대시보드 이상 ↔ Valve 추출 실패" 원인 체인 확인 가능**.

연결 포인트:
- Valve 의 `ops.emit_failure_webhook` URL 을 오케스트레이터 엔드포인트로 설정.
- 오케스트레이터가 Valve ↔ flow 이벤트를 상관시켜 flow 에 통보.
- 반대로 flow 가 "최근 데이터 없다" 알림을 내면 오케스트레이터가 Valve `diagnose` 쳐서 원인 확인.

## 8. 종합 점수 — 연결성 9/10

| 항목 | 평가 |
|:-:|---|
| 포맷·파티셔닝 | ✅ 완전 일치 |
| 스키마 | ✅ 어댑터가 이미 소화 |
| 폴더 규약 | ⚠ prefix 1줄 정렬 필요 (Option A 또는 B) |
| 원자성 | ✅ Valve 측 안전 |
| 실시간성 | ✅ SSE 로 진행상황 가시 |
| 에이전트 | ✅ orchestrator 를 가운데 놓으면 자연스러운 2-방향 연동 |
| 관측성 | ✅ /metrics + /jobs/history 로 진단 가능 |
| 배포 유연성 | ✅ S3 / fake_local / mock 3가지 시나리오 모두 지원 |

**감점 1점은** "_SUCCESS 마커 부재로 인한 이론적 race 가능성". 실무에선 `date=오늘` 파티션을 읽을 때 flow 가 이미 관대하게 fallback 하므로 체감 영향 작음. 마커 추가는 v0.2 에 1시간 작업.

## 9. 즉시 해볼 수 있는 연동 검증

```bash
# 1) Valve 를 fake_local 모드로 flow 의 db_root 에 직접 쓰게
#    (settings.json 편집)
"fake_local_path": "/config/work/sharedworkspace/DB",
"bucket": "db",
"prefix": ""

# 2) executor 의 s3_key 를 flow 규약에 맞춤 (1줄)
#    executor.py:196
s3_key = f"1.RAWDATA_DB_{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"

# 3) Valve UI 에서 제품 enqueue
POST /api/jobs/enqueue-product {"product": "PRODA"}

# 4) flow UI 에서 바로 SplitTable/Dashboard 열면 PRODA 데이터가 보임
```
