# Valve × Orchestrator Agent 연동 설계

## 1. 설계 원칙

**Robust > Smart.** 사내 에이전트는 오픈소스 수준(Llama-3-8B / Qwen-2-7B 급) 을 전제한다. 따라서:

1. **단일 툴 호출 = 단일 검증 가능한 액션**. 복합 리즈닝을 에이전트에 맡기지 않는다.
2. **모든 응답은 ID 기반 매칭**. 텍스트 매칭은 에이전트 실수(오탈자·번역) 로 깨진다. `chunk_id / plan_id / product / source / date` 4튜플만 사용.
3. **Idempotent**. 같은 툴을 같은 인자로 두 번 호출해도 같은 효과. retry 안전.
4. **White-list of actions**. 에이전트는 정의된 액션 외에는 호출 불가. free-form SQL/파일 쓰기 일체 금지.
5. **Dry-run default**. `/api/agent/apply-fix` 는 기본적으로 `dry_run: true` → 에이전트가 의도한 변경을 JSON 으로 미리 검증한 뒤, 사람/상위 오케스트레이터가 `dry_run: false` 로 확정.
6. **모든 호출 감사 로그**. `logs/agent_audit.jsonl` 에 호출자·인자·결과를 append-only 로 남김.
7. **Cooldown & rate limit**. 에이전트 루프가 폭주할 때 자동 차단. `apply-fix` 는 분당 30회, 동일 chunk 에 대한 재시도는 60초 cooldown.

## 2. 에이전트 역할 정의 (3-tier)

### T1 — Watcher (모니터 에이전트)
- **Scope**: `GET /api/agent/diagnose` 를 주기(1~5분) 호출 → 이상 목록만 리턴받음.
- **Output**: 구조화된 이상 JSON. 자체 수정 액션 금지.
- **Robustness**: 서버가 이상 사유·카테고리를 직접 계산해서 반환하므로 에이전트는 "읽고 보고" 만 함.
- **실패 모드**: 에이전트가 응답을 못 읽어도 서버 쪽은 영향 없음 (멱등 GET).

### T2 — Suggester (진단·제안 에이전트)
- **Scope**: `POST /api/agent/suggest-fix` — T1 이 발견한 이상 1건을 JSON 으로 주면, 서버가 룰 기반으로 후보 fix 를 돌려줌.
- **Output**: `[{action, args, confidence, rationale}]` 배열. 에이전트는 자연어로 요약만 하고 action/args 는 그대로 사용.
- **Robustness**: 에이전트가 action 을 "창작" 하지 않도록 가능한 action 은 서버 응답 안의 것들로 제한. 에이전트는 후보 중 하나를 고르기만 함.
- **선택 기준**: `confidence` 높은 순 + 과거 성공률 (`/api/agent/history` 로 반영).

### T3 — Executor (실행 에이전트 — 선택)
- **Scope**: `POST /api/agent/apply-fix` — T2 가 고른 action 을 `dry_run` 으로 먼저 검증 → 결과 OK 시 `dry_run=false` 로 재호출.
- **Output**: 실행 결과. 실패 시 에러 타입 명확.
- **Robustness**: 모든 action 은 서버 쪽 화이트리스트. action 이름/인자 스펙은 OpenAPI 에 명시.
- **안전장치**:
  - dry_run 단계에서 검증 실패하면 real 호출 차단.
  - 같은 chunk 에 대한 apply 는 60초 cooldown.
  - 연속 실패 3회 시 해당 에이전트는 해당 chunk 에 대해 차단 (관리자 수동 해제).

## 3. 구체 액션 카탈로그

각 액션은 다음 스펙으로 고정:

| action | 무엇을 하는가 | 인자 | 가역성 | 안전등급 |
|---|---|---|---|---|
| `retry_chunk` | 실패한 chunk 를 재실행 | `{chunk_id}` | ✅ 재실행은 idempotent (staging overwrite) | LOW |
| `retry_partition` | (product,source,date) 전체 재실행 | `{product, source, date}` | ✅ | LOW |
| `reshard_source` | 소스의 `shard_hierarchy` 에 컬럼 1개 추가 (2단→3단) | `{product, source, add_shard_key}` | ⚠ products.yaml 변경 — 되돌리기 필요 | MEDIUM |
| `toggle_probe_skip` | probe 상습 실패 소스의 `probe_skip` 토글 | `{product, source, value: bool}` | ✅ | LOW |
| `invalidate_probe_cache` | probe 캐시 무효화 | `{product?, source?}` | ✅ | LOW |
| `lower_backfill_override` | backfill_days_override 를 줄임 (초기 시딩 종료) | `{product, new_days}` | ⚠ | MEDIUM |
| `enqueue_product_seed` | 제품 전체 초기 시딩 (수동 600일) | `{product, days?}` | ⚠ 대량 API 호출 — 명시 확인 필요 | HIGH |
| `adjust_chunk_rows` | `target_chunk_rows` 조정 | `{product, source, new_value}` | ✅ | MEDIUM |

**HIGH 등급 액션은 에이전트 단독 실행 금지** — 반드시 사람/상위 오케스트레이터의 명시 승인 후에만 `dry_run=false` 허용.

## 4. 엔드포인트 계약

### `GET /api/agent/diagnose`

**응답**:
```json
{
  "generated_at": 1776991200,
  "anomalies": [
    {
      "id": "anomaly-001",
      "kind": "chunk_failed",
      "severity": "high",
      "chunk_id": "PRODA-FAB-2026-04-24-00",
      "product": "PRODA", "source": "FAB", "date": "2026-04-24",
      "error_type": "HY000Error",
      "error": "[HY000] ODBC driver error ...",
      "retry_count": 2,
      "age_sec": 300,
      "tags": ["retryable", "recent"]
    },
    {
      "id": "anomaly-002",
      "kind": "probe_error",
      "severity": "medium",
      "product": "PRODB", "source": "INLINE",
      "error": "TimeoutError after 290s",
      "tags": ["probe", "considered_probe_skip"]
    },
    {
      "id": "anomaly-003",
      "kind": "stuck_in_progress",
      "severity": "high",
      "chunk_id": "...",
      "age_sec": 3600,
      "tags": ["stuck", "force_requery"]
    }
  ]
}
```

**Kind 분류** (서버가 판정 — 에이전트 판정 금지):
- `chunk_failed` — chunk status in (failed, timeout_reshard, upload_failed)
- `probe_error` — plan.probe_meta.error 존재
- `partition_partial` — partition status == partial_failed
- `stuck_in_progress` — chunk in_progress + started_at > 30분 전
- `completeness_miss` — actual_rows 가 expected 대비 tolerance 초과

### `POST /api/agent/suggest-fix`

**요청**: `{anomaly_id}` 또는 위 anomaly 객체 그대로

**응답**:
```json
{
  "anomaly_id": "anomaly-001",
  "suggestions": [
    {"action": "retry_chunk", "args": {"chunk_id": "..."},
     "confidence": 0.82, "rationale": "HY000 은 retryable, 2회 시도함"},
    {"action": "reshard_source", "args": {"product": "...", "source": "...", "add_shard_key": "lot_id"},
     "confidence": 0.50, "rationale": "4시간 이상 실행 시 2단 shard 로 분할 효과"}
  ]
}
```

서버는 anomaly.kind × severity × 과거 성공률 을 인덱스로 하드코딩된 규칙표를 순회해 후보를 만든다. 에이전트는 LLM 창의성 배제.

### `POST /api/agent/apply-fix`

**요청**:
```json
{"action": "retry_chunk", "args": {"chunk_id": "..."}, "dry_run": true}
```

**응답 (dry_run=true)**: `{"ok": true, "plan": "...what would happen..."}` 또는 `{"ok": false, "error": "..."}`

**응답 (dry_run=false)**: 실제 실행 결과.

## 5. 오케스트레이터 연동 흐름

```
[Orchestrator]
     │
     ├── T1: GET /api/agent/diagnose          (1~5분 주기)
     │        ▼
     │    anomalies: [...]
     │        ▼
     ├── 우선순위 sort (severity desc, age_sec asc)
     │        ▼
     │    for each anomaly (max N per cycle):
     │
     ├── T2: POST /api/agent/suggest-fix
     │        ▼
     │    suggestions: [...]
     │        ▼
     │    선택: confidence >= 0.7 && safety != HIGH 만 자동진행
     │        ▼
     ├── T3 (dry): POST /api/agent/apply-fix {dry_run: true}
     │        ▼
     │    plan 검증 OK?
     │        ▼
     ├── T3 (real): POST /api/agent/apply-fix {dry_run: false}
     │        ▼
     │    결과 기록 → diagnose 재호출로 이상 소거 확인
     │
     └── HIGH 등급은 사람에게 pending queue
```

### 에러 핸들링 원칙

- **에이전트 응답 파싱 실패** → 해당 사이클 스킵, 다음 사이클 재시도. 1시간 내 연속 실패 5회면 해당 에이전트 suspend.
- **action 실행 실패** → diagnose 에서 새 anomaly 로 나타날 것. 루프가 자연 종료 (cooldown 덕분).
- **cooldown/rate-limit hit** → 429 반환, 에이전트는 즉시 재시도하지 말고 다음 주기로.

## 6. 감사 로그 (audit)

`logs/agent_audit.jsonl` — 모든 `/api/agent/*` 호출을 append-only 기록:

```json
{"ts": 1776991200, "endpoint": "/api/agent/apply-fix",
 "caller": "watcher-01", "args": {...}, "result": {...},
 "dry_run": false, "took_ms": 120}
```

조회: `GET /api/agent/audit?limit=...` — 관리자/오케스트레이터에서 리플레이 가능.

## 7. 인증 (이월 → v0.2)

현재 open. v0.2 에서:
- `X-Agent-Key` 헤더 + settings.json 에 허용 키 목록.
- 키별 rate-limit (apply-fix 는 키당 분당 30회).
- 키별 액션 화이트리스트 (LOW only agent vs MEDIUM까지 agent).

## 8. 로컬 테스트 가이드

```bash
# 1) 서버 기동 (mock mode)
python -m uvicorn app:app --port 8090

# 2) 현재 이상 목록
curl http://127.0.0.1:8090/api/agent/diagnose | jq

# 3) 제안 받기
curl -X POST http://127.0.0.1:8090/api/agent/suggest-fix \
  -H 'Content-Type: application/json' \
  -d '{"anomaly_id": "anomaly-001"}'

# 4) dry-run 적용
curl -X POST http://127.0.0.1:8090/api/agent/apply-fix \
  -H 'Content-Type: application/json' \
  -d '{"action":"retry_chunk","args":{"chunk_id":"..."},"dry_run":true}'

# 5) 실적용
curl -X POST http://127.0.0.1:8090/api/agent/apply-fix \
  -H 'Content-Type: application/json' \
  -d '{"action":"retry_chunk","args":{"chunk_id":"..."},"dry_run":false}'
```

## 9. 실패에 대한 안전 예시

**시나리오**: 에이전트가 `apply-fix` 에 `action="drop_products_yaml"` 같은 존재하지 않는 액션을 보냄.

- 서버 화이트리스트에 없으므로 **400 Bad Request** + `{"error": "unknown action", "allowed": [...]}`.
- 감사 로그에 기록.
- 에이전트는 후속 호출에서 `allowed` 리스트를 참고해 재시도.

**시나리오**: 에이전트가 `retry_chunk` 를 실패한 chunk_id 에 1분 내 5회 호출.

- 서버 cooldown (60초) 로 두 번째 호출부터 **429 Too Many Requests** + `{"retry_after": 45}`.
- 에이전트 루프 폭주 차단.

**시나리오**: 에이전트가 `reshard_source` 로 존재하지 않는 shard key 를 추가 시도.

- dry_run 단계에서 서버가 source_types 레지스트리에 있는 컬럼 풀과 비교 → unknown column 이면 dry_run=false 호출 자체가 거부됨.

---

이 설계는 "에이전트가 LLM 으로 판단" 하는 부분을 최소화하고, "서버가 규칙으로 판단, 에이전트는 읽고 고르고 호출만" 으로 압축해 오픈소스 모델 수준에서도 안정 동작하도록 한다.
