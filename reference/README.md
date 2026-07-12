# reference — 레거시 참조용 (배포 제외)

Valve 통합 이전에 단독 스크립트로 쓰던 원본들. **어디서도 import 되지 않으며**
setup 패키징에도 포함되지 않는다. 현재 로직과 비교/검증할 때만 참고.

| 파일 | 원래 역할 | 현재 대체 위치 |
|---|---|---|
| `Ref_raw_query.py` | raw 쿼리 → 1.RAWDATA_DB | `backend/core/feature_pipeline.py` · `run_raw_query()` |
| `Ref_event.py` | FAB raw → event 매칭 → 2.EVENT_DB | 〃 `run_event()` |
| `Ref_feature.py` | 규칙 기반 feature 생성 → 3.FEATURE_STORE | 〃 `run_feature()` + `config/feature_funcs.py` |
| `Ref_ppid_feature.py` | ppid→knob feature | 〃 `run_feature()` knob 카테고리 + `knob_map()` |
| `wide_form.py` | feature 병합 ML_TABLE → 4.WIDE_FORM | 〃 `run_wide()` |
| `send_form.py` | prefix 그룹 분리 저장 → 5.SEND_FORM | 〃 `run_send_form()` |
| `file_setting_col.txt` | 초기 컬럼 설정 메모 | `config/pipeline.yaml` sources |

새 기능은 반드시 `backend/core/feature_pipeline.py` 쪽에 구현할 것.
이 폴더의 코드는 수정하지 않는다 (원본 보존 목적).
