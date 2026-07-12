# Valve 커스텀 feature 함수 — 이 파일에 함수를 추가하면 재시작 없이 즉시 사용된다.
# (run_feature 실행 시점마다 fresh 로드 · 오류는 알람 탭 skipped 사유로 노출)
#
# ① 값 생성 함수:  def <이름>(): return <polars 표현식>
#    → config/feature_rules/fab.csv 의 feature_name 으로 사용.
#      내장(eqp_id/chamber_id/unit_id/part_id/reticle_id/ppid/tkout_time/
#      tkout_status/sleuth_order/eqpall/ecuall/reticleall)과 같은 이름을
#      정의하면 이 파일이 우선한다.
#    event 컬럼을 pl.col 로 참조: root_lot_id, wafer_id, part_id, tkout_time,
#    step_id, step_desc, ppid, reticle_id, eqp_id, eqp_model, chamber_id, unit_id …
#
# ② 집계 함수:    def agg_<이름>(): return <pl.col("val") 기반 표현식>
#    → fab/mask/knob_ppid/inline/vm csv 의 agg 컬럼에서 <이름> 으로 사용.
#      tkout_time(수치는 time) 정렬 후 wafer 단위 그룹 안에서 평가된다.
#      내장 agg: first / last / last_valid / concat / valid_eqp / agg(유니크 join)
#      knob 특이 케이스(여러 ppid 중 last 가 아닌 선택)에 특히 유용.
#
# 헬퍼: pl(polars) · clean_str(col) — 문자열 컬럼 정리(strip, 빈값→null)
#
# 아래는 Ref_feature.py 의 실제 함수를 옮긴 예시.

# ── ecuall — eqp_chamber_unit 결합, '-'/빈값 제외 (Ref build_ecu_all — 내장과 동일) ──
def ecuall():
    def dash(col):
        c = clean_str(col)
        return pl.when((c == "-") | c.is_null()).then(None).otherwise(c)
    return pl.concat_str([dash("eqp_id"), dash("chamber_id"), dash("unit_id")],
                         separator="_", ignore_nulls=True)


# ── valid_eqp — '_뒤에 숫자' 있는 유효 장비값만 남기고 첫 값 (Ref aggregate_feature 동일) ──
#    fab.csv 사용 예: GATE_ETCH,ecuall,valid_eqp   (내장 valid_eqp 와 동일 동작)
def agg_valid_eqp():
    v = pl.col("val").cast(pl.Utf8).str.strip_chars()
    return v.filter(v.str.contains(r"_[A-Za-z0-9]*[0-9]")).first()
