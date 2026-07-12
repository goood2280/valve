"""
Valve · feature_pipeline
------------------------
Ref_raw_query / Ref_event / Ref_feature 3단계 파이프라인의 Valve 통합판.

  1) raw   : vehicle 설정(QueryTimeSpan/SplitTimeSpan)대로 split 을 나눠
             FAB · INLINE · VM · ET 를 쿼리 → db/1.RAWDATA_DB/{SOURCE}/{vehicle}/date=…
             · ET 는 auto report 와 동일한 reformatter 인식 —
               config/reformatter/{vehicle}_reformatter.csv 의 REAL ITEMID 만 대상
  2) event : FAB raw 를 vehicle_matching(step_id↔step_desc) inner join +
             root_lot prefix 필터 → db/2.EVENT_DB/{vehicle}/date=…
  3) feature: 카테고리별 규칙 CSV (fab / knob_ppid / mask / inline / vm) 에 따라
             FAB_… KNOB_… MASK_… INLINE_… VM_… feature parquet 생성
             → db/3.FEATURE_STORE/{vehicle}/
             · 값 생성/집계 함수는 config/feature_funcs.py 로 관리자 확장 가능
               (def <이름> → fab.csv feature_name · def agg_<이름> → agg 컬럼)
             · knob 은 기본 last 집계 — knob_ppid.csv 의 agg 컬럼으로 step 별 조정
  4) wide  : vehicle 의 feature 전부를 KEY(root_lot·wafer) left join 으로 병합한
             ML_TABLE (PRODUCT 컬럼 포함) → db/4.WIDE_FORM/ML_TABLE_{vehicle}.parquet
  5) send  : 전 vehicle ML_TABLE 을 합쳐 prefix 그룹별로 분리 저장
             (0.KNOB / 1.FAB(+MASK) / 2.VM / 3.INLINE) → db/5.SEND_FORM/

부가 리포트:
  · unmatched scan : FAB raw 의 step_id 중 vehicle_matching 에 없는 step 을 제품별로 노출.
                     pipeline.yaml 의 unmatched_scan.exclude (eqp_id/eqp_model fnmatch 패턴)
                     에 걸리는 조합은 excluded 목록으로 분리 (사유 표시).
  · knob miss      : knob_ppid 설정에 step 은 있으나 ppid 가 매핑에 없어 knob 화되지 못하고
                     RO(raw ppid) 로 남은 경우 — vehicle / split / step / ppid / lot·wafer 단위 리포트.

mock 모드에서는 결정적(seed) 합성 데이터를 생성해 전체 흐름을 재현한다.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import random
import re
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

KEY_COLS = ["root_lot_id", "wafer_id"]

# 추출 소스 기본값 — config/pipeline.yaml 의 sources 로 override (테이블명/컬럼 조절)
DEFAULT_SOURCES = {
    "FAB": {
        "table": "RAW_FAB_DATA",
        "columns": ["root_lot_id", "wafer_id", "part_id", "tkout_time", "step_id",
                    "step_desc", "ppid", "reticle_id", "eqp_id", "eqp_model",
                    "chamber_id", "unit_id", "sleuth_order"],
    },
    "INLINE": {
        "table": "RAW_INLINE_DATA",
        "columns": ["root_lot_id", "wafer_id", "item_id", "value", "measure_pos", "time"],
    },
    "VM": {
        "table": "RAW_VM_DATA",
        "columns": ["root_lot_id", "wafer_id", "sensor_id", "eqp_id", "step_id",
                    "predicted_value", "actual_value", "residual", "time"],
    },
}

EVENT_KEEP_COLS = [
    "root_lot_id", "wafer_id", "part_id", "tkout_time",
    "step_id", "step_desc", "ppid", "reticle_id",
    "eqp_id", "eqp_model", "chamber_id", "unit_id", "sleuth_order", "split",
]

WIDE_KEY = ["PRODUCT", "ROOT_LOT_ID", "WAFER_ID"]

# send form 그룹 — prefix 컬럼을 그룹 디렉토리로 분리 저장. MASK_ 는 FAB 그룹에 포함.
SEND_GROUPS = {
    "0.KNOB": ["KNOB_"],
    "1.FAB": ["FAB_", "MASK_"],
    "2.VM": ["VM_"],
    "3.INLINE": ["INLINE_"],
}


def first_number_after(prefix: str, col: str) -> float:
    """컬럼 정렬키 — prefix 뒤 첫 숫자(공정 순서). 숫자 없으면 맨 뒤."""
    part = col.split(prefix, 1)[-1]
    m = re.search(r"\d+(?:\.\d+)?", part)
    return float(m.group()) if m else float("inf")


# ─────────────────────────────────────────────
# Ref_raw_query.get_split_date_ranges 그대로
# ─────────────────────────────────────────────
def get_split_date_ranges(query_span_days: int, split_span_days: int, today: date | None = None):
    today = today or datetime.today().date()
    start_base = today - timedelta(days=query_span_days)
    ranges = []
    current_start = start_base
    while current_start < today:
        current_end = current_start + timedelta(days=split_span_days)
        if current_end > today:
            current_end = today
        ranges.append((current_start, current_end))
        current_start += timedelta(days=split_span_days)
    ranges.append((today, today))
    return ranges


def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:\*\?"<>\|]+', "_", name)
    name = re.sub(r"\s+", "_", name)
    return name.rstrip(".")


# ─────────────────────────────────────────────
# Ref_feature 의 값 생성/집계 규칙 (polars)
# ─────────────────────────────────────────────
def _clean_str(col):
    return pl.col(col).cast(pl.Utf8).str.strip_chars().replace("", None)


def build_eqp_all():
    tool = pl.concat_str([_clean_str("eqp_id"), _clean_str("chamber_id"), _clean_str("unit_id")],
                         separator="_", ignore_nulls=True)
    return pl.concat_str([_clean_str("tkout_time"), tool], separator="|", ignore_nulls=True)


def build_ecu_all():
    def dash(col):
        c = _clean_str(col)
        return pl.when((c == "-") | c.is_null()).then(None).otherwise(c)
    return pl.concat_str([dash("eqp_id"), dash("chamber_id"), dash("unit_id")],
                         separator="_", ignore_nulls=True)


def build_part_reticle():
    return pl.concat_str([_clean_str("part_id").str.slice(0, 10), _clean_str("reticle_id")],
                         separator="|", ignore_nulls=True)


FEATURE_RULES = {
    "eqp_id": lambda: pl.col("eqp_id").cast(pl.Utf8),
    "chamber_id": lambda: pl.col("chamber_id").cast(pl.Utf8),
    "unit_id": lambda: pl.col("unit_id").cast(pl.Utf8),
    "part_id": lambda: pl.col("part_id").cast(pl.Utf8).str.slice(0, 10),
    "reticle_id": lambda: pl.col("reticle_id").cast(pl.Utf8),
    "ppid": lambda: pl.col("ppid").cast(pl.Utf8),
    "tkout_time": lambda: pl.col("tkout_time").cast(pl.Utf8),
    "tkout_status": lambda: (
        pl.when(pl.col("tkout_time").is_not_null())
          .then(pl.lit("PASSED")).otherwise(pl.lit("NOT_PASSED"))
    ),
    "sleuth_order": lambda: pl.col("sleuth_order").cast(pl.Utf8),
    "eqpall": build_eqp_all,
    "ecuall": build_ecu_all,
    "reticleall": build_part_reticle,
}


def aggregate_feature(df: pl.DataFrame, feature_col: str, agg_type: str,
                      custom_aggs: dict | None = None) -> pl.DataFrame:
    # 커스텀 집계 (config/feature_funcs.py 의 agg_<이름>) — pl.col("val") 기반 표현식
    if custom_aggs and agg_type in custom_aggs:
        return (df.sort("tkout_time").group_by(KEY_COLS)
                  .agg(custom_aggs[agg_type]().alias(feature_col)))
    if agg_type == "first":
        return df.sort("tkout_time").group_by(KEY_COLS).agg(pl.col("val").first().alias(feature_col))
    if agg_type == "last":
        return df.sort("tkout_time").group_by(KEY_COLS).agg(pl.col("val").last().alias(feature_col))
    if agg_type == "concat":
        return (df.sort("tkout_time").group_by(KEY_COLS)
                  .agg(pl.col("val").cast(pl.Utf8).str.strip_chars().str.join("_").alias(feature_col)))
    if agg_type == "last_valid":
        c = pl.col("val").cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        return (df.sort("tkout_time")
                  .with_columns(pl.when(c.is_null() | (c == "") | (c == "-") | c.str.contains("SKIP"))
                                  .then(None).otherwise(c).alias("val_clean"))
                  .group_by(KEY_COLS)
                  .agg(pl.col("val_clean").drop_nulls().last().alias(feature_col)))
    if agg_type == "valid_eqp":
        # Ref_feature 동일 — '_뒤에 숫자' 가 있는 유효 장비값만 남기고 첫 값
        # (ecuall/eqp_id 처럼 EQP_01 형태만 유효로 취급, 그 외는 제외)
        return (df.with_columns(pl.col("val").cast(pl.Utf8).str.strip_chars().alias("val_str"))
                  .filter(pl.col("val_str").str.contains(r"_[A-Za-z0-9]*[0-9]"))
                  .sort("tkout_time").group_by(KEY_COLS)
                  .agg(pl.col("val_str").first().alias(feature_col)))
    if agg_type == "agg":
        return df.group_by(KEY_COLS).agg(pl.col("val").unique().sort().str.join("_").alias(feature_col))
    raise ValueError(f"unknown agg type: {agg_type}")


def _knob_cond_expr(op: str, value: str) -> pl.Expr:
    """knob SKIP 블록의 조건 연산자 (사내 Ref_ppid_feature.build_condition 대응).
    "v" = 해당 step 의 wafer 마지막 ppid. step 값이 없는 wafer 는 _null 을 제외한
    모든 연산에서 False — 매칭 문제로 빈 값이 조건을 통과하지 않게 보수적으로."""
    v = pl.col("v")
    if op == "eq":
        return v == value
    if op == "neq":
        return v.is_not_null() & (v != value)
    if op == "contains":
        return v.str.contains(value)
    if op == "in":
        return v.is_in(value.split("|"))
    if op == "not_in":
        return v.is_not_null() & ~v.is_in(value.split("|"))
    if op == "_null":
        return v.is_null()
    if op == "not_null":
        return v.is_not_null()
    return pl.lit(True)  # op 미지정 = 조건 없음 (Ref 동일)


NUM_AGGS = {
    "mean": lambda col, name: pl.col(col).mean().alias(name),
    "max": lambda col, name: pl.col(col).max().alias(name),
    "min": lambda col, name: pl.col(col).min().alias(name),
    "last": lambda col, name: pl.col(col).last().alias(name),
    "first": lambda col, name: pl.col(col).first().alias(name),
}


def numeric_agg_expr(agg: str, col: str, name: str, custom_aggs: dict | None = None):
    """INLINE/VM 수치 집계 표현식. 내장 NUM_AGGS 우선, 없으면 커스텀 agg_<이름>
    (값 컬럼을 val 로 alias 해 두고 호출하는 쪽에서 with_columns 처리)."""
    if agg in NUM_AGGS:
        return NUM_AGGS[agg](col, name), False
    if custom_aggs and agg in custom_aggs:
        return custom_aggs[agg]().alias(name), True   # pl.col("val") 기반 → val alias 필요
    raise ValueError(f"unknown agg type: {agg}")


# ─────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────
class FeaturePipeline:
    def __init__(self, root: Path, settings: dict):
        self.root = Path(root)
        self.settings = settings

    # ── config loaders (호출 시점마다 fresh 로드 → 웹에서 수정 즉시 반영) ──
    def global_cfg(self) -> dict:
        return yaml.safe_load((self.root / "config" / "pipeline.yaml").read_text(encoding="utf-8")) or {}

    def save_global_cfg(self, cfg: dict):
        (self.root / "config" / "pipeline.yaml").write_text(
            yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")

    def vehicles(self) -> dict:
        return yaml.safe_load((self.root / "config" / "vehicles.yaml").read_text(encoding="utf-8")) or {}

    def vehicle_cfg(self, vehicle: str) -> dict:
        cfg = self.vehicles()
        if vehicle not in cfg:
            raise ValueError(f"{vehicle} not found in vehicles.yaml")
        return cfg[vehicle]

    def step_map(self, vehicle: str) -> pl.DataFrame:
        path = self.root / self.global_cfg()["step_matching"]
        df = pl.read_csv(path).with_columns(pl.col("step_id").cast(pl.Utf8))
        return df.filter(pl.col("vehicle") == vehicle)

    def knob_map(self, vehicle: str) -> pl.DataFrame | None:
        """knob 룰 CSV → vehicle 직접 매핑 (vehicle,step_id,step_desc,ppid,knob[,agg]).

        두 형식을 지원한다 (flow 룰북 순환 — flow 가 판정 반영한 ppid_knob.csv 소비):
          · 직접 매핑(legacy): vehicle,step_id,step_desc,ppid,knob[,agg] — vehicle 필터만
          · 사내 룰 형식: feature_name,function_step|step_desc,rule_order,operator,
            value,category[,use] — eq + R{n} 룰만 vehicle_matching(step_desc) 과 조인해
            매핑 생성. 같은 step 의 중복 ppid 는 낮은 rule 번호 우선(first-match).
            RO 는 fallback — 미매핑 ppid 는 기존대로 raw 유지 + knob_miss 리포트.
        """
        rules = self.rules_csv("knob")
        if rules is None:
            return None
        cols = set(rules.columns)
        if {"vehicle", "step_id", "ppid", "knob"} <= cols:
            return rules.filter(pl.col("vehicle") == vehicle)

        empty = pl.DataFrame(schema={"vehicle": pl.Utf8, "step_id": pl.Utf8,
                                     "step_desc": pl.Utf8, "ppid": pl.Utf8,
                                     "knob": pl.Utf8})
        fs_col = "function_step" if "function_step" in cols else "step_desc"
        if not ({fs_col, "rule_order", "value", "category"} <= cols):
            return empty
        df = rules
        if "feature_name" in cols:
            # 같은 feature_name+rule_order 의 복수 행 = AND 조건 블록 (사내 원본의
            # "다음 main step 통과 → SKIP" 판정 등) — 행 단위 eq 매핑이 아니므로
            # 여기서 제외하고 knob_skip_blocks() 가 소비 (섞이면 조건 step 에 엉뚱한
            # ppid→category 매핑이 생긴다)
            df = df.filter(pl.len().over(["feature_name", "rule_order"]) == 1)
        if "use" in cols:
            df = df.filter(pl.col("use").fill_null("").str.to_uppercase()
                           .str.strip_chars().is_in(["", "Y", "1", "TRUE"]))
        if "operator" in cols:
            df = df.filter(pl.col("operator").fill_null("").str.strip_chars() == "eq")
        df = df.filter(
            (pl.col("rule_order").fill_null("").str.strip_chars().str.to_uppercase() != "RO")
            & (pl.col("value").fill_null("").str.strip_chars() != "")
            & (pl.col("category").fill_null("").str.strip_chars() != ""))
        if df.height == 0:
            return empty
        df = df.with_columns(
            pl.col("rule_order").str.extract(r"(\d+)").cast(pl.Int64, strict=False)
              .fill_null(1_000_000).alias("_rule_num"),
            pl.col(fs_col).fill_null("").str.strip_chars().alias("_fs"))
        smap = self.step_map(vehicle).select(["step_id", "step_desc"]).unique()
        out_cols = [pl.lit(vehicle).alias("vehicle"),
                    pl.col("step_id").cast(pl.Utf8),
                    pl.col("step_desc"),
                    pl.col("value").alias("ppid"),
                    pl.col("category").alias("knob")]
        if "agg" in cols:  # step 별 집계 조정(옵션) 은 rule 형식에서도 유지
            out_cols.append(pl.col("agg"))
        return (smap.join(df, left_on="step_desc", right_on="_fs", how="inner")
                    .sort("_rule_num")
                    .unique(subset=["step_id", "value"], keep="first", maintain_order=True)
                    .select(out_cols))

    def knob_skip_blocks(self) -> list[dict]:
        """rule 형식 ppid_knob.csv 의 조건 블록 — 같은 feature_name+rule_order 의
        복수 행(또는 단일 비-eq 행)을 AND 조건으로 해석 (사내 Ref_ppid_feature 동일).
        대표 용례: "knob step _null AND 다음 main step not_null → SKIP".

        반환 블록: {feature, rule_order, category, conds: [{step, op, value}],
                    target_steps: 해당 feature 의 값 행(eq/RO)들의 function_step}
        category 가 SKIP 인 블록만 skip 판정에 쓰이고, 그 외 AND 값 블록은
        미지원으로 리포트된다 (조용히 사라지지 않게).
        """
        rules = self.rules_csv("knob")
        if rules is None:
            return []
        cols = set(rules.columns)
        if {"vehicle", "step_id", "ppid", "knob"} <= cols:
            return []  # legacy 직접 매핑 형식 — 블록 개념 없음
        fs_col = "function_step" if "function_step" in cols else "step_desc"
        if not ({fs_col, "rule_order", "category"} <= cols) or "feature_name" not in cols:
            return []
        df = rules
        if "use" in cols:
            df = df.filter(pl.col("use").fill_null("").str.to_uppercase()
                           .str.strip_chars().is_in(["", "Y", "1", "TRUE"]))
        def _c(name):
            return (pl.col(name).cast(pl.Utf8).fill_null("").str.strip_chars()
                    if name in cols else pl.lit(""))
        df = df.with_columns(
            _c("feature_name").alias("_feat"),
            _c(fs_col).alias("_fs"),
            _c("rule_order").str.to_uppercase().alias("_ro"),
            _c("operator").alias("_op"),
            _c("value").alias("_val"),
            _c("category").alias("_cat"),
        )
        # feature 별 값 step — eq 매핑 행(R{n}) + RO 행의 function_step
        val_steps: dict[str, set] = {}
        for r in df.iter_rows(named=True):
            is_ro = r["_ro"] == "RO"
            is_eq_val = bool(re.fullmatch(r"R\d+", r["_ro"])) and r["_op"] == "eq" \
                        and r["_cat"] and r["_cat"].upper() != "SKIP"
            if (is_ro or is_eq_val) and r["_fs"]:
                val_steps.setdefault(r["_feat"], set()).add(r["_fs"])
        blocks = []
        for (feat, ro), grp in df.filter(pl.col("_ro") != "RO") \
                                 .group_by(["_feat", "_ro"], maintain_order=True):
            ops = grp["_op"].to_list()
            if grp.height == 1 and ops[0] == "eq":
                continue  # 단일 eq 행 = knob_map 의 per-step 매핑 경로가 처리
            cats = [c for c in grp["_cat"].to_list() if c]
            blocks.append({
                "feature": feat, "rule_order": ro,
                "category": cats[0] if cats else "",
                "conds": [{"step": r["_fs"], "op": r["_op"], "value": r["_val"]}
                          for r in grp.iter_rows(named=True)],
                "target_steps": sorted(val_steps.get(feat, set())),
            })
        return blocks

    def sources_cfg(self) -> dict:
        """소스별 {table, columns}. 기본 3종(FAB/INLINE/VM) + pipeline.yaml sources 에
        추가한 신규 소스(ET·QTIME 등)도 포함 → 코드 수정 없이 소스 확장."""
        cfg = self.global_cfg().get("sources") or {}
        out = {}
        names = list(DEFAULT_SOURCES) + [n for n in cfg if n not in DEFAULT_SOURCES]
        for name in names:
            dflt = DEFAULT_SOURCES.get(name, {})
            user = cfg.get(name) or {}
            cols = user.get("columns") or dflt.get("columns")
            if not cols:
                continue  # 컬럼 정의 없는 소스는 skip
            out[name] = {
                "table": user.get("table") or dflt.get("table") or f"RAW_{name}_DATA",
                "columns": [str(c) for c in cols],
            }
        return out

    def save_sources_cfg(self, sources: dict):
        cfg = self.global_cfg()
        cfg["sources"] = sources
        self.save_global_cfg(cfg)

    # ── reformatter (auto report 와 동일 형식) — ET 등 item 소스의 쿼리 대상 정의 ──
    def reformatter_path(self, vehicle: str, source: str) -> Path | None:
        """소스에 reformatter 디렉토리가 설정된 경우 vehicle 별 CSV 경로.
        pipeline.yaml: sources: { ET: { reformatter: config/reformatter } }
        → config/reformatter/{vehicle}_reformatter.csv (미설정 소스는 None)."""
        rel = ((self.global_cfg().get("sources") or {}).get(source) or {}).get("reformatter")
        return (self.root / rel / f"{vehicle}_reformatter.csv") if rel else None

    def reformatter_items(self, vehicle: str, source: str) -> list[str] | None:
        """auto report reformatter 인식 — CATEGORY=REAL 행의 ITEMID 가 raw 쿼리/저장
        대상 항목 (ADDP 는 파생 계산식이라 raw 에 없음). 호출 시점마다 fresh 로드.
        반환: None = reformatter 미사용 소스(항목 필터 없음)
              []   = 설정됐으나 파일 없음/형식 불일치/REAL 항목 없음 → 해당 vehicle 스킵"""
        path = self.reformatter_path(vehicle, source)
        if path is None:
            return None
        if not path.exists():
            return []
        try:
            df = pl.read_csv(path, infer_schema_length=0)
        except Exception:
            return []
        if not {"CATEGORY", "ITEMID"} <= set(df.columns):
            return []
        items = (df.filter(pl.col("CATEGORY").fill_null("").str.strip_chars()
                             .str.to_uppercase() == "REAL")
                   .get_column("ITEMID").drop_nulls().to_list())
        return list(dict.fromkeys(i.strip() for i in items if i and i.strip()))

    def feature_funcs(self) -> tuple[dict, dict, list]:
        """config/feature_funcs.py 의 관리자 커스텀 함수 로드 — 호출 시점마다 fresh
        (파일 수정 즉시 반영, 재시작 불필요).
          · def <이름>()      → 값 생성 함수. fab.csv 의 feature_name 으로 사용
                                (FEATURE_RULES 와 병합, 같은 이름이면 커스텀이 우선)
          · def agg_<이름>()  → 집계 함수. 규칙 csv 의 agg 컬럼에서 <이름> 으로 사용
                                (pl.col('val') 기반 표현식 — tkout/time 정렬 후 wafer 단위)
        반환 (value_funcs, agg_funcs, errors). 파일 오류는 feature skip 사유로 노출."""
        fp = self.root / "config" / "feature_funcs.py"
        if not fp.exists():
            return {}, {}, []
        ns: dict = {"pl": pl, "clean_str": _clean_str}
        try:
            exec(compile(fp.read_text(encoding="utf-8"), str(fp), "exec"), ns)
        except Exception as e:
            return {}, {}, [{"feature": "feature_funcs.py", "reason": f"로드 실패: {e}"}]
        vals, aggs = {}, {}
        for name, fn in ns.items():
            code = getattr(fn, "__code__", None)
            if name.startswith("_") or code is None or code.co_filename != str(fp):
                continue  # import 된 객체/헬퍼는 제외 — 이 파일에 정의된 함수만 등록
            if name.startswith("agg_"):
                aggs[name[len("agg_"):]] = fn
            else:
                vals[name] = fn
        return vals, aggs, []

    def rules_csv(self, category: str) -> pl.DataFrame | None:
        rel = (self.global_cfg().get("feature_rules") or {}).get(category)
        if not rel:
            return None
        path = self.root / rel
        if not path.exists():
            return None
        return pl.read_csv(path, infer_schema_length=0)  # 전부 문자열로

    # ── db 경로 ──
    def db_root(self) -> Path:
        return self.root / self.global_cfg().get("db_root", "db")

    def raw_dir(self, vehicle: str, source: str) -> Path:
        # raw 는 소스 > vehicle > date=hive 파티션 (FAB/{vehicle}/date=…).
        # event/feature 와 동일하게 vehicle 기준으로 통일.
        return self.db_root() / "1.RAWDATA_DB" / source / vehicle

    def event_dir(self, vehicle: str, source: str = "FAB") -> Path:
        return self.db_root() / "2.EVENT_DB" / vehicle / source

    # ── event 매칭 입력 (소스별) — FAB/VM 은 vehicle_matching, INLINE 은 inline matching ──
    def source_match(self, source: str) -> dict:
        """소스별 event 매칭 규칙. kind: step | item | none.
        신규 소스(ET 등)는 pipeline.yaml 에서 확장 —
          sources: { ET: { match: { kind: item, rules: et, id_col: test_item } } }
        기본값: INLINE=item(inline/item_id), 그 외=step(vehicle_matching)."""
        user = ((self.global_cfg().get("sources") or {}).get(source) or {}).get("match")
        if isinstance(user, dict) and user.get("kind"):
            return {"kind": user["kind"], "rules": user.get("rules"), "id_col": user.get("id_col")}
        if source == "INLINE":
            return {"kind": "item", "rules": "inline", "id_col": "item_id"}
        return {"kind": "step", "rules": None, "id_col": None}

    def matching_file(self, source: str) -> Path | None:
        m = self.source_match(source)
        if m["kind"] == "item" and m["rules"]:
            rel = (self.global_cfg().get("feature_rules") or {}).get(m["rules"])
            return (self.root / rel) if rel else None
        if m["kind"] == "step":
            return self.root / self.global_cfg()["step_matching"]
        return None  # none — 추가 매칭 파일 없음 (root_lot prefix 만)

    def matching_sha(self, source: str) -> str | None:
        fp = self.matching_file(source)
        if not fp or not fp.exists():
            return None
        return hashlib.sha1(fp.read_bytes()).hexdigest()[:12]

    def event_version(self, vehicle: str, source: str) -> str:
        """event 생성에 영향을 주는 설정 전체의 버전 —
        매칭 파일 내용(sha) + vehicle 의 event_lot_startwith + 소스 match 규칙.
        어느 하나라도 바뀌면 해당 소스 event DB 전체 재생성 대상(stale)."""
        payload = {
            "matching_sha": self.matching_sha(source),
            "prefix": str(self.vehicle_cfg(vehicle).get("event_lot_startwith") or ""),
            "match": self.source_match(source),
        }
        return hashlib.sha1(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]

    def feature_dir(self, vehicle: str) -> Path:
        return self.db_root() / "3.FEATURE_STORE" / vehicle

    def wide_dir(self) -> Path:
        return self.db_root() / "4.WIDE_FORM"

    def send_dir(self) -> Path:
        return self.db_root() / "5.SEND_FORM"

    def report_dir(self, vehicle: str) -> Path:
        return self.db_root() / "REPORTS" / vehicle

    # ─────────────────────────────────────────
    # 1) RAW QUERY  (mock: 결정적 합성 데이터)
    # ─────────────────────────────────────────
    def _date_ranges(self, cfg: dict):
        """raw 조회 날짜 범위. runtime.raw_days/split_days 가 있으면 그 값으로
        (기본 5일치를 1일씩), 없으면 vehicle 의 QueryTimeSpan/SplitTimeSpan."""
        rt = self.global_cfg().get("runtime") or {}
        days = int(rt.get("raw_days") or cfg["QueryTimeSpan"])
        split = int(rt.get("split_days") or cfg["SplitTimeSpan"])
        return get_split_date_ranges(days, split)

    def _raw_units(self, cfg: dict) -> list[tuple]:
        """(source, start, end, split_label) 병렬 실행 단위 목록.
        DB(source) × 날짜(1일) 로 쪼갠다 → 스케줄러가 워커에 분배."""
        sources = self.sources_cfg()
        units = []
        for start, end in self._date_ranges(cfg):
            split = f"{start}~{end}"
            for source in sources:
                units.append((source, start, end, split))
        return units

    # 소스별 mock 생성기 — 없는 소스(ET 등)는 _mock_generic 로 컬럼 기반 합성.
    def _mock_for(self, source: str):
        return {"FAB": self._mock_fab, "INLINE": self._mock_inline, "VM": self._mock_vm}.get(source)

    def _run_raw_unit(self, cfg: dict, source: str, start, end, split: str) -> int:
        """한 (source, 날짜) 파티션을 생성·저장. 반환 rows. 스레드에서 병렬 호출됨
        (서로 다른 파티션 파일만 씀 → race 없음).
        reformatter 소스(ET)는 REAL ITEMID 만 쿼리/저장 — 파일 없으면 해당 vehicle 스킵."""
        sc = self.sources_cfg()[source]
        items = self.reformatter_items(cfg["vehicle"], source)
        if items is not None and not items:
            return 0  # reformatter 설정됐으나 이 vehicle 의 파일/REAL 항목 없음
        gen = self._mock_for(source)
        df = (gen(cfg, start, end, split) if gen
              else self._mock_generic(cfg, start, end, split, source, sc["columns"], items=items))
        if items and "item_id" in df.columns:
            df = df.filter(pl.col("item_id").is_in(items))  # 실 어댑터 교체 대비 안전망
        keep = [c for c in sc["columns"] if c in df.columns] + ["split"]
        df = df.select(keep)
        out = self.raw_dir(cfg["vehicle"], source) / f"date={start}"
        out.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out / "part-000.parquet", compression="zstd", compression_level=3)
        return df.height

    def run_raw_query(self, vehicle: str) -> dict:
        """전 (source, 날짜) 유닛을 순차 실행 (병렬은 pipeline_runner 가 담당)."""
        cfg = self.vehicle_cfg(vehicle)
        sources = self.sources_cfg()
        stats = {"splits": [], "rows": {name: 0 for name in sources},
                 "tables": {name: sc["table"] for name, sc in sources.items()}}
        for name in sources:  # reformatter 소스(ET) 인식 현황 — 파일/항목 수 노출
            it = self.reformatter_items(vehicle, name)
            if it is not None:
                p = self.reformatter_path(vehicle, name)
                stats.setdefault("reformatter", {})[name] = {
                    "file": str(p.relative_to(self.root)), "found": p.exists(), "items": len(it)}
        seen = set()
        for source, start, end, split in self._raw_units(cfg):
            stats["rows"][source] += self._run_raw_unit(cfg, source, start, end, split)
            if split not in seen:
                seen.add(split)
                stats["splits"].append(split)
        return stats

    # mock 생성기 — seed(vehicle+split) 고정 → 재실행해도 동일 데이터
    def _rng(self, cfg: dict, split: str, source: str) -> random.Random:
        return random.Random(f"{cfg['vehicle']}|{split}|{source}")

    def _mock_fab(self, cfg: dict, start: date, end: date, split: str) -> pl.DataFrame:
        rng = self._rng(cfg, split, "FAB")
        vehicle = cfg["vehicle"]
        matched = self.step_map(vehicle).select(["step_id", "step_desc"]).rows()
        # 미매칭 step 풀: (step_id, step_desc, eqp_id, eqp_model)
        unmatched_pool = [
            ("MT100200", "MEASURE_CD", "MET_CD_01", "MEA-500"),   # eqp_model 제외 대상
            ("AX550000", "AUX_CLEAN", "AUX_01", "AX-9"),          # eqp_id 제외 대상
            ("XX777700", "IMP_WELL", "IMP_01", "I-2000"),         # 진짜 미매칭 → 리포트 노출
        ]
        eqp_pool = {
            "GATE_ETCH": [("ETCH_01", "E-3000"), ("ETCH_02", "E-3000")],
            "SPACER_CVD": [("CVD_02", "C-500")],
            "GATE_PHOTO": [("PHO_01", "NSR-S635")],
            "CONTACT_ETCH": [("ETCH_05", "E-3000")],
            "METAL_ETCH": [("ETCH_11", "E-5000")],
        }
        # knob 매핑이 있는 step 의 ppid 풀 — 일부러 매핑에 없는 ppid 를 섞음 (knob-miss 재현)
        knob = self.knob_map(vehicle)
        ppid_pool: dict[str, list[str]] = {}
        if knob is not None:
            for r in knob.iter_rows(named=True):
                ppid_pool.setdefault(r["step_id"], []).append(r["ppid"])
        for sid, pool in ppid_pool.items():
            pool.append(f"PP_X9_{sid[-4:]}")  # 매핑에 없는 raw ppid

        n_lots = 8
        rows = []
        seq = 0
        span_sec = max(int((datetime.combine(end, datetime.min.time())
                            - datetime.combine(start, datetime.min.time())).total_seconds()), 3600)
        route = matched + [(u[0], u[1]) for u in unmatched_pool]
        slot_sec = max(span_sec // max(len(route), 1), 2)  # step 별 시간 구간 (route 순 단조)
        for li in range(n_lots):
            # 일부 lot 은 prefix 미충족 (event 필터에서 제거되는 것 재현)
            lot = f"R{rng.randint(0, 199):03d}" if rng.random() > 0.15 else f"Q{rng.randint(0, 99):03d}"
            for w in range(1, rng.randint(3, 6)):
                for si, (sid, sdesc) in enumerate(route):
                    if rng.random() < 0.1:
                        continue
                    um = next((u for u in unmatched_pool if u[0] == sid), None)
                    if um:
                        eqp, model = um[2], um[3]
                    else:
                        eqp, model = rng.choice(eqp_pool.get(sdesc, [("EQP_00", "GEN-1")]))
                    if sid in ppid_pool:
                        # ~85% 는 매핑된 ppid, 나머지는 매핑 없는 ppid → knob-miss
                        pool = ppid_pool[sid]
                        ppid = pool[-1] if rng.random() < 0.15 else rng.choice(pool[:-1])
                    else:
                        ppid = f"PP_{sdesc[:4]}_STD"
                    seq += 1
                    # tkout_time 은 route(공정) 순서대로 단조 증가 — 실 fab 동일.
                    # knob skip 의 auto 판정(시간 상대순서 기반 뒤쪽 step 판별) 재현
                    tk = datetime.combine(start, datetime.min.time()) \
                        + timedelta(seconds=si * slot_sec + rng.randint(0, slot_sec - 1))
                    rows.append({
                        "root_lot_id": lot,
                        "wafer_id": str(w),
                        "part_id": f"{cfg['product']}-PART-{li % 3}",
                        "tkout_time": tk.strftime("%Y-%m-%d %H:%M:%S"),
                        "step_id": sid,
                        "step_desc": sdesc,
                        "ppid": ppid,
                        "reticle_id": f"RET_{rng.randint(1, 3):03d}" if "PHOTO" in sdesc else "-",
                        "eqp_id": eqp,
                        "eqp_model": model,
                        "chamber_id": rng.choice(["CH_A", "CH_B"]),
                        "unit_id": rng.choice(["U1", "-"]),
                        "sleuth_order": str(seq),
                        "split": split,
                    })
        return pl.DataFrame(rows)

    def _mock_inline(self, cfg: dict, start: date, end: date, split: str) -> pl.DataFrame:
        rng = self._rng(cfg, split, "INLINE")
        items = ["ITEM_CD_001", "ITEM_THK_002", "ITEM_OVL_003"]
        rows = []
        for _ in range(8):
            lot = f"R{rng.randint(0, 199):03d}"
            for w in range(1, 5):
                for item in items:
                    rows.append({
                        "root_lot_id": lot, "wafer_id": str(w), "item_id": item,
                        "value": round(rng.gauss(100, 8), 4),
                        "measure_pos": str(rng.randint(1, 9)),
                        "time": f"{start} 0{rng.randint(0, 9)}:00:00",
                        "split": split,
                    })
        return pl.DataFrame(rows)

    def _mock_vm(self, cfg: dict, start: date, end: date, split: str) -> pl.DataFrame:
        rng = self._rng(cfg, split, "VM")
        sensors = ["SNS_TEMP_01", "SNS_PRES_02"]
        rows = []
        for _ in range(8):
            lot = f"R{rng.randint(0, 199):03d}"
            for w in range(1, 5):
                for s in sensors:
                    pred = rng.gauss(50, 3)
                    act = pred + rng.gauss(0, 0.8)
                    # 일부는 매칭에 없는 step — event 단계에서 걸러짐
                    step = "CC942300" if rng.random() > 0.25 else "XX777700"
                    rows.append({
                        "root_lot_id": lot, "wafer_id": str(w), "sensor_id": s,
                        "eqp_id": "ETCH_01", "step_id": step,
                        "predicted_value": round(pred, 4), "actual_value": round(act, 4),
                        "residual": round(act - pred, 4),
                        "time": f"{start} 0{rng.randint(0, 9)}:00:00",
                        "split": split,
                    })
        return pl.DataFrame(rows)

    def _mock_generic(self, cfg: dict, start: date, end: date, split: str,
                      source: str, columns: list[str], items: list[str] | None = None) -> pl.DataFrame:
        """설정된 columns 만으로 합성 raw 생성 — 새 소스(ET·QTIME 등)를 코드 수정 없이 mock.
        step_id/item_id/value/time 등 흔한 컬럼은 의미있게, 나머지는 난수 문자열.
        items(reformatter REAL ITEMID)가 있으면 lot·wafer 별로 항목당 1행 —
        사내 쿼리가 item_id 목록으로 조회하는 것과 동일한 모양."""
        rng = self._rng(cfg, split, source)
        matched = self.step_map(cfg["vehicle"]).select("step_id").to_series().to_list()
        prefix = str(cfg.get("event_lot_startwith") or "R")
        rows = []
        for _ in range(8):
            lot = f"{prefix}{rng.randint(0, 199):03d}"
            for w in range(1, 5):
                for it in (items or [None]):
                    row = {"root_lot_id": lot, "wafer_id": str(w), "split": split}
                    for c in columns:
                        if c in row:
                            continue
                        if c == "step_id":
                            row[c] = rng.choice(matched) if matched else "CC000000"
                        elif c in ("item_id", "sensor_id", "test_item", "pattern_id"):
                            row[c] = it if it is not None else f"{source}_{rng.randint(1, 3):02d}"
                        elif c in ("value", "predicted_value", "actual_value", "residual", "et_value"):
                            row[c] = round(rng.gauss(100, 10), 4)
                        elif "time" in c:
                            row[c] = f"{start} 0{rng.randint(0, 9)}:00:00"
                        elif c in ("eqp_id", "chamber_id", "unit_id"):
                            row[c] = f"{c[:3].upper()}_{rng.randint(1, 4):02d}"
                        else:
                            row[c] = f"{c}_{rng.randint(0, 9)}"
                    rows.append(row)
        return pl.DataFrame(rows)

    # ─────────────────────────────────────────
    # 2) EVENT — 3소스 모두 매칭 필터.
    #    FAB/VM: vehicle_matching 의 step_id · INLINE: inline matching 의 item_id.
    #    설정 버전(event_version: 매칭 sha + lot prefix + match 규칙)이 바뀌면
    #    해당 소스 event DB 전체를 raw 재스캔으로 재생성.
    #    적용된 버전은 파티션 옆 _meta.json 에 기록 (히트맵/현황 표시용).
    # ─────────────────────────────────────────
    def run_event(self, vehicle: str) -> dict:
        cfg = self.vehicle_cfg(vehicle)
        prefix = str(cfg.get("event_lot_startwith") or "")
        step_ids = set(self.step_map(vehicle)["step_id"].to_list())

        # 구 레이아웃(vehicle 바로 아래 date=*) 잔재 제거
        legacy_root = self.db_root() / "2.EVENT_DB" / vehicle
        for d in legacy_root.glob("date=*"):
            shutil.rmtree(d, ignore_errors=True)

        results = {}
        for source in self.sources_cfg():
            match = self.source_match(source)
            item_ids = set()
            if match["kind"] == "item" and match["rules"]:
                r = self.rules_csv(match["rules"])
                if r is not None and match["id_col"] in r.columns:
                    item_ids = set(r[match["id_col"]].to_list())
            edir = self.event_dir(vehicle, source)
            ver = self.event_version(vehicle, source)
            meta_path = edir / "_meta.json"
            meta = {}
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    meta = {}
            rebuild = meta.get("ver") != ver  # 구 meta(ver 없음) 도 1회 전체 재생성
            if rebuild:
                for d in edir.glob("date=*"):
                    shutil.rmtree(d, ignore_errors=True)

            rows_in = rows_out = parts = 0
            for date_dir in sorted(self.raw_dir(vehicle, source).glob("date=*")):
                raw_path = date_dir / "part-000.parquet"
                out_path = edir / date_dir.name / "part-000.parquet"
                if not raw_path.exists() or (out_path.exists() and not rebuild):
                    continue
                raw = pl.read_parquet(raw_path)
                rows_in += raw.height
                event = raw.filter(pl.col("root_lot_id").cast(pl.Utf8).str.starts_with(prefix))
                if match["kind"] == "item" and match["id_col"] in event.columns:
                    event = event.filter(pl.col(match["id_col"]).is_in(sorted(item_ids)))
                elif match["kind"] == "step" and "step_id" in event.columns:
                    event = event.with_columns(pl.col("step_id").cast(pl.Utf8)) \
                                 .filter(pl.col("step_id").is_in(sorted(step_ids)))
                # kind == "none" → root_lot prefix 필터만 적용
                if source == "FAB":
                    keep = [c for c in EVENT_KEEP_COLS if c in event.columns]
                    event = event.select(keep).select(pl.all().cast(pl.String))
                out_path.parent.mkdir(parents=True, exist_ok=True)
                event.write_parquet(out_path)
                rows_out += event.height
                parts += 1

            edir.mkdir(parents=True, exist_ok=True)
            mf = self.matching_file(source)
            meta_path.write_text(json.dumps({
                "ver": ver, "sha": self.matching_sha(source), "ts": time.time(),
                "file": str(mf.relative_to(self.root)) if mf else None,
                "prefix": prefix, "match": self.source_match(source),
            }, ensure_ascii=False), encoding="utf-8")
            results[source] = {"raw_rows": rows_in, "event_rows": rows_out,
                               "partitions": parts, "rebuilt": rebuild}
        return results

    def _load_event(self, vehicle: str, source: str = "FAB") -> pl.DataFrame | None:
        files = sorted(self.event_dir(vehicle, source).glob("date=*/part-000.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files])

    def _load_raw(self, vehicle: str, source: str) -> pl.DataFrame | None:
        files = sorted(self.raw_dir(vehicle, source).glob("date=*/part-000.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files])

    def event_date_count(self, vehicle: str) -> int:
        """event DB 에 쌓인 전체 날짜 파티션 수 (소스 통합). feature 는 이 전체를 대상으로 산출."""
        dates = set()
        for source in self.sources_cfg():
            for p in self.event_dir(vehicle, source).glob("date=*"):
                dates.add(p.name[5:])
        return len(dates)

    # ─────────────────────────────────────────
    # 3) FEATURE  (fab / knob / mask / inline / vm)
    #    ※ 특정 기간이 아니라 event DB 에 쌓인 "전체" 를 대상으로 산출한다
    #      (_load_event 가 date=* 파티션 전부 로드).
    # ─────────────────────────────────────────
    def run_feature(self, vehicle: str) -> dict:
        event = self._load_event(vehicle, "FAB")
        if event is None:
            raise RuntimeError("event DB 없음 — raw/event 단계를 먼저 실행하세요")
        fdir = self.feature_dir(vehicle)
        fdir.mkdir(parents=True, exist_ok=True)

        features: dict[str, list[str]] = {"fab": [], "knob": [], "mask": [], "inline": [], "vm": []}
        skipped: list[dict] = []  # 컬럼 미추출 등으로 건너뛴 feature (사유 포함)

        # 관리자 커스텀 함수 (config/feature_funcs.py) — 값 생성은 내장과 병합, agg 는 별도 전달
        custom_vals, custom_aggs, func_errors = self.feature_funcs()
        skipped.extend(func_errors)
        value_rules = {**FEATURE_RULES, **custom_vals}

        # FAB — Ref_feature 그대로: step_desc × feature_name × agg
        rules = self.rules_csv("fab")
        if rules is not None:
            for r in rules.iter_rows(named=True):
                step, fname, agg = r["step_desc"], r["feature_name"], r["agg"]
                df = event.filter(pl.col("step_desc") == step)
                if df.height == 0:
                    continue
                col = f"FAB_{step}_{fname}"
                if fname not in value_rules:
                    skipped.append({"feature": col,
                                    "reason": f"알 수 없는 feature_name {fname!r} — "
                                              "config/feature_funcs.py 에 함수 추가 가능"})
                    continue
                try:
                    feat = aggregate_feature(df.with_columns(value_rules[fname]().alias("val")),
                                             col, agg, custom_aggs)
                    self._save_feature(fdir, feat, col, features["fab"])
                except Exception as e:
                    skipped.append({"feature": col, "reason": str(e)})

        # KNOB — knob 룰(직접 매핑 또는 사내 rule 형식 → knob_map 이 통합) 적용.
        # 매핑 실패분은 raw ppid(RO) 유지 + miss 리포트.
        # step 미통과(비어있는) wafer 는 skip 판정(명시 SKIP 블록/auto) → "SKIP" 값
        knob_miss_rows: list[dict] = []
        knob_skip_rows: list[dict] = []
        knob_feats: dict[str, dict] = {}  # col → {feat, sdesc, step_ids}
        vknob = self.knob_map(vehicle)
        if vknob is not None and "ppid" not in event.columns:
            skipped.append({"feature": "KNOB_*", "reason": "ppid 컬럼 미추출 (sources.FAB.columns 확인)"})
            vknob = None
        if vknob is not None:
            for (sid,), grp in vknob.group_by(["step_id"], maintain_order=True):
                mapping = {r["ppid"]: r["knob"] for r in grp.iter_rows(named=True)}
                sdesc = grp["step_desc"][0]
                df = event.filter(pl.col("step_id") == sid)
                if df.height == 0:
                    continue
                df = df.with_columns(
                    pl.col("ppid").replace_strict(mapping, default=None).alias("knob_val")
                )
                miss = df.filter(pl.col("knob_val").is_null())
                if miss.height:
                    agg_miss = (
                        miss.group_by(["split", "ppid"])
                            .agg(
                                pl.col("root_lot_id").n_unique().alias("n_lots"),
                                pl.col("wafer_id").n_unique().alias("n_wafers"),
                                pl.col("root_lot_id").unique().sort().head(5).alias("lots"),
                            )
                            .sort(["split", "ppid"])
                    )
                    for m in agg_miss.iter_rows(named=True):
                        knob_miss_rows.append({
                            "vehicle": vehicle, "split": m["split"],
                            "step_id": sid, "step_desc": sdesc,
                            "ppid": m["ppid"],
                            "n_lots": m["n_lots"], "n_wafers": m["n_wafers"],
                            "lots": list(m["lots"]),
                        })
                # feature 값: 매핑되면 knob, 아니면 raw ppid 그대로 (RO)
                # 집계는 기본 last — knob csv 의 agg 컬럼으로 step 별 조정 가능
                # (first/last_valid/concat/agg + feature_funcs.py 의 agg_<이름>)
                knob_agg = "last"
                if "agg" in grp.columns:
                    set_aggs = [a for a in grp["agg"].to_list() if a and str(a).strip()]
                    if set_aggs:
                        knob_agg = str(set_aggs[0]).strip()
                col = f"KNOB_{sdesc}_ppid"
                df = df.with_columns(
                    pl.coalesce([pl.col("knob_val"), pl.col("ppid")]).alias("val"))
                try:
                    feat = aggregate_feature(df, col, knob_agg, custom_aggs)
                except Exception as e:
                    skipped.append({"feature": col, "reason": f"agg {knob_agg!r}: {e}"})
                    continue
                ent = knob_feats.setdefault(col, {"sdesc": sdesc, "step_ids": []})
                ent["feat"] = feat  # 같은 step_desc 의 복수 step_id 는 기존과 동일하게 마지막 승자
                ent["step_ids"].append(sid)
            self._knob_skip_layer(vehicle, event, knob_feats, knob_skip_rows, skipped)
            for col, ent in knob_feats.items():
                self._save_feature(fdir, ent["feat"], col, features["knob"])

        # MASK — photo step 의 part|reticle
        mask = self.rules_csv("mask")
        if mask is not None:
            for r in mask.iter_rows(named=True):
                step, agg = r["step_desc"], r["agg"]
                df = event.filter(pl.col("step_desc") == step)
                if df.height == 0:
                    continue
                col = f"MASK_{step}_reticle"
                try:
                    feat = aggregate_feature(df.with_columns(build_part_reticle().alias("val")),
                                             col, agg, custom_aggs)
                    self._save_feature(fdir, feat, col, features["mask"])
                except Exception as e:
                    skipped.append({"feature": col, "reason": str(e)})

        # INLINE — item_id 별 수치 집계 (INLINE event DB — inline matching 필터 적용본)
        inline_ev = self._load_event(vehicle, "INLINE")
        inline_rules = self.rules_csv("inline")
        if inline_ev is not None and inline_rules is not None:
            for r in inline_rules.iter_rows(named=True):
                item, agg = r["item_id"], r["agg"]
                df = inline_ev.filter(pl.col("item_id") == item)
                if df.height == 0:
                    continue
                col = f"INLINE_{item}_{agg}"
                try:
                    expr, need_val = numeric_agg_expr(agg, "value", col, custom_aggs)
                    if need_val:
                        df = df.with_columns(pl.col("value").alias("val"))
                    feat = df.sort("time").group_by(KEY_COLS).agg(expr)
                    self._save_feature(fdir, feat, col, features["inline"])
                except Exception as e:
                    skipped.append({"feature": col, "reason": str(e)})

        # VM — sensor_id 별 residual 집계 (VM event DB — vehicle_matching step 필터 적용본)
        vm_ev = self._load_event(vehicle, "VM")
        vm_rules = self.rules_csv("vm")
        if vm_ev is not None and vm_rules is not None:
            for r in vm_rules.iter_rows(named=True):
                sensor, agg = r["sensor_id"], r["agg"]
                df = vm_ev.filter(pl.col("sensor_id") == sensor)
                if df.height == 0:
                    continue
                col = f"VM_{sensor}_residual_{agg}"
                try:
                    expr, need_val = numeric_agg_expr(agg, "residual", col, custom_aggs)
                    if need_val:
                        df = df.with_columns(pl.col("residual").alias("val"))
                    feat = df.sort("time").group_by(KEY_COLS).agg(expr)
                    self._save_feature(fdir, feat, col, features["vm"])
                except Exception as e:
                    skipped.append({"feature": col, "reason": str(e)})

        # knob miss / skip 리포트 저장
        rdir = self.report_dir(vehicle)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "knob_miss.json").write_text(
            json.dumps(knob_miss_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        (rdir / "knob_skip.json").write_text(
            json.dumps(knob_skip_rows, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "features": {k: len(v) for k, v in features.items()},
            "files": features,
            "knob_miss": knob_miss_rows,
            "knob_skip": knob_skip_rows,
            "skipped": skipped,
            "event_dates": self.event_date_count(vehicle),  # feature 가 커버한 전체 event 날짜 수
        }

    def _knob_skip_layer(self, vehicle: str, event: pl.DataFrame,
                         knob_feats: dict, knob_skip_rows: list, skipped: list):
        """knob step 미통과(값이 빈) wafer 의 skip 판정 → feature 에 "SKIP" 채움.

        비어있음의 원인 3분류:
          · pending — 아직 step 미도달 → null 유지 (다음 실행에서 재판정)
          · skip    — 뒤쪽 step 을 이미 지남 = 그 step 을 타지 않는 wafer → "SKIP"
          · 의심    — step 매칭 문제로 event 가 안 잡힘 → skip 하지 않고 리포트

        판정 경로:
          1) 명시 SKIP 블록 (ppid_knob.csv AND 블록, category=SKIP) — 사내 원본의
             "다음 main step not_null → SKIP" 패턴. 조건 step 이 이 vehicle 매칭에
             없으면 블록 미적용 + 리포트 (매칭 오류가 skip 으로 둔갑하지 않게).
          2) auto (명시 블록이 없는 feature, pipeline.yaml knob_skip.auto) —
             뒤쪽 step 판별은 "두 step 을 모두 가진 wafer 들의 tkout_time 상대순서"
             (sleuth_order 는 wafer 소량 투입 시 비어 판단 기준으로 쓰지 않음).
             공동 통과 표본(min_support)/시간역전 비율(after_fraction) 미달이면
             보류 → null 유지 (skip 을 확신할 수 없으면 안 한다).
        """
        if not knob_feats:
            return
        cfg = self.global_cfg().get("knob_skip") or {}
        if not cfg.get("enabled", True):
            return
        auto = bool(cfg.get("auto", True))
        min_support = int(cfg.get("min_support", 5))
        after_frac = float(cfg.get("after_fraction", 0.8))

        ev_last = (event.sort("tkout_time")
                        .group_by(KEY_COLS + ["step_id"])
                        .agg(pl.col("ppid").drop_nulls().last().alias("ppid"),
                             pl.col("tkout_time").last().alias("t")))
        ev_steps = set(ev_last["step_id"].to_list())
        universe = event.select(KEY_COLS).unique()
        wafer_split = (event.sort("tkout_time").group_by(KEY_COLS)
                            .agg(pl.col("split").drop_nulls().last().alias("split")))
        desc2ids: dict[str, list[str]] = {}
        for r in self.step_map(vehicle).iter_rows(named=True):
            desc2ids.setdefault(r["step_desc"], []).append(r["step_id"])

        def missing_keys(ent) -> pl.DataFrame:
            # skip 후보 = 해당 step 에 event 자체가 없는 wafer.
            # (event 는 있는데 agg 가 값을 못 고른 wafer 는 통과한 것 — skip 아님)
            have = (ev_last.filter(pl.col("step_id").is_in(ent["step_ids"]))
                           .select(KEY_COLS).unique())
            return (universe.join(have, on=KEY_COLS, how="anti")
                            .join(ent["feat"].select(KEY_COLS), on=KEY_COLS, how="anti"))

        def apply_skip(col: str, ent: dict, keys: pl.DataFrame, mode: str, extra: dict):
            if keys.height == 0:
                return
            skip_df = keys.with_columns(pl.lit("SKIP").alias(col)).select(ent["feat"].columns)
            ent["feat"] = pl.concat([ent["feat"], skip_df])
            grp = (keys.join(wafer_split, on=KEY_COLS, how="left")
                       .group_by("split")
                       .agg(pl.len().alias("n_wafers"),
                            pl.col("root_lot_id").n_unique().alias("n_lots"),
                            pl.col("root_lot_id").unique().sort().head(5).alias("lots"))
                       .sort("split"))
            for g in grp.iter_rows(named=True):
                knob_skip_rows.append({
                    "vehicle": vehicle, "split": g["split"],
                    "feature": col, "step_desc": ent["sdesc"],
                    "step_id": ",".join(ent["step_ids"]),
                    "mode": mode, "n_wafers": g["n_wafers"],
                    "n_lots": g["n_lots"], "lots": list(g["lots"]), **extra,
                })

        # ── 1) 명시 SKIP 블록 ──
        targeted: set[str] = set()  # 블록이 있는 feature 는 auto 로 덮지 않음
        for b in self.knob_skip_blocks():
            label = f"{b['feature']}/{b['rule_order']}"
            if (b.get("category") or "").upper() != "SKIP":
                skipped.append({"feature": f"KNOB({label})",
                                "reason": "AND 값 블록 미지원 — SKIP 블록만 지원 "
                                          "(값 룰은 eq 단일행으로 분리 필요)"})
                continue
            targeted.update(f"KNOB_{s}_ppid" for s in b["target_steps"])
            tcols = [c for c, e in knob_feats.items() if e["sdesc"] in b["target_steps"]]
            if not tcols:
                skipped.append({"feature": f"KNOB({label})",
                                "reason": "SKIP 블록 대상 step 에 event 없음 — "
                                          "vehicle_matching 확인 필요 (skip 미적용)"})
                continue
            ok, valid = universe, True
            for c in b["conds"]:
                ids = desc2ids.get(c["step"]) or \
                      ([c["step"]] if c["step"] in ev_steps else None)
                if ids is None:
                    skipped.append({"feature": f"KNOB({label})",
                                    "reason": f"SKIP 블록 조건 step {c['step']!r} 이 "
                                              "이 vehicle 매칭에 없음 — skip 미적용 (매칭 확인)"})
                    valid = False
                    break
                vals = (ev_last.filter(pl.col("step_id").is_in(ids))
                               .sort("t").group_by(KEY_COLS)
                               .agg(pl.col("ppid").last().alias("v")))
                m = (universe.join(vals, on=KEY_COLS, how="left")
                             .with_columns(_knob_cond_expr(c["op"], c["value"])
                                           .fill_null(False).alias("_ok")))
                ok = ok.join(m.filter(pl.col("_ok")).select(KEY_COLS),
                             on=KEY_COLS, how="semi")
            if not valid:
                continue
            for col in tcols:
                ent = knob_feats[col]
                keys = missing_keys(ent).join(ok, on=KEY_COLS, how="semi")
                apply_skip(col, ent, keys, "rule", {"rule": label})

        # ── 2) auto skip ──
        if not auto:
            return
        for col, ent in knob_feats.items():
            if col in targeted:
                continue
            cand = missing_keys(ent)
            if cand.height == 0:
                continue
            r_t = (ev_last.filter(pl.col("step_id").is_in(ent["step_ids"]))
                          .group_by(KEY_COLS).agg(pl.col("t").max().alias("t_r")))
            stats = (ev_last.filter(~pl.col("step_id").is_in(ent["step_ids"]))
                            .join(r_t, on=KEY_COLS, how="inner")
                            .group_by("step_id")
                            .agg(pl.len().alias("n"),
                                 (pl.col("t") > pl.col("t_r")).mean().alias("frac")))
            anchors = (stats.filter((pl.col("n") >= min_support)
                                    & (pl.col("frac") >= after_frac))
                       ["step_id"].to_list())
            if not anchors:
                skipped.append({"feature": col,
                                "reason": "auto skip 보류 — 뒤쪽 step 판별 불가 "
                                          f"(공동 통과 표본 부족, 빈 wafer {cand.height}건 null 유지)"})
                continue
            passed = (ev_last.filter(pl.col("step_id").is_in(anchors))
                             .select(KEY_COLS).unique())
            keys = cand.join(passed, on=KEY_COLS, how="semi")
            apply_skip(col, ent, keys, "auto", {"anchors": sorted(anchors)[:5]})

    @staticmethod
    def _save_feature(fdir: Path, feat: pl.DataFrame, col: str, bucket: list):
        # 값이 하나도 없는 feature 는 저장하지 않음 (Ref_feature 동일)
        s = feat[col]
        if s.dtype == pl.String:
            has_val = feat.select((pl.col(col).is_not_null()
                                   & (pl.col(col).str.strip_chars() != "")).any()).item()
        else:
            has_val = feat.select(pl.col(col).is_not_null().any()).item()
        if not has_val:
            return
        fname = f"{safe_filename(col)}.parquet"
        feat.write_parquet(fdir / fname)
        bucket.append(fname)

    # ─────────────────────────────────────────
    # 4) WIDE — vehicle 의 feature 전부를 KEY 기준 left join 한 ML_TABLE.
    #    PRODUCT 컬럼(vehicles.yaml 의 product) 을 붙여 send form 에서 vehicle 간 concat.
    # ─────────────────────────────────────────
    def run_wide(self, vehicle: str) -> dict:
        files = sorted(self.feature_dir(vehicle).glob("*.parquet"))
        if not files:
            raise RuntimeError("feature 없음 — feature 단계를 먼저 실행하세요")
        dfs = []
        for f in files:
            dfs.append(
                pl.read_parquet(f)
                  .with_columns(
                      pl.col("root_lot_id").cast(pl.Utf8).str.strip_chars(),
                      pl.col("wafer_id").cast(pl.Utf8).str.strip_chars()
                        .str.replace_all(r"\D", "").cast(pl.Int64, strict=False))
                  .unique(subset=KEY_COLS))

        base = pl.concat([d.select(KEY_COLS) for d in dfs]).unique().sort(KEY_COLS)
        wide = base
        for df in dfs:
            wide = wide.join(df, on=KEY_COLS, how="left")

        product = str(self.vehicle_cfg(vehicle).get("product") or vehicle)
        wide = wide.with_columns(pl.lit(product).alias("PRODUCT")) \
                   .rename({"root_lot_id": "ROOT_LOT_ID", "wafer_id": "WAFER_ID"})

        # 값이 전부 null 인 feature 컬럼 제거
        counts = wide.select(pl.all().count()).row(0)
        wide = wide.select([c for c, n in zip(wide.columns, counts) if n > 0 or c in WIDE_KEY])

        # 컬럼 정렬 — KEY → KNOB → FAB → MASK → INLINE → VM → 기타 (prefix 뒤 첫 숫자)
        cols = wide.columns
        ordered = list(WIDE_KEY)
        for prefix in ("KNOB_", "FAB_", "MASK_", "INLINE_", "VM_"):
            ordered += sorted((c for c in cols if c.startswith(prefix)),
                              key=lambda c, p=prefix: first_number_after(p, c))
        ordered += [c for c in cols if c not in ordered]
        wide = wide.select(ordered)

        wdir = self.wide_dir()
        wdir.mkdir(parents=True, exist_ok=True)
        out = wdir / f"ML_TABLE_{vehicle}.parquet"
        wide.write_parquet(out, compression="zstd", statistics=True)
        return {"rows": wide.height, "features": wide.width - len(WIDE_KEY),
                "path": str(out.relative_to(self.root))}

    # ─────────────────────────────────────────
    # 5) SEND FORM — 전 vehicle ML_TABLE 병합 후 prefix 그룹별 분리 저장.
    #    wafer 중복은 최신(keep last) 우선 · MASK_ 는 FAB 그룹에 포함.
    # ─────────────────────────────────────────
    def run_send_form(self) -> dict:
        files = sorted(self.wide_dir().glob("ML_TABLE_*.parquet"))
        if not files:
            raise RuntimeError("wide form 없음 — wide 단계를 먼저 실행하세요")
        df = pl.concat([pl.scan_parquet(f) for f in files], how="diagonal_relaxed") \
               .unique(subset=WIDE_KEY, keep="last")
        cols = df.collect_schema().names()

        groups = {}
        for group, prefixes in SEND_GROUPS.items():
            gcols = []
            for p in prefixes:
                gcols += sorted((c for c in cols if c.startswith(p)),
                                key=lambda c, pf=p: first_number_after(pf, c))
            if not gcols:
                groups[group] = {"rows": 0, "cols": 0, "skipped": "해당 prefix 컬럼 없음"}
                continue
            gdir = self.send_dir() / group
            gdir.mkdir(parents=True, exist_ok=True)
            name = group.split(".", 1)[-1]
            gdf = df.select(WIDE_KEY + gcols).collect()
            gdf.write_csv(gdir / f"{name}_ML_TABLE.csv")
            gdf.write_parquet(gdir / f"{name}_ML_TABLE.parquet",
                              compression="zstd", statistics=True)
            groups[group] = {"rows": gdf.height, "cols": len(gcols)}
        return {"tables": [f.name for f in files], "groups": groups}

    # ─────────────────────────────────────────
    # 미매칭 step 스캔 (제품별) + 전역 exclude 적용
    # ─────────────────────────────────────────
    def scan_unmatched(self, vehicle: str) -> dict:
        cfg = self.vehicle_cfg(vehicle)
        raw = self._load_raw(vehicle, "FAB")
        if raw is None:
            raise RuntimeError("FAB raw 없음 — raw 단계를 먼저 실행하세요")
        matched = set(self.step_map(vehicle)["step_id"].to_list())
        excl = ((self.global_cfg().get("unmatched_scan") or {}).get("exclude") or {})
        eqp_pats = [str(p) for p in (excl.get("eqp_id") or [])]
        model_pats = [str(p) for p in (excl.get("eqp_model") or [])]

        group_cols = [c for c in ("step_id", "step_desc", "eqp_id", "eqp_model")
                      if c in raw.columns]
        combos = (
            raw.group_by(group_cols)
               .agg(pl.len().alias("rows"),
                    pl.col("root_lot_id").n_unique().alias("n_lots"))
               .filter(~pl.col("step_id").is_in(list(matched)))
               .sort(group_cols)
        )

        def _match(val: str, pats: list[str]) -> str | None:
            for p in pats:
                if fnmatch.fnmatch(str(val), p):
                    return p
            return None

        unmatched, excluded = [], []
        for r in combos.iter_rows(named=True):
            reason = None
            p = _match(r.get("eqp_id", ""), eqp_pats) if "eqp_id" in r else None
            if p:
                reason = f"eqp_id ~ '{p}'"
            elif "eqp_model" in r:
                p = _match(r.get("eqp_model", ""), model_pats)
                if p:
                    reason = f"eqp_model ~ '{p}'"
            row = {"product": cfg["product"], "vehicle": vehicle, **r}
            if reason:
                row["excluded_by"] = reason
                excluded.append(row)
            else:
                unmatched.append(row)

        report = {"product": cfg["product"], "vehicle": vehicle,
                  "unmatched": unmatched, "excluded": excluded,
                  "exclude_config": {"eqp_id": eqp_pats, "eqp_model": model_pats}}
        rdir = self.report_dir(vehicle)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "unmatched.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return report

    # ─────────────────────────────────────────
    # 전체 실행
    # ─────────────────────────────────────────
    def run_all(self, vehicle: str) -> dict:
        raw = self.run_raw_query(vehicle)
        event = self.run_event(vehicle)
        feature = self.run_feature(vehicle)
        wide = self.run_wide(vehicle)
        unmatched = self.scan_unmatched(vehicle)
        return {"vehicle": vehicle, "raw": raw, "event": event,
                "feature": feature, "wide": wide, "unmatched": unmatched}

    def status(self, vehicle: str) -> dict:
        """raw/event/feature 처리 현황 — 소스별로 event 가 raw 대비 어디까지 처리됐는지,
        매칭 파일(sha) 변경으로 전체 재생성이 필요한(stale) 소스는 어디인지,
        각 event DB 가 언제/어떤 매칭 버전으로 갱신됐는지(applied_ts/sha)."""
        cfg = self.vehicle_cfg(vehicle)

        raw, event = {}, {}
        for source in self.sources_cfg():
            raw[source] = [p.parent.name[5:] for p in
                           sorted(self.raw_dir(vehicle, source).glob("date=*/part-000.parquet"))]
            edir = self.event_dir(vehicle, source)
            dates = [p.parent.name[5:] for p in sorted(edir.glob("date=*/part-000.parquet"))]
            meta = {}
            meta_path = edir / "_meta.json"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    meta = {}
            event[source] = {
                "dates": dates,
                "pending": [d for d in raw[source] if d not in dates],
                "stale": bool(dates) and meta.get("ver") != self.event_version(vehicle, source),
                "applied_ts": meta.get("ts"),
                "matching_file": meta.get("file"),
                "matching_sha": meta.get("sha"),
            }

        features = {k: 0 for k in ("fab", "knob", "mask", "inline", "vm")}
        fdir = self.feature_dir(vehicle)
        if fdir.exists():
            for f in fdir.glob("*.parquet"):
                cat = f.name.split("_", 1)[0].lower()
                if cat in features:
                    features[cat] += 1

        matching_path = self.matching_file("FAB")
        return {
            "vehicle": vehicle,
            "product": cfg["product"],
            "matching": {"steps": self.step_map(vehicle).height,
                         "mtime": matching_path.stat().st_mtime if matching_path.exists() else None},
            "raw": raw,
            "event": event,
            "features": features,
        }

    def load_report(self, vehicle: str, name: str):
        path = self.report_dir(vehicle) / f"{name}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def list_features(self, vehicle: str) -> dict:
        fdir = self.feature_dir(vehicle)
        out: dict[str, list[dict]] = {"fab": [], "knob": [], "mask": [], "inline": [], "vm": []}
        if not fdir.exists():
            return out
        for f in sorted(fdir.glob("*.parquet")):
            cat = f.name.split("_", 1)[0].lower()
            if cat not in out:
                continue
            df = pl.read_parquet(f)
            col = [c for c in df.columns if c not in KEY_COLS]
            sample = df.head(3).to_dicts()
            out[cat].append({"file": f.name, "rows": df.height,
                             "column": col[0] if col else "", "sample": sample})
        return out
