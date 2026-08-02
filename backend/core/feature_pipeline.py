"""
Valve · feature_pipeline
------------------------
Ref_raw_query / Ref_event / Ref_feature 3단계 파이프라인의 Valve 통합판.

  1) raw   : vehicle 설정(QueryTimeSpan/SplitTimeSpan)대로 split 을 나눠
             FAB · INLINE · VM · ET 를 쿼리 →
             db/1.RAWDATA_DB/{SOURCE}/{vehicle}/date={YYYY-MM-DD}/data.parquet
             · auto report 의 daily DB 와 동일한 hive partitioning —
               파티션 키는 "데이터의 시간 컬럼(tkout_time/time) 날짜" (쿼리 날짜 아님),
               파일명은 data.parquet (auto report: {DB_et_daily}/date=…/data.parquet)
             · ET 는 auto report 와 동일한 reformatter 인식 —
               config/reformatter/{vehicle}_reformatter.csv 의 REAL ITEMID 만 대상
  2) event : FAB raw 를 vehicle_matching(step_id↔step_desc) inner join +
             root_lot prefix 필터 → db/2.EVENT_DB/{vehicle}/{SOURCE}/date=…/data.parquet
             · 소스별 event 여부는 pipeline.yaml sources.<name>.event 로 제어 —
               FAB/INLINE/VM 은 raw+event, ET 는 raw 전용(event: false)
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
import asyncio
import hashlib
import json
import os
import random
import re
import shutil
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import yaml

KEY_COLS = ["root_lot_id", "wafer_id"]

# 매칭알람(unmatched_step)에 실어 보낼 raw 열 기본값 — pipeline.yaml
# unmatched_scan.alert_cols 로 override (알람 탭 ⚙). raw 에 없는 열은 조용히 빠진다.
ALERT_COLS_DEFAULT = ["eqp_id", "eqp_model", "area"]
ALERT_EXAMPLE_LIMIT_DEFAULT = 3

# 신규 step 의 앞뒤 이웃 step 컨텍스트 (flow 의 function step 추천 입력) 기본값 —
# pipeline.yaml unmatched_scan.hint 로 override (알람 탭 ⚙).
HINT_DEFAULT = {
    "enabled": True,
    "days": 7,          # FAB raw 에서 볼 최근 날짜 파티션 수
    "neighbors": 3,     # 앞/뒤 각각 실어 보낼 이웃 step 수
    "cols": ["ppid", "eqp_id", "eqp_model", "area"],
    "value_limit": 12,  # 열 하나에 실을 unique 값 상한
}


def alert_scan_cols(us_cfg: dict) -> list[str]:
    """unmatched_scan 설정에서 알람 전송 열 목록 (미설정 시 코드 기본값)."""
    cols = us_cfg.get("alert_cols")
    if not isinstance(cols, list):
        cols = ALERT_COLS_DEFAULT
    out: list[str] = []
    for c in cols:
        c = str(c).strip()
        if c and c not in out:
            out.append(c)
    return out


def alert_hint_cfg(us_cfg: dict) -> dict:
    """unmatched_scan.hint — 이웃 step 컨텍스트 설정 (미설정 시 코드 기본값).

    config/ 는 seed-only 라 기존 설치의 pipeline.yaml 에는 이 키가 없다 —
    기본값을 코드에 두어야 업그레이드만으로 동작한다."""
    raw = us_cfg.get("hint")
    cfg = dict(HINT_DEFAULT)
    cfg["cols"] = list(HINT_DEFAULT["cols"])
    if isinstance(raw, dict):
        cfg["enabled"] = bool(raw.get("enabled", True))
        for key, lo, hi in (("days", 1, 90), ("neighbors", 1, 10), ("value_limit", 1, 50)):
            try:
                cfg[key] = max(lo, min(hi, int(raw.get(key) or HINT_DEFAULT[key])))
            except (TypeError, ValueError):
                pass
        if isinstance(raw.get("cols"), list):
            out: list[str] = []
            for c in raw["cols"]:
                c = str(c).strip()
                if c and c not in out:
                    out.append(c)
            cfg["cols"] = out or list(HINT_DEFAULT["cols"])
    return cfg


_STEP_NUM_RE = re.compile(r"^(.*?)(\d+)$")


def split_step_id(step_id) -> tuple[str, int, int] | None:
    """'AA100002' → ('AA', 100002, 6). 끝이 숫자가 아니면 None.

    같은 prefix·같은 자릿수의 step 끼리만 번호가 공정 순서를 뜻한다 —
    자릿수가 다르면 체계가 다른 step 이라 이웃으로 보지 않는다."""
    m = _STEP_NUM_RE.match(str(step_id or "").strip())
    if not m:
        return None
    return m.group(1), int(m.group(2)), len(m.group(2))

# 추출 소스 기본값 — config/pipeline.yaml 의 sources 로 override (테이블명/컬럼 조절)
# time_col = raw 를 date= 파티션으로 나누는 기준 열. 전 소스를 tkout_time(공정 진행
# 시각)으로 통일해 두면 소스가 달라도 같은 날짜 축으로 정렬된다 — INLINE/VM 의
# 측정 시각(time)은 track-out 보다 늦게 찍혀 하루가 밀릴 수 있다.
DEFAULT_SOURCES = {
    "FAB": {
        "table": "RAW_FAB_DATA",
        "columns": ["root_lot_id", "wafer_id", "part_id", "tkout_time", "step_id",
                    "step_desc", "ppid", "reticle_id", "eqp_id", "eqp_model",
                    "chamber_id", "unit_id", "sleuth_order"],
        "time_col": "tkout_time",
    },
    "INLINE": {
        "table": "RAW_INLINE_DATA",
        "columns": ["root_lot_id", "wafer_id", "item_id", "value", "measure_pos",
                    "tkout_time", "time"],
        "time_col": "tkout_time",
    },
    "VM": {
        "table": "RAW_VM_DATA",
        "columns": ["root_lot_id", "wafer_id", "sensor_id", "eqp_id", "step_id",
                    "predicted_value", "actual_value", "residual", "tkout_time", "time"],
        "time_col": "tkout_time",
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


def raw_query_window(start: date, end: date) -> tuple[date, date]:
    """raw 유닛 (start, end) → 실제 쿼리 구간 [from, to) — to 는 **배타적**.

    get_split_date_ranges 의 마지막 유닛은 (today, today) 라 그대로 쓰면 폭이 0 이다
    (사내 API 에 dateFrom==dateTo 로 나가면 당일 데이터가 통째로 안 잡힌다).
    반대로 양끝을 포함으로 해석하면 이웃 유닛과 하루씩 겹쳐 같은 날을 두 번 쿼리하고
    같은 파티션을 두 유닛이 쓴다. 그래서 항상 반열림으로 통일한다:
      (T-5, T-4) → [T-5, T-4)   하루
      (T,   T  ) → [T,   T+1)   오늘 하루
    저장은 어차피 시간 컬럼(tkout_time 등)의 날짜로 파티셔닝되므로(_write_raw_partitions)
    구간이 여러 날에 걸쳐도 행은 각자 맞는 date= 파티션으로 들어간다.
    """
    return start, (end if end > start else start + timedelta(days=1))


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


# 유효 장비값 판정 — 마지막 '_' 뒤 구간에 숫자가 있는가 (EQP_01 · A_B_1 · EQP_01_CH_2).
# 마지막 구간만 본다: A_1_B 는 뒤가 'B' 라 무효, EQP_01_CH_A 도 무효.
VALID_TOOL_SUFFIX = r"_[^_]*[0-9][^_]*$"


def _blank_to_null(col: str):
    """빈 문자열·'-' 를 결측으로 — 사내 raw 는 미기입을 둘 다 쓴다."""
    c = _clean_str(col)
    return pl.when(c == "-").then(None).otherwise(c)


def build_sleuth_order():
    """sleuth_order — 비어있는 값을 (step_id, root_lot_id) 안에서
    wafer_id 오름차순 순번으로 채운다.

    소량 투입 lot 은 wafer 가 섞이지 않고 wafer_id 순서 그대로 투입되므로
    낮은 wafer_id 가 낮은 순번을 갖는다. 이미 값이 있는 행은 건드리지 않는다.

    wafer_id 는 문자열이라 그냥 정렬하면 "10" < "2" 가 되므로 숫자로 변환해
    zero-pad 한 키로 순위를 매긴다 (숫자가 아니면 원래 문자열 순서).
    dense rank 라 같은 wafer 가 한 step 에 여러 행이어도 같은 순번을 받는다.
    """
    num = pl.col("wafer_id").cast(pl.Utf8).str.strip_chars().cast(pl.Int64, strict=False)
    order_key = (pl.when(num.is_not_null())
                   .then(num.cast(pl.Utf8).str.zfill(9))
                   .otherwise(pl.col("wafer_id").cast(pl.Utf8)))
    seq = (order_key.rank("dense").over(["step_id", "root_lot_id"])
                    .cast(pl.Int64).cast(pl.Utf8))
    return pl.coalesce(_blank_to_null("sleuth_order"), seq)


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
    "sleuth_order": build_sleuth_order,
    "eqpall": build_eqp_all,
    "ecuall": build_ecu_all,
    "reticleall": build_part_reticle,
}

# feature_name 별 고정 집계 — 룰북(fab.csv)의 agg 와 무관하게 항상 이 규칙으로 뽑는다.
#   ecuall : 마지막 '_' 뒤에 숫자가 있는 값(유효 장비값) 중 tkout_time 이 가장 늦은 것.
#            그런 값이 하나도 없으면 tkout_time last.
# step 별 예외를 두지 않는 것이 요구사항이라 csv 로는 못 바꾼다 — 규칙 자체를 바꾸려면
# 여기를 고친다. 무시된 csv agg 는 run_feature 결과의 agg_overrides 로 노출된다.
FORCED_AGG = {"ecuall": "valid_or_last"}


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
        # ※ 유효값이 하나도 없는 wafer 는 행 자체가 사라진다 → valid_or_last 참고
        return (df.with_columns(pl.col("val").cast(pl.Utf8).str.strip_chars().alias("val_str"))
                  .filter(pl.col("val_str").str.contains(r"_[A-Za-z0-9]*[0-9]"))
                  .sort("tkout_time").group_by(KEY_COLS)
                  .agg(pl.col("val_str").first().alias(feature_col)))
    if agg_type == "valid_or_last":
        # 유효 장비값(마지막 '_' 뒤에 숫자) 중 tkout_time 이 가장 늦은 것.
        # 유효값이 하나도 없으면 tkout_time last 값으로 대체 — wafer 가 통째로
        # 빠지는 valid_eqp 와 달리 항상 한 값을 남긴다.
        v = pl.col("val").cast(pl.Utf8).str.strip_chars().replace("", None)
        valid = pl.when(v.str.contains(VALID_TOOL_SUFFIX)).then(v).otherwise(None)
        return (df.sort("tkout_time").group_by(KEY_COLS)
                  .agg(pl.coalesce(valid.drop_nulls().last(),
                                   v.drop_nulls().last()).alias(feature_col)))
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
    def __init__(self, root: Path, settings: dict, lake_api=None):
        self.root = Path(root)
        self.settings = settings
        self.lake_api = lake_api

    def product_source_cfg(self, product: str, source: str) -> dict:
        """products.yaml의 제품별 실제 테이블 설정을 읽는다."""
        data = yaml.safe_load(
            (self.root / "config" / "products.yaml").read_text(encoding="utf-8")) or {}
        prod = next((p for p in data.get("products", []) if p.get("product") == product), None)
        src = next((s for s in (prod or {}).get("sources", []) if s.get("name") == source), None)
        if not src:
            raise ValueError(f"product/source config not found: {product}/{source}")
        table = src.get("table_name") or src.get("table")
        if not isinstance(table, str) or not table.strip():
            raise ValueError(f"table_name is required for product={product!r}, source={source!r}")
        return src

    def _query_raw(self, cfg: dict, source: str, q_from: date, q_to: date) -> pl.DataFrame:
        src = self.product_source_cfg(str(cfg["product"]), source)
        params = {
            "table_name": src.get("table_name") or src.get("table"),
            "datefrom": q_from.isoformat(),
            "dateto": q_to.isoformat(),
        }
        for key in ("process_id", "line_id"):
            value = cfg.get(key)
            if value is not None and value != "" and value != []:
                params[key] = value

        async def invoke():
            return await self.lake_api.query(params, [])

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(invoke())
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, invoke()).result()

    # ── config loaders (호출 시점마다 fresh 로드 → 웹에서 수정 즉시 반영) ──
    def global_cfg(self) -> dict:
        return yaml.safe_load((self.root / "config" / "pipeline.yaml").read_text(encoding="utf-8")) or {}

    @staticmethod
    def _save_yaml(path: Path, cfg: dict):
        """yaml.safe_dump 은 주석을 전부 버린다 — 파일 맨 앞 주석 블록(설정 설명)만은
        보존해서 다시 붙인다. 웹에서 값 하나 바꿀 때마다 문서가 사라지지 않도록."""
        header = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("#") or not line.strip():
                    header.append(line)
                else:
                    break
            while header and not header[-1].strip():
                header.pop()
        body = yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False)
        text = ("\n".join(header) + "\n" + body) if header else body
        path.write_text(text, encoding="utf-8")

    def save_global_cfg(self, cfg: dict):
        self._save_yaml(self.root / "config" / "pipeline.yaml", cfg)

    def vehicles(self) -> dict:
        return yaml.safe_load((self.root / "config" / "vehicles.yaml").read_text(encoding="utf-8")) or {}

    def save_vehicles(self, cfg: dict):
        self._save_yaml(self.root / "config" / "vehicles.yaml", cfg)

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
        """파이프라인 DB 루트 — 우선순위: VALVE_DB_ROOT 환경변수 > pipeline.yaml
        db_root (절대경로 허용, 예: D:/Valve_DB) > ROOT/db.
        설치본과 다른 드라이브에 DB 를 둘 수 있다 (사내: 앱은 C:, DB 는 D:)."""
        try:
            cfg_val = self.global_cfg().get("db_root")
        except Exception:      # pipeline.yaml 미생성(첫 기동/테스트) — 기본값
            cfg_val = None
        raw = os.environ.get("VALVE_DB_ROOT") or str(cfg_val or "db")
        p = Path(raw).expanduser()
        return p if p.is_absolute() else (self.root / p)

    def display_path(self, path: Path) -> str:
        """프로젝트 내부는 상대경로, 외부 DB 루트는 안전하게 절대경로로 표시."""
        try:
            return str(path.relative_to(self.root))
        except ValueError:
            return str(path)

    def raw_dir(self, vehicle: str, source: str) -> Path:
        # raw 는 소스 > vehicle > date=hive 파티션 (FAB/{vehicle}/date=…).
        # event/feature 와 동일하게 vehicle 기준으로 통일.
        return self.db_root() / "1.RAWDATA_DB" / source / vehicle

    def event_dir(self, vehicle: str, source: str = "FAB") -> Path:
        return self.db_root() / "2.EVENT_DB" / vehicle / source

    def event_enabled(self, source: str) -> bool:
        """소스별 event DB 생성 여부 — pipeline.yaml sources.<name>.event (기본 true).
        ET 처럼 raw 만 뽑는 소스는 event: false 로 지정 (raw 전용)."""
        user = (self.global_cfg().get("sources") or {}).get(source) or {}
        return bool(user.get("event", True))

    def event_sources(self) -> list[str]:
        return [s for s in self.sources_cfg() if self.event_enabled(s)]

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
        source_cfg = ((self.global_cfg().get("sources") or {}).get(source) or {})
        payload = {
            "matching_sha": self.matching_sha(source),
            "prefix": str(self.vehicle_cfg(vehicle).get("event_lot_startwith") or ""),
            "match": self.source_match(source),
            # EVENT의 물리 schema도 버전에 포함한다. 조회 컬럼을 바꾼 뒤 예전
            # 파티션을 그대로 두면 날짜별 parquet 폭이 달라져 feature 단계의 concat이
            # 반복 실패한다. table/time_col도 파티션 의미에 영향을 주므로 함께 묶는다.
            "source": {k: source_cfg.get(k) for k in
                       ("table", "columns", "time_col", "event")},
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

    # ── DB 사용량 (제품별 경고 임계) ────────────────────────────
    @staticmethod
    def _dir_bytes(root: Path) -> tuple[int, int]:
        """(바이트, 파일수) — 없으면 (0, 0). scandir 재귀 (심볼릭 링크는 따라가지 않음)."""
        total = files = 0
        stack = [str(root)]
        while stack:
            d = stack.pop()
            try:
                with os.scandir(d) as it:
                    for e in it:
                        try:
                            if e.is_dir(follow_symlinks=False):
                                stack.append(e.path)
                            elif e.is_file(follow_symlinks=False):
                                total += e.stat().st_size
                                files += 1
                        except OSError:
                            continue
            except OSError:
                continue
        return total, files

    def db_warn_bytes(self) -> int:
        """제품 하나의 DB 가 이 크기를 넘으면 경고 — runtime.db_warn_gb (기본 40GB)."""
        try:
            gb = float((self.global_cfg().get("runtime") or {}).get("db_warn_gb") or 40)
        except (TypeError, ValueError):
            gb = 40.0
        if gb <= 0:                 # 0/음수 = 잘못 적은 값 — 기본값으로
            gb = 40.0
        return int(gb * (1024 ** 3))

    def db_usage(self, ttl: float = 600.0, force: bool = False) -> dict:
        """제품별 DB 사용량 — raw/event/feature/report 합계와 경고 여부.

        전체 트리를 걷는 작업이라 기본 10분 캐시한다 (화면 폴링마다 걷지 않도록).
        보존 정책은 두지 않는다 — 임계를 넘으면 알려만 주고, 무엇을 지울지는 사람이 판단한다."""
        now = time.time()
        cache = getattr(self, "_usage_cache", None)
        if not force and cache and (now - cache.get("ts", 0)) < ttl:
            return {**cache, "cached": True}

        warn_at = self.db_warn_bytes()
        gb = 1024 ** 3
        vehicles: dict[str, dict] = {}
        by_source: dict[str, dict] = {}
        for v in self.vehicles():
            parts = {"raw": 0, "event": 0, "feature": 0, "reports": 0}
            files = 0
            # 소스(FAB/INLINE/VM/ET)별 raw·event — "어느 DB 가 큰지" 가 정리 판단의 기준
            sources: dict[str, dict] = {}
            for source in self.sources_cfg():
                rb, rn = self._dir_bytes(self.raw_dir(v, source))
                eb, en = self._dir_bytes(self.event_dir(v, source))
                try:
                    tcol = self._time_col(source)
                except ValueError:      # time_col 설정 오류 — 용량 조회까지 막지는 않는다
                    tcol = None
                sources[source] = {
                    "raw": rb, "event": eb, "bytes": rb + eb,
                    "gb": round((rb + eb) / gb, 2), "files": rn + en,
                    "event_enabled": self.event_enabled(source), "time_col": tcol,
                }
                parts["raw"] += rb
                parts["event"] += eb
                files += rn + en
                agg = by_source.setdefault(source, {"raw": 0, "event": 0, "bytes": 0})
                agg["raw"] += rb
                agg["event"] += eb
                agg["bytes"] += rb + eb
            for key, path in (("feature", self.feature_dir(v)),
                              ("reports", self.report_dir(v))):
                b, n = self._dir_bytes(path)
                parts[key] += b
                files += n
            total = sum(parts.values())
            vehicles[v] = {"bytes": total, "gb": round(total / gb, 2),
                           "files": files, "parts": parts, "sources": sources,
                           "warn": total >= warn_at}
        for agg in by_source.values():
            agg["gb"] = round(agg["bytes"] / gb, 2)
        shared = {}
        for key, path in (("wide", self.wide_dir()), ("send", self.send_dir())):
            b, n = self._dir_bytes(path)
            shared[key] = {"bytes": b, "gb": round(b / gb, 2), "files": n}
        out = {
            "ts": now, "cached": False,
            "db_root": str(self.db_root()),
            "warn_gb": round(warn_at / gb, 2),
            "vehicles": vehicles, "shared": shared, "by_source": by_source,
            "total_bytes": sum(x["bytes"] for x in vehicles.values())
                           + sum(x["bytes"] for x in shared.values()),
            "warn_vehicles": sorted(v for v, x in vehicles.items() if x["warn"]),
        }
        self._usage_cache = out
        return out

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
        # 쿼리 구간은 반열림 [q_from, q_to) — 마지막 (today, today) 유닛도 하루가 된다
        q_from, q_to = raw_query_window(start, end)
        if self.lake_api is not None:
            df = self._query_raw(cfg, source, q_from, q_to)
        else:
            gen = self._mock_for(source)
            df = (gen(cfg, q_from, q_to, split) if gen
                  else self._mock_generic(cfg, q_from, q_to, split, source, sc["columns"],
                                          items=items))
        if items and "item_id" in df.columns:
            df = df.filter(pl.col("item_id").is_in(items))  # 실 어댑터 교체 대비 안전망
        df = self._normalize_raw(df, source, split)
        if df.height == 0:
            # 그 구간에 lot 이 없으면 조회 결과가 빈다 — 에러가 아니라 정상이다.
            # **파티션을 쓰지 않는다**: (1) 0행 파티션이 '최신' 이 되면 진단이 빈 날을
            # 실패로 읽고, (2) 일시적인 빈 응답이 이미 받아 둔 그 날 데이터를 덮어쓴다.
            return 0
        self._write_raw_partitions(cfg["vehicle"], source, df, fallback_date=start)
        return df.height

    def _normalize_raw(self, df: pl.DataFrame | None, source: str, split: str) -> pl.DataFrame:
        """조회 결과를 저장 가능한 모양으로 맞춘다.

        사내 어댑터는 '해당 조건 데이터 없음' 을 None·빈 list 로 주고 lake_api 가
        0행 0열 DataFrame 으로 바꾼다. 여기에 split 리터럴을 그냥 붙이면
        **없는 행이 하나 생긴다** — polars 는 열이 없는 프레임에 리터럴을 붙이면
        길이 1이 된다. 그래서 빈 결과는 리터럴 대신 설정된 조회 컬럼 스키마를 가진
        0행 프레임으로 만든다 (컬럼 없는 parquet 이 저장되면 event 단계가
        root_lot_id 를 못 찾아 그 제품 전체가 죽는다)."""
        if df is None:
            df = pl.DataFrame()
        if df.height:
            if "split" not in df.columns:
                df = df.with_columns(pl.lit(split).alias("split"))
            return df
        cols = list(self.sources_cfg()[source].get("columns") or [])
        ordered = list(dict.fromkeys(list(df.columns) + cols + ["split"]))
        schema = df.schema
        return pl.DataFrame({c: pl.Series(c, [], dtype=schema.get(c, pl.Utf8))
                             for c in ordered})

    def _time_col(self, source: str) -> str | None:
        """raw 를 date= 파티션으로 나누는 **기준 열**.

        해석 순서 (알람 탭 ⚙ '조회 컬럼 · 파티션 기준 열' 에서 편집):
          1) pipeline.yaml sources.<name>.time_col 로 명시한 열 — 없는 열이면
             조용히 엉뚱한 날짜로 저장하는 대신 실패시킨다 (기준 열을 못 찾으면
             하루치가 통째로 쿼리 시작일 파티션으로 들어간다)
          2) 코드 기본값(DEFAULT_SOURCES.time_col = tkout_time) — 단 그 열이 실제
             조회 컬럼에 있을 때만. 기존 설치의 pipeline.yaml 은 seed-only 라
             tkout_time 이 없을 수 있는데, 그때 실패시키면 업그레이드가 파이프라인을
             멈춘다. 그런 경우는 3) 으로 내려가 종전처럼 동작한다
          3) 컬럼 목록에서 'time' 이 들어간 첫 컬럼 (예전 방식)"""
        sc = self.sources_cfg()[source]
        cols = sc["columns"]
        user = ((self.global_cfg().get("sources") or {}).get(source) or {}).get("time_col")
        if user:
            user = str(user).strip()
            if user not in cols:
                raise ValueError(
                    f"{source}.time_col={user!r} 이 columns 에 없습니다 — "
                    f"파티션 기준 열은 조회 컬럼이어야 합니다")
            return user
        dflt = (DEFAULT_SOURCES.get(source) or {}).get("time_col")
        if dflt and dflt in cols:
            return dflt
        for c in cols:
            if "time" in c.lower():
                return c
        return None

    def sources_view(self) -> dict:
        """소스 설정 + 해석된 기준 열 (웹 편집기용).
        time_col = 명시값(없으면 ''), resolved = 실제로 쓰이는 열."""
        out = {}
        gcfg = self.global_cfg().get("sources") or {}
        for name, sc in self.sources_cfg().items():
            user = str((gcfg.get(name) or {}).get("time_col") or "").strip()
            try:
                resolved, error = self._time_col(name), ""
            except ValueError as e:
                resolved, error = None, str(e)
            out[name] = {
                "table": sc["table"], "columns": sc["columns"],
                "time_col": user, "resolved_time_col": resolved, "error": error,
                "event_enabled": self.event_enabled(name),
            }
        return out

    @staticmethod
    def _write_partition(df: pl.DataFrame, pdir: Path):
        """한 date= 파티션 저장 — auto report 와 동일한 data.parquet 파일명.
        구 파일명(part-000)이 남아있으면 제거 (중복 로드 방지)."""
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / "part-000.parquet").unlink(missing_ok=True)
        FeaturePipeline._write_parquet_atomic(
            df, pdir / "data.parquet", compression="zstd", compression_level=3)

    @staticmethod
    def _write_parquet_atomic(df: pl.DataFrame, path: Path, **kwargs):
        """완성된 parquet만 최종 이름에 노출한다.

        파이프라인과 S3/file browser는 서로 다른 worker에서 파일을 읽을 수 있다.
        최종 경로에 직접 쓰면 reader가 footer가 아직 없는 parquet을 집을 수 있으므로
        같은 디렉터리의 임시 파일을 쓴 뒤 os.replace로 교체한다.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            df.write_parquet(tmp, **kwargs)
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)

    @staticmethod
    def _replace_dir_snapshot(build_dir: Path, final_dir: Path):
        """build_dir의 완성된 세대를 final_dir로 교체하고 실패 시 이전 세대를 복원."""
        stamp = f"{os.getpid()}-{time.time_ns()}"
        backup = final_dir.parent / f".{final_dir.name}.backup-{stamp}"
        had_old = final_dir.exists()
        if had_old:
            os.replace(final_dir, backup)
        try:
            os.replace(build_dir, final_dir)
        except Exception:
            if had_old and backup.exists() and not final_dir.exists():
                os.replace(backup, final_dir)
            raise
        if backup.exists():
            shutil.rmtree(backup, ignore_errors=True)

    def _write_raw_partitions(self, vehicle: str, source: str, df: pl.DataFrame, fallback_date):
        """raw 를 auto report daily DB 와 같은 hive partitioning 으로 저장 —
        데이터의 시간 컬럼 날짜별로 date={YYYY-MM-DD}/data.parquet.
        (raw 유닛의 쿼리 구간이 서로 겹치지 않아 유닛 간 같은 파티션을 쓰지 않는다.)
        시간 컬럼이 없거나 비어있으면 쿼리 시작일(fallback_date) 파티션 하나로 저장."""
        root = self.raw_dir(vehicle, source)
        tc = self._time_col(source)
        if df.height and tc and tc in df.columns:
            dated = df.with_columns(
                pl.col(tc).cast(pl.Utf8).str.slice(0, 10).alias("_pdate"))
            for key, part in dated.partition_by("_pdate", as_dict=True).items():
                d = key[0] if isinstance(key, tuple) else key
                if not d:
                    d = str(fallback_date)
                self._write_partition(part.drop("_pdate"), root / f"date={d}")
        else:
            self._write_partition(df, root / f"date={fallback_date}")

    @staticmethod
    def _prune_date_partitions(root: Path, keep_days: int,
                               today: date | None = None) -> list[str]:
        """최근 keep_days개 달력일 밖의 date=YYYY-MM-DD 파티션을 제거."""
        if keep_days <= 0:
            return []
        cutoff = (today or datetime.today().date()) - timedelta(days=keep_days - 1)
        removed = []
        for p in root.glob("date=*"):
            try:
                day = date.fromisoformat(p.name[5:])
            except ValueError:
                continue
            if day < cutoff:
                shutil.rmtree(p, ignore_errors=True)
                removed.append(p.name[5:])
        return sorted(removed)

    def retention_days(self, vehicle: str) -> int:
        cfg = self.vehicle_cfg(vehicle)
        rt = self.global_cfg().get("runtime") or {}
        return max(1, int(cfg.get("event_days_back") or rt.get("raw_days") or 7))

    def prune_raw(self, vehicle: str) -> dict[str, list[str]]:
        days = self.retention_days(vehicle)
        return {source: self._prune_date_partitions(self.raw_dir(vehicle, source), days)
                for source in self.sources_cfg()}

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
                    "file": self.display_path(p), "found": p.exists(), "items": len(it)}
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
            # 매칭된 step 과 같은 prefix·자릿수 (CC955100 SPACER_CVD 바로 뒤) +
            # 같은 설비 — function step 추천(앞뒤 이웃 step 비교) 재현용
            ("CC955200", "SPACER_CVD_B", "CVD_02", "C-500"),
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

    @staticmethod
    def _mock_tkout(rng, start: date, end: date) -> datetime:
        """공정 진행 시각 — 쿼리 구간 [start, end) 안 (파티션 기준 열)."""
        span = max(int((end - start).days) * 86400, 86400)
        return datetime.combine(start, datetime.min.time()) + timedelta(
            seconds=rng.randint(0, span - 1))

    def _mock_inline(self, cfg: dict, start: date, end: date, split: str) -> pl.DataFrame:
        rng = self._rng(cfg, split, "INLINE")
        items = ["ITEM_CD_001", "ITEM_THK_002", "ITEM_OVL_003"]
        fmt = "%Y-%m-%d %H:%M:%S"
        rows = []
        for _ in range(8):
            lot = f"R{rng.randint(0, 199):03d}"
            for w in range(1, 5):
                tk = self._mock_tkout(rng, start, end)      # wafer 단위 track-out
                for item in items:
                    # 측정 시각은 track-out 보다 뒤 — 자정을 넘겨 다음 날이 되기도 한다.
                    # 파티션은 tkout_time 기준이라 그래도 같은 날에 머문다.
                    meas = tk + timedelta(minutes=rng.randint(5, 180))
                    rows.append({
                        "root_lot_id": lot, "wafer_id": str(w), "item_id": item,
                        "value": round(rng.gauss(100, 8), 4),
                        "measure_pos": str(rng.randint(1, 9)),
                        "tkout_time": tk.strftime(fmt), "time": meas.strftime(fmt),
                        "split": split,
                    })
        return pl.DataFrame(rows)

    def _mock_vm(self, cfg: dict, start: date, end: date, split: str) -> pl.DataFrame:
        rng = self._rng(cfg, split, "VM")
        sensors = ["SNS_TEMP_01", "SNS_PRES_02"]
        fmt = "%Y-%m-%d %H:%M:%S"
        rows = []
        for _ in range(8):
            lot = f"R{rng.randint(0, 199):03d}"
            for w in range(1, 5):
                tk = self._mock_tkout(rng, start, end)
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
                        "tkout_time": tk.strftime(fmt),
                        "time": (tk + timedelta(minutes=rng.randint(1, 90))).strftime(fmt),
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
        step_map = (self.step_map(vehicle)
                    .select(
                        pl.col("step_id").cast(pl.Utf8).str.strip_chars(),
                        pl.col("step_desc").cast(pl.Utf8).str.strip_chars())
                    .filter((pl.col("step_id") != "") & (pl.col("step_desc") != "")))
        conflicts = (step_map.group_by("step_id")
                     .agg(pl.col("step_desc").n_unique().alias("_n"))
                     .filter(pl.col("_n") > 1))
        if conflicts.height:
            ids = ", ".join(conflicts["step_id"].head(10).to_list())
            raise ValueError(f"vehicle_matching에 서로 다른 step_desc가 중복됨: {ids}")
        step_map = step_map.unique(subset=["step_id"], keep="last")

        # 구 레이아웃(vehicle 바로 아래 date=*) 잔재 제거
        legacy_root = self.db_root() / "2.EVENT_DB" / vehicle
        for d in legacy_root.glob("date=*"):
            shutil.rmtree(d, ignore_errors=True)

        results = {}
        for source in self.sources_cfg():
            if not self.event_enabled(source):
                # raw 전용 소스(ET) — 과거에 만들어진 event DB 가 있으면 정리
                shutil.rmtree(self.event_dir(vehicle, source), ignore_errors=True)
                continue
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
            skipped: list[str] = []      # 빈/스키마 없는 raw 파티션 (현황에 남긴다)
            for date_dir in sorted(self.raw_dir(vehicle, source).glob("date=*")):
                raw_files = sorted(date_dir.glob("*.parquet"))  # data.parquet (구 part-000 호환)
                out_dir = edir / date_dir.name
                if not raw_files or (not rebuild
                                     and self._event_partition_fresh(raw_files, out_dir)):
                    continue
                # 파티션마다 열이 다를 수 있다 — 사내 조회는 전부 null 인 열을 생략하기도
                # 하고, 예전 빌드가 남긴 컬럼 없는 빈 parquet 이 섞여 있을 수도 있다.
                raw = pl.concat([pl.read_parquet(f) for f in raw_files],
                                how="diagonal_relaxed")
                rows_in += raw.height
                if raw.height == 0 or "root_lot_id" not in raw.columns:
                    # 빈 날(그 구간에 lot 이 없음)이거나 KEY 열이 없는 파티션.
                    # 여기서 예외를 올리면 그 제품의 event 단계가 통째로 멈춘다 —
                    # 한 날짜의 빈 조각이 나머지 날짜까지 못 만들게 하지 않는다.
                    skipped.append(date_dir.name[5:])
                    continue
                event = raw.filter(pl.col("root_lot_id").cast(pl.Utf8).str.starts_with(prefix))
                if match["kind"] == "item" and match["id_col"] in event.columns:
                    event = event.filter(pl.col(match["id_col"]).is_in(sorted(item_ids)))
                elif match["kind"] == "step" and "step_id" in event.columns:
                    # vehicle_matching은 단순 허용 목록이 아니라
                    # step_id → function step(step_desc) 계약이다. raw step_desc를
                    # 그대로 두면 flow에서 승인한 신규 step이 FAB 룰과 연결되지 않는다.
                    event = (event.with_columns(pl.col("step_id").cast(pl.Utf8).str.strip_chars())
                             .join(step_map.rename({"step_desc": "_matched_step_desc"}),
                                   on="step_id", how="inner"))
                    if "step_desc" in event.columns:
                        event = event.drop("step_desc")
                    event = event.rename({"_matched_step_desc": "step_desc"})
                # kind == "none" → root_lot prefix 필터만 적용
                configured = self.sources_cfg()[source].get("columns") or []
                keep = [c for c in configured if c in event.columns]
                if "split" in event.columns and "split" not in keep:
                    keep.append("split")
                event = event.select(keep)
                if source == "FAB":
                    event = event.select(pl.all().cast(pl.String))
                self._write_partition(event, out_dir)
                rows_out += event.height
                parts += 1

            edir.mkdir(parents=True, exist_ok=True)
            mf = self.matching_file(source)
            meta_path.write_text(json.dumps({
                "ver": ver, "sha": self.matching_sha(source), "ts": time.time(),
                "file": self.display_path(mf) if mf else None,
                "prefix": prefix, "match": self.source_match(source),
            }, ensure_ascii=False), encoding="utf-8")
            results[source] = {"raw_rows": rows_in, "event_rows": rows_out,
                               "partitions": parts, "rebuilt": rebuild,
                               "empty_partitions": skipped,
                               "pruned": self._prune_date_partitions(
                                   edir, self.retention_days(vehicle))}
        return results

    @staticmethod
    def _event_partition_fresh(raw_files: list, out_dir: Path) -> bool:
        """이 날짜의 event 파티션이 raw 보다 최신인가.

        raw 는 롤링 윈도우(raw_days)로 매 실행 재조회되고 실패 유닛도 재시도로
        다시 받아진다 — 그때마다 파티션이 새 데이터로 덮인다. 예전엔 'event
        파일이 있으면 skip' 이라 늦게 도착한 데이터가 event 에 영영 반영되지
        않았다(첫 스냅샷 고정). raw 가 event 보다 새로 쓰였으면 다시 만든다."""
        if not out_dir.exists():
            return False
        out_files = list(out_dir.glob("*.parquet"))
        if not out_files:
            return False
        try:
            raw_m = max(f.stat().st_mtime for f in raw_files)
            ev_m = max(f.stat().st_mtime for f in out_files)
        except OSError:
            return False
        return ev_m >= raw_m

    @staticmethod
    def _partition_files(root: Path) -> list[Path]:
        """hive 파티션(date=*) 아래 parquet 전부 — data.parquet (구 part-000 호환)."""
        return sorted(root.glob("date=*/*.parquet"))

    def _load_event(self, vehicle: str, source: str = "FAB") -> pl.DataFrame | None:
        files = self._partition_files(self.event_dir(vehicle, source))
        if not files:
            return None
        # 사내 조회 결과는 전부 null인 열을 날짜에 따라 생략할 수 있고, 설정 변경
        # 직후에는 이전/새 schema 파티션이 잠시 공존할 수 있다. 열 이름 기준 union과
        # 공통 dtype 승격으로 읽어 누락 열은 null로 채운다.
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")

    def _load_raw(self, vehicle: str, source: str) -> pl.DataFrame | None:
        files = self._partition_files(self.raw_dir(vehicle, source))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")

    def event_dates(self, vehicle: str, source: str = "FAB") -> list[str]:
        """소스 event DB 의 날짜 파티션 목록 (오름차순). feature 커버 구간의 근거."""
        return sorted(p.name[5:] for p in self.event_dir(vehicle, source).glob("date=*"))

    def event_date_count(self, vehicle: str) -> int:
        """event DB 에 쌓인 전체 날짜 파티션 수 (event 소스 통합). feature 는 이 전체 대상."""
        dates = set()
        for source in self.event_sources():
            dates.update(self.event_dates(vehicle, source))
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
        final_fdir = self.feature_dir(vehicle)
        final_fdir.parent.mkdir(parents=True, exist_ok=True)
        backups = sorted(final_fdir.parent.glob(f".{final_fdir.name}.backup-*"),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        if not final_fdir.exists() and backups:
            os.replace(backups[0], final_fdir)
            backups = backups[1:]
        for stale in backups:
            shutil.rmtree(stale, ignore_errors=True)
        # 이전 실패가 남긴 비공개 build 디렉터리는 산출물이 아니므로 제거한다.
        for stale in final_fdir.parent.glob(f".{final_fdir.name}.build-*"):
            shutil.rmtree(stale, ignore_errors=True)
        fdir = final_fdir.parent / (
            f".{final_fdir.name}.build-{os.getpid()}-{time.time_ns()}")
        fdir.mkdir(parents=True, exist_ok=False)

        features: dict[str, list[str]] = {"fab": [], "knob": [], "mask": [], "inline": [], "vm": []}
        skipped: list[dict] = []  # 컬럼 미추출 등으로 건너뛴 feature (사유 포함)

        # 관리자 커스텀 함수 (config/feature_funcs.py) — 값 생성은 내장과 병합, agg 는 별도 전달
        custom_vals, custom_aggs, func_errors = self.feature_funcs()
        skipped.extend(func_errors)
        value_rules = {**FEATURE_RULES, **custom_vals}

        # FAB — Ref_feature 그대로: step_desc × feature_name × agg
        #   단, FORCED_AGG 에 있는 feature(ecuall)는 룰북 agg 를 무시하고 고정 규칙 —
        #   step 별로 다르게 뽑지 않는다. 무엇이 바뀌었는지는 agg_overrides 로 노출.
        agg_overrides: list[dict] = []
        rules = self.rules_csv("fab")
        if rules is not None:
            for r in rules.iter_rows(named=True):
                step, fname, agg = r["step_desc"], r["feature_name"], r["agg"]
                forced = FORCED_AGG.get(fname)
                if forced and agg != forced:
                    agg_overrides.append({"feature": f"FAB_{step}_{fname}",
                                          "csv_agg": agg, "applied": forced})
                    agg = forced
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

        # feature 커버 구간 기록 — feature 는 "그때 event DB 에 있던 전체" 를 대상으로
        # 산출되므로, 무엇을 며칠치 담았는지는 지금 남겨두지 않으면 나중에 알 수 없다
        # (feature parquet 은 root_lot·wafer 키만 있고 날짜 컬럼이 없다).
        # 카테고리 → 입력 소스: fab/knob/mask ← FAB · inline ← INLINE · vm ← VM
        cov = {}
        for src in ("FAB", "INLINE", "VM"):
            ds = self.event_dates(vehicle, src)
            if ds:
                cov[src] = {"days": len(ds), "start": ds[0], "end": ds[-1]}
        (fdir / "_meta.json").write_text(
            json.dumps({"ts": time.time(), "sources": cov,
                        "features": {k: len(v) for k, v in features.items()}},
                       ensure_ascii=False, indent=2), encoding="utf-8")

        # knob miss / skip 리포트 저장
        rdir = self.report_dir(vehicle)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "knob_miss.json").write_text(
            json.dumps(knob_miss_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        (rdir / "knob_skip.json").write_text(
            json.dumps(knob_skip_rows, ensure_ascii=False, indent=2), encoding="utf-8")

        # 이 실행에서 생성된 파일만 feature store의 최신 세대로 승격한다.
        # 삭제된 룰, 이름이 바뀐 룰, 이번 실행에서 값이 없어진 feature의 구 parquet은
        # 이전 디렉터리와 함께 제거되어 run_wide에 다시 섞이지 않는다.
        self._replace_dir_snapshot(fdir, final_fdir)

        return {
            "features": {k: len(v) for k, v in features.items()},
            "files": features,
            "knob_miss": knob_miss_rows,
            "knob_skip": knob_skip_rows,
            "skipped": skipped,
            "agg_overrides": agg_overrides,   # 룰북 agg 대신 고정 규칙을 쓴 feature
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
        FeaturePipeline._write_parquet_atomic(feat, fdir / fname)
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

        if wide.height == 0:
            raise RuntimeError("wide 검증 실패: row가 0건입니다")
        null_keys = wide.select(
            pl.any_horizontal([pl.col(c).is_null() for c in WIDE_KEY]).sum()).item()
        if null_keys:
            raise RuntimeError(f"wide 검증 실패: KEY null {null_keys}건")
        if wide.unique(subset=WIDE_KEY).height != wide.height:
            raise RuntimeError("wide 검증 실패: PRODUCT/root_lot/wafer KEY 중복")

        wdir = self.wide_dir()
        wdir.mkdir(parents=True, exist_ok=True)
        out = wdir / f"ML_TABLE_{vehicle}.parquet"
        self._write_parquet_atomic(wide, out, compression="zstd", statistics=True)
        return {"rows": wide.height, "features": wide.width - len(WIDE_KEY),
                "path": self.display_path(out)}

    def run_flow_tables(self) -> dict:
        """vehicle별 내부 wide를 PRODUCT별 canonical ML_TABLE로 DB 루트에 발행.

        Flow는 db_root 직하의 ``ML_TABLE_*.parquet``만 제품으로 인식한다. 내부
        ``4.WIDE_FORM/ML_TABLE_{vehicle}`` 구조는 유지하되, 전달 계약은
        ``db_root/ML_TABLE_{product}.parquet``로 고정한다. 같은 product에 vehicle이
        여러 개면 diagonal concat 후 wafer KEY 중복은 마지막 vehicle 산출을 사용한다.
        """
        files = sorted(self.wide_dir().glob("ML_TABLE_*.parquet"))
        by_product: dict[str, list[pl.DataFrame]] = {}
        for f in files:
            df = pl.read_parquet(f)
            if not set(WIDE_KEY) <= set(df.columns):
                raise RuntimeError(f"Flow 발행 검증 실패: {f.name} KEY 컬럼 누락")
            for (product,), part in df.partition_by("PRODUCT", as_dict=True).items():
                product = str(product or "").strip()
                if product:
                    by_product.setdefault(product, []).append(part)
        if not by_product:
            raise RuntimeError("Flow 발행할 PRODUCT wide table이 없습니다")

        root = self.db_root()
        manifest_path = root / ".valve_ml_tables.json"
        previous: set[str] = set()
        if manifest_path.exists():
            try:
                previous = set(json.loads(manifest_path.read_text(encoding="utf-8")).get("files") or [])
            except Exception:
                previous = set()
        published = []
        for product, frames in sorted(by_product.items()):
            out_df = (pl.concat(frames, how="diagonal_relaxed")
                      .unique(subset=WIDE_KEY, keep="last")
                      .sort(WIDE_KEY))
            name = f"ML_TABLE_{safe_filename(product)}.parquet"
            self._write_parquet_atomic(out_df, root / name,
                                       compression="zstd", statistics=True)
            published.append(name)

        # Valve가 이전에 관리하던 product만 정리한다. 운영자가 둔 다른 ML_TABLE은
        # manifest에 없으므로 건드리지 않는다.
        for name in previous - set(published):
            (root / name).unlink(missing_ok=True)
        meta = {"schema": 1, "ts": time.time(), "files": published}
        tmp = manifest_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, manifest_path)
        return {"files": published, "products": len(published)}

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
            csv_out = gdir / f"{name}_ML_TABLE.csv"
            csv_tmp = csv_out.with_name(
                f".{csv_out.name}.{os.getpid()}.{threading.get_ident()}.tmp")
            try:
                gdf.write_csv(csv_tmp)
                os.replace(csv_tmp, csv_out)
            finally:
                csv_tmp.unlink(missing_ok=True)
            self._write_parquet_atomic(
                gdf, gdir / f"{name}_ML_TABLE.parquet",
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
        us_cfg = self.global_cfg().get("unmatched_scan") or {}
        excl = us_cfg.get("exclude") or {}
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

        new_steps = {x["step_id"] for x in unmatched}
        step_extras, present_cols = self._alert_step_extras(raw, new_steps, us_cfg)
        step_hints = self._step_match_hints(raw, new_steps, us_cfg, vehicle)
        report = {"product": cfg["product"], "vehicle": vehicle,
                  "unmatched": unmatched, "excluded": excluded,
                  "exclude_config": {"eqp_id": eqp_pats, "eqp_model": model_pats},
                  "alert_cols": present_cols, "step_extras": step_extras,
                  "step_hints": step_hints}
        rdir = self.report_dir(vehicle)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "unmatched.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return report

    def _alert_step_extras(self, raw: pl.DataFrame, steps: set,
                           us_cfg: dict) -> tuple[dict, list[str]]:
        """미매칭 step 별 알람 부가정보 — flow 매칭알람 화면과 function step
        추천(LLM)의 입력이 된다.

        반환: ({step_id: {examples: [{root_lot_id, wafer_id}, …],
                          cols: {열: "값1, 값2"}}}, 실제 존재한 전송 열 목록)
        examples 는 (root_lot_id, wafer_id) 쌍 최대 example_limit 개,
        cols 는 unmatched_scan.alert_cols 중 raw 에 있는 열의 대표값(unique 최대 5개).
        """
        try:
            limit = max(1, min(20, int(us_cfg.get("example_limit")
                                       or ALERT_EXAMPLE_LIMIT_DEFAULT)))
        except (TypeError, ValueError):
            limit = ALERT_EXAMPLE_LIMIT_DEFAULT
        use_cols = [c for c in alert_scan_cols(us_cfg)
                    if c in raw.columns and c not in ("step_id", "step_desc")]
        if not steps:
            return {}, use_cols
        sub = raw.filter(pl.col("step_id").is_in(list(steps)))
        pair_cols = [c for c in KEY_COLS if c in sub.columns]
        aggs = []
        if pair_cols:
            # limit 보다 넉넉히 뽑아 두고 아래에서 lot 다양성 우선으로 추린다
            aggs.append(pl.struct(pair_cols).unique(maintain_order=True)
                        .head(max(50, limit * 10)).alias("_examples"))
        for c in use_cols:
            aggs.append(pl.col(c).cast(pl.Utf8).drop_nulls()
                        .unique(maintain_order=True).head(5).alias(f"_col_{c}"))
        if not aggs:
            return {}, use_cols
        out: dict[str, dict] = {}
        for r in sub.group_by("step_id").agg(aggs).iter_rows(named=True):
            entry: dict = {"examples": [], "cols": {}}
            pairs = [{k: ("" if v is None else str(v)) for k, v in (e or {}).items()}
                     for e in (r.get("_examples") or [])]
            # 서로 다른 lot 을 우선(lot 당 첫 wafer), 모자라면 나머지 쌍으로 채움
            seen_lots: set = set()
            picked: list[dict] = []
            for p in pairs:
                lot = p.get("root_lot_id")
                if lot in seen_lots:
                    continue
                seen_lots.add(lot)
                picked.append(p)
                if len(picked) >= limit:
                    break
            for p in pairs:
                if len(picked) >= limit:
                    break
                if p not in picked:
                    picked.append(p)
            entry["examples"] = picked
            for c in use_cols:
                vals = [str(v) for v in (r.get(f"_col_{c}") or []) if str(v or "")]
                entry["cols"][c] = ", ".join(vals)
            out[str(r["step_id"])] = entry
        return out, use_cols

    def _recent_raw(self, df: pl.DataFrame, source: str,
                    days: int) -> tuple[pl.DataFrame, dict]:
        """raw 를 "가장 최근 N 개 날짜" 로 줄인다 (파티션 기준 열의 날짜).

        오늘 기준이 아니라 데이터에 실제로 있는 날짜 기준이다 — DB 가 며칠 밀려
        있어도 최근 N 일치가 남는다."""
        try:
            tc = self._time_col(source)
        except ValueError:      # time_col 설정 오류 — 추천 컨텍스트까지 막지는 않는다
            tc = None
        if not tc or tc not in df.columns or df.is_empty():
            return df, {}
        dated = df.with_columns(pl.col(tc).cast(pl.Utf8).str.slice(0, 10).alias("_pdate"))
        dates = sorted({d for d in dated["_pdate"].to_list() if d})
        if not dates:
            return df, {}
        keep = dates[-max(1, int(days)):]
        sub = dated.filter(pl.col("_pdate").is_in(keep)).drop("_pdate")
        return sub, {"from": keep[0], "to": keep[-1], "dates": len(keep), "time_col": tc}

    def _step_match_hints(self, raw: pl.DataFrame, steps: set,
                          us_cfg: dict, vehicle: str) -> dict:
        """신규(미매칭) step 별 "앞뒤 이웃 step" 컨텍스트 — flow 의 function step
        추천(GPT OSS 120B)이 쓰는 근거 자료.

        step_id 는 같은 prefix·자릿수 안에서 번호가 공정 순서를 뜻한다
        (AA100002 는 AA100000 과 AA100006 사이). 그래서 번호만 가까운 것을 고르는
        대신, 최근 며칠치 FAB raw 에서 그 이웃 step 들이 실제로 어떤
        ppid/eqp_id/eqp_model/area 로 돌았는지를 같이 실어 보낸다. 판단은 flow 가 한다:
          · 신규 step 의 ppid 가 이웃 step 의 ppid 에 있으면 그 step 과 같은 function step
          · 새 ppid 면 eqp_id·eqp_model·area 가 겹치는 이웃 step 이 후보

        반환: {step_id: {step_id, prefix, number, days, window, cols,
                         rows, n_lots, values{열: [값…]},
                         neighbors: [{step_id, step_desc, direction, gap,
                                      rows, n_lots, values{…}}]}}
        """
        cfg = alert_hint_cfg(us_cfg)
        if not cfg["enabled"] or not steps:
            return {}
        cols = [c for c in cfg["cols"] if c in raw.columns and c != "step_id"]
        if not cols or "step_id" not in raw.columns:
            return {}
        recent, window = self._recent_raw(raw, "FAB", cfg["days"])
        try:
            smap = {str(r["step_id"]): str(r.get("step_desc") or "")
                    for r in self.step_map(vehicle).iter_rows(named=True)}
        except Exception:
            smap = {}

        aggs: list = [pl.len().alias("_rows")]
        has_lot = "root_lot_id" in recent.columns
        if has_lot:
            aggs.append(pl.col("root_lot_id").n_unique().alias("_n_lots"))
        for c in cols:
            aggs.append(pl.col(c).cast(pl.Utf8).drop_nulls().unique(maintain_order=True)
                        .head(cfg["value_limit"]).alias(f"_v_{c}"))
        stat: dict[str, dict] = {}
        for r in recent.group_by("step_id").agg(aggs).iter_rows(named=True):
            stat[str(r["step_id"])] = {
                "rows": int(r.get("_rows") or 0),
                "n_lots": int(r.get("_n_lots") or 0) if has_lot else 0,
                "values": {c: [str(v) for v in (r.get(f"_v_{c}") or []) if str(v or "")]
                           for c in cols},
            }

        empty = {"rows": 0, "n_lots": 0, "values": {c: [] for c in cols}}
        out: dict[str, dict] = {}
        for sid in sorted(str(s) for s in steps):
            me = stat.get(sid) or empty
            entry = {"step_id": sid, "days": cfg["days"], "window": window,
                     "cols": cols, "rows": me["rows"], "n_lots": me["n_lots"],
                     "values": me["values"], "neighbors": []}
            parsed = split_step_id(sid)
            if parsed:
                prefix, num, width = parsed
                entry["prefix"], entry["number"] = prefix, num
                cands = []
                for other, info in stat.items():
                    # 이웃은 "이미 매칭된 step" 만 — function step 이 있어야 추천이 된다
                    if other == sid or not smap.get(other):
                        continue
                    p2 = split_step_id(other)
                    if not p2 or p2[0] != prefix or p2[2] != width:
                        continue
                    cands.append((abs(p2[1] - num), p2[1], other, info))
                near = (sorted((c for c in cands if c[1] < num))[:cfg["neighbors"]]
                        + sorted((c for c in cands if c[1] > num))[:cfg["neighbors"]])
                for gap, onum, other, info in sorted(near, key=lambda c: c[1]):
                    entry["neighbors"].append({
                        "step_id": other, "step_desc": smap.get(other, ""),
                        "direction": "prev" if onum < num else "next", "gap": gap,
                        "rows": info["rows"], "n_lots": info["n_lots"],
                        "values": info["values"],
                    })
            out[sid] = entry
        return out

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
            raw[source] = sorted({p.parent.name[5:] for p in
                                  self._partition_files(self.raw_dir(vehicle, source))})
            if not self.event_enabled(source):
                continue  # raw 전용 소스(ET) — event 현황 없음 (UI 는 raw 전용으로 표시)
            edir = self.event_dir(vehicle, source)
            dates = sorted({p.parent.name[5:] for p in self._partition_files(edir)})
            meta = {}
            meta_path = edir / "_meta.json"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    meta = {}
            # 미처리 = event 파티션이 없거나, raw 가 그 뒤에 다시 받아진 날짜
            # (롤링 재조회·재시도 — run_event 의 _event_partition_fresh 와 같은 기준)
            pending = []
            for d in raw[source]:
                rf = sorted((self.raw_dir(vehicle, source) / f"date={d}").glob("*.parquet"))
                if rf and not self._event_partition_fresh(rf, edir / f"date={d}"):
                    pending.append(d)
            event[source] = {
                "dates": dates,
                "pending": pending,
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

        # feature 가 실제로 담은 구간 — 산출 시점에 남긴 _meta.json 이 정답이다.
        # 메타가 없는 기존 산출물은 현재 event 파티션으로 갈음하고 approx 로 표시한다
        # (그 사이 event 가 늘었다면 실제 커버보다 넓게 보일 수 있음).
        feature_cov, feature_ts = {}, None
        meta_path = fdir / "_meta.json"
        if meta_path.exists():
            try:
                fm = json.loads(meta_path.read_text(encoding="utf-8"))
                feature_cov = fm.get("sources") or {}
                feature_ts = fm.get("ts")
            except Exception:
                feature_cov = {}
        for source, ev in event.items():
            ds = ev.get("dates") or []
            if source not in feature_cov and ds:
                feature_cov[source] = {"days": len(ds), "start": ds[0], "end": ds[-1],
                                       "approx": True}

        matching_path = self.matching_file("FAB")
        return {
            "vehicle": vehicle,
            "product": cfg["product"],
            "matching": {"steps": self.step_map(vehicle).height,
                         "mtime": matching_path.stat().st_mtime if matching_path.exists() else None},
            "raw": raw,
            "event": event,
            "features": features,
            "feature_cov": feature_cov,   # 소스 → {days, start, end, approx?}
            "feature_ts": feature_ts,     # feature 산출 시각 (epoch)
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
