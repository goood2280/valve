"""
Valve · feature_pipeline
------------------------
Ref_raw_query / Ref_event / Ref_feature 3단계 파이프라인의 Valve 통합판.

  1) raw   : vehicle 설정(QueryTimeSpan/SplitTimeSpan)대로 split 을 나눠
             FAB · INLINE · VM 을 쿼리 → db/1.RAWDATA_DB/{SOURCE}/{product}/date=… (flow canonical)
  2) event : FAB raw 를 vehicle_matching(step_id↔step_desc) inner join +
             root_lot prefix 필터 → db/2.EVENT_DB/{vehicle}/date=…
  3) feature: 카테고리별 규칙 CSV (fab / knob_ppid / mask / inline / vm) 에 따라
             FAB_… KNOB_… MASK_… INLINE_… VM_… feature parquet 생성
             → db/3.FEATURE_STORE/{vehicle}/

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


def aggregate_feature(df: pl.DataFrame, feature_col: str, agg_type: str) -> pl.DataFrame:
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
    if agg_type == "agg":
        return df.group_by(KEY_COLS).agg(pl.col("val").unique().sort().str.join("_").alias(feature_col))
    raise ValueError(f"unknown agg type: {agg_type}")


NUM_AGGS = {
    "mean": lambda col, name: pl.col(col).mean().alias(name),
    "max": lambda col, name: pl.col(col).max().alias(name),
    "min": lambda col, name: pl.col(col).min().alias(name),
    "last": lambda col, name: pl.col(col).last().alias(name),
    "first": lambda col, name: pl.col(col).first().alias(name),
}


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

    def raw_dir(self, product: str, source: str) -> Path:
        # raw 는 소스 > 제품 > date=hive 파티션. (event/feature 는 vehicle 기준)
        return self.db_root() / "1.RAWDATA_DB" / source / product

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

    def feature_dir(self, vehicle: str) -> Path:
        return self.db_root() / "3.FEATURE_STORE" / vehicle

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
        (서로 다른 파티션 파일만 씀 → race 없음)."""
        sc = self.sources_cfg()[source]
        gen = self._mock_for(source)
        df = gen(cfg, start, end, split) if gen else self._mock_generic(cfg, start, end, split, source, sc["columns"])
        keep = [c for c in sc["columns"] if c in df.columns] + ["split"]
        df = df.select(keep)
        out = self.raw_dir(cfg["product"], source) / f"date={start}"
        out.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out / "part-000.parquet", compression="zstd", compression_level=3)
        return df.height

    def run_raw_query(self, vehicle: str) -> dict:
        """전 (source, 날짜) 유닛을 순차 실행 (병렬은 pipeline_runner 가 담당)."""
        cfg = self.vehicle_cfg(vehicle)
        sources = self.sources_cfg()
        stats = {"splits": [], "rows": {name: 0 for name in sources},
                 "tables": {name: sc["table"] for name, sc in sources.items()}}
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
        knob = self.rules_csv("knob")
        ppid_pool: dict[str, list[str]] = {}
        if knob is not None:
            for r in knob.filter(pl.col("vehicle") == vehicle).iter_rows(named=True):
                ppid_pool.setdefault(r["step_id"], []).append(r["ppid"])
        for sid, pool in ppid_pool.items():
            pool.append(f"PP_X9_{sid[-4:]}")  # 매핑에 없는 raw ppid

        n_lots = 8
        rows = []
        seq = 0
        span_sec = max(int((datetime.combine(end, datetime.min.time())
                            - datetime.combine(start, datetime.min.time())).total_seconds()), 3600)
        for li in range(n_lots):
            # 일부 lot 은 prefix 미충족 (event 필터에서 제거되는 것 재현)
            lot = f"R{rng.randint(0, 199):03d}" if rng.random() > 0.15 else f"Q{rng.randint(0, 99):03d}"
            for w in range(1, rng.randint(3, 6)):
                for sid, sdesc in matched + [(u[0], u[1]) for u in unmatched_pool]:
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
                    tk = datetime.combine(start, datetime.min.time()) + timedelta(seconds=rng.randint(0, span_sec))
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
                      source: str, columns: list[str]) -> pl.DataFrame:
        """설정된 columns 만으로 합성 raw 생성 — 새 소스(ET·QTIME 등)를 코드 수정 없이 mock.
        step_id/item_id/value/time 등 흔한 컬럼은 의미있게, 나머지는 난수 문자열."""
        rng = self._rng(cfg, split, source)
        matched = self.step_map(cfg["vehicle"]).select("step_id").to_series().to_list()
        prefix = str(cfg.get("event_lot_startwith") or "R")
        rows = []
        for _ in range(8):
            lot = f"{prefix}{rng.randint(0, 199):03d}"
            for w in range(1, 5):
                row = {"root_lot_id": lot, "wafer_id": str(w), "split": split}
                for c in columns:
                    if c in row:
                        continue
                    if c == "step_id":
                        row[c] = rng.choice(matched) if matched else "CC000000"
                    elif c in ("item_id", "sensor_id", "test_item", "pattern_id"):
                        row[c] = f"{source}_{rng.randint(1, 3):02d}"
                    elif c in ("value", "predicted_value", "actual_value", "residual"):
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
    #    매칭 파일 내용(sha)이 바뀌면 해당 소스 event DB 전체 재생성.
    #    적용된 매칭 버전은 파티션 옆 _meta.json 에 기록 (히트맵/현황 표시용).
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
            sha = self.matching_sha(source)
            meta_path = edir / "_meta.json"
            meta = {}
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    meta = {}
            rebuild = meta.get("sha") != sha
            if rebuild:
                for d in edir.glob("date=*"):
                    shutil.rmtree(d, ignore_errors=True)

            rows_in = rows_out = parts = 0
            for date_dir in sorted(self.raw_dir(cfg["product"], source).glob("date=*")):
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
                "sha": sha, "ts": time.time(),
                "file": str(mf.relative_to(self.root)) if mf else None,
            }, ensure_ascii=False), encoding="utf-8")
            results[source] = {"raw_rows": rows_in, "event_rows": rows_out,
                               "partitions": parts, "rebuilt": rebuild}
        return results

    def _load_event(self, vehicle: str, source: str = "FAB") -> pl.DataFrame | None:
        files = sorted(self.event_dir(vehicle, source).glob("date=*/part-000.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files])

    def _load_raw(self, product: str, source: str) -> pl.DataFrame | None:
        files = sorted(self.raw_dir(product, source).glob("date=*/part-000.parquet"))
        if not files:
            return None
        return pl.concat([pl.read_parquet(f) for f in files])

    # ─────────────────────────────────────────
    # 3) FEATURE  (fab / knob / mask / inline / vm)
    # ─────────────────────────────────────────
    def run_feature(self, vehicle: str) -> dict:
        event = self._load_event(vehicle, "FAB")
        if event is None:
            raise RuntimeError("event DB 없음 — raw/event 단계를 먼저 실행하세요")
        fdir = self.feature_dir(vehicle)
        fdir.mkdir(parents=True, exist_ok=True)

        features: dict[str, list[str]] = {"fab": [], "knob": [], "mask": [], "inline": [], "vm": []}
        skipped: list[dict] = []  # 컬럼 미추출 등으로 건너뛴 feature (사유 포함)

        # FAB — Ref_feature 그대로: step_desc × feature_name × agg
        rules = self.rules_csv("fab")
        if rules is not None:
            for r in rules.iter_rows(named=True):
                step, fname, agg = r["step_desc"], r["feature_name"], r["agg"]
                df = event.filter(pl.col("step_desc") == step)
                if df.height == 0:
                    continue
                col = f"FAB_{step}_{fname}"
                try:
                    feat = aggregate_feature(df.with_columns(FEATURE_RULES[fname]().alias("val")), col, agg)
                    self._save_feature(fdir, feat, col, features["fab"])
                except Exception as e:
                    skipped.append({"feature": col, "reason": str(e)})

        # KNOB — knob_ppid 매핑. 매핑 실패분은 raw ppid(RO) 유지 + miss 리포트
        knob_miss_rows: list[dict] = []
        knob = self.rules_csv("knob")
        if knob is not None and "ppid" not in event.columns:
            skipped.append({"feature": "KNOB_*", "reason": "ppid 컬럼 미추출 (sources.FAB.columns 확인)"})
            knob = None
        if knob is not None:
            vknob = knob.filter(pl.col("vehicle") == vehicle)
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
                col = f"KNOB_{sdesc}_ppid"
                df = df.with_columns(
                    pl.coalesce([pl.col("knob_val"), pl.col("ppid")]).alias("val"))
                feat = aggregate_feature(df, col, "last")
                self._save_feature(fdir, feat, col, features["knob"])

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
                    feat = aggregate_feature(df.with_columns(build_part_reticle().alias("val")), col, agg)
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
                    feat = (df.sort("time").group_by(KEY_COLS)
                              .agg(NUM_AGGS[agg]("value", col)))
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
                    feat = (df.sort("time").group_by(KEY_COLS)
                              .agg(NUM_AGGS[agg]("residual", col)))
                    self._save_feature(fdir, feat, col, features["vm"])
                except Exception as e:
                    skipped.append({"feature": col, "reason": str(e)})

        # knob miss 리포트 저장
        rdir = self.report_dir(vehicle)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "knob_miss.json").write_text(
            json.dumps(knob_miss_rows, ensure_ascii=False, indent=2), encoding="utf-8")

        return {
            "features": {k: len(v) for k, v in features.items()},
            "files": features,
            "knob_miss": knob_miss_rows,
            "skipped": skipped,
        }

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
    # 미매칭 step 스캔 (제품별) + 전역 exclude 적용
    # ─────────────────────────────────────────
    def scan_unmatched(self, vehicle: str) -> dict:
        cfg = self.vehicle_cfg(vehicle)
        raw = self._load_raw(cfg["product"], "FAB")
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
        unmatched = self.scan_unmatched(vehicle)
        return {"vehicle": vehicle, "raw": raw, "event": event,
                "feature": feature, "unmatched": unmatched}

    def status(self, vehicle: str) -> dict:
        """raw/event/feature 처리 현황 — 소스별로 event 가 raw 대비 어디까지 처리됐는지,
        매칭 파일(sha) 변경으로 전체 재생성이 필요한(stale) 소스는 어디인지,
        각 event DB 가 언제/어떤 매칭 버전으로 갱신됐는지(applied_ts/sha)."""
        cfg = self.vehicle_cfg(vehicle)

        raw, event = {}, {}
        for source in self.sources_cfg():
            raw[source] = [p.parent.name[5:] for p in
                           sorted(self.raw_dir(cfg["product"], source).glob("date=*/part-000.parquet"))]
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
                "stale": bool(dates) and meta.get("sha") != self.matching_sha(source),
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
