"""Valve fab_scanner -- FAB DB scan for missing steps + unmatched PPIDs."""
from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import yaml

from backend.core.feature_pipeline import FeaturePipeline

if TYPE_CHECKING:
    from backend.core.lake_api import LakeAPI

logger = logging.getLogger(__name__)


class FabDbClient(ABC):
    """FAB DB query abstract interface."""
    @abstractmethod
    def query_step_data(self, vehicle_cfg: dict, scan_cfg: dict) -> pl.DataFrame: ...


class LocalFabDbClient(FabDbClient):
    """Reads local pipeline FAB parquet or generates mock."""

    def __init__(self, pipe: FeaturePipeline):
        self.pipe = pipe

    def query_step_data(self, vehicle_cfg: dict, scan_cfg: dict) -> pl.DataFrame:
        vehicle = vehicle_cfg["vehicle"]
        raw = self.pipe._load_raw(vehicle, "FAB")
        if raw is not None:
            return self._filter_raw(raw, scan_cfg)
        return self._mock(vehicle_cfg, scan_cfg)

    def _filter_raw(self, raw: pl.DataFrame, scan_cfg: dict) -> pl.DataFrame:
        eqp_filter: list[str] = scan_cfg.get("eqp_filter") or []
        mode = str(scan_cfg.get("eqp_filter_mode", "eqp_id"))
        if eqp_filter and mode in raw.columns:
            raw = raw.filter(pl.col(mode).is_in(eqp_filter))
        base = ["step_id", "root_lot_id", "wafer_id", "ppid"]
        extra: list[str] = scan_cfg.get("extra_columns") or []
        filter_cols = ["step_desc", "eqp_model"]
        keep = list(dict.fromkeys(base + extra + [c for c in filter_cols if c in raw.columns]))
        keep = [c for c in keep if c in raw.columns]
        return raw.select(keep)

    def _mock(self, vehicle_cfg: dict, scan_cfg: dict) -> pl.DataFrame:
        vehicle = vehicle_cfg["vehicle"]
        rng = random.Random(f"scan|{vehicle}")
        matched = self.pipe.step_map(vehicle).select("step_id").to_series().to_list()
        unmatched = [f"XX{rng.randint(100000, 999999)}" for _ in range(3)]
        knob = self.pipe.knob_map(vehicle)
        ppid_map: dict[str, list[str]] = {}
        if knob is not None:
            for r in knob.iter_rows(named=True):
                ppid_map.setdefault(r["step_id"], []).append(r["ppid"])
        extra: list[str] = scan_cfg.get("extra_columns") or []
        rows: list[dict] = []
        for sid in matched + unmatched:
            for _ in range(rng.randint(1, 5)):
                lot = f"R{rng.randint(100, 999)}"
                for w in range(1, rng.randint(2, 5)):
                    ppid = (
                        rng.choice(ppid_map[sid] + [f"PPID_NEW_{rng.randint(10, 99)}"])
                        if sid in ppid_map
                        else f"PPID_STD_{sid[-4:]}"
                    )
                    row: dict = {
                        "step_id": sid, "root_lot_id": lot,
                        "wafer_id": str(w), "ppid": ppid,
                        "step_desc": f"STEP_{sid[:2]}",
                        "eqp_model": "GEN-1",
                    }
                    for col in extra:
                        if col in row:
                            continue
                        if col == "eqp_id":
                            row[col] = f"EQP_{rng.choice('ABC')}{rng.randint(1, 5):02d}"
                        elif col == "recipe_id":
                            row[col] = f"RCP_{sid[-4:]}_{rng.randint(1, 3)}"
                        else:
                            row[col] = f"{col}_{rng.randint(0, 9)}"
                    rows.append(row)
        return pl.DataFrame(rows)


StubFabDbClient = LocalFabDbClient


class LakeFabDbClient(FabDbClient):
    """Production client via LakeAPI."""
    def __init__(self, lake_api: "LakeAPI", pipe: FeaturePipeline):
        self.api = lake_api
        self.pipe = pipe

    def query_step_data(self, vehicle_cfg: dict, scan_cfg: dict) -> pl.DataFrame:
        vehicle = vehicle_cfg["vehicle"]
        query_days = int(scan_cfg.get("scan_query_days") or vehicle_cfg.get("QueryTimeSpan", 6))
        sources = self.pipe.sources_cfg()
        fab_src = sources.get("FAB", {})
        table = fab_src.get("table", "RAW_FAB_DATA")
        base = ["step_id", "root_lot_id", "wafer_id", "ppid"]
        extra: list[str] = scan_cfg.get("extra_columns") or []
        columns = list(dict.fromkeys(base + extra))
        today = datetime.today()
        date_from = (today - timedelta(days=query_days)).strftime("%Y-%m-%dT00:00:00")
        date_to = today.strftime("%Y-%m-%dT23:59:59")
        params: dict = {"table": table, "dateFrom": date_from, "dateTo": date_to}
        line_id = vehicle_cfg.get("line_id")
        if line_id:
            ids = line_id if isinstance(line_id, list) else [line_id]
            params["line_id"] = {"op": "in", "value": ids}
        process_id = vehicle_cfg.get("process_id")
        if process_id:
            params["process_id"] = {"op": "eq", "value": process_id}
        eqp_filter: list[str] = scan_cfg.get("eqp_filter") or []
        eqp_mode = str(scan_cfg.get("eqp_filter_mode", "eqp_id"))
        if eqp_filter:
            params[eqp_mode] = {"op": "in", "value": eqp_filter}
        df = self._run_query(params, columns)
        if "step_id" in df.columns:
            df = df.with_columns(pl.col("step_id").cast(pl.Utf8))
        return df

    def _run_query(self, params: dict, columns: list[str]) -> pl.DataFrame:
        coro = self.api.query(params, columns)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()


DEFAULT_SCAN_CFG: dict = {
    "eqp_filter": [],
    "eqp_filter_mode": "eqp_id",
    "extra_columns": ["eqp_id", "recipe_id"],
    "max_hits": 10,
    "main_step_only": True,
    "main_step_exclude": {
        "step_desc": ["MEASURE*", "AUX*", "INSPECT*", "ALIGN*"],
        "eqp_model": ["MEA-*", "SEM-*", "CD-*"],
    },
    "scan_query_days": None,
}


class FabScanner:
    """FAB DB scanner -- missing steps + unmatched PPIDs -> S3."""

    def __init__(self, root: Path, pipe: FeaturePipeline,
                 db_client: FabDbClient, s3_uploader, settings: dict):
        self.root = Path(root)
        self.pipe = pipe
        self.db = db_client
        self.s3 = s3_uploader
        self.prefix = (
            (settings.get("alerts") or {}).get("s3_prefix") or "valve-alerts"
        ).strip("/")

    def scan_dir(self, vehicle: str) -> Path:
        return self.root / "config" / "fab_scan" / vehicle

    def scan_config(self, vehicle: str) -> dict:
        path = self.scan_dir(vehicle) / "scan_config.yaml"
        cfg = dict(DEFAULT_SCAN_CFG)
        if path.exists():
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            cfg.update({k: v for k, v in loaded.items() if k != "vehicle"})
        cfg["vehicle"] = vehicle
        return cfg

    def save_scan_config(self, vehicle: str, cfg: dict):
        d = self.scan_dir(vehicle)
        d.mkdir(parents=True, exist_ok=True)
        (d / "scan_config.yaml").write_text(
            yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")

    def scan_ignore(self, vehicle: str) -> list[dict]:
        path = self.scan_dir(vehicle) / "scan_ignore.json"
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []

    def save_scan_ignore(self, vehicle: str, items: list[dict]) -> list[dict]:
        d = self.scan_dir(vehicle)
        d.mkdir(parents=True, exist_ok=True)
        clean = [
            {"type": str(x.get("type") or "step"),
             "key": str(x.get("key") or ""),
             "reason": str(x.get("reason") or "")}
            for x in items if str(x.get("key") or "").strip()
        ]
        (d / "scan_ignore.json").write_text(
            json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")
        return clean

    def run(self, vehicle: str) -> dict:
        vcfg = self.pipe.vehicle_cfg(vehicle)
        scan_cfg = self.scan_config(vehicle)
        data = self.db.query_step_data(vcfg, scan_cfg)
        extra_cols: list[str] = scan_cfg.get("extra_columns") or []
        if data.height == 0:
            result = self._empty_result(vehicle, extra_cols)
            self._save_and_publish(vehicle, result)
            return result
        if scan_cfg.get("main_step_only"):
            data = self._apply_main_step_filter(data, scan_cfg.get("main_step_exclude") or {})
        ignore = self.scan_ignore(vehicle)
        missing = self._find_missing_steps(vehicle, data, scan_cfg)
        unmatched = self._find_unmatched_ppids(vehicle, data, scan_cfg)
        missing = self._apply_ignore(missing, ignore, "step")
        unmatched = self._apply_ignore(unmatched, ignore, "ppid")
        matched_steps = self.pipe.step_map(vehicle)["step_id"].to_list()
        knob = self.pipe.knob_map(vehicle)
        knob_steps = 0
        if knob is not None:
            knob_steps = knob.select("step_id").n_unique()
        ign_s = sorted({e.get("key", "") for e in ignore if e.get("type") == "step"})
        ign_p = sorted({e.get("key", "") for e in ignore if e.get("type") == "ppid"})
        result = {
            "vehicle": vehicle,
            "scan_ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "extra_columns": extra_cols,
            "missing_steps": missing,
            "unmatched_ppids": unmatched,
            "summary": {
                "total_fab_rows": data.height,
                "matching_steps_count": len(matched_steps),
                "knob_steps_count": knob_steps,
                "missing_count": len(missing),
                "unmatched_count": len(unmatched),
                "ignored": {"steps": ign_s, "ppids": ign_p},
            },
        }
        self._save_and_publish(vehicle, result)
        return result

    def run_all(self) -> dict:
        out: dict[str, dict] = {}
        for v in self.pipe.vehicles():
            try:
                out[v] = self.run(v)
            except Exception as e:
                out[v] = {"vehicle": v, "error": str(e)[:300]}
        return out

    @staticmethod
    def _apply_main_step_filter(df: pl.DataFrame, exclude: dict) -> pl.DataFrame:
        desc_pats: list[str] = exclude.get("step_desc") or []
        model_pats: list[str] = exclude.get("eqp_model") or []
        if desc_pats and "step_desc" in df.columns:
            unique_descs = df["step_desc"].unique().to_list()
            excluded = {d for d in unique_descs
                        if d is not None and any(fnmatch.fnmatch(str(d), p) for p in desc_pats)}
            if excluded:
                df = df.filter(~pl.col("step_desc").is_in(sorted(excluded)))
        if model_pats and "eqp_model" in df.columns:
            unique_models = df["eqp_model"].unique().to_list()
            excluded = {m for m in unique_models
                        if m is not None and any(fnmatch.fnmatch(str(m), p) for p in model_pats)}
            if excluded:
                df = df.filter(~pl.col("eqp_model").is_in(sorted(excluded)))
        return df

    def _find_missing_steps(self, vehicle: str, data: pl.DataFrame, scan_cfg: dict) -> list[dict]:
        matched_ids = set(self.pipe.step_map(vehicle)["step_id"].to_list())
        max_hits: int = int(scan_cfg.get("max_hits") or 10)
        extra_cols: list[str] = scan_cfg.get("extra_columns") or []
        unmatched = data.filter(~pl.col("step_id").cast(pl.Utf8).is_in(list(matched_ids)))
        if unmatched.height == 0:
            return []
        hit_cols = ["root_lot_id", "wafer_id"] + [c for c in extra_cols if c in unmatched.columns]
        results: list[dict] = []
        for (step_id,), grp in unmatched.group_by(["step_id"], maintain_order=True):
            lot_count = grp.select(pl.col("root_lot_id").n_unique()).item()
            hits = grp.select(hit_cols).unique().head(max_hits).to_dicts()
            results.append({"step_id": step_id, "lot_count": lot_count, "hits": hits})
        results.sort(key=lambda r: (-r["lot_count"], r["step_id"]))
        return results

    def _find_unmatched_ppids(self, vehicle: str, data: pl.DataFrame, scan_cfg: dict) -> list[dict]:
        vknob = self.pipe.knob_map(vehicle)
        if vknob is None or "ppid" not in data.columns:
            return []
        if vknob.height == 0:
            return []
        max_hits: int = int(scan_cfg.get("max_hits") or 10)
        extra_cols: list[str] = scan_cfg.get("extra_columns") or []
        hit_cols = ["root_lot_id", "wafer_id"] + [
            c for c in extra_cols if c in data.columns and c not in ("root_lot_id", "wafer_id")]
        ppid_map: dict[str, set[str]] = {}
        for r in vknob.iter_rows(named=True):
            ppid_map.setdefault(r["step_id"], set()).add(r["ppid"])
        results: list[dict] = []
        for step_id, known_ppids in sorted(ppid_map.items()):
            step_data = data.filter(pl.col("step_id") == step_id)
            if step_data.height == 0:
                continue
            miss = step_data.filter(~pl.col("ppid").cast(pl.Utf8).is_in(list(known_ppids)))
            if miss.height == 0:
                continue
            for (ppid,), pgrp in miss.group_by(["ppid"], maintain_order=True):
                hits = pgrp.select(hit_cols).unique().head(max_hits).to_dicts()
                results.append({
                    "step_id": step_id, "ppid": ppid,
                    "existing_splits": sorted(known_ppids), "hits": hits})
        results.sort(key=lambda r: (r["step_id"], r["ppid"]))
        return results

    @staticmethod
    def _apply_ignore(items: list[dict], ignore: list[dict], item_type: str) -> list[dict]:
        if not ignore:
            return items
        keys: set[str] = {e.get("key", "") for e in ignore if e.get("type") == item_type}
        if not keys:
            return items
        if item_type == "step":
            return [i for i in items if i["step_id"] not in keys]
        if item_type == "ppid":
            return [i for i in items if f"{i['step_id']}:{i['ppid']}" not in keys]
        return items

    def _empty_result(self, vehicle: str, extra_columns: list[str]) -> dict:
        return {
            "vehicle": vehicle,
            "scan_ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "extra_columns": extra_columns,
            "missing_steps": [], "unmatched_ppids": [],
            "summary": {
                "total_fab_rows": 0, "matching_steps_count": 0,
                "knob_steps_count": 0, "missing_count": 0,
                "unmatched_count": 0,
                "ignored": {"steps": [], "ppids": []},
            },
        }

    def _save_and_publish(self, vehicle: str, result: dict) -> None:
        text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
        rdir = self.pipe.report_dir(vehicle)
        rdir.mkdir(parents=True, exist_ok=True)
        (rdir / "scan_result.json").write_text(text, encoding="utf-8")
        try:
            self.s3.put_text(f"{self.prefix}/scan/{vehicle}.json", text)
        except Exception:
            pass

    def list_vehicles(self) -> list[str]:
        scan_root = self.root / "config" / "fab_scan"
        if not scan_root.exists():
            return []
        return sorted(d.name for d in scan_root.iterdir()
                      if d.is_dir() and (d / "scan_config.yaml").exists())

    def last_result(self, vehicle: str) -> dict | None:
        path = self.pipe.report_dir(vehicle) / "scan_result.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
