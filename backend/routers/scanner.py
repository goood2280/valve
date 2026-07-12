# -*- coding: utf-8 -*-
"""FAB DB scan router -- missing steps + unmatched PPIDs API.

  GET  /api/scanner/vehicles           vehicle list (all + configured)
  POST /api/scanner/run/{vehicle}      scan single vehicle
  POST /api/scanner/run-all            scan all vehicles
  GET  /api/scanner/result/{vehicle}   last scan result
  GET  /api/scanner/config/{vehicle}   scan config
  PUT  /api/scanner/config/{vehicle}   save scan config
  GET  /api/scanner/ignore/{vehicle}   ignore items
  PUT  /api/scanner/ignore/{vehicle}   save ignore items
"""
from __future__ import annotations

import yaml
from fastapi import APIRouter, Body, HTTPException

from backend.core.fab_scanner import (
    FabScanner, LocalFabDbClient, LakeFabDbClient, StubFabDbClient,
)
from backend.core.feature_pipeline import FeaturePipeline

router = APIRouter()

scanner: FabScanner | None = None


def deps(root, settings, s3_uploader, pipe: FeaturePipeline, lake_api=None):
    """Initialize scanner.

    Default: LocalFabDbClient -- scans local parquet DB from pipeline.
    Lake used only when lake_api.mode != "mock" and lake_api is provided.
    """
    global scanner
    mode = (settings.get("lake_api") or {}).get("mode", "mock")
    if mode != "mock" and lake_api is not None:
        db_client = LakeFabDbClient(lake_api, pipe)
    else:
        db_client = LocalFabDbClient(pipe)
    scanner = FabScanner(root, pipe, db_client, s3_uploader, settings)


def _s() -> FabScanner:
    if scanner is None:
        raise HTTPException(500, "scanner not initialized")
    return scanner


@router.get("/api/scanner/vehicles")
def vehicles():
    return {
        "vehicles": list(_s().pipe.vehicles().keys()),
        "configured": _s().list_vehicles(),
    }


@router.post("/api/scanner/run/{vehicle}")
def run(vehicle: str):
    try:
        return _s().run(vehicle)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/api/scanner/run-all")
def run_all():
    return _s().run_all()


@router.get("/api/scanner/result/{vehicle}")
def result(vehicle: str):
    r = _s().last_result(vehicle)
    if r is None:
        raise HTTPException(404, f"no scan result: {vehicle}")
    return r


@router.get("/api/scanner/config/{vehicle}")
def get_config(vehicle: str):
    return _s().scan_config(vehicle)


@router.put("/api/scanner/config/{vehicle}")
def put_config(vehicle: str, body: dict = Body(...)):
    cfg = _s().scan_config(vehicle)
    for k in ("eqp_filter", "eqp_filter_mode", "extra_columns",
              "max_hits", "main_step_only", "main_step_exclude",
              "scan_query_days"):
        if k in body:
            cfg[k] = body[k]
    cfg["vehicle"] = vehicle
    _s().save_scan_config(vehicle, cfg)
    return {"ok": True, "config": cfg}


@router.get("/api/scanner/ignore/{vehicle}")
def get_ignore(vehicle: str):
    return _s().scan_ignore(vehicle)


@router.put("/api/scanner/ignore/{vehicle}")
def put_ignore(vehicle: str, body: list[dict] = Body(...)):
    """Save ignore items. body: [{type, key, reason}]."""
    saved = _s().save_scan_ignore(vehicle, body)
    return {"ok": True, "ignore": saved}
