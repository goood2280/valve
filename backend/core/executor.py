"""
Valve · executor
----------------
asyncio chunk worker. max_concurrent=3 Semaphore.
흐름:
  chunk → query() → staging parquet 저장
  모든 chunk 완료 → polars concat → completeness check → S3 atomic put
  chunk timeout → fallback(한 단계 더 쪼갬) 을 "차회 사이클" 로 기록
"""
from __future__ import annotations

import asyncio
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from .planner import Chunk, ChunkPlan, raw_query_params


class ChunkExecutor:
    def __init__(self, lake_api, planner, s3, state, settings: dict, staging_root: Path,
                 db_root: Path | None = None, product_vehicles: dict[str, list[str]] | None = None):
        self.api = lake_api
        self.planner = planner
        self.s3 = s3
        self.state = state
        self.settings = settings
        self.staging_root = Path(staging_root)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.db_root = Path(db_root) if db_root else self.staging_root.parent
        self.product_vehicles = product_vehicles or {}
        self.product_filters: dict[str, dict] = {}

        self.max_concurrent = int(settings["lake_api"].get("max_concurrent", 3))
        self._sem = asyncio.Semaphore(self.max_concurrent)
        self._cancel_set: set[str] = set()

    # ─── public ───
    async def run_plan(self, plan: ChunkPlan, prod_cfg: dict, source_cfg: dict) -> dict:
        self.state.record_plan(plan.to_dict())

        # staging 해당 파티션 폴더 초기화 (overwrite 보장)
        date_dir = self._staging_date_dir(plan)
        if date_dir.exists():
            shutil.rmtree(date_dir, ignore_errors=True)
        date_dir.mkdir(parents=True, exist_ok=True)

        # chunk 병렬 실행
        tasks = [asyncio.create_task(self._execute_chunk(c, prod_cfg, source_cfg)) for c in plan.chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # completeness + upload
        ok = await self._finalize(plan, prod_cfg, source_cfg, results)
        return ok

    def cancel(self, chunk_id: str):
        self._cancel_set.add(chunk_id)

    # ─── chunk ───
    async def _execute_chunk(self, chunk: Chunk, prod_cfg: dict, source_cfg: dict):
        if chunk.chunk_id in self._cancel_set:
            self.state.update_chunk(chunk.chunk_id, self._chunk_meta(chunk, status="cancelled"))
            return None

        async with self._sem:
            self.state.update_chunk(chunk.chunk_id, self._chunk_meta(
                chunk, status="in_progress", started_at=time.time()))

            params = self._build_params(chunk, prod_cfg, source_cfg)
            t_start = time.time()
            try:
                df = await self.api.query(params, [])
                rows = 0 if df is None else len(df)   # '데이터 없음' 은 실패가 아니다
                self._save_staging(chunk, df)
                self.state.update_chunk(chunk.chunk_id, self._chunk_meta(
                    chunk,
                    status="success",
                    ended_at=time.time(),
                    actual_rows=rows,
                    duration_sec=round(time.time() - t_start, 2),
                ))
                return {"ok": True, "rows": rows}
            except Exception as e:
                is_timeout = "Timeout" in type(e).__name__ or "timeout" in str(e).lower()
                status = "timeout_reshard" if is_timeout else "failed"
                self.state.update_chunk(chunk.chunk_id, self._chunk_meta(
                    chunk,
                    status=status,
                    ended_at=time.time(),
                    error_type=type(e).__name__,
                    error=str(e)[:500],
                    duration_sec=round(time.time() - t_start, 2),
                ))
                # best-effort 통합 알람 (S3 + flow + webhook 병렬 dispatch)
                try:
                    from backend.routers import ops as _ops
                    import asyncio as _asyncio
                    _asyncio.create_task(_ops.dispatch_alert({
                        "source": "valve.executor",
                        "kind": "chunk_" + status,
                        "severity": "error" if status == "failed" else "warn",
                        "title": f"{chunk.product}/{chunk.source}/{chunk.date} chunk {status}",
                        "chunk_id": chunk.chunk_id,
                        "product": chunk.product, "source": chunk.source, "date": chunk.date,
                        "status": status, "error_type": type(e).__name__, "error": str(e)[:300],
                    }))
                except Exception:
                    pass
                raise

    def _chunk_meta(self, chunk: Chunk, **update) -> dict:
        base = {
            "product": chunk.product,
            "source": chunk.source,
            "date": chunk.date,
            "shard_filters": chunk.shard_filters,
            "expected_rows": chunk.expected_rows,
        }
        base.update(update)
        return base

    def _build_params(self, chunk: Chunk, prod_cfg: dict, source_cfg: dict) -> dict:
        """사내 API 포맷: {필터..., "table_name": ..., "dateFrom": ..., "dateTo": ...}."""
        t0 = datetime.fromisoformat(f"{chunk.date}T00:00:00")
        t1 = t0 + timedelta(days=1)
        params = raw_query_params(chunk.product, source_cfg, prod_cfg,
                                  self.product_filters, t0.isoformat(), t1.isoformat())

        # shard filter → 해당 컬럼명을 키로 그대로 IN 필터 주입 (기존 값 override).
        # 빈 목록은 주입하지 않는다 — `in ()` 는 사내 API 가 SQL 에러를 낸다.
        for col, vals in (chunk.shard_filters or {}).items():
            if vals:
                params[col] = list(vals)
        return params

    # ─── staging ───
    def _staging_date_dir(self, plan: ChunkPlan) -> Path:
        return self.staging_root / plan.product / plan.source / f"date={plan.date}"

    def _save_staging(self, chunk: Chunk, df):
        """pandas DataFrame 도 polars 로 변환해서 저장 — pandas.to_parquet 엔진 루커업 버그 회피."""
        path = self.staging_root / chunk.product / chunk.source / f"date={chunk.date}" / f"{chunk.chunk_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        if df is None:                      # 어댑터가 '데이터 없음' 을 None 으로 주는 경우
            df = pl.DataFrame()
        if isinstance(df, pl.DataFrame):
            df.write_parquet(str(path))
        elif isinstance(df, pd.DataFrame):
            pl.from_pandas(df).write_parquet(str(path))
        else:
            pl.from_pandas(pd.DataFrame(df)).write_parquet(str(path))

    # ─── finalize ───
    async def _finalize(self, plan: ChunkPlan, prod_cfg, source_cfg, results) -> dict:
        success_chunks = [c for c, r in zip(plan.chunks, results) if isinstance(r, dict) and r.get("ok")]
        if not success_chunks:
            self.state.update_partition(
                f"{plan.product}/{plan.source}/{plan.date}",
                {"status": "failed", "total_chunks": len(plan.chunks), "done_chunks": 0},
            )
            return {"ok": False, "reason": "all_chunks_failed"}

        date_dir = self._staging_date_dir(plan)
        parts = sorted(date_dir.glob("*.parquet"))
        if not parts:
            return {"ok": False, "reason": "no_parts"}

        try:
            dfs = [pl.read_parquet(str(p)) for p in parts]
            merged = pl.concat(dfs, how="diagonal_relaxed")
        except Exception as e:
            return {"ok": False, "reason": f"merge_error: {e}"}

        total_rows = merged.height
        expected = sum(c.expected_rows for c in plan.chunks if c.expected_rows)
        tolerance = float(self.settings["schedule"].get("tolerance_pct", 0.5)) / 100.0
        completeness = {"actual": total_rows, "expected": expected, "tolerance_pct": tolerance * 100}

        # 전 chunk 가 성공했는데 0행 = 그 조건에 데이터가 없는 날이다.
        # expected 는 probe **캐시**(기본 7일)라 lot 이 사라진 뒤에도 큰 값으로 남는다 —
        # 없는 lot 을 '불완전' 으로 처리하면 재시도 큐와 알람이 영원히 울린다.
        # 이때는 DB/S3 도 건드리지 않는다: 빈 결과로 이미 받아 둔 파티션을 덮어쓰면
        # 일시적인 빈 응답 한 번에 그 날 데이터가 사라진다.
        if total_rows == 0 and len(success_chunks) == len(plan.chunks):
            self.state.update_partition(
                f"{plan.product}/{plan.source}/{plan.date}",
                {"status": "success", "empty": True, "total_rows": 0,
                 "completeness": completeness, "s3_status": "skipped_empty"},
            )
            return {"ok": True, "rows": 0, "empty": True, "s3_key": None,
                    "upload_status": "skipped_empty", "db_paths": []}

        if expected > 0:
            diff = abs(total_rows - expected) / max(expected, 1)
            completeness["diff_pct"] = round(diff * 100, 3)
            if len(success_chunks) < len(plan.chunks):
                # 일부 chunk 실패 → completeness 의미 없음, 업로드 보류
                self.state.update_partition(
                    f"{plan.product}/{plan.source}/{plan.date}",
                    {"status": "partial_failed", "completeness": completeness},
                )
                return {"ok": False, "reason": "partial_failure"}
            if diff > tolerance:
                self.state.update_partition(
                    f"{plan.product}/{plan.source}/{plan.date}",
                    {"status": "completeness_failed", "completeness": completeness},
                )
                return {"ok": False, "reason": "completeness_failed", "completeness": completeness}

        # merge 파일 생성
        merged_path = date_dir / "_merged.parquet"
        merged.write_parquet(str(merged_path))

        # direct-query 결과도 파이프라인이 읽는 RAW DB에 원자적으로 반영한다.
        # product와 연결된 vehicle이 없으면 product 이름 자체를 안전한 fallback으로 쓴다.
        db_paths = []
        targets = self.product_vehicles.get(plan.product) or [plan.product]
        if merged.width == 0:
            # 열이 하나도 없는 parquet — 파이프라인이 이 파일을 읽는 순간
            # root_lot_id 를 못 찾아 그 제품 event 단계가 통째로 죽는다. 남기지 않는다.
            targets = []
        for vehicle in targets:
            db_path = (self.db_root / "1.RAWDATA_DB" / plan.source / vehicle /
                       f"date={plan.date}" / "data.parquet")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = db_path.with_suffix(".parquet.tmp")
            shutil.copy2(merged_path, tmp_path)
            tmp_path.replace(db_path)
            db_paths.append(str(db_path))

        # S3 업로드 모드 분기 (immediate / interval / manual)
        s3_key = f"{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"
        mode = (self.settings.get("s3", {}) or {}).get("upload_mode", "immediate")
        partition_key = f"{plan.product}/{plan.source}/{plan.date}"

        s3_configured = bool(self.s3 and getattr(self.s3, "is_configured", lambda: True)())
        if not s3_configured:
            upload_status = "skipped_disabled"
            partition_status = "success"
        elif mode == "immediate":
            try:
                await self.s3.put_atomic(merged_path, s3_key)
            except Exception as e:
                self.state.update_partition(
                    partition_key,
                    {"status": "upload_failed", "error": str(e)[:300], "completeness": completeness},
                )
                return {"ok": False, "reason": f"upload_failed: {e}"}
            upload_status = "success"
            partition_status = "success"
        else:
            # interval / manual — pending 큐에 저장, 실제 업로드는 스케줄러/수동
            try:
                from backend.core import s3_queue as _s3q
                _s3q.enqueue(partition_key, str(merged_path), s3_key, mode=mode)
                upload_status = "pending_upload"
                partition_status = "pending_upload"
            except Exception as e:
                return {"ok": False, "reason": f"queue_error: {e}"}

        # staging part 파일 정리 (_merged.parquet 는 유지 — 브라우저에서 확인 가능)
        for p in parts:
            try:
                p.unlink()
            except Exception:
                pass

        self.state.update_partition(
            partition_key,
            {"status": partition_status, "total_rows": total_rows, "completeness": completeness,
             "s3_key": s3_key if s3_configured else None, "s3_status": upload_status,
             "db_paths": db_paths},
        )
        return {"ok": True, "rows": total_rows,
                "s3_key": s3_key if s3_configured else None,
                "upload_status": upload_status, "db_paths": db_paths}
