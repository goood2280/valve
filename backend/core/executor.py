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

from .planner import Chunk, ChunkPlan


class ChunkExecutor:
    def __init__(self, lake_api, planner, s3, state, settings: dict, staging_root: Path):
        self.api = lake_api
        self.planner = planner
        self.s3 = s3
        self.state = state
        self.settings = settings
        self.staging_root = Path(staging_root)
        self.staging_root.mkdir(parents=True, exist_ok=True)

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
                cc = source_cfg.get("custom_col") or prod_cfg.get("custom_col") or []
                df = await self.api.query(params, cc)
                self._save_staging(chunk, df)
                self.state.update_chunk(chunk.chunk_id, self._chunk_meta(
                    chunk,
                    status="success",
                    ended_at=time.time(),
                    actual_rows=len(df),
                    duration_sec=round(time.time() - t_start, 2),
                ))
                return {"ok": True, "rows": len(df)}
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
                # best-effort webhook — import 위에 두지 않는 이유: 테스트/단독 실행 시 ops 미로드 허용
                try:
                    from backend.routers import ops as _ops
                    import asyncio as _asyncio
                    _asyncio.create_task(_ops.emit_failure_webhook({
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
        t0 = datetime.fromisoformat(f"{chunk.date}T00:00:00")
        t1 = t0 + timedelta(days=1)
        params = dict(prod_cfg.get("params_template", {}))
        params["table"] = source_cfg["table"]
        params["dateFrom"] = t0.isoformat()
        params["dateTo"] = t1.isoformat()

        # shard filter → cat 슬롯에 채우기
        used_slots = {k for k in params if k.startswith("cat")}
        slot_pool = [f"cat{l}" for l in "bcdefghij"]
        free_slots = [s for s in slot_pool if s not in used_slots]

        for col, vals in (chunk.shard_filters or {}).items():
            if not free_slots:
                break
            slot = free_slots.pop(0)
            params[slot] = {"column": col, "op": "in", "value": list(vals)}

        return params

    # ─── staging ───
    def _staging_date_dir(self, plan: ChunkPlan) -> Path:
        return self.staging_root / plan.product / plan.source / f"date={plan.date}"

    def _save_staging(self, chunk: Chunk, df):
        """pandas DataFrame 도 polars 로 변환해서 저장 — pandas.to_parquet 엔진 루커업 버그 회피."""
        path = self.staging_root / chunk.product / chunk.source / f"date={chunk.date}" / f"{chunk.chunk_id}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
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

        # S3 atomic put
        s3_key = f"{plan.source}/{plan.product}/date={plan.date}/part-0.parquet"
        try:
            await self.s3.put_atomic(merged_path, s3_key)
        except Exception as e:
            self.state.update_partition(
                f"{plan.product}/{plan.source}/{plan.date}",
                {"status": "upload_failed", "error": str(e)[:300], "completeness": completeness},
            )
            return {"ok": False, "reason": f"upload_failed: {e}"}

        # staging part 파일 정리 (_merged.parquet 는 유지 — 브라우저에서 확인 가능)
        for p in parts:
            try:
                p.unlink()
            except Exception:
                pass

        self.state.update_partition(
            f"{plan.product}/{plan.source}/{plan.date}",
            {"status": "success", "total_rows": total_rows, "completeness": completeness, "s3_key": s3_key},
        )
        return {"ok": True, "rows": total_rows, "s3_key": s3_key}
