"""
Valve · pipeline_runner
-----------------------
파이프라인 병렬 오케스트레이터 + 주기 스케줄러.

기본 동작 (run_all):
  vehicle 을 순회하며 (vehicle_workers 만큼 동시에)
    1) raw   : (source × 1일) 유닛을 raw_workers 스레드풀로 병렬 쿼리
    2) event : 5일치를 event DB 화 (소스별 매칭 필터)
    3) feature: 전체를 feature 산출
  워커 수는 runtime_env.plan_workers 가 호스트 코어/메모리로 산정
  (pipeline.yaml['runtime'] 로 override).

스케줄러:
  pipeline.yaml['runtime']['interval_hours'] > 0 이고 schedule_enabled 면
  app startup 에서 백그라운드 asyncio 루프가 그 간격으로 run_all 을 돈다.
  (blocking 작업이라 asyncio.to_thread 로 실행 → event loop 안 막음)
"""
from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from backend.core.feature_pipeline import FeaturePipeline
from backend.core.runtime_env import WorkerPlan, plan_workers


class PipelineRunner:
    def __init__(self, pipe: FeaturePipeline):
        self.pipe = pipe
        self._bg_task: asyncio.Task | None = None
        self.last_run: dict | None = None
        self.on_vehicle_done = None  # Callable[[str, dict], None] — 알람 발행 훅

    # ── 워커 계획 ──
    def runtime_cfg(self) -> dict:
        return self.pipe.global_cfg().get("runtime") or {}

    def plan(self) -> WorkerPlan:
        return plan_workers(self.runtime_cfg())

    # ── 단일 vehicle: 병렬 raw → event → feature ──
    def run_vehicle(self, vehicle: str, plan: WorkerPlan | None = None) -> dict:
        plan = plan or self.plan()
        t0 = time.time()
        cfg = self.pipe.vehicle_cfg(vehicle)
        units = self.pipe._raw_units(cfg)

        rows: dict[str, int] = {}
        errors: list[dict] = []
        # (source × 날짜) 병렬 raw — 서로 다른 파티션 파일만 쓰므로 스레드 안전
        with ThreadPoolExecutor(max_workers=plan.raw_workers,
                                thread_name_prefix=f"raw-{vehicle}") as ex:
            futs = {ex.submit(self.pipe._run_raw_unit, cfg, *u): u for u in units}
            for f in as_completed(futs):
                src, start, *_ = futs[f]
                try:
                    rows[src] = rows.get(src, 0) + f.result()
                except Exception as e:
                    errors.append({"source": src, "date": str(start), "error": str(e)[:300]})

        event = self.pipe.run_event(vehicle)
        feature = self.pipe.run_feature(vehicle)
        result = {
            "vehicle": vehicle, "product": cfg["product"],
            "raw_rows": rows, "raw_units": len(units), "errors": errors,
            "event": {s: e["event_rows"] for s, e in event.items()},
            "feature": feature["features"],
            "elapsed_sec": round(time.time() - t0, 2),
        }
        if self.on_vehicle_done:
            try:
                self.on_vehicle_done(vehicle, result)
            except Exception:
                pass
        return result

    # ── 전 vehicle 순회 (vehicle 도 병렬) ──
    def run_all(self, plan: WorkerPlan | None = None) -> dict:
        plan = plan or self.plan()
        t0 = time.time()
        vehicles = list(self.pipe.vehicles().keys())
        results: dict[str, dict] = {}
        # vehicle 병렬 — 각 vehicle 이 내부에서 raw_workers 를 다시 쓰므로
        # 전체 동시 스레드 = vehicle_workers × raw_workers 가 되지 않도록,
        # vehicle 병렬 시엔 vehicle 당 raw 워커를 나눠 배분.
        per_vehicle = max(1, plan.raw_workers // max(1, plan.vehicle_workers))
        sub = WorkerPlan(**{**plan.__dict__, "raw_workers": per_vehicle})
        with ThreadPoolExecutor(max_workers=plan.vehicle_workers,
                                thread_name_prefix="vehicle") as ex:
            futs = {ex.submit(self.run_vehicle, v, sub): v for v in vehicles}
            for f in as_completed(futs):
                v = futs[f]
                try:
                    results[v] = f.result()
                except Exception as e:
                    results[v] = {"vehicle": v, "error": str(e)[:300]}
        summary = {
            "ok": all("error" not in r for r in results.values()),
            "vehicles": results,
            "plan": plan.__dict__,
            "elapsed_sec": round(time.time() - t0, 2),
            "ts": t0,
        }
        self.last_run = summary
        return summary

    # ── 주기 스케줄러 ──
    def schedule_enabled(self) -> bool:
        rt = self.runtime_cfg()
        return bool(rt.get("schedule_enabled")) and float(rt.get("interval_hours") or 0) > 0

    async def _loop(self):
        while True:
            rt = self.runtime_cfg()
            interval = float(rt.get("interval_hours") or 0) * 3600
            if not (rt.get("schedule_enabled") and interval > 0):
                await asyncio.sleep(60)   # 비활성 — 설정 변경 폴링
                continue
            try:
                await asyncio.to_thread(self.run_all)
            except Exception:
                pass
            await asyncio.sleep(max(60.0, interval))

    def start_background(self):
        if self._bg_task is None or self._bg_task.done():
            self._bg_task = asyncio.get_event_loop().create_task(self._loop())

    def stop_background(self):
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
        self._bg_task = None
