"""
Valve · pipeline_runner
-----------------------
파이프라인 병렬 오케스트레이터 + 진행상황(stage) 추적 + 주기/루프 스케줄러.

기본 동작 (run_all):
  vehicle 을 순회하며 (vehicle_workers 만큼 동시에)
    1) raw     : (source × 1일) 유닛을 raw_workers 스레드풀로 병렬 쿼리
    2) event   : 5일치를 event DB 화 (소스별 매칭 필터)
    3) feature : event DB "전체" 를 대상으로 feature 산출 (특정 기간 아님)
  진행 중에는 self.progress 가 vehicle 별 현재 단계(raw/event/feature)를 담아
  /api/pipeline/progress 로 노출 → 백필이 지금 어느 DB 단계인지 실시간 확인.

스케줄:
  · schedule_enabled + interval_hours>0 : interval_hours 간격 자동 실행
  · loop_enabled                        : 쉬지 않고 계속 반복 실행 (루프 실행)
  둘 다 백그라운드 asyncio 루프가 to_thread 로 run_all 을 돌린다.
  _run_lock 으로 수동/스케줄/루프 실행이 겹치지 않게 직렬화.
"""
from __future__ import annotations

import asyncio
import copy
import threading
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
        self._lock = threading.Lock()       # progress 보호
        self._run_lock = threading.Lock()   # run_all 중복 실행 방지
        self._raw_sem = threading.BoundedSemaphore(3)  # 전역 raw 동시 상한 (run_all 시 재설정)
        self._loop_count = 0
        self.progress: dict = {"running": False, "mode": None, "loop": False,
                               "loop_iter": 0, "started": None, "ts": None, "vehicles": {}}

    # ── 설정/계획 ──
    def runtime_cfg(self) -> dict:
        return self.pipe.global_cfg().get("runtime") or {}

    def plan(self) -> WorkerPlan:
        return plan_workers(self.runtime_cfg())

    def schedule_enabled(self) -> bool:
        rt = self.runtime_cfg()
        return bool(rt.get("schedule_enabled")) and float(rt.get("interval_hours") or 0) > 0

    def loop_enabled(self) -> bool:
        return bool(self.runtime_cfg().get("loop_enabled"))

    # ── progress ──
    def _prog(self, vehicle: str, **kw):
        with self._lock:
            self.progress.setdefault("vehicles", {}).setdefault(vehicle, {}).update(kw)
            self.progress["ts"] = time.time()

    def snapshot(self) -> dict:
        with self._lock:
            return copy.deepcopy(self.progress)

    def _raw_guarded(self, cfg, source, start, end, split) -> int:
        """전역 raw 세마포어를 잡고 raw 유닛 실행 — 전 vehicle 합쳐 동시 raw ≤ raw_api_max."""
        with self._raw_sem:
            return self.pipe._run_raw_unit(cfg, source, start, end, split)

    # ── 단일 vehicle: 병렬 raw → event → feature (단계별 progress 갱신) ──
    def run_vehicle(self, vehicle: str, plan: WorkerPlan | None = None) -> dict:
        plan = plan or self.plan()
        t0 = time.time()
        cfg = self.pipe.vehicle_cfg(vehicle)
        units = self.pipe._raw_units(cfg)

        # 1) RAW — (source × 날짜) 병렬. 실제 동시 실행은 전역 raw 세마포어가 제한(≤ raw_api_max).
        self._prog(vehicle, stage="raw", raw_done=0, raw_total=len(units), source=None)
        rows: dict[str, int] = {}
        errors: list[dict] = []
        done = 0
        with ThreadPoolExecutor(max_workers=max(1, plan.raw_workers),
                                thread_name_prefix=f"raw-{vehicle}") as ex:
            futs = {ex.submit(self._raw_guarded, cfg, *u): u for u in units}
            for f in as_completed(futs):
                src, start, *_ = futs[f]
                done += 1
                try:
                    rows[src] = rows.get(src, 0) + f.result()
                except Exception as e:
                    errors.append({"source": src, "date": str(start), "error": str(e)[:300]})
                self._prog(vehicle, raw_done=done, source=src)

        # 2) EVENT
        self._prog(vehicle, stage="event", source=None)
        event = self.pipe.run_event(vehicle)

        # 3) FEATURE — event DB 전체 대상
        ev_dates = self.pipe.event_date_count(vehicle)
        self._prog(vehicle, stage="feature", event_dates=ev_dates)
        feature = self.pipe.run_feature(vehicle)

        self._prog(vehicle, stage="done", elapsed=round(time.time() - t0, 2))
        result = {
            "vehicle": vehicle, "product": cfg["product"],
            "raw_rows": rows, "raw_units": len(units), "errors": errors,
            "event": {s: e["event_rows"] for s, e in event.items()},
            "feature": feature["features"], "event_dates": ev_dates,
            "elapsed_sec": round(time.time() - t0, 2),
        }
        if self.on_vehicle_done:
            try:
                self.on_vehicle_done(vehicle, result)
            except Exception:
                pass
        return result

    # ── 전 vehicle 순회 (중복 실행 방지) ──
    def run_all(self, plan: WorkerPlan | None = None, mode: str = "manual") -> dict:
        if not self._run_lock.acquire(blocking=False):
            return {"ok": False, "skipped": "이미 실행 중", "progress": self.snapshot()}
        try:
            plan = plan or self.plan()
            t0 = time.time()
            vehicles = list(self.pipe.vehicles().keys())
            if mode == "loop":
                self._loop_count += 1
            with self._lock:
                self.progress = {"running": True, "mode": mode, "loop": self.loop_enabled(),
                                 "loop_iter": self._loop_count, "started": t0, "ts": t0,
                                 "vehicles": {v: {"stage": "queued"} for v in vehicles}}

            # 전역 raw 동시 상한 재설정 — 전 vehicle 합쳐 raw 쿼리 ≤ raw_workers(=raw_api_max).
            self._raw_sem = threading.BoundedSemaphore(max(1, plan.raw_workers))
            results: dict[str, dict] = {}
            with ThreadPoolExecutor(max_workers=plan.vehicle_workers,
                                    thread_name_prefix="vehicle") as ex:
                futs = {ex.submit(self.run_vehicle, v, plan): v for v in vehicles}
                for f in as_completed(futs):
                    v = futs[f]
                    try:
                        results[v] = f.result()
                    except Exception as e:
                        results[v] = {"vehicle": v, "error": str(e)[:300]}
                        self._prog(v, stage="error", error=str(e)[:200])

            summary = {
                "ok": all("error" not in r for r in results.values()),
                "mode": mode, "vehicles": results, "plan": plan.__dict__,
                "elapsed_sec": round(time.time() - t0, 2), "ts": t0,
            }
            self.last_run = summary
            return summary
        finally:
            with self._lock:
                self.progress["running"] = False
                self.progress["ts"] = time.time()
            self._run_lock.release()

    # ── 백그라운드 루프 (schedule / loop) ──
    async def _loop(self):
        while True:
            rt = self.runtime_cfg()
            loop_on = bool(rt.get("loop_enabled"))
            interval = float(rt.get("interval_hours") or 0) * 3600
            sched_on = bool(rt.get("schedule_enabled")) and interval > 0
            if loop_on:
                try:
                    await asyncio.to_thread(self.run_all, None, "loop")
                except Exception:
                    pass
                await asyncio.sleep(max(2.0, float(rt.get("loop_gap_sec") or 3)))
            elif sched_on:
                try:
                    await asyncio.to_thread(self.run_all, None, "schedule")
                except Exception:
                    pass
                await asyncio.sleep(max(60.0, interval))
            else:
                await asyncio.sleep(5)    # 비활성 — 루프/스케줄 토글 빠르게 감지

    def start_background(self):
        if self._bg_task is None or self._bg_task.done():
            self._bg_task = asyncio.get_event_loop().create_task(self._loop())

    def stop_background(self):
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
        self._bg_task = None
