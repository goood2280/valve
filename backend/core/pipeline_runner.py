"""
Valve · pipeline_runner
-----------------------
파이프라인 병렬 오케스트레이터 + 진행상황(stage) 추적 + 주기/루프 스케줄러.

기본 동작 (run_all):
  vehicle 을 순회하며 (vehicle_workers 만큼 동시에)
    1) raw     : (source × 1일) 유닛을 raw_workers 스레드풀로 병렬 쿼리
    2) event   : 5일치를 event DB 화 (소스별 매칭 필터)
    3) feature : event DB "전체" 를 대상으로 feature 산출 (특정 기간 아님)
    4) wide    : vehicle 의 feature 를 ML_TABLE 로 병합 (4.WIDE_FORM)
  전 vehicle 완료 후:
    5) send    : ML_TABLE 전체 병합 → KNOB/FAB(+MASK)/VM/INLINE 분리 (5.SEND_FORM)
  진행 중에는 self.progress 가 vehicle 별 현재 단계(raw/event/feature)를 담아
  /api/pipeline/progress 로 노출 → 백필이 지금 어느 DB 단계인지 실시간 확인.

스케줄 (제품별 독립):
  · vehicles.yaml 의 `runs_per_day` 로 제품마다 하루 실행 횟수를 따로 준다.
    (일 3회 = 8시간 간격 · 일 6회 = 4시간 간격 · 0 이면 자동 실행 안 함)
    값이 없는 제품은 전역 runtime.interval_hours 를 쓴다 (24/interval_hours 회).
  · 백그라운드 루프가 sched_tick_sec(기본 60초)마다 "지금 돌 때가 된 제품"만
    골라 run_all(vehicles=due) 한다. 마지막 실행 시각의 근거는 run_log —
    메모리에 두면 재기동마다 전 제품이 due 로 잡혀 매번 전량 재실행된다.
  · loop_enabled : 위 주기를 무시하고 전 제품을 쉬지 않고 반복 (개발/백필용)

실행 락 (_run_lock) — 산출물을 건드리는 **모든** 진입점이 공유한다:
  · 스케줄/루프/수동 전체 실행 (run_all)
  · 수동 단건 실행            (run_vehicle_once  ← POST /api/pipeline/run/{vehicle})
  · 매칭 csv 갱신 후 재생성   (rebuild_after_config_change ← csv_sync on_updated)
  · wide / send_form 단독 실행 (run_wide_once · run_send_form_once)
  락을 공유하지 않으면 두 스레드가 같은 2.EVENT_DB/{vehicle}/{source}/ 를 동시에
  rmtree + write 한다. 그때 생기는 손상 세 가지:
    (a) 한쪽이 rebuild 로 파티션을 지우는 사이 다른 쪽이 "이미 있음"으로 판정해
        건너뛴다 → event DB 에 날짜 구멍. 예외 없이 조용히 틀린 feature 가 나간다.
    (b) 같은 data.parquet 에 동시 write → 잘린 parquet → 다음 _load_event 가 실패.
    (c) _meta.json 을 나중에 쓴 쪽이 이긴다 → 옛 매칭 기준 event 에 새 ver 이
        찍히면 stale 로 안 잡혀 영영 재생성되지 않는다.
"""
from __future__ import annotations

import asyncio
import copy
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from backend.core.feature_pipeline import FeaturePipeline
from backend.core.pipeline_retry import PipelineRetryStore
from backend.core.run_log import RunLog
from backend.core.runtime_env import WorkerPlan, plan_workers

DAY_SEC = 86400.0
# 실행 금지 시간대 기본값 — pipeline.yaml 에 키가 없는 기존 설치에도 적용된다
QUIET_DEFAULT = ("00:00", "02:00")


TASK_HISTORY = 20        # 끝난 작업은 최근 N 건만 큐 화면에 남긴다


class PipelineBusy(RuntimeError):
    """다른 실행(스케줄/수동/재생성)이 산출물을 쓰고 있어 지금은 진입 불가."""


class PipelineCancelled(RuntimeError):
    """사용자가 큐에서 취소 — 안전 지점(유닛/단계 경계)에서 중단됐다."""


class InterProcessLock:
    """threading.Lock 호환 + 같은 DB root를 쓰는 다른 프로세스까지 직렬화."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Lock()
        self._fh = None

    def _try_os_lock(self) -> bool:
        fh = open(self.path, "a+b")
        try:
            if fh.seek(0, os.SEEK_END) == 0:
                fh.write(b"0")
                fh.flush()
            fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fh = fh
            return True
        except (OSError, IOError):
            fh.close()
            return False

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        deadline = None if timeout is None or timeout < 0 else time.monotonic() + timeout
        while True:
            if self._thread.acquire(blocking=False):
                if self._try_os_lock():
                    return True
                self._thread.release()
            if not blocking or (deadline is not None and time.monotonic() >= deadline):
                return False
            time.sleep(0.05)

    def release(self):
        fh, self._fh = self._fh, None
        if fh is not None:
            try:
                fh.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
            finally:
                fh.close()
        self._thread.release()

    def locked(self) -> bool:
        return self._thread.locked()


class PipelineRunner:
    def __init__(self, pipe: FeaturePipeline, run_log: RunLog | None = None):
        self.pipe = pipe
        self.runs = run_log or RunLog(pipe.root / "logs" / "pipeline_runs.jsonl")
        self.retries = PipelineRetryStore(pipe.root / "logs" / "pipeline_retries.json")
        self._bg_task: asyncio.Task | None = None
        self.last_run: dict | None = None
        self.last_rebuild: dict | None = None   # 매칭 갱신 재생성 결과 (조용한 실패 방지)
        self.on_vehicle_done = None  # Callable[[str, dict], None] — 알람 발행 훅
        self._lock = threading.Lock()       # progress 보호
        # 산출물(event/feature/wide/send)을 쓰는 모든 진입점의 공용 락 — 모듈 docstring 참조
        lock_root = pipe.db_root() if callable(getattr(pipe, "db_root", None)) \
            else (Path(pipe.root) / "db")
        self._run_lock = InterProcessLock(lock_root / ".valve_pipeline.lock")
        self._raw_sem = threading.BoundedSemaphore(3)  # 전역 raw 동시 상한 (run_all 시 재설정)
        self._loop_count = 0
        # 작업 큐 — 실행/대기/최근 작업을 한 곳에서 보고 취소한다 (/api/pipeline/queue)
        self._tasks: dict[str, dict] = {}
        self._task_seq = 0
        self._current_task: dict | None = None
        self.progress: dict = {"running": False, "mode": None, "loop": False,
                               "loop_iter": 0, "started": None, "ts": None, "vehicles": {}}

    # ── 설정/계획 ──
    def runtime_cfg(self) -> dict:
        return self.pipe.global_cfg().get("runtime") or {}

    def plan(self) -> WorkerPlan:
        return plan_workers(self.runtime_cfg())

    def retry_delays(self) -> list[int]:
        values = self.runtime_cfg().get("retry_delays_sec") or [300, 900, 1800, 3600]
        try:
            parsed = [max(1, int(v)) for v in values]
        except (TypeError, ValueError):
            parsed = [300, 900, 1800, 3600]
        return parsed or [300, 900, 1800, 3600]

    def max_retry_attempts(self) -> int:
        """이 횟수만큼 연속 실패하면 자동 재시도를 멈추고 critical 로 올린다.
        (일시 장애가 아니라 자격증명 만료 같은 사람이 고칠 문제로 본다)"""
        try:
            return max(1, int(self.runtime_cfg().get("retry_max_attempts") or 5))
        except (TypeError, ValueError):
            return 5

    def retry_severity(self, vehicle: str, now: float | None = None) -> str:
        summary = self.retries.summary(vehicle, now)
        rt = self.runtime_cfg()
        critical_age = int(rt.get("retry_critical_after_sec") or 43200)
        critical_attempts = int(rt.get("retry_critical_attempts") or 5)
        if summary.get("blocked"):
            return "critical"        # 자동 재시도 중단 — 사람이 봐야 한다
        if summary["pending"] and (summary["oldest_age_sec"] >= critical_age
                                   or summary["max_attempts"] >= critical_attempts):
            return "critical"
        return "warning" if summary["pending"] else "info"

    # ── 작업 큐 (실행 중 · 락 대기 · 예정) + 취소 ──────────────
    # 파이프라인의 모든 쓰기 진입점은 하나의 실행 락을 공유한다. 예전에는 그 락을
    # 기다리는 작업(특히 매칭 갱신 재생성)이 화면에 안 보여서 "왜 아무 일도 안 하지" 로
    # 보였다. 여기에 등록해 두면 무엇이 돌고 무엇이 대기 중인지 보이고 취소도 된다.
    def _task_new(self, kind: str, label: str, vehicles=None,
                  cancellable: bool = True) -> dict:
        with self._lock:
            self._task_seq += 1
            task = {"id": f"t{self._task_seq}", "kind": kind, "label": label,
                    "vehicles": list(vehicles or []), "state": "waiting",
                    "enqueued_ts": time.time(), "started_ts": None, "ended_ts": None,
                    "cancel": False, "cancellable": bool(cancellable), "result": None}
            self._tasks[task["id"]] = task
            self._trim_tasks_locked()
            return task

    def _task_set(self, task: dict, **kw):
        with self._lock:
            task.update(kw)

    def _task_end(self, task: dict, state: str, result: str | None = None):
        with self._lock:
            task.update({"state": state, "ended_ts": time.time(), "result": result})
            self._trim_tasks_locked()

    def _trim_tasks_locked(self):
        done = [t for t in self._tasks.values() if t["state"] not in ("waiting", "running")]
        done.sort(key=lambda t: t.get("ended_ts") or 0)
        for t in done[:-TASK_HISTORY]:
            self._tasks.pop(t["id"], None)

    def _cancelled(self) -> bool:
        task = self._current_task
        return bool(task and task.get("cancel"))

    def _checkpoint(self, what: str):
        """안전 지점에서만 취소를 반영한다 — event/feature 를 쓰는 도중에 끊으면
        파티션이 반만 남는다. 단계 경계와 raw 유닛 경계에서만 부른다."""
        if self._cancelled():
            raise PipelineCancelled(f"{what} 직전 취소됨")

    def cancel_task(self, task_id: str) -> dict:
        """대기 중이면 즉시 취소, 실행 중이면 다음 안전 지점에서 중단(cancelling)."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return {"ok": False, "error": f"알 수 없는 작업: {task_id}"}
            if task["state"] not in ("waiting", "running"):
                return {"ok": False, "error": "이미 끝난 작업입니다"}
            if not task["cancellable"]:
                return {"ok": False, "error": "취소할 수 없는 작업입니다"}
            task["cancel"] = True
            if task["state"] == "waiting":
                task.update({"state": "cancelled", "ended_ts": time.time(),
                             "result": "대기 중 취소됨"})
                return {"ok": True, "state": "cancelled", "id": task_id}
        return {"ok": True, "state": "cancelling", "id": task_id}

    def queue(self, now: float | None = None) -> dict:
        """작업 큐 스냅샷 — 실행 중 · 락 대기 · 예정(스케줄/재시도) · 최근 완료."""
        now = now or time.time()
        with self._lock:
            tasks = [dict(t) for t in self._tasks.values()]
        tasks.sort(key=lambda t: t.get("enqueued_ts") or 0)
        upcoming = []
        try:
            for v, p in self.schedule_plan(now).items():
                if not p["enabled"] or p["next_ts"] is None:
                    continue
                upcoming.append({
                    "vehicle": v, "next_ts": p["next_ts"], "due": p["due"],
                    "retry_pending": p["retry_pending"], "retry_blocked": p.get("retry_blocked", 0),
                    "quiet_blocked": p["quiet_blocked"]})
            upcoming.sort(key=lambda x: x["next_ts"])
        except Exception:
            upcoming = []
        return {
            "running": [t for t in tasks if t["state"] == "running"],
            "waiting": [t for t in tasks if t["state"] == "waiting"],
            "recent": [t for t in tasks if t["state"] not in ("running", "waiting")][::-1][:10],
            "upcoming": upcoming,
            "busy": self._run_lock.locked(),
            "retry": self.retries.summary(),
            "ts": now,
        }

    def schedule_enabled(self) -> bool:
        """자동 실행 마스터 스위치 — 켜져 있고 실제 주기를 가진 제품이 하나라도 있으면 True."""
        if not self.runtime_cfg().get("schedule_enabled"):
            return False
        return any(p["interval_sec"] > 0 for p in self.schedule_plan().values())

    def loop_enabled(self) -> bool:
        return bool(self.runtime_cfg().get("loop_enabled"))

    # ── 실행 금지 시간대 (quiet window) ──────────────────────
    # 야간 백업/사내 API 점검처럼 파이프라인이 돌면 안 되는 구간을 비워둔다.
    #   quiet_start/quiet_end : "HH:MM" (서버 로컬 시각) · start > end 면 자정을 넘는 구간
    #   대상은 자동 실행(스케줄·루프)뿐 — 사람이 직접 누른 수동 실행은 막지 않는다.
    #   이미 돌고 있는 실행은 중단하지 않는다 (중간에 끊으면 event 파티션이 반만 남는다).
    @staticmethod
    def parse_hhmm(text) -> int | None:
        """'HH:MM' → 자정 기준 분. 형식이 틀리면 None (24:00 = 1440 허용).

        yaml 을 손으로 고쳐 `quiet_start: 9:00` 처럼 따옴표 없이 적으면 YAML 1.1 의
        60진수 규칙에 걸려 정수 540(=9*60) 으로 읽힌다 — 그 값도 분으로 받아준다.
        (`09:00` 처럼 0 으로 시작하면 문자열로 남는다. UI 저장은 항상 문자열.)
        """
        if isinstance(text, int) and not isinstance(text, bool):
            return text if 0 <= text <= 1440 else None
        m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(text or ""))
        if not m:
            return None
        h, mi = int(m.group(1)), int(m.group(2))
        if h > 24 or mi > 59 or (h == 24 and mi):
            return None
        return h * 60 + mi

    def quiet_cfg(self) -> tuple[bool, str, str]:
        """(사용여부, 시작, 종료). 키가 아예 없는 기존 설치도 기본값(00:00~02:00)을 받는다 —
        config/ 는 seed-only 라 업그레이드해도 pipeline.yaml 이 갱신되지 않기 때문.
        빈 문자열은 '지웠다' 는 뜻이므로 기본값으로 되살리지 않는다."""
        rt = self.runtime_cfg()
        return (bool(rt.get("quiet_enabled", True)),
                rt["quiet_start"] if "quiet_start" in rt else QUIET_DEFAULT[0],
                rt["quiet_end"] if "quiet_end" in rt else QUIET_DEFAULT[1])

    def quiet_window(self) -> tuple[int, int] | None:
        """(시작분, 종료분). 꺼져 있거나 값이 없거나 시작==종료(빈 구간)면 None."""
        enabled, start, end = self.quiet_cfg()
        if not enabled:
            return None
        s, e = self.parse_hhmm(start), self.parse_hhmm(end)
        if s is None or e is None or s == e:
            return None
        return s, e

    def quiet_now(self, now: float | None = None) -> bool:
        w = self.quiet_window()
        if not w:
            return False
        s, e = w
        lt = time.localtime(now if now is not None else time.time())
        cur = lt.tm_hour * 60 + lt.tm_min
        return (s <= cur < e) if s < e else (cur >= s or cur < e)

    def quiet_until(self, now: float | None = None) -> float | None:
        """지금이 금지 시간대면 해제 시각(epoch), 아니면 None."""
        w = self.quiet_window()
        if not w or not self.quiet_now(now):
            return None
        now = now if now is not None else time.time()
        lt = time.localtime(now)
        midnight = now - (lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec)
        end = midnight + w[1] * 60
        return end if end > now else end + DAY_SEC

    def quiet_state(self, now: float | None = None) -> dict:
        """UI 표시용 — 설정값 + 지금 막고 있는지 + 언제 풀리는지."""
        enabled, start, end = self.quiet_cfg()
        w = self.quiet_window()
        return {
            "enabled": enabled,
            "start": str(start or ""),
            "end": str(end or ""),
            "valid": w is not None,
            "now": self.quiet_now(now),
            "until": self.quiet_until(now),
        }

    # ── 제품별 주기 ──────────────────────────────────────────
    def default_runs_per_day(self) -> float:
        """runs_per_day 가 없는 제품의 기본값 — 전역 interval_hours 에서 환산."""
        ih = float(self.runtime_cfg().get("interval_hours") or 0)
        return round(DAY_SEC / (ih * 3600), 4) if ih > 0 else 0.0

    def runs_per_day(self, vehicle_cfg: dict) -> tuple[float, str]:
        """(하루 실행 횟수, 출처). 0 이면 자동 실행 안 함."""
        rpd = vehicle_cfg.get("runs_per_day")
        if rpd is None or str(rpd).strip() == "":
            return self.default_runs_per_day(), "global"
        try:
            return max(0.0, float(rpd)), "vehicle"
        except (TypeError, ValueError):
            return self.default_runs_per_day(), "global"

    def schedule_plan(self, now: float | None = None) -> dict[str, dict]:
        """제품별 주기 현황 — 화면/스케줄러 공용.
        {vehicle: {runs_per_day, source, interval_sec, last_ts, next_ts, due, enabled}}"""
        now = now or time.time()
        last = self.runs.last_started()
        master = bool(self.runtime_cfg().get("schedule_enabled"))
        quiet_until = self.quiet_until(now)   # 금지 시간대면 해제 시각, 아니면 None
        out = {}
        for v, cfg in self.pipe.vehicles().items():
            rpd, src = self.runs_per_day(cfg or {})
            interval = DAY_SEC / rpd if rpd > 0 else 0.0
            t = last.get(v)
            normal_next = (t + interval) if (t and interval > 0) else (now if interval > 0 else None)
            retry = self.retries.summary(v, now)
            retry_next = retry.get("next_retry_at") if retry.get("pending") else None
            candidates = [x for x in (normal_next, retry_next) if x is not None]
            next_ts = min(candidates) if candidates else None
            # 금지 시간대에 걸린 예정은 해제 시각으로 밀린다 (건너뛰는 게 아니라 미뤄짐)
            if quiet_until and next_ts is not None and next_ts < quiet_until:
                next_ts = quiet_until
            normal_due = bool(interval > 0 and (t is None or now - t >= interval))
            retry_due = bool(interval > 0 and retry.get("due"))
            out[v] = {
                "vehicle": v, "product": (cfg or {}).get("product"),
                "runs_per_day": rpd, "source": src,
                "interval_sec": interval,
                "interval_hours": round(interval / 3600, 2) if interval else 0,
                "last_ts": t, "next_ts": next_ts,
                "due": bool((normal_due or retry_due) and not quiet_until),
                "retry_pending": retry.get("pending", 0),
                "retry_due": retry.get("due", 0),
                # blocked = 연속 실패로 자동 재시도가 멈춘 유닛 (resume 필요)
                "retry_blocked": retry.get("blocked", 0),
                "retry_hint": retry.get("hint", ""),
                "retry_oldest_age_sec": retry.get("oldest_age_sec", 0),
                "retry_next_ts": retry_next,
                "quiet_blocked": bool(quiet_until and interval > 0),
                "enabled": bool(master and interval > 0),
            }
        return out

    def due_vehicles(self, now: float | None = None) -> list[str]:
        """지금 돌 때가 된 제품. 이력이 없으면(최초 기동) 즉시 due."""
        return [v for v, p in self.schedule_plan(now).items() if p["enabled"] and p["due"]]

    # ── progress ──
    def _prog(self, vehicle: str, **kw):
        with self._lock:
            self.progress.setdefault("vehicles", {}).setdefault(vehicle, {}).update(kw)
            self.progress["ts"] = time.time()

    def snapshot(self) -> dict:
        with self._lock:
            snap = copy.deepcopy(self.progress)
        if self.last_rebuild is not None:
            snap["last_rebuild"] = self.last_rebuild
        # running 만 보면 회차 사이(loop_gap_sec)·스케줄 tick 사이에 잠깐 False 가 되어
        # 화면이 "실행 중 ↔ 대기" 를 오간다. 루프/스케줄이 켜져 있다는 사실을 같이 실어
        # 프론트가 그 공백을 "실행 중(다음 회차 준비)" 으로 계속 표시하게 한다.
        try:
            rt = self.runtime_cfg()
        except Exception:
            rt = {}
        snap["loop_enabled"] = bool(rt.get("loop_enabled"))
        snap["schedule_enabled"] = bool(rt.get("schedule_enabled"))
        snap["busy"] = self._run_lock.locked()   # 재생성 등 락을 잡은 다른 작업 포함
        try:
            snap["quiet"] = self.quiet_state()
        except Exception:
            snap["quiet"] = {"enabled": False, "now": False}
        snap["retry"] = self.retries.summary()
        return snap

    def _raw_guarded(self, cfg, source, start, end, split) -> int:
        """전역 raw 세마포어를 잡고 raw 유닛 실행 — 전 vehicle 합쳐 동시 raw ≤ raw_api_max.
        취소되면 아직 시작하지 않은 유닛은 실행하지 않는다 (실패로 기록하지 않는다)."""
        self._checkpoint(f"{source} {start} raw")
        with self._raw_sem:
            self._checkpoint(f"{source} {start} raw")
            return self.pipe._run_raw_unit(cfg, source, start, end, split)

    # ── 단일 vehicle: 병렬 raw → event → feature (단계별 progress + 실행 로그) ──
    def run_vehicle(self, vehicle: str, plan: WorkerPlan | None = None,
                    mode: str = "manual") -> dict:
        plan = plan or self.plan()
        t0 = time.time()
        # 단계별 소요/산출을 여기 모아 run_log 에 그대로 남긴다 (화면 로그의 원본)
        stages: dict[str, dict] = {}
        rec = {"ts": t0, "vehicle": vehicle, "mode": mode, "ok": False, "stages": stages}
        try:
            cfg = self.pipe.vehicle_cfg(vehicle)
            rec["product"] = cfg.get("product")
            units = self.pipe._raw_units(cfg)
            # Failed units remain retryable after they leave the rolling raw_days window.
            # De-duplicate because recent failed dates are also present in normal units.
            unit_keys = {(str(s), str(a), str(b), str(label)) for s, a, b, label in units}
            for unit in self.retries.due_units(vehicle):
                key = tuple(str(v) for v in unit)
                if key not in unit_keys:
                    units.append(unit)
                    unit_keys.add(key)

            # 1) RAW — (source × 날짜) 병렬. 동시 실행은 전역 raw 세마포어가 제한(≤ raw_api_max).
            ts = time.time()
            self._prog(vehicle, stage="raw", raw_done=0, raw_total=len(units), source=None)
            rows: dict[str, int] = {}
            errors: list[dict] = []
            done = 0
            cancelled_units = 0
            with ThreadPoolExecutor(max_workers=max(1, plan.raw_workers),
                                    thread_name_prefix=f"raw-{vehicle}") as ex:
                futs = {ex.submit(self._raw_guarded, cfg, *u): u for u in units}
                recovered = []
                for f in as_completed(futs):
                    src, start, end, split = futs[f]
                    done += 1
                    try:
                        rows[src] = rows.get(src, 0) + f.result()
                        resolved = self.retries.mark_success(vehicle, src, start, end, split)
                        if resolved:
                            recovered.append({"source": src, "date": str(start),
                                              "attempts": resolved.get("attempts")})
                    except PipelineCancelled:
                        cancelled_units += 1     # 취소로 안 돈 유닛 — 실패가 아니다
                    except Exception as e:
                        retry = self.retries.record_failure(
                            vehicle, src, start, end, split, str(e), self.retry_delays(),
                            max_attempts=self.max_retry_attempts())
                        errors.append({"source": src, "date": str(start), "error": str(e)[:300],
                                       "attempts": retry["attempts"],
                                       "blocked": retry["status"] == "blocked",
                                       "next_retry_at": retry["next_retry_at"]})
                    self._prog(vehicle, raw_done=done, source=src)
            stages["raw"] = {"sec": round(time.time() - ts, 2), "units": len(units),
                             "rows": rows, "errors": errors, "recovered": recovered,
                             "cancelled_units": cancelled_units,
                             "pruned": self.pipe.prune_raw(vehicle)}

            # 2) EVENT — 취소는 여기(단계 경계)까지만. raw 만 갱신된 채 끝나도
            # 다음 실행에서 event 가 따라잡는다 (파티션 신선도 비교).
            self._checkpoint("event")
            ts = time.time()
            self._prog(vehicle, stage="event", source=None)
            event = self.pipe.run_event(vehicle)
            stages["event"] = {
                "sec": round(time.time() - ts, 2),
                "sources": {s: {k: e.get(k) for k in
                                ("raw_rows", "event_rows", "partitions", "rebuilt")}
                            for s, e in event.items()},
                "rebuilt": [s for s, e in event.items() if e.get("rebuilt")],
            }

            # 3) FEATURE — event DB 전체 대상
            self._checkpoint("feature")
            ts = time.time()
            ev_dates = self.pipe.event_date_count(vehicle)
            self._prog(vehicle, stage="feature", event_dates=ev_dates)
            if ev_dates == 0:
                # 조회는 끝났는데 event 가 한 건도 없다 = 그 구간에 lot 이 없다
                # (새 제품·비가동 구간·조건에 맞는 데이터 없음).
                # run_feature 는 이때 RuntimeError 를 내는데, 그대로 두면 '데이터 없음' 이
                # 매 회차 critical 알람이 된다. 데이터가 없는 것은 고장이 아니다 —
                # 단계를 건너뛰고 기록만 남긴다 (기존 feature/ML_TABLE 도 보존).
                feature = {"features": {}, "empty": True}
                stages["feature"] = {"sec": 0.0, "counts": {}, "event_dates": 0,
                                     "skipped": ["event 0건 — 조회 구간에 데이터가 없습니다"]}
                self._prog(vehicle, stage="feature_skipped", event_dates=0)
            else:
                feature = self.pipe.run_feature(vehicle)
                stages["feature"] = {
                    "sec": round(time.time() - ts, 2), "counts": feature["features"],
                    "event_dates": ev_dates,
                    "skipped": feature.get("skipped") or [],
                    "agg_overrides": feature.get("agg_overrides") or [],
                    "knob_miss": len(feature.get("knob_miss") or []),
                    "knob_skip": len(feature.get("knob_skip") or []),
                }

            # 4) WIDE — raw 일부라도 실패한 회차는 이전 정상 ML_TABLE을 보존한다.
            # event/feature 진단은 계속 수행하지만 부분 입력으로 정상본을 덮지 않는다.
            self._checkpoint("wide")
            ts = time.time()
            if errors or feature.get("empty"):
                self._prog(vehicle, stage="wide_skipped")
                wide = ({"skipped": "event 0건 — 이전 ML_TABLE 보존"} if feature.get("empty")
                        else {"skipped": "raw 조회 실패로 이전 정상본 보존",
                              "raw_errors": len(errors)})
                stages["wide"] = {"sec": 0.0, **wide}
            else:
                self._prog(vehicle, stage="wide")
                wide = self.pipe.run_wide(vehicle)
                stages["wide"] = {"sec": round(time.time() - ts, 2),
                                  **{k: wide.get(k) for k in ("rows", "features", "path")
                                     if k in wide}}
                # Flow가 읽는 db_root/ML_TABLE_{product}.parquet도 같은 성공 회차에서만 교체.
                stages["wide"]["flow_publish"] = self.pipe.run_flow_tables()

            self._prog(vehicle, stage="done", elapsed=round(time.time() - t0, 2))
            result = {
                "vehicle": vehicle, "product": cfg["product"],
                "raw_rows": rows, "raw_units": len(units), "errors": errors,
                "event": {s: e["event_rows"] for s, e in event.items()},
                "feature": feature["features"], "event_dates": ev_dates,
                "wide": wide,
                "elapsed_sec": round(time.time() - t0, 2),
            }
            retry_summary = self.retries.summary(vehicle)
            rec["ok"] = not errors
            rec["severity"] = self.retry_severity(vehicle)
            rec["retry"] = {k: retry_summary[k] for k in
                            ("pending", "due", "oldest_age_sec", "next_retry_at", "max_attempts")}
            result["severity"] = rec["severity"]
            result["retry"] = dict(rec["retry"])
            self._prog(vehicle, stage="done", severity=rec["severity"],
                       retry_pending=retry_summary["pending"])
            return result
        except PipelineCancelled as e:
            # 취소는 장애가 아니다 — 안전 지점에서 멈췄고, 남은 일은 다음 실행이 잇는다.
            # error 를 채워 두면 알람 발행 훅이 옛 리포트를 내보내지 않는다.
            rec["cancelled"] = True
            rec["error"] = str(e)[:500]
            rec["severity"] = "warning"
            self._prog(vehicle, stage="cancelled", error=str(e)[:200])
            return {"vehicle": vehicle, "cancelled": True, "reason": str(e),
                    "stages": stages, "elapsed_sec": round(time.time() - t0, 2)}
        except Exception as e:
            rec["error"] = str(e)[:500]
            rec["severity"] = "critical"
            raise
        finally:
            rec["ended_ts"] = time.time()
            rec["elapsed_sec"] = round(rec["ended_ts"] - t0, 2)
            self.runs.append(rec)
            # 알람 발행 훅 — raw 유닛 일부 실패(ok=False)는 feature 까지 정상이라 발행하지만,
            # 중간에 터져 단계가 덜 돈 경우(error)는 옛 리포트를 발행하지 않는다.
            if self.on_vehicle_done and not rec.get("error"):
                try:
                    self.on_vehicle_done(vehicle, rec)
                except Exception:
                    pass

    # ── vehicle 순회 (중복 실행 방지). vehicles=None 이면 전 제품 ──
    def run_all(self, plan: WorkerPlan | None = None, mode: str = "manual",
                vehicles: list[str] | None = None) -> dict:
        label = {"schedule": "자동 실행", "loop": "루프 실행"}.get(mode, "전체 실행")
        task = self._task_new("run", label, vehicles)
        if not self._run_lock.acquire(blocking=False):
            self._task_end(task, "skipped", "이미 다른 실행이 진행 중")
            return {"ok": False, "skipped": "이미 실행 중", "progress": self.snapshot()}
        self._task_set(task, state="running", started_ts=time.time())
        self._current_task = task
        try:
            plan = plan or self.plan()
            t0 = time.time()
            known = list(self.pipe.vehicles().keys())
            vehicles = [v for v in (vehicles or known) if v in known]
            if not vehicles:
                self._task_end(task, "done", "대상 제품 없음")
                return {"ok": True, "skipped": "대상 제품 없음", "mode": mode, "vehicles": {}}
            self._task_set(task, vehicles=list(vehicles))
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
                futs = {ex.submit(self.run_vehicle, v, plan, mode): v for v in vehicles}
                for f in as_completed(futs):
                    v = futs[f]
                    try:
                        results[v] = f.result()
                    except Exception as e:
                        results[v] = {"vehicle": v, "error": str(e)[:300]}
                        self._prog(v, stage="error", error=str(e)[:200])

            # 5) SEND FORM — 전 vehicle가 모두 정상인 회차에서만 교체한다.
            cancelled = self._cancelled()
            run_errors = any("error" in r or r.get("errors") for r in results.values())
            if cancelled or run_errors:
                send_form = {"skipped": "취소됨" if cancelled else
                             "일부 vehicle raw/파이프라인 실패 — 이전 정상본 보존"}
            else:
                with self._lock:
                    self.progress["send_form"] = "running"
                    self.progress["ts"] = time.time()
                try:
                    send_form = self.pipe.run_send_form()
                    send_form["flow_publish"] = self.pipe.run_flow_tables()
                except Exception as e:
                    send_form = {"error": str(e)[:300]}
                with self._lock:
                    self.progress["send_form"] = "error" if "error" in send_form else "done"

            summary = {
                "ok": (not cancelled
                       and all("error" not in r and not r.get("errors")
                               for r in results.values())),
                "cancelled": cancelled,
                "mode": mode, "vehicles": results, "plan": plan.__dict__,
                "send_form": send_form,
                "elapsed_sec": round(time.time() - t0, 2), "ts": t0,
            }
            self.last_run = summary
            return summary
        finally:
            with self._lock:
                self.progress["running"] = False
                self.progress["ts"] = time.time()
            self._current_task = None
            if task["state"] == "running":
                self._task_end(task, "cancelled" if task["cancel"] else "done")
            self._run_lock.release()

    # ── 락을 공유하는 단독 진입점들 ───────────────────────────────
    def _enter(self, what: str, kind: str = "job", vehicles=None,
               cancellable: bool = False) -> dict:
        """산출물 쓰기 구간 진입. 다른 실행 중이면 PipelineBusy.
        작업 큐에 등록해서 무엇이 돌고 있는지 화면에 보이게 한다."""
        task = self._task_new(kind, what, vehicles, cancellable=cancellable)
        if not self._run_lock.acquire(blocking=False):
            self._task_end(task, "skipped", "다른 실행이 진행 중")
            raise PipelineBusy(
                f"{what}: 다른 파이프라인 실행이 진행 중입니다 "
                f"(mode={self.snapshot().get('mode')}) — 끝난 뒤 다시 시도하세요")
        self._task_set(task, state="running", started_ts=time.time())
        return task

    def _leave(self, task: dict, state: str = "done", result: str | None = None):
        if task["state"] == "running":
            self._task_end(task, "cancelled" if task["cancel"] else state, result)
        self._run_lock.release()

    def run_vehicle_once(self, vehicle: str) -> dict:
        """수동 단건 실행 (raw→event→feature→wide→unmatched). 스케줄러와 락 공유.
        스케줄 실행과 같은 경로를 타므로 실행 로그·진행상황이 똑같이 남는다."""
        self.pipe.vehicle_cfg(vehicle)          # 없는 vehicle 이면 여기서 ValueError → 404
        task = self._enter(f"{vehicle} 수동 실행", "run", [vehicle], cancellable=True)
        self._current_task = task
        try:
            plan = self.plan()
            self._raw_sem = threading.BoundedSemaphore(max(1, plan.raw_workers))
            with self._lock:
                self.progress = {"running": True, "mode": "manual", "loop": False,
                                 "loop_iter": self._loop_count, "started": time.time(),
                                 "ts": time.time(), "vehicles": {vehicle: {"stage": "queued"}}}
            result = self.run_vehicle(vehicle, plan, mode="manual")
            if result.get("cancelled"):
                return result
            try:
                result["unmatched"] = self.pipe.scan_unmatched(vehicle)
            except Exception as e:
                result["unmatched"] = {"error": str(e)[:300]}
            return result
        finally:
            with self._lock:
                self.progress["running"] = False
                self.progress["ts"] = time.time()
            self._current_task = None
            self._leave(task)

    def run_wide_once(self, vehicle: str) -> dict:
        task = self._enter(f"{vehicle} wide 병합", "wide", [vehicle])
        try:
            out = self.pipe.run_wide(vehicle)
            out["flow_publish"] = self.pipe.run_flow_tables()
            return out
        finally:
            self._leave(task)

    def run_send_form_once(self) -> dict:
        task = self._enter("send form 생성", "send")
        try:
            return self.pipe.run_send_form()
        finally:
            self._leave(task)

    def rebuild_after_config_change(self, timeout: float = 1800) -> dict:
        """매칭 csv 갱신 후 전 vehicle event/feature/wide 재생성 + 알람 재발행.

        여기서는 skip 하지 않고 **기다린다** — 건너뛰면 옛 매칭 기준 산출물이
        그대로 남아 flow 로 나간다. 실행 중인 run_all 이 끝나면 이어서 돈다.
        결과는 last_rebuild 에 남긴다 (예전엔 예외를 조용히 삼켜 실패가 안 보였다).
        """
        t0 = time.time()
        task = self._task_new("rebuild", "매칭 갱신 재생성", cancellable=True)
        # 락 대기 중에도 큐 화면에 "대기" 로 보이고, 대기 상태에서 취소할 수 있다
        if not self._acquire_for(task, timeout):
            cancelled = task["cancel"]
            out = {"ok": False, "cancelled": cancelled,
                   "waited_sec": round(time.time() - t0, 1),
                   "error": ("대기 중 취소됨" if cancelled else
                             f"실행 락 대기 시간 초과({timeout}s) — 재생성 건너뜀")}
            if not cancelled:
                self._task_end(task, "error", out["error"])
            self.last_rebuild = out
            return out
        self._task_set(task, state="running", started_ts=time.time())
        self._current_task = task
        try:
            done, errors = [], {}
            cancelled = False
            for v in self.pipe.vehicles():
                if self._cancelled():
                    cancelled = True
                    break                      # vehicle 경계에서만 중단 (안전 지점)
                try:
                    self._prog(v, stage="rebuild")
                    self.pipe.run_event(v)
                    self.pipe.run_feature(v)
                    wide = self.pipe.run_wide(v)
                    done.append(v)
                    self._prog(v, stage="done")
                    if self.on_vehicle_done:
                        self.on_vehicle_done(v, wide)
                except Exception as e:
                    errors[v] = str(e)[:300]
                    self._prog(v, stage="error", error=str(e)[:200])
            if not cancelled:
                try:
                    self.pipe.run_send_form()
                    self.pipe.run_flow_tables()
                except Exception as e:
                    errors["send_form"] = str(e)[:300]
            out = {"ok": not errors and not cancelled, "cancelled": cancelled,
                   "vehicles": done, "errors": errors,
                   "waited_sec": round(time.time() - t0, 1), "ts": t0}
            self.last_rebuild = out
            return out
        finally:
            self._current_task = None
            self._leave(task, result=f"{len(done)}개 제품 재생성" if done else None)

    def _acquire_for(self, task: dict, timeout: float | None) -> bool:
        """실행 락을 기다리되 취소를 반영한다. 취소/시간초과면 False."""
        deadline = None if timeout is None else time.time() + float(timeout)
        while True:
            if task.get("cancel"):
                return False
            if self._run_lock.acquire(timeout=0.5):
                if task.get("cancel"):      # 대기 중 취소 → 잡은 락을 바로 놓는다
                    self._run_lock.release()
                    return False
                return True
            if deadline is not None and time.time() >= deadline:
                return False

    # ── 백그라운드 루프 (제품별 주기 / loop) ──
    async def _loop(self):
        """sched_tick_sec 마다 due 인 제품만 골라 돌린다.
        제품마다 runs_per_day 가 달라도 한 루프가 전부 커버한다."""
        while True:
            rt = self.runtime_cfg()
            # 실행 금지 시간대 — 자동/루프 모두 새 실행을 띄우지 않는다.
            # (이미 돌고 있던 실행은 그대로 끝까지 간다 — 중간에 끊으면 산출물이 반만 남는다)
            if self.quiet_now():
                await asyncio.sleep(30)
                continue
            if rt.get("loop_enabled"):
                try:
                    await asyncio.to_thread(self.run_all, None, "loop")
                except Exception:
                    pass
                await asyncio.sleep(max(2.0, float(rt.get("loop_gap_sec") or 3)))
                continue
            tick = max(10.0, float(rt.get("sched_tick_sec") or 60))
            if not rt.get("schedule_enabled"):
                await asyncio.sleep(5)    # 비활성 — 토글을 빠르게 감지
                continue
            try:
                due = self.due_vehicles()
                if due:
                    await asyncio.to_thread(self.run_all, None, "schedule", due)
            except Exception:
                pass
            await asyncio.sleep(tick)

    def start_background(self):
        if self._bg_task is None or self._bg_task.done():
            self._bg_task = asyncio.get_event_loop().create_task(self._loop())

    def stop_background(self):
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
        self._bg_task = None
