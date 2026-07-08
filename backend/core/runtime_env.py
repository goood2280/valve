"""
Valve · runtime_env
-------------------
호스트 자원(코어/메모리)을 읽어 파이프라인 워커 수를 산정한다.
파이프라인 스케줄러(pipeline_runner)가 이 계획대로 ThreadPool 을 띄운다.

산정 원칙 (auto):
  · raw_workers    = min(cores-2, 메모리예산 // mem_per_worker_gb)
                     → (source × day) raw 쿼리를 동시에 몇 개 돌릴지.
  · vehicle_workers= 동시에 처리할 vehicle 수 (기본 min(raw_workers, 4)).
  · feature_workers= feature 산출 병렬 (polars 자체가 멀티스레드라 보수적).

pipeline.yaml 의 `runtime` 블록으로 전부 override 가능:
  runtime:
    raw_days: 5              # 조회 일수
    split_days: 1            # 분할 단위(일)
    max_workers: auto        # auto | 정수
    vehicle_workers: auto
    feature_workers: auto
    mem_per_worker_gb: 4
    cpu_cores: auto          # auto | 정수 (테스트/컨테이너 제한용)
"""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass


def _auto(v) -> bool:
    return v is None or str(v).strip().lower() in ("", "auto")


def total_mem_gb() -> float | None:
    try:
        import psutil
        return psutil.virtual_memory().total / (1024 ** 3)
    except Exception:
        pass
    try:  # posix fallback
        return (os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")) / (1024 ** 3)
    except Exception:
        return None


def avail_mem_gb() -> float | None:
    try:
        import psutil
        return psutil.virtual_memory().available / (1024 ** 3)
    except Exception:
        return None


@dataclass
class WorkerPlan:
    cpu_cores: int
    total_mem_gb: float | None
    avail_mem_gb: float | None
    mem_per_worker_gb: float
    raw_workers: int         # (source × day) 동시 raw 쿼리 수
    vehicle_workers: int     # 동시 처리 vehicle 수
    feature_workers: int     # feature 병렬 산출 수
    sizing: str              # "auto" | "config"
    reason: str              # 산정 근거 (UI/로그 표시용)


def plan_workers(cfg: dict | None = None) -> WorkerPlan:
    """runtime 설정(cfg)을 받아 워커 계획을 산출. cfg 는 pipeline.yaml['runtime']."""
    cfg = cfg or {}

    cores = os.cpu_count() or 4
    if not _auto(cfg.get("cpu_cores")):
        cores = max(1, int(cfg["cpu_cores"]))

    total = total_mem_gb()
    avail = avail_mem_gb()
    mem_per = float(cfg.get("mem_per_worker_gb") or 4)

    if not _auto(cfg.get("max_workers")):
        raw_w = max(1, int(cfg["max_workers"]))
        sizing = "config"
        reason = f"max_workers={raw_w} (수동)"
    else:
        sizing = "auto"
        cpu_cap = max(1, cores - 2)          # 2 코어는 event loop/OS 여유
        raw_w = cpu_cap
        reason = f"cores {cores}-2={cpu_cap}"
        # 전용 파이프라인 호스트 가정 → 머신 총메모리 기준(available 이 아닌 total)으로
        # 상한 산정. total 이 없으면 available fallback.
        budget = total or avail
        if budget:
            mem_cap = max(1, int((budget * 0.8) // mem_per))
            if mem_cap < raw_w:
                raw_w = mem_cap
                reason = f"mem {budget:.0f}GB*0.8/{mem_per:.0f}={mem_cap}"

    vehicle_w = (max(1, int(cfg["vehicle_workers"])) if not _auto(cfg.get("vehicle_workers"))
                 else max(1, min(raw_w, 4)))
    feature_w = (max(1, int(cfg["feature_workers"])) if not _auto(cfg.get("feature_workers"))
                 else max(1, min(raw_w, max(2, cores // 4))))

    return WorkerPlan(
        cpu_cores=cores,
        total_mem_gb=round(total, 1) if total else None,
        avail_mem_gb=round(avail, 1) if avail else None,
        mem_per_worker_gb=mem_per,
        raw_workers=raw_w,
        vehicle_workers=vehicle_w,
        feature_workers=feature_w,
        sizing=sizing,
        reason=reason,
    )


def plan_dict(cfg: dict | None = None) -> dict:
    return asdict(plan_workers(cfg))
