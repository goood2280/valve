"""StateStore — append-only log + snapshot + 크래시 복구."""
from __future__ import annotations

from backend.core.state import StateStore


def test_record_plan_creates_planned_partition(tmp_path):
    s = StateStore(tmp_path / "logs" / "jobs.jsonl")
    plan = {"plan_id": "PRODA-FAB-2026-04-24", "product": "PRODA", "source": "FAB",
            "date": "2026-04-24", "chunks": [
                {"chunk_id": "PRODA-FAB-2026-04-24-00", "product": "PRODA",
                 "source": "FAB", "date": "2026-04-24", "status": "pending"}
            ]}
    s.record_plan(plan)
    snap = s.snapshot()
    assert "PRODA-FAB-2026-04-24" in snap["plans"]
    assert snap["partitions"]["PRODA/FAB/2026-04-24"]["status"] == "planned"


def test_chunk_success_updates_partition(tmp_path):
    s = StateStore(tmp_path / "logs" / "jobs.jsonl")
    plan = {"plan_id": "PRODA-FAB-D", "product": "PRODA", "source": "FAB", "date": "D",
            "chunks": [{"chunk_id": "c1", "status": "pending"}]}
    s.record_plan(plan)
    s.update_chunk("c1", {"product": "PRODA", "source": "FAB", "date": "D",
                          "status": "success", "actual_rows": 100})
    part = s.snapshot()["partitions"]["PRODA/FAB/D"]
    assert part["status"] == "success"
    assert part["done_chunks"] == 1


def test_crash_recovery_flips_in_progress_to_pending(tmp_path):
    log = tmp_path / "logs" / "jobs.jsonl"
    s1 = StateStore(log)
    plan = {"plan_id": "X", "product": "X", "source": "F", "date": "D",
            "chunks": [{"chunk_id": "c1"}]}
    s1.record_plan(plan)
    s1.update_chunk("c1", {"product": "X", "source": "F", "date": "D",
                           "status": "in_progress", "started_at": 1.0})
    # 강제 재시작 (같은 로그 다시 로드)
    s2 = StateStore(log)
    assert s2.snapshot()["chunks"]["c1"]["status"] == "pending"
    assert s2.snapshot()["chunks"]["c1"].get("recovered") is True


def test_append_log_survives_restart(tmp_path):
    log = tmp_path / "logs" / "jobs.jsonl"
    s1 = StateStore(log)
    s1.record_plan({"plan_id": "p1", "product": "A", "source": "F", "date": "D", "chunks": []})
    s1.update_chunk("c1", {"product": "A", "source": "F", "date": "D", "status": "success"})

    s2 = StateStore(log)
    assert "p1" in s2.snapshot()["plans"]
    assert s2.snapshot()["chunks"]["c1"]["status"] == "success"


def test_partition_failed_when_chunk_failed(tmp_path):
    s = StateStore(tmp_path / "logs" / "jobs.jsonl")
    # plan_id 는 {product}-{source}-{date} 규약 — state.py 가 그렇게 조립함
    plan = {"plan_id": "X-F-D", "product": "X", "source": "F", "date": "D",
            "chunks": [{"chunk_id": "c1"}, {"chunk_id": "c2"}]}
    s.record_plan(plan)
    s.update_chunk("c1", {"product": "X", "source": "F", "date": "D", "status": "success"})
    s.update_chunk("c2", {"product": "X", "source": "F", "date": "D", "status": "failed",
                          "error": "HY000"})
    assert s.snapshot()["partitions"]["X/F/D"]["status"] == "partial_failed"
