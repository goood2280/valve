"""StateStore rotate — 크기 초과 시 회전 + snapshot 라인 복원."""
from __future__ import annotations

from backend.core.state import StateStore


def test_rotate_creates_numbered_backup(tmp_path):
    log = tmp_path / "logs" / "jobs.jsonl"
    # 아주 작은 임계값 — plan 2개 record 하면 rotate 트리거
    s = StateStore(log, max_bytes=1024, keep=3)
    # 큰 plan 3개 반복 기록 → rotate 최소 1회
    for i in range(6):
        plan = {"plan_id": f"p{i}", "product": "A", "source": "F",
                "date": f"2026-04-{20+i}", "chunks": [{"chunk_id": f"c{i}", "status": "pending"}]}
        s.record_plan(plan)

    # 현재 로그 + .1 백업이 존재
    assert log.exists()
    assert log.with_suffix(log.suffix + ".1").exists()


def test_rotate_snapshot_allows_replay(tmp_path):
    log = tmp_path / "logs" / "jobs.jsonl"
    s = StateStore(log, max_bytes=1200, keep=3)
    # 최종 상태: 여러 plan + chunk 업데이트
    for i in range(5):
        pid = f"plan-{i}"
        s.record_plan({"plan_id": pid, "product": "A", "source": "F", "date": f"D{i}",
                       "chunks": [{"chunk_id": f"c{i}", "status": "pending"}]})
        s.update_chunk(f"c{i}", {"product": "A", "source": "F", "date": f"D{i}",
                                 "status": "success"})
    memory_plans = len(s.snapshot()["plans"])
    memory_chunks = len(s.snapshot()["chunks"])
    assert memory_plans == 5
    assert memory_chunks == 5

    # 새 StateStore 인스턴스로 파일 replay — rotate snapshot 으로도 동일 상태 복원돼야 함
    s2 = StateStore(log, max_bytes=1200, keep=3)
    assert len(s2.snapshot()["plans"]) == memory_plans
    assert len(s2.snapshot()["chunks"]) == memory_chunks


def test_rotate_keep_limit(tmp_path):
    log = tmp_path / "logs" / "jobs.jsonl"
    s = StateStore(log, max_bytes=1024, keep=2)
    # 많이 쌓아서 .3 이상이 만들어지지 않는지 확인
    for i in range(20):
        s.record_plan({"plan_id": f"p{i}", "product": "A", "source": "F", "date": f"D{i}",
                       "chunks": [{"chunk_id": f"c{i}"}]})

    # .1 / .2 까지만 존재해야 함 (keep=2)
    assert log.with_suffix(log.suffix + ".1").exists()
    # .3 은 없어야 함
    assert not log.with_suffix(log.suffix + ".3").exists()
