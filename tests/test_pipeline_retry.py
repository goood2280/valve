from datetime import date
import time

from backend.core.pipeline_retry import PipelineRetryStore
from backend.core.pipeline_runner import PipelineRunner
from backend.core.run_log import RunLog


def test_pipeline_retry_store_persists_until_success(tmp_path):
    path = tmp_path / "pipeline_retries.json"
    store = PipelineRetryStore(path)
    item = store.record_failure("VH_PRODA", "INLINE", date(2026, 1, 1),
                                date(2026, 1, 2), "old-split", "timeout",
                                delays=[5], now=1000)
    assert item["attempts"] == 1 and item["next_retry_at"] == 1005

    restored = PipelineRetryStore(path)
    assert restored.summary("VH_PRODA", now=1006)["due"] == 1
    assert restored.due_units("VH_PRODA", now=1006)[0][0] == "INLINE"
    assert restored.mark_success("VH_PRODA", "INLINE", date(2026, 1, 1),
                                 date(2026, 1, 2), "old-split")
    assert PipelineRetryStore(path).summary()["pending"] == 0


def test_retry_attempts_back_off_and_become_critical(tmp_path):
    store = PipelineRetryStore(tmp_path / "pipeline_retries.json")
    args = ("VH_PRODA", "INLINE", date(2026, 1, 1), date(2026, 1, 2), "split")
    first = store.record_failure(*args, "timeout", delays=[5, 15], now=1000)
    second = store.record_failure(*args, "timeout again", delays=[5, 15], now=1010)
    assert first["next_retry_at"] == 1005
    assert second["attempts"] == 2 and second["next_retry_at"] == 1025
    assert store.summary(now=1030)["max_attempts"] == 2


def test_retry_blocks_after_max_attempts_and_resumes(tmp_path):
    """무한 재시도 방지 — 5회 연속 실패하면 자동 재시도에서 빠지고(critical),
    원인을 고친 뒤 resume 으로 다시 큐에 들어간다."""
    path = tmp_path / "pipeline_retries.json"
    store = PipelineRetryStore(path)
    args = ("VH_PRODA", "FAB", date(2026, 1, 1), date(2026, 1, 2), "split")
    item = None
    for i in range(5):
        item = store.record_failure(*args, "auth expired", delays=[5],
                                    now=1000 + i, max_attempts=5)
    assert item["status"] == "blocked" and item["next_retry_at"] is None

    s = store.summary(now=99999)
    assert (s["blocked"], s["waiting"], s["due"]) == (1, 0, 0)
    assert s["next_retry_at"] is None and s["hint"]
    assert store.due_units("VH_PRODA", now=99999) == []      # 자동 재시도 안 함

    # 재기동 후에도 blocked 유지 → resume 하면 다시 대상
    reopened = PipelineRetryStore(path)
    assert reopened.summary()["blocked"] == 1
    assert reopened.resume(vehicle="VH_PRODA", now=2000) == 1
    assert reopened.summary(now=2001)["blocked"] == 0
    assert reopened.due_units("VH_PRODA", now=2001)[0][0] == "FAB"
    # attempts 는 리셋되고 누적은 total_attempts 로 남는다 (한 번 더 실패해도 즉시 blocked 아님)
    again = reopened.record_failure(*args, "still failing", delays=[5], now=2002,
                                    max_attempts=5)
    assert again["attempts"] == 1 and again["total_attempts"] == 6
    assert again["status"] == "retry_wait"


class _DummyPipe:
    def __init__(self, root):
        self.root = root

    def global_cfg(self):
        return {"runtime": {
            "interval_hours": 6, "schedule_enabled": True,
            "quiet_enabled": False, "retry_critical_attempts": 2,
        }}

    def vehicles(self):
        return {"VH_PRODA": {"vehicle": "VH_PRODA", "product": "PRODA"}}


def test_due_retry_overrides_normal_interval_and_survives_restart(tmp_path):
    pipe = _DummyPipe(tmp_path)
    runner = PipelineRunner(pipe)
    now = time.time()
    runner.runs.append({"ts": now, "vehicle": "VH_PRODA", "ok": True})
    runner.retries.record_failure("VH_PRODA", "INLINE", date(2026, 1, 1),
                                  date(2026, 1, 2), "old-split", "timeout",
                                  delays=[1], now=now - 10)

    restarted = PipelineRunner(pipe)
    plan = restarted.schedule_plan(now)
    assert plan["VH_PRODA"]["retry_due"] == 1
    assert plan["VH_PRODA"]["due"] is True


def test_blocked_retry_is_critical(tmp_path):
    runner = PipelineRunner(_DummyPipe(tmp_path))
    args = ("VH_PRODA", "FAB", date(2026, 1, 1), date(2026, 1, 2), "split")
    runner.retries.record_failure(*args, "auth expired", delays=[999999],
                                  now=time.time(), max_attempts=1)
    assert runner.retries.summary("VH_PRODA")["blocked"] == 1
    assert runner.retry_severity("VH_PRODA") == "critical"
    # 자동 재시도 대상이 아니므로 due 로도 잡히지 않는다
    assert runner.schedule_plan()["VH_PRODA"]["retry_due"] == 0


def test_queue_shows_waiting_task_and_cancels_it(tmp_path):
    """락을 기다리는 작업이 큐에 보이고, 대기 상태에서 취소된다."""
    runner = PipelineRunner(_DummyPipe(tmp_path))
    other = runner._task_new("run", "다른 실행")
    runner._run_lock.acquire()               # 다른 작업이 락을 잡고 있는 상황
    runner._task_set(other, state="running", started_ts=time.time())
    try:
        waiting = runner._task_new("rebuild", "매칭 갱신 재생성", cancellable=True)
        q = runner.queue()
        assert [t["label"] for t in q["running"]] == ["다른 실행"]
        assert [t["label"] for t in q["waiting"]] == ["매칭 갱신 재생성"]
        assert q["busy"] is True

        assert runner.cancel_task(waiting["id"])["state"] == "cancelled"
        assert runner._acquire_for(waiting, timeout=5) is False   # 취소됐으니 안 잡는다
        assert runner.queue()["waiting"] == []
        # 실행 중인 작업은 즉시 죽이지 않고 중단 요청만 (안전 지점에서 멈춘다)
        assert runner.cancel_task(other["id"])["state"] == "cancelling"
        assert runner._cancelled() is False   # _current_task 가 아니면 영향 없음
        runner._current_task = other
        assert runner._cancelled() is True
    finally:
        runner._current_task = None
        runner._run_lock.release()


def test_run_log_assigns_and_filters_severity(tmp_path):
    log = RunLog(tmp_path / "runs.jsonl")
    log.append({"ts": 1, "vehicle": "V", "ok": True})
    log.append({"ts": 2, "vehicle": "V", "ok": False})
    log.append({"ts": 3, "vehicle": "V", "ok": False, "error": "boom"})
    assert [r["severity"] for r in log.tail(limit=3)] == ["critical", "warning", "info"]
    assert len(log.tail(limit=10, severity="warning")) == 1
