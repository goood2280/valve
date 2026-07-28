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


def test_run_log_assigns_and_filters_severity(tmp_path):
    log = RunLog(tmp_path / "runs.jsonl")
    log.append({"ts": 1, "vehicle": "V", "ok": True})
    log.append({"ts": 2, "vehicle": "V", "ok": False})
    log.append({"ts": 3, "vehicle": "V", "ok": False, "error": "boom"})
    assert [r["severity"] for r in log.tail(limit=3)] == ["critical", "warning", "info"]
    assert len(log.tail(limit=10, severity="warning")) == 1
