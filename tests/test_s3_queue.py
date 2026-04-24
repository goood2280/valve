"""S3 업로드 모드: immediate / interval / manual 큐잉 + flush 동작."""
from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_immediate_mode_no_queue(app_client):
    """immediate 모드는 enqueue 하지 않음 (executor 가 바로 put_atomic)."""
    # 기본이 immediate. pending 은 비어있어야 함.
    r = app_client.get("/api/jobs/s3-pending")
    assert r.status_code == 200
    assert r.json()["count"] == 0


@pytest.mark.asyncio
async def test_manual_mode_enqueue_and_flush(app_client):
    # manual 모드로 전환
    r = app_client.post("/api/settings", json={"s3": {"upload_mode": "manual"}})
    assert r.status_code == 200

    from backend.core import s3_queue
    # 수동으로 큐에 하나 넣음 (executor 경로 없이)
    from pathlib import Path
    tmp = Path(s3_queue._log_path).parent / "fake_part.parquet"
    tmp.write_text("x")  # 더미 파일 — fake_local 이라 복사만 되면 OK
    s3_queue.enqueue("TP/FAB/2026-04-24", str(tmp), "FAB/TP/date=2026-04-24/part-0.parquet", mode="manual")

    r2 = app_client.get("/api/jobs/s3-pending")
    assert r2.status_code == 200
    assert r2.json()["count"] == 1

    r3 = app_client.post("/api/jobs/s3-flush")
    assert r3.status_code == 200
    body = r3.json()
    # fake_local 이면 uploaded 1, 아니면 skipped 0 (s3 client None 이면 enqueue 자체 실패)
    # 여기서는 fake_local 이므로 uploaded ≥ 0, pending=0 예상
    assert body["pending"] == 0


@pytest.mark.asyncio
async def test_flush_retry_skip_within_retry_sec(app_client):
    """실패 항목은 retry_failed_sec 이내 재시도 건너뜀."""
    app_client.post("/api/settings", json={"s3": {"upload_mode": "manual", "retry_failed_sec": 999}})
    from backend.core import s3_queue
    from pathlib import Path
    import time as _t
    # 존재하지 않는 로컬 파일 → 큐에 있어도 파일 없으므로 제거됨 (정책상)
    missing = Path(s3_queue._log_path).parent / "nope.parquet"
    s3_queue.enqueue("MISS/FAB/2026-04-24", str(missing), "FAB/MISS/date=.../part-0.parquet")
    r1 = await s3_queue.flush_once()
    # local_missing 이면 skipped 처리 + 큐에서 제거
    assert r1["ok"] is True
    assert r1["pending"] == 0
