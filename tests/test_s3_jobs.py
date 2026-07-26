"""s3_jobs — 항목 CRUD · 업/다운로드 · 수동 실행/중지 · 진행률 · 이력."""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from backend.core.s3_jobs import S3Jobs
from backend.core.s3_up import S3Uploader


@pytest.fixture()
def env(tmp_path):
    root = tmp_path / "valve"
    (root / "config").mkdir(parents=True)
    (root / "staging").mkdir()
    up = S3Uploader({"s3": {"bucket": "b", "fake_local_path": str(tmp_path / "s3")}})
    jobs = S3Jobs(root=root, uploader_for=lambda _d: up,
                  roots={"config": root / "config", "staging": root / "staging"})
    return {"root": root, "up": up, "jobs": jobs}


def _run_sync(jobs: S3Jobs, item_id: str, timeout: float = 10.0):
    """큐가 아니라 직접 실행 — 테스트에서 워커 스레드를 기다리지 않도록."""
    from backend.core import s3_jobs as mod
    with mod._cond:
        mod._running[item_id] = {"started": time.time(), "cancel": False,
                                 "progress": {"done": 0, "total": 0, "current": ""}}
    try:
        jobs._execute(item_id)
    finally:
        with mod._cond:
            mod._running.pop(item_id, None)


# ── CRUD / 검증 ──────────────────────────────────────────
def test_upsert_and_delete(env):
    jobs = env["jobs"]
    jobs.upsert({"id": "a1", "direction": "download", "root": "config",
                 "target": "x.csv", "key": "p/x.csv"})
    assert [i["id"] for i in jobs.items()] == ["a1"]
    jobs.upsert({"id": "a1", "direction": "download", "root": "config",
                 "target": "x.csv", "key": "p/x.csv", "mode": "cp"})
    assert len(jobs.items()) == 1 and jobs.item("a1")["mode"] == "cp"
    assert jobs.delete("a1") and jobs.items() == []


@pytest.mark.parametrize("patch,msg", [
    ({"id": "bad id!"}, "id"),
    ({"direction": "sideways"}, "direction"),
    ({"root": "nope"}, "root"),
    ({"mode": "rsync"}, "mode"),
    ({"key": ""}, "key"),
])
def test_validate_rejects_bad_values(env, patch, msg):
    base = {"id": "ok", "direction": "download", "root": "config",
            "target": "x.csv", "key": "p/x.csv", "mode": "sync"}
    with pytest.raises(ValueError) as e:
        env["jobs"].validate({**base, **patch})
    assert msg in str(e.value)


def test_target_cannot_escape_root(env):
    with pytest.raises(ValueError):
        env["jobs"].validate({"id": "esc", "direction": "upload", "root": "config",
                              "target": "../../etc", "key": "k"})


# ── 업로드 ───────────────────────────────────────────────
def test_upload_folder_and_sync_skips_unchanged(env):
    jobs, up, root = env["jobs"], env["up"], env["root"]
    (root / "config" / "sub").mkdir()
    (root / "config" / "a.csv").write_text("A", encoding="utf-8")
    (root / "config" / "sub" / "b.csv").write_text("B", encoding="utf-8")
    jobs.upsert({"id": "up1", "direction": "upload", "root": "config", "target": "",
                 "key": "valve-config", "mode": "sync"})

    n_files = sum(1 for p in (root / "config").rglob("*") if p.is_file())
    _run_sync(jobs, "up1")
    st = jobs.status_all()["up1"]
    assert st["last_status"] == "ok" and st["moved"] == n_files
    assert up.get_text("valve-config/a.csv") == "A"
    assert up.get_text("valve-config/sub/b.csv") == "B"    # 하위 폴더까지 재귀

    _run_sync(jobs, "up1")                     # 두 번째는 전부 생략
    st2 = jobs.status_all()["up1"]
    assert st2["skipped"] == n_files and st2["moved"] == 0


def test_upload_single_file_uses_key_as_is(env):
    jobs, up, root = env["jobs"], env["up"], env["root"]
    (root / "config" / "one.csv").write_text("hi", encoding="utf-8")
    jobs.upsert({"id": "up2", "direction": "upload", "root": "config",
                 "target": "one.csv", "key": "flow/artifacts/one.csv"})
    _run_sync(jobs, "up2")
    assert up.get_text("flow/artifacts/one.csv") == "hi"


# ── 다운로드 ─────────────────────────────────────────────
def test_download_single_key_to_target(env):
    jobs, up, root = env["jobs"], env["up"], env["root"]
    up.put_text("flow/artifacts/matching/Vehicle_matching.csv", "vehicle,step_id\nV,S\n")
    jobs.upsert({"id": "dl1", "direction": "download", "root": "config",
                 "target": "step_matching/vehicle_matching.csv",
                 "key": "flow/artifacts/matching/Vehicle_matching.csv"})
    _run_sync(jobs, "dl1")
    dst = root / "config" / "step_matching" / "vehicle_matching.csv"
    assert dst.read_text(encoding="utf-8").startswith("vehicle,step_id")
    assert jobs.status_all()["dl1"]["moved"] == 1


def test_download_prefix_fans_out(env):
    jobs, up, root = env["jobs"], env["up"], env["root"]
    for n in ("a", "b"):
        up.put_text(f"flow/artifacts/matching/{n}.csv", n)
    jobs.upsert({"id": "dl2", "direction": "download", "root": "config",
                 "target": "feature_rules", "key": "flow/artifacts/matching"})
    _run_sync(jobs, "dl2")
    got = {p.name: p.read_text(encoding="utf-8")
           for p in (root / "config" / "feature_rules").glob("*.csv")}
    assert got == {"a.csv": "a", "b.csv": "b"}


def test_download_missing_key_is_an_error(env):
    jobs = env["jobs"]
    jobs.upsert({"id": "dl3", "direction": "download", "root": "config",
                 "target": "x.csv", "key": "nope/none.csv"})
    with pytest.raises(ValueError):
        _run_sync(jobs, "dl3")


def test_download_fires_hook_with_written_paths(env):
    jobs, up = env["jobs"], env["up"]
    seen = []
    jobs.on_downloaded = seen.extend
    up.put_text("flow/artifacts/matching/fab.csv", "step_desc,feature_name,agg\n")
    jobs.upsert({"id": "dl4", "direction": "download", "root": "config",
                 "target": "feature_rules/fab.csv", "key": "flow/artifacts/matching/fab.csv"})
    _run_sync(jobs, "dl4")
    assert seen and seen[0].endswith("fab.csv")


# ── 중지 ─────────────────────────────────────────────────
def test_stop_dequeues_a_waiting_item(env):
    from backend.core import s3_jobs as mod
    jobs = env["jobs"]
    jobs.upsert({"id": "q1", "direction": "upload", "root": "config",
                 "target": "", "key": "k"})
    with mod._cond:
        mod._queued.append("q1")
    try:
        r = jobs.stop("q1")
        assert r["dequeued"] is True
        with mod._cond:
            assert "q1" not in mod._queued
    finally:
        with mod._cond:
            if "q1" in mod._queued:
                mod._queued.remove("q1")


def test_stop_cancels_mid_transfer_and_keeps_finished_files(env):
    """중지는 파일 경계에서 — 이미 옮긴 파일은 남고 나머지가 멈춘다 (반쪽 파일 없음)."""
    from backend.core import s3_jobs as mod
    jobs, up, root = env["jobs"], env["up"], env["root"]
    for i in range(30):
        (root / "config" / f"f{i:02d}.csv").write_text(f"v{i}", encoding="utf-8")
    jobs.upsert({"id": "big", "direction": "upload", "root": "config",
                 "target": "", "key": "bulk", "mode": "cp"})

    with mod._cond:
        mod._running["big"] = {"started": time.time(), "cancel": False,
                               "progress": {"done": 0, "total": 0, "current": ""}}

    def cancel_soon():
        for _ in range(200):                    # 몇 개 옮긴 뒤 취소 플래그
            with mod._lock:
                slot = mod._running.get("big")
                if slot and slot["progress"].get("done", 0) >= 3:
                    slot["cancel"] = True
                    return
            time.sleep(0.005)

    t = threading.Thread(target=cancel_soon, daemon=True)
    t.start()
    try:
        jobs._execute("big")
    finally:
        with mod._cond:
            mod._running.pop("big", None)
    t.join(3)

    st = jobs.status_all()["big"]
    assert st["last_status"] == "cancelled"
    uploaded = [k for k in up.list_objects("bulk")]
    assert 0 < len(uploaded) < 30               # 일부만 올라갔다
    for k in uploaded:                          # 올라간 건 온전하다
        assert up.get_text(k).startswith("v")
    assert jobs.history("big")[0]["status"] == "cancelled"


# ── 상태/이력/조회 ────────────────────────────────────────
def test_list_with_status_reports_schedule_and_due(env):
    jobs = env["jobs"]
    jobs.upsert({"id": "s1", "direction": "download", "root": "config",
                 "target": "x.csv", "key": "k", "interval_min": 30})
    jobs.upsert({"id": "s2", "direction": "download", "root": "config",
                 "target": "y.csv", "key": "k2", "interval_min": 0})
    by = {i["id"]: i for i in jobs.list_with_status()["items"]}
    assert by["s1"]["scheduled"] and by["s1"]["due"]        # 이력 없으면 즉시 due
    assert not by["s2"]["scheduled"] and not by["s2"]["due"]   # 주기 0 = 수동 전용

    jobs.set_auto(download=False)
    by = {i["id"]: i for i in jobs.list_with_status()["items"]}
    assert not by["s1"]["scheduled"]           # 마스터 OFF 면 주기가 있어도 안 돈다


def test_history_records_each_run(env):
    jobs, root = env["jobs"], env["root"]
    (root / "config" / "h.csv").write_text("h", encoding="utf-8")
    jobs.upsert({"id": "h1", "direction": "upload", "root": "config",
                 "target": "h.csv", "key": "k/h.csv", "mode": "cp"})
    _run_sync(jobs, "h1")
    _run_sync(jobs, "h1")
    hist = jobs.history("h1")
    assert len(hist) == 2 and all(h["status"] == "ok" for h in hist)
    assert all("duration_sec" in h for h in hist)


def test_browse_keys_lists_folders_and_files(env):
    jobs, up = env["jobs"], env["up"]
    up.put_text("flow/artifacts/matching/a.csv", "a")
    up.put_text("flow/artifacts/other/b.csv", "b")
    b = jobs.browse_keys("default", "flow/artifacts")
    assert set(b["folders"]) == {"flow/artifacts/matching", "flow/artifacts/other"}
    assert "flow/artifacts/matching/a.csv" in b["keys"]


# ── 이관 ─────────────────────────────────────────────────
def test_migration_from_csv_sync_and_transfer_rules(env):
    from backend.core.csv_sync import CsvSync
    jobs, root = env["jobs"], env["root"]
    cs = CsvSync(root, env["up"])
    cs.save_config({"enabled": True, "interval_min": 30, "s3_prefix": "flow/artifacts",
                    "files": [{"key": "matching/Vehicle_matching.csv",
                               "dest": "config/step_matching/vehicle_matching.csv"},
                              {"key": "matching/scan_ignore_A.json",
                               "dest": "config/fab_scan/A/scan_ignore.json"},
                              {"key": "matching/scan_ignore_B.json",
                               "dest": "config/fab_scan/B/scan_ignore.json"}]})
    n = jobs.migrate_if_empty(cs, {"config": {"mode": "cp", "targets": [
        {"dest": "default", "prefix": "valve-config"}]}})
    assert n == 4
    items = {i["id"]: i for i in jobs.items()}
    assert len(items) == 4                       # id 충돌 없이 전부 살아남는다
    dl = next(i for i in items.values() if i["target"].endswith("vehicle_matching.csv"))
    assert dl["direction"] == "download" and dl["interval_min"] == 30
    assert dl["key"] == "flow/artifacts/matching/Vehicle_matching.csv"
    up_item = next(i for i in items.values() if i["direction"] == "upload")
    assert up_item["enabled"] is False           # 업로드는 수동으로 시작 (사고 방지)

    assert jobs.migrate_if_empty(cs, {}) == 0    # 두 번째 호출은 아무것도 안 함
