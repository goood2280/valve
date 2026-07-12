"""browser s3-transfer — 전송 규칙(설정=cp·DB=sync) + 다중 S3 연결(destinations) × 이름(prefix)."""
import pytest
import yaml

from backend.core.s3_up import S3Uploader
from backend.routers import browser


@pytest.fixture()
def env(tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "db" / "5.SEND_FORM" / "1.FAB").mkdir(parents=True)
    (tmp_path / "staging").mkdir()
    (tmp_path / "config" / "vehicles.yaml").write_text("VH: {a: 1}\n", encoding="utf-8")
    (tmp_path / "db" / "5.SEND_FORM" / "1.FAB" / "FAB_ML_TABLE.parquet").write_bytes(b"\x00" * 64)
    s3 = S3Uploader({"s3": {"bucket": "test", "fake_local_path": str(tmp_path / "s3_local")}})
    browser.deps(tmp_path / "staging", tmp_path / "s3_local",
                 extra_roots={"config": tmp_path / "config", "db": tmp_path / "db"},
                 s3=s3)
    return tmp_path, s3


def test_default_rules_config_cp_db_sync(env):
    rules = browser.transfer_rules()
    assert rules["config"]["mode"] == "cp"
    assert rules["db"]["mode"] == "sync"
    assert rules["db"]["targets"] == [{"dest": "default", "prefix": "valve-export/db"}]
    # default 연결은 항상 존재
    assert browser.transfer_destinations()["default"]["builtin"]


def test_rules_dest_and_prefix_editable_and_persisted(env):
    tmp, _ = env
    saved = browser.save_transfer_rules({
        "destinations": {"flow2": {"bucket": "second", "fake_local_path": str(tmp / "s3_local")}},
        "rules": {"db": {"mode": "sync", "targets": [{"dest": "flow2", "prefix": "flow/DB/"}]}},
    })
    assert saved["rules"]["db"]["targets"] == [{"dest": "flow2", "prefix": "flow/DB"}]  # 슬래시 정리
    assert "flow2" in saved["destinations"]
    assert (tmp / "config" / "s3_transfer.yaml").exists()
    # 재로드해도 유지 + 표시용 key 는 첫 타겟 기준
    assert browser.transfer_rules()["db"]["targets"][0]["dest"] == "flow2"
    assert browser.s3_key_for("db", "x.parquet") == "flow/DB/x.parquet"
    # 나머지 root 는 기본값 유지
    assert browser.transfer_rules()["config"]["mode"] == "cp"


def test_legacy_prefix_format_still_loads(env):
    tmp, _ = env
    (tmp / "config" / "s3_transfer.yaml").write_text(
        yaml.safe_dump({"rules": {"db": {"mode": "sync", "prefix": "old/DB"}}}), encoding="utf-8")
    assert browser.transfer_rules()["db"]["targets"] == [{"dest": "default", "prefix": "old/DB"}]


def test_unknown_dest_rejected(env):
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        browser.save_transfer_rules(
            {"rules": {"db": {"mode": "sync", "targets": [{"dest": "nope", "prefix": "x"}]}}})


def test_config_cp_always_overwrites_sync_skips_same(env):
    r1 = browser.s3_transfer({"root": "config", "path": "vehicles.yaml", "mode": "cp"})
    assert r1["status"] == "uploaded"
    r2 = browser.s3_transfer({"root": "config", "path": "vehicles.yaml", "mode": "cp"})
    assert r2["status"] == "uploaded"                   # cp 는 항상 덮어씀
    r3 = browser.s3_transfer({"root": "config", "path": "vehicles.yaml", "mode": "sync"})
    assert r3["status"] == "unchanged"                  # sync 는 내용 같으면 skip


def test_db_dir_sync_uploads_changed_only(env):
    tmp, s3 = env
    # mode 미지정 → 규칙 기본(sync), 디렉토리 재귀 (parquet 바이너리)
    r1 = browser.s3_transfer({"root": "db", "path": ""})
    assert r1["mode"] == "sync" and r1["uploaded"] == 1 and r1["errors"] == 0
    assert s3.head("valve-export/db/5.SEND_FORM/1.FAB/FAB_ML_TABLE.parquet")

    r2 = browser.s3_transfer({"root": "db", "path": ""})
    assert r2["uploaded"] == 0 and r2["unchanged"] == 1  # 변경 없음 → skip

    (tmp / "db" / "5.SEND_FORM" / "1.FAB" / "FAB_ML_TABLE.parquet").write_bytes(b"\x01" * 128)
    r3 = browser.s3_transfer({"root": "db", "path": "5.SEND_FORM"})
    assert r3["uploaded"] == 1                           # 크기 변경 → 재업로드


def test_multi_key_targets_upload_to_both(env):
    tmp, s3 = env
    # S3 key 2개 — default(test 버킷) + second(다른 버킷/자격) 로 db 를 동시 전송
    browser.save_transfer_rules({
        "destinations": {"second": {"bucket": "second-bucket",
                                    "fake_local_path": str(tmp / "s3_local")}},
        "rules": {"db": {"mode": "sync", "targets": [
            {"dest": "default", "prefix": "valve-export/db"},
            {"dest": "second", "prefix": "share/DB"},
        ]}},
    })
    r = browser.s3_transfer({"root": "db", "path": ""})
    assert r["targets"] == ["default:valve-export/db", "second:share/DB"]
    assert r["uploaded"] == 2 and r["errors"] == 0       # 파일 1개 × 연결 2개
    assert s3.head("valve-export/db/5.SEND_FORM/1.FAB/FAB_ML_TABLE.parquet")
    assert (tmp / "s3_local" / "second-bucket" / "share" / "DB" /
            "5.SEND_FORM" / "1.FAB" / "FAB_ML_TABLE.parquet").exists()
    # 재전송 — 두 연결 모두 unchanged
    r2 = browser.s3_transfer({"root": "db", "path": ""})
    assert r2["uploaded"] == 0 and r2["unchanged"] == 2
    # dest 지정 시 해당 연결만
    r3 = browser.s3_transfer({"root": "db", "path": "", "dest": "second", "mode": "cp"})
    assert r3["uploaded"] == 1 and r3["targets"] == ["second:share/DB"]
