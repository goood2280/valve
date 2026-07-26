"""s3_jobs 라우터 — S3 업/다운로드 항목 CRUD · 수동 실행/중지 · 진행률 · 이력.

탐색기 톱니바퀴(⚙) 모달이 소비한다. flow 의 /api/s3ingest 와 같은 사용 모델.

  GET    /api/s3/items          항목 + 상태 + 진행률 (5초 폴링)
  POST   /api/s3/save           항목 추가/수정
  POST   /api/s3/delete         항목 삭제
  POST   /api/s3/run            수동 실행 (큐에 넣음)
  POST   /api/s3/stop           중지 — 대기중이면 큐에서 제거, 실행중이면 파일 경계에서 취소
  POST   /api/s3/auto-sync      자동 다운로드/업로드 마스터 토글
  GET    /api/s3/history        실행 이력
  GET    /api/s3/browse-keys    S3 key 고르기 (prefix 아래 목록)
  GET    /api/s3/destinations   S3 연결 목록 (s3_transfer.yaml)
"""
from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

router = APIRouter(prefix="/api/s3", tags=["s3"])

_jobs = None


def deps(jobs):
    global _jobs
    _jobs = jobs


def _j():
    if _jobs is None:
        raise HTTPException(500, "s3_jobs not initialized")
    return _jobs


@router.get("/items")
def items():
    return _j().list_with_status()


@router.post("/save")
def save(body: dict = Body(...)):
    try:
        return {"ok": True, "item": _j().upsert(body)}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/delete")
def delete(body: dict = Body(...)):
    item_id = str(body.get("id") or "").strip()
    if not item_id:
        raise HTTPException(400, "id 필요")
    return {"ok": _j().delete(item_id)}


@router.post("/run")
def run(body: dict = Body(...)):
    try:
        return _j().run(str(body.get("id") or "").strip())
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.post("/stop")
def stop(body: dict = Body(...)):
    return _j().stop(str(body.get("id") or "").strip())


@router.post("/auto-sync")
def auto_sync(body: dict = Body(...)):
    cfg = _j().set_auto(download=body.get("auto_download_enabled"),
                        upload=body.get("auto_upload_enabled"))
    return {"ok": True, "auto_download_enabled": cfg["auto_download_enabled"],
            "auto_upload_enabled": cfg["auto_upload_enabled"]}


@router.get("/history")
def history(id: str = "", limit: int = 100):
    return {"history": _j().history(item_id=id, limit=limit)}


@router.get("/browse-keys")
def browse_keys(dest: str = "default", prefix: str = "", limit: int = 300):
    """S3 를 훑어 key 를 고르게 — 항목 추가 폼의 'key 선택' 버튼."""
    try:
        return _j().browse_keys(dest, prefix, limit)
    except Exception as e:
        raise HTTPException(400, str(e)[:300])


@router.get("/destinations")
def destinations():
    from backend.routers.browser import transfer_destinations
    return {"destinations": transfer_destinations()}


@router.get("/local-paths")
def local_paths(root: str = Query("config"), limit: int = 400):
    """target 자동완성 — root 아래 파일/폴더 상대경로."""
    jobs = _j()
    if root not in jobs.roots:
        raise HTTPException(400, f"알 수 없는 root {root!r}")
    base = jobs.roots[root]
    out = []
    try:
        for p in sorted(base.rglob("*")):
            rel = str(p.relative_to(base)).replace("\\", "/")
            out.append({"path": rel, "is_dir": p.is_dir()})
            if len(out) >= limit:
                break
    except Exception:
        pass
    return {"root": root, "paths": out}
