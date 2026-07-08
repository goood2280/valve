"""browser 라우터 — staging · s3_local · config · db 파일탐색기 (flow FileBrowser 경량 버전)."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

from backend.core import s3_link

router = APIRouter(prefix="/api/browser", tags=["browser"])

_roots: dict[str, Path] = {}
_annotate = None  # s3 연동 신호등 어노테이터 (app.py 가 주입)


def deps(staging_root: Path, s3_local_root: Path | None, extra_roots: dict[str, Path] | None = None,
         annotator=None):
    """extra_roots — 항상 노출할 추가 루트 (config: csv 설정파일, db: 파이프라인 산출물).
    annotator — s3_link.build_annotator(...) 결과 (파일별 다운로드/업로드 신호등)."""
    global _roots, _annotate
    _roots = {"staging": Path(staging_root)}
    if s3_local_root:
        _roots["s3_local"] = Path(s3_local_root)
    for name, p in (extra_roots or {}).items():
        _roots[name] = Path(p)
    _annotate = annotator


def resolve(root: str, rel: str) -> Path:
    """root 이름 + 상대경로 → 검증된 절대경로 (query 라우터도 공유)."""
    base = _roots.get(root)
    if base is None:
        raise HTTPException(404, f"unknown root {root!r}")
    base = base.resolve()
    target = (base / rel).resolve() if rel else base
    try:
        target.relative_to(base)
    except ValueError:
        raise HTTPException(400, "path escape")
    return target


@router.get("/roots")
def list_roots():
    return {"roots": [{"name": n, "path": str(p), **s3_link.root_role(n)}
                      for n, p in _roots.items()
                      if n in ("staging", "config") or p.exists()]}


@router.get("/list")
def list_dir(root: str = Query(...), path: str = Query("")):
    base = _roots.get(root)
    if base is None:
        raise HTTPException(404, f"unknown root {root!r}")
    base.mkdir(parents=True, exist_ok=True)
    target = resolve(root, path)
    if not target.exists():
        return {"entries": [], "path": path}
    if not target.is_dir():
        raise HTTPException(400, "path is not a directory")

    # config 루트에 노출할 설정파일 확장자 (csv 는 표로, 나머지는 텍스트로 열람)
    CONFIG_SUFFIXES = {".csv", ".yaml", ".yml", ".json", ".txt", ".md"}
    entries = []
    for p in sorted(target.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
        # config 루트는 설정파일(csv/yaml/json/…)만 노출 — 그 외 잡파일은 숨김
        if root == "config" and p.is_file() and p.suffix.lower() not in CONFIG_SUFFIXES:
            continue
        try:
            stat = p.stat()
            rel = f"{path}/{p.name}" if path else p.name
            entries.append({
                "name": p.name,
                "is_dir": p.is_dir(),
                "size": stat.st_size if not p.is_dir() else 0,
                "mtime": stat.st_mtime,
                "suffix": p.suffix,
                "_rel": rel,
                "_abs": str(p),
            })
        except Exception:
            continue

    # s3 연동 신호등 부착 (다운로드↓/업로드↑ · ok/pending/error/idle)
    if _annotate is not None:
        try:
            sync = _annotate(root, [(e["_rel"], e["is_dir"], e["_abs"]) for e in entries])
            for e in entries:
                e["sync"] = sync.get(e["_rel"])
        except Exception:
            pass
    for e in entries:
        e.pop("_rel", None)
        e.pop("_abs", None)
    return {"entries": entries, "path": path, "root": root}
