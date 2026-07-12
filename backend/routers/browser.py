"""browser 라우터 — staging · s3_local · config · db 파일탐색기 (flow FileBrowser 경량 버전)."""
from __future__ import annotations

from pathlib import Path

import yaml
from fastapi import APIRouter, Body, HTTPException, Query

from backend.core import s3_link

router = APIRouter(prefix="/api/browser", tags=["browser"])

# config 루트에 노출/전송할 설정파일 확장자 (csv 는 표로, 나머지는 텍스트로 열람)
CONFIG_SUFFIXES = {".csv", ".yaml", ".yml", ".json", ".txt", ".md", ".py"}

_roots: dict[str, Path] = {}
_annotate = None       # s3 연동 신호등 어노테이터 (app.py 가 주입)
_s3 = None             # S3Uploader — s3 전송(put)
_csv_sync = None       # CsvSync — dest↔key 매핑으로 flow round-trip key 계산
_config_prefix = "valve-config"


def deps(staging_root: Path, s3_local_root: Path | None, extra_roots: dict[str, Path] | None = None,
         annotator=None, s3=None, csv_sync=None, config_prefix: str = "valve-config"):
    """extra_roots — 항상 노출할 추가 루트 (config: 설정파일, db: 파이프라인 산출물).
    annotator — 파일별 다운로드/업로드 신호등. s3/csv_sync — 파일별 S3 전송(업로드)용."""
    global _roots, _annotate, _s3, _csv_sync, _config_prefix
    _roots = {"staging": Path(staging_root)}
    if s3_local_root:
        _roots["s3_local"] = Path(s3_local_root)
    for name, p in (extra_roots or {}).items():
        _roots[name] = Path(p)
    _annotate = annotator
    _s3 = s3
    _csv_sync = csv_sync
    _config_prefix = (config_prefix or "valve-config").strip("/")


# ── S3 전송 규칙 (config/s3_transfer.yaml) ──
#   destinations : 이름별 S3 연결 (bucket/endpoint/access·secret key …).
#                  default = settings.json 의 s3 (앱 기본 연결 — 수정은 설정 탭).
#   rules        : root 별 mode(cp/sync) + targets[{dest, prefix}].
#                  같은 root 를 여러 S3 연결/이름으로 동시 전송 가능.
#   · cp   : 항상 덮어쓰기 업로드 — 설정파일용
#   · sync : 변경분만 업로드 (텍스트=내용 비교 · 바이너리=크기 비교) — DB 산출물용
TRANSFER_CFG_NAME = "s3_transfer.yaml"
DEST_FIELDS = ("endpoint_url", "bucket", "prefix", "access_key", "secret_key", "fake_local_path")
_dest_cache: dict[str, object] = {}   # dest 설정 → S3Uploader 재사용


def _default_rules() -> dict:
    return {
        "config": {"mode": "cp", "targets": [{"dest": "default", "prefix": _config_prefix}]},
        "db": {"mode": "sync", "targets": [{"dest": "default", "prefix": "valve-export/db"}]},
        "staging": {"mode": "sync", "targets": [{"dest": "default", "prefix": "valve-export/staging"}]},
    }


def _transfer_cfg_path() -> Path | None:
    base = _roots.get("config")
    return (base / TRANSFER_CFG_NAME) if base else None


def _load_transfer_cfg() -> dict:
    fp = _transfer_cfg_path()
    if fp and fp.exists():
        try:
            return yaml.safe_load(fp.read_text(encoding="utf-8")) or {}
        except Exception:
            return {}
    return {}


def transfer_destinations() -> dict:
    """이름별 S3 연결. default 는 항상 존재 (settings.json 의 s3)."""
    out = {"default": {"builtin": True}}
    for name, v in (_load_transfer_cfg().get("destinations") or {}).items():
        if name == "default" or not isinstance(v, dict):
            continue
        out[str(name)] = {k: str(v.get(k) or "") for k in DEST_FIELDS if str(v.get(k) or "").strip()}
    return out


def transfer_rules() -> dict:
    """기본 규칙 + config/s3_transfer.yaml 사용자 override.
    구 포맷 {mode, prefix} 는 단일 default 타겟으로 해석."""
    rules = _default_rules()
    user = _load_transfer_cfg().get("rules") or {}
    dests = transfer_destinations()
    for root, v in user.items():
        if root not in rules or not isinstance(v, dict):
            continue
        if v.get("mode") in ("cp", "sync"):
            rules[root]["mode"] = v["mode"]
        targets = v.get("targets")
        if targets is None and str(v.get("prefix") or "").strip():
            targets = [{"dest": "default", "prefix": v["prefix"]}]
        norm = []
        for t in targets or []:
            if not isinstance(t, dict):
                continue
            dest = str(t.get("dest") or "default").strip()
            prefix = str(t.get("prefix") or "").strip().strip("/")
            if prefix and dest in dests:
                norm.append({"dest": dest, "prefix": prefix})
        if norm:
            rules[root]["targets"] = norm
    return rules


def save_transfer_rules(body: dict) -> dict:
    """규칙/연결 저장 → config/s3_transfer.yaml.
    body: {rules: {root: {mode, targets: [{dest, prefix}]}}, destinations: {name: {...}}}."""
    cur = _load_transfer_cfg()
    if body.get("destinations") is None:
        dests_out = {k: v for k, v in (cur.get("destinations") or {}).items()
                     if k != "default" and isinstance(v, dict)}
    else:
        dests_out = {}
        for name, v in body["destinations"].items():
            name = str(name).strip()
            if not name or name == "default" or not isinstance(v, dict) or v.get("builtin"):
                continue
            d = {k: str(v.get(k) or "").strip() for k in DEST_FIELDS}
            if not d["bucket"]:
                raise HTTPException(400, f"S3 연결 {name}: bucket 필요")
            dests_out[name] = {k: val for k, val in d.items() if val}
    valid_dests = {"default", *dests_out}

    rules = _default_rules()
    for root, v in (body.get("rules") or {}).items():
        if root not in rules or not isinstance(v, dict):
            continue
        mode = str(v.get("mode") or rules[root]["mode"]).lower()
        if mode not in ("cp", "sync"):
            raise HTTPException(400, f"{root}: mode 는 cp | sync")
        targets = []
        for t in (v.get("targets") or []):
            dest = str((t or {}).get("dest") or "default").strip()
            prefix = str((t or {}).get("prefix") or "").strip().strip("/")
            if dest not in valid_dests:
                raise HTTPException(400, f"{root}: 알 수 없는 S3 연결 {dest!r}")
            if not prefix:
                raise HTTPException(400, f"{root}: prefix(이름) 가 비어있음")
            targets.append({"dest": dest, "prefix": prefix})
        if not targets:
            raise HTTPException(400, f"{root}: 전송 대상이 최소 1개 필요")
        rules[root] = {"mode": mode, "targets": targets}

    fp = _transfer_cfg_path()
    if fp is None:
        raise HTTPException(503, "config 루트 미설정")
    _dest_cache.clear()
    fp.write_text(yaml.safe_dump({"destinations": dests_out, "rules": rules},
                                 allow_unicode=True, sort_keys=False), encoding="utf-8")
    return {"destinations": dests_out, "rules": rules}


def _uploader_for(dest: str):
    """dest 이름 → S3Uploader. default 는 앱 기본 연결, 그 외는 destinations 설정으로 생성."""
    if dest == "default":
        return _s3
    cfg = transfer_destinations().get(dest)
    if not cfg or cfg.get("builtin"):
        raise HTTPException(400, f"알 수 없는 S3 연결 {dest!r}")
    import json
    ck = f"{dest}:{json.dumps(cfg, sort_keys=True)}"
    up = _dest_cache.get(ck)
    if up is None:
        from backend.core.s3_up import S3Uploader
        up = S3Uploader({"s3": cfg})
        _dest_cache[ck] = up
    return up


def _csv_roundtrip_key(rel: str) -> str | None:
    """config 파일이 csv_sync 다운로드 dest 면 그 S3 key (flow round-trip 유지)."""
    if _csv_sync is None:
        return None
    try:
        cfg = _csv_sync.load_config()
        for f in cfg.get("files", []):
            dest = (f.get("dest") or "").replace("\\", "/")
            if dest.startswith("config/"):
                dest = dest[len("config/"):]
            if dest == rel:
                return _csv_sync.full_key(cfg, f["key"])
    except Exception:
        pass
    return None


def _key_for_target(root: str, rel: str, target: dict) -> str:
    if root == "config" and target["dest"] == "default":
        k = _csv_roundtrip_key(rel)
        if k:
            return k
    return f"{target['prefix']}/{rel}"


def s3_key_for(root: str, rel: str) -> str:
    """표시용 대표 S3 key — 규칙의 첫 타겟 기준 (전송 자체는 전 타겟에 수행)."""
    rel = (rel or "").replace("\\", "/")
    targets = (transfer_rules().get(root) or {}).get("targets") \
        or [{"dest": "default",
             "prefix": _config_prefix if root == "config" else f"valve-export/{root}"}]
    return _key_for_target(root, rel, targets[0])


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


@router.get("/config-files")
def config_files():
    """config 루트의 설정파일을 재귀적으로 평평하게 — Roots 최상위 빠른 목록 + S3 전송용.
    각 파일에 신호등(sync)과 목표 S3 key 를 함께 반환."""
    base = _roots.get("config")
    if base is None:
        return {"files": []}
    base = base.resolve()
    items = []
    for p in sorted(base.rglob("*"), key=lambda x: str(x).lower()):
        if not p.is_file() or p.suffix.lower() not in CONFIG_SUFFIXES:
            continue
        rel = str(p.relative_to(base)).replace("\\", "/")
        try:
            st = p.stat()
        except Exception:
            continue
        items.append({"rel": rel, "name": p.name, "suffix": p.suffix,
                      "size": st.st_size, "mtime": st.st_mtime,
                      "s3_key": s3_key_for("config", rel), "_abs": str(p)})
    if _annotate is not None:
        try:
            sync = _annotate("config", [(it["rel"], False, it["_abs"]) for it in items])
            for it in items:
                it["sync"] = sync.get(it["rel"])
        except Exception:
            pass
    for it in items:
        it.pop("_abs", None)
    return {"files": items}


@router.get("/s3-transfer/config")
def s3_transfer_config():
    """전송 규칙 조회 — rules(root 별 mode+targets) + destinations(이름별 S3 연결)."""
    return {"rules": transfer_rules(), "destinations": transfer_destinations()}


@router.put("/s3-transfer/config")
def s3_transfer_config_save(body: dict = Body(...)):
    """전송 규칙/연결 저장 → config/s3_transfer.yaml."""
    saved = save_transfer_rules(body)
    return {"ok": True, "rules": saved["rules"],
            "destinations": {"default": {"builtin": True}, **saved["destinations"]}}


def _transfer_one(uploader, key: str, target: Path, mode: str, dest: str, rel: str) -> dict:
    """파일 1개 × 연결 1개 업로드. sync 는 변경분만 —
    텍스트(설정)는 내용, 바이너리(parquet 등)는 크기 비교."""
    is_text = target.suffix.lower() in CONFIG_SUFFIXES
    if mode == "sync":
        if is_text:
            local = target.read_text(encoding="utf-8")
            remote = uploader.get_text(key)
            if remote is not None and remote == local:
                return {"path": rel, "dest": dest, "status": "unchanged", "s3_key": key}
        else:
            head = uploader.head(key)
            if head is not None and head.get("size") == target.stat().st_size:
                return {"path": rel, "dest": dest, "status": "unchanged", "s3_key": key}
    ok = uploader.put_file(key, target)
    return {"path": rel, "dest": dest, "status": "uploaded" if ok else "error", "s3_key": key}


@router.post("/s3-transfer")
def s3_transfer(body: dict = Body(...)):
    """로컬 파일/디렉토리를 규칙의 전 타겟(S3 연결 × prefix)으로 업로드.
      · mode 미지정 시 전송 규칙(root 별 기본 — 설정=cp, DB=sync) 적용
      · cp   : 항상 덮어쓰기 · sync : 변경분만 (텍스트=내용 · 바이너리=크기 비교)
      · 디렉토리는 재귀 전송 (config 루트는 설정파일만)
      · dest 지정 시 해당 연결로만 전송
    body: {root, path, mode?, dest?}."""
    if _s3 is None:
        raise HTTPException(503, "s3 미설정")
    root = str(body.get("root") or "config")
    path = str(body.get("path") or "").strip()
    rules = transfer_rules()
    rule = rules.get(root) or {"mode": "sync",
                               "targets": [{"dest": "default", "prefix": f"valve-export/{root}"}]}
    mode = str(body.get("mode") or rule["mode"]).lower()
    if mode not in ("cp", "sync"):
        raise HTTPException(400, "mode 는 cp | sync")
    targets = rule["targets"]
    if body.get("dest"):
        targets = [t for t in targets if t["dest"] == str(body["dest"])]
        if not targets:
            raise HTTPException(400, f"규칙에 없는 S3 연결 {body['dest']!r}")
    target = resolve(root, path)
    if not target.exists():
        raise HTTPException(400, "경로 없음")
    if target.is_file() and root == "config" and target.suffix.lower() not in CONFIG_SUFFIXES:
        raise HTTPException(400, "설정파일(csv/yaml/json/txt/md)만 전송 지원")

    files: list[tuple[str, Path]] = []
    if target.is_file():
        files.append((path, target))
    else:
        base = _roots[root].resolve()
        for p in sorted(target.rglob("*")):
            if not p.is_file():
                continue
            if root == "config" and p.suffix.lower() not in CONFIG_SUFFIXES:
                continue
            files.append((str(p.relative_to(base)).replace("\\", "/"), p))
    if not files:
        raise HTTPException(400, "전송할 파일 없음")

    results = []
    for t in targets:
        uploader = _uploader_for(t["dest"])
        for rel, p in files:
            results.append(_transfer_one(uploader, _key_for_target(root, rel, t),
                                         p, mode, t["dest"], rel))
    summary = {"mode": mode, "root": root, "path": path,
               "targets": [f"{t['dest']}:{t['prefix']}" for t in targets],
               "uploaded": sum(r["status"] == "uploaded" for r in results),
               "unchanged": sum(r["status"] == "unchanged" for r in results),
               "errors": sum(r["status"] == "error" for r in results),
               "files": results}
    if len(results) == 1:  # 단일 파일 × 단일 연결 — 기존 응답 형태 유지
        summary.update(status=results[0]["status"], s3_key=results[0]["s3_key"])
    else:
        summary["status"] = "error" if summary["errors"] else "done"
    return summary
