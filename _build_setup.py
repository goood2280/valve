#!/usr/bin/env python3
"""Build the Valve self-contained installer.

Run from the Valve/ directory:

    python _build_setup.py

Output: overwrites setup.py at the repo root (flow 의 _build_setup.py 와 동일 패턴).

번들 정책:
  · 코드(app.py, backend/, frontend/, docs/, scripts/*.py, tests/)는 항상 교체
  · config/ 는 seed-only — 설치 대상에 이미 있으면 절대 덮어쓰지 않음
    (flow 판정이 반영되는 룰북 csv·설정 json 보존)
  · db/, logs/, staging/, s3_local/ 런타임 데이터는 번들에 넣지도 건드리지도 않음
  · reference/ (사내 원본 참고 스크립트) 는 배포 대상 아님
  · config/settings.json 은 빌드 시 s3.fake_local_path 를 비워서 임베드
    (로컬 데모 경로가 설치본 seed 에 새지 않게)
"""
import base64
import gzip
import json
import py_compile
import subprocess
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).parent

CODE_DIRS = ["backend", "frontend", "docs", "tests"]
CODE_FILES = ["app.py", "README.md", "VERSION.json", "requirements.txt", "pytest.ini"]
EXCLUDE_PARTS = {
    "__pycache__", ".git", "node_modules", "dist",
    # 런타임/보관 디렉토리 — 어떤 경로 아래에 있어도 제외 (defense in depth)
    "db", "logs", "staging", "s3_local", "s3_outbox", "data",
    "reference", "backup", "archive", "valve.egg-info", ".claude",
}
# s3_jobs.yaml 은 설치 사이트의 csv_sync.yaml/s3_transfer.yaml 에서 최초 기동 때
# 자동 생성된다 — 개발 PC 의 key/dest 가 seed 로 새어나가지 않게 번들에서 제외.
CONFIG_EXCLUDE_NAMES = {"probe_cache.json", "settings.local.json", "s3_jobs.yaml"}


def _excluded(p: Path) -> bool:
    return any(part in EXCLUDE_PARTS for part in p.parts) or p.suffix == ".pyc"


def gather_code() -> list[Path]:
    out, seen = [], set()

    def add(p: Path):
        if p.is_file() and p not in seen and not _excluded(p):
            seen.add(p)
            out.append(p)

    for rel in CODE_FILES:
        add(ROOT / rel)
    for d in CODE_DIRS:
        base = ROOT / d
        if base.is_dir():
            for p in sorted(base.rglob("*")):
                add(p)
    scripts = ROOT / "scripts"
    if scripts.is_dir():
        for p in sorted(scripts.glob("*.py")):
            add(p)
    return sorted(out)


def gather_config() -> list[Path]:
    base = ROOT / "config"
    return sorted(p for p in base.rglob("*")
                  if p.is_file() and not _excluded(p)
                  and p.name not in CONFIG_EXCLUDE_NAMES)


def encode_bytes(data: bytes) -> str:
    return base64.b64encode(gzip.compress(data, compresslevel=9, mtime=0)).decode("ascii")


def read_for_bundle(p: Path) -> bytes:
    """빌드 시 sanitize — settings.json 의 로컬 데모 경로 제거."""
    data = p.read_bytes()
    if p.name == "settings.json" and p.parent.name == "config":
        try:
            cfg = json.loads(data.decode("utf-8"))
            if isinstance(cfg.get("s3"), dict):
                cfg["s3"]["fake_local_path"] = ""
            data = (json.dumps(cfg, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        except Exception:
            pass
    return data


def format_payload(b64: str, indent: int = 8) -> str:
    lines = textwrap.wrap(b64, width=72, break_long_words=True, break_on_hyphens=False)
    sp = " " * indent
    return "\n".join(f"{sp}'{ln}'" for ln in lines)


def dict_block(name: str, files: list[Path]) -> str:
    entries = []
    for p in files:
        rel = p.relative_to(ROOT).as_posix()
        payload = format_payload(encode_bytes(read_for_bundle(p)))
        entries.append(f"    {rel!r}: (\n{payload}\n    ),")
    return f"{name} = {{\n" + "\n".join(entries) + "\n}\n"


HEADER = '''#!/usr/bin/env python3
"""Valve self-contained installer.

Usage (fresh machine):

    python setup.py                # extract + install deps
    python setup.py extract        # extract embedded sources only
    python setup.py install-deps   # pip install backend deps only
    python setup.py version        # print mtime-based version label
    python setup.py sync-version   # rewrite VERSION.json metadata
    python setup.py snapshots      # list config snapshots (~/.valve_backups)
    python setup.py restore [latest|<timestamp>]   # restore config snapshot

Run the server afterwards:

    uvicorn app:app --host 0.0.0.0 --port 8090

This file embeds @N_CODE@ code files (always replaced) and @N_CONFIG@ config
seed files (written ONLY when absent). Runtime data (db/, logs/, staging/,
s3_local/) is NEVER bundled and NEVER touched — re-running setup.py on an
existing install preserves the rulebook csv / settings edited via flow 판정
반영이나 웹 설정 화면.

보존 정책 (요약):
  - config/ 전체: 있으면 절대 덮어쓰지 않음 (신규 설치에만 seed)
  - db/, logs/, staging/, s3_local/: 번들에 없고 쓰기 가드로도 차단
  - 추출 직전 config/ 스냅샷 → ~/.valve_backups/ (restore 명령으로 복구)
"""
from __future__ import annotations

import base64
import datetime
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Windows cp949 콘솔에서 non-ASCII print 가 터지지 않게 UTF-8 reconfigure.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

'''

FOOTER = '''

# ── 쓰기 가드 ──────────────────────────────────────────────────────────
_ALLOWED_TOP = {
    'app.py', 'README.md', 'VERSION.json', 'requirements.txt', 'pytest.ini',
    'backend', 'frontend', 'docs', 'scripts', 'tests', 'config',
}
# 런타임 데이터 세그먼트 — 경로 어디에 있어도 쓰기 금지 (defense in depth)
_FORBIDDEN_SEGMENTS = {'db', 'logs', 'staging', 's3_local', 'data', '__pycache__', '.git'}


def _guard_ok(rel: str) -> bool:
    parts = [p for p in rel.replace('\\\\', '/').split('/') if p and p != '.']
    if not parts or parts[0] not in _ALLOWED_TOP:
        return False
    if any(seg in _FORBIDDEN_SEGMENTS for seg in parts):
        return False
    return True


def _write(rel: str, payload, overwrite: bool = True) -> str:
    """반환: written | preserved | guarded"""
    if not _guard_ok(rel):
        return 'guarded'
    dst = ROOT / rel
    if dst.exists() and not overwrite:
        return 'preserved'
    b64 = ''.join(payload) if isinstance(payload, (list, tuple)) else payload
    data = gzip.decompress(base64.b64decode(b64))
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(data)
    return 'written'


def _version_time_label() -> str:
    times = []
    for fp in (ROOT / 'VERSION.json', ROOT / 'setup.py'):
        try:
            times.append(fp.stat().st_mtime)
        except OSError:
            pass
    if not times:
        return 'unknown'
    return datetime.datetime.fromtimestamp(max(times)).isoformat(timespec='seconds')


# ── config 스냅샷 / 복구 ───────────────────────────────────────────────
def _backups_dir() -> Path:
    d = Path(os.path.expanduser('~')) / '.valve_backups'
    d.mkdir(parents=True, exist_ok=True)
    return d


def _config_files() -> list:
    cdir = ROOT / 'config'
    if not cdir.is_dir():
        return []
    return [p for p in cdir.rglob('*') if p.is_file() and '__pycache__' not in p.parts]


def _snapshot_config() -> Path | None:
    files = _config_files()
    if not files:
        print('[snapshot] no config/ yet - skipping (fresh install)')
        return None
    stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    snap = _backups_dir() / f'v{VERSION}-{stamp}'
    for p in files:
        dst = snap / p.relative_to(ROOT)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(p), str(dst))
    print(f'[snapshot] config/ {len(files)} files -> {snap}')
    return snap


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def _verify_and_restore(snap: Path | None) -> None:
    """추출 후 config/ 가 스냅샷과 동일한지 검증 — 달라졌으면 즉시 복구.
    (config 는 seed-only 라 바뀔 일이 없어야 정상)"""
    if snap is None or not snap.is_dir():
        return
    bad = 0
    for src in snap.rglob('*'):
        if not src.is_file():
            continue
        rel = src.relative_to(snap)
        now = ROOT / rel
        if not now.exists() or _sha(now) != _sha(src):
            now.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(now))
            print(f'  [restore] {rel}')
            bad += 1
    print('[verify] config integrity OK' if not bad
          else f'[verify] !!! {bad} config files were changed - restored from snapshot')


def restore(argv: list = None) -> int:
    argv = argv or []
    want = (argv[0] if argv else 'latest').strip()
    snaps = sorted(p for p in _backups_dir().iterdir() if p.is_dir())
    if not snaps:
        print(f'[restore] no snapshots in {_backups_dir()}')
        return 1
    chosen = snaps[-1] if want == 'latest' else next((p for p in snaps if want in p.name), None)
    if chosen is None:
        print(f"[restore] no match for '{want}'. Available:")
        for p in snaps[-10:]:
            print(f'  - {p.name}')
        return 1
    n = 0
    for src in chosen.rglob('*'):
        if not src.is_file():
            continue
        dst = ROOT / src.relative_to(chosen)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(dst))
        n += 1
    print(f'[restore] {n} files restored from {chosen}')
    return 0


def list_snapshots(argv: list = None) -> int:
    snaps = sorted(p for p in _backups_dir().iterdir() if p.is_dir())
    if not snaps:
        print(f'[snapshots] (none) at {_backups_dir()}')
        return 0
    print(f'[snapshots] {_backups_dir()}:')
    for p in snaps[-20:]:
        n = sum(1 for f in p.rglob('*') if f.is_file())
        print(f'  {p.name}  ({n} files)')
    return 0


# ── 명령 ───────────────────────────────────────────────────────────────
def _ensure_db_root() -> None:
    """사내 배치 표준 — 설치본이 C: 드라이브(바탕화면 등)에 있고 D: 드라이브가
    있으면 파이프라인 DB 는 D:/Valve_DB 에 둔다. config/pipeline.yaml 의 db_root 가
    기본값 'db' 그대로일 때만 바꾸고(운영자가 지정한 값은 존중), 해당 줄만 교체해
    파일의 다른 내용/주석은 건드리지 않는다. 앱 쪽 우선순위:
    VALVE_DB_ROOT 환경변수 > pipeline.yaml db_root > ROOT/db."""
    if os.name != 'nt':
        return
    try:
        if str(ROOT.drive).upper() != 'C:' or not Path('D:/').exists():
            return
        cfgp = ROOT / 'config' / 'pipeline.yaml'
        if not cfgp.exists():
            return
        lines = cfgp.read_text(encoding='utf-8').splitlines(keepends=True)
        for i, ln in enumerate(lines):
            if not ln.startswith('db_root:'):
                continue
            val = ln.split(':', 1)[1].strip().strip('\\'"')
            if val != 'db':
                return                       # 운영자가 이미 지정 — 존중
            nl = '\\r\\n' if ln.endswith('\\r\\n') else '\\n'
            lines[i] = 'db_root: D:/Valve_DB' + nl
            cfgp.write_text(''.join(lines), encoding='utf-8')
            Path('D:/Valve_DB').mkdir(parents=True, exist_ok=True)
            print('[extract] db_root -> D:/Valve_DB (설치본이 C: — DB 는 D: 드라이브)')
            return
    except Exception as e:
        print(f'[extract] db_root 자동 설정 실패(무시): {e}')


def extract() -> int:
    print(f'[extract] valve {_version_time_label()} starting')
    snap = None
    if os.environ.get('VALVE_SKIP_SNAPSHOT') == '1':
        print('[snapshot] skipped (VALVE_SKIP_SNAPSHOT=1)')
    else:
        try:
            snap = _snapshot_config()
        except Exception as e:
            print(f'[snapshot] WARN failed: {e}')

    n_code = sum(1 for rel, p in FILES.items() if _write(rel, p) == 'written')
    seeded = preserved = 0
    for rel, p in CONFIG_FILES.items():
        r = _write(rel, p, overwrite=False)
        seeded += r == 'written'
        preserved += r == 'preserved'
    for sub in ('db', 'logs', 'staging'):
        (ROOT / sub).mkdir(parents=True, exist_ok=True)
    (ROOT / 'VERSION.json').write_text(
        json.dumps(VERSION_META, indent=2, ensure_ascii=False), encoding='utf-8')
    try:
        _verify_and_restore(snap)
    except Exception as e:
        print(f'[verify] WARN failed: {e}')
    _ensure_db_root()
    print(f'[extract] code {n_code} written · config seed {seeded} written / '
          f'{preserved} preserved -> {ROOT}')
    print('[extract] manual restore: python setup.py restore [latest|<timestamp>]')
    return 0


def verify_bundle() -> int:
    """Decode every payload and compile embedded Python before extraction."""
    checked = python_files = 0
    for group in (FILES, CONFIG_FILES):
        for rel, payload in group.items():
            b64 = ''.join(payload) if isinstance(payload, (list, tuple)) else payload
            try:
                data = gzip.decompress(base64.b64decode(b64, validate=True))
                if rel.endswith('.py'):
                    compile(data, rel, 'exec')
                    python_files += 1
            except Exception as e:
                print(f'[verify] invalid embedded file {rel}: {e}', file=sys.stderr)
                return 4
            checked += 1
    print(f'[verify] bundle OK: {checked} files ({python_files} Python)')
    return 0


def install_deps() -> int:
    req = ROOT / 'requirements.txt'
    if req.exists():
        pkgs = [ln.strip() for ln in req.read_text(encoding='utf-8').splitlines()
                if ln.strip() and not ln.lstrip().startswith('#')]
    else:
        pkgs = ['fastapi', 'uvicorn[standard]', 'pydantic', 'polars', 'pandas',
                'pyarrow', 'boto3', 'python-multipart', 'pyyaml', 'sse-starlette']
    # 리스트 argv 로 실행 — 문자열+shell=True 는 Windows cmd 에서 shlex.quote 의
    # 작은따옴표가 벗겨지지 않고 '>=' 의 > 가 리다이렉션으로 해석돼 설치가 깨진다.
    rc = subprocess.run(
        [sys.executable, '-m', 'pip', 'install', '--disable-pip-version-check', *pkgs],
        cwd=str(ROOT)).returncode
    return rc


def print_version() -> int:
    print(f'valve {_version_time_label()} - codename {CODENAME}')
    return 0


def sync_version_json() -> int:
    (ROOT / 'VERSION.json').write_text(
        json.dumps(VERSION_META, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'VERSION.json mtime -> {_version_time_label()}')
    return 0


def all_steps() -> int:
    rc = verify_bundle() or extract() or install_deps()
    if rc == 0:
        print(f'\\n[done] uvicorn app:app --host 0.0.0.0 --port 8090   (run from {ROOT})')
    return rc


COMMANDS = {
    'extract': extract,
    'install-deps': install_deps,
    'version': print_version,
    'sync-version': sync_version_json,
    'verify': verify_bundle,
    'all': all_steps,
    'restore': restore,
    'snapshots': list_snapshots,
}


# 앱(backend/)이 3.10+ 타입 문법을 쓴다. 낮은 버전으로 설치하면 pip 이 낡은
# 의존성을 고르고 서버도 TypeError 로 죽으므로 설치 계열 명령은 여기서 막는다.
_MIN_PY = (3, 10)
_PY_EXEMPT = {'version', 'snapshots', 'restore'}  # 복구/조회는 낮은 버전에서도 허용


def _check_python(cmd: str) -> bool:
    if sys.version_info >= _MIN_PY or cmd in _PY_EXEMPT:
        return True
    cur = '.'.join(str(v) for v in sys.version_info[:3])
    print(f'[valve] Python {_MIN_PY[0]}.{_MIN_PY[1]} 이상이 필요합니다 - 현재 {cur}.',
          file=sys.stderr)
    print('        여러 버전이 있다면:  py -3.11 setup.py', file=sys.stderr)
    return False


def main(argv):
    if not argv:
        if not _check_python('all'):
            return 3
        return all_steps()
    cmd = argv[0]
    if cmd in ('-h', '--help', 'help'):
        print(__doc__)
        print('\\nCommands: ' + ', '.join(sorted(COMMANDS)))
        return 0
    if not _check_python(cmd):
        return 3
    fn = COMMANDS.get(cmd)
    if not fn:
        print(f'Unknown command: {cmd}', file=sys.stderr)
        return 2
    if cmd == 'restore':
        return restore(argv[1:])
    return fn()


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
'''


def build() -> str:
    code = gather_code()
    config = gather_config()
    version = json.loads((ROOT / "VERSION.json").read_text(encoding="utf-8"))
    meta = {
        "name": "Valve",
        "version": version.get("version", ""),
        "codename": version.get("codename", "valve"),
        "pairs_with": version.get("pairs_with", "flow"),
        "purpose": version.get("purpose", ""),
        "tagline": version.get("tagline", ""),
    }
    header = (HEADER
              .replace("@N_CODE@", str(len(code)))
              .replace("@N_CONFIG@", str(len(config))))
    version_block = (
        f"VERSION = {version.get('version', '0')!r}\n"
        f"CODENAME = {version.get('codename', 'valve')!r}\n"
        f"VERSION_META = {json.dumps(meta, ensure_ascii=False)}\n\n"
    )
    return (header + version_block
            + dict_block("FILES", code) + "\n"
            + dict_block("CONFIG_FILES", config) + FOOTER)


def main():
    out = build()
    dst = ROOT / "setup.py"
    dst.write_text(out, encoding="utf-8")
    py_compile.compile(str(dst), doraise=True)
    subprocess.run([sys.executable, str(dst), "verify"], cwd=str(ROOT), check=True)
    print(f"wrote {dst} ({dst.stat().st_size:,} bytes, "
          f"{len(gather_code())} code + {len(gather_config())} config files)")


if __name__ == "__main__":
    main()
