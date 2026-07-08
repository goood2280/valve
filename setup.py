"""Valve — setup.py

DataLake 수도꼭지: 사내 API → S3 → flow 공급 + raw→event→feature 파이프라인.
버전/의존성은 각각 VERSION.json · requirements.txt 를 단일 소스로 사용한다.

설치:
    pip install -e .        # 개발 모드 (frontend/config 를 소스에서 그대로 사용)
실행:
    valve                   # uvicorn 으로 기동 (VALVE_HOST / VALVE_PORT 로 조절)
    # 또는
    python -m uvicorn app:app --host 0.0.0.0 --port 8090 --reload
"""
from __future__ import annotations

import json
from pathlib import Path

from setuptools import find_packages, setup

ROOT = Path(__file__).parent

_meta = json.loads((ROOT / "VERSION.json").read_text(encoding="utf-8"))

_req = [
    line.strip()
    for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
    if line.strip() and not line.lstrip().startswith("#")
]

_readme = (ROOT / "README.md")
_long_desc = _readme.read_text(encoding="utf-8") if _readme.exists() else _meta.get("purpose", "")

setup(
    name="valve",
    version=_meta["version"],
    description=_meta.get("purpose", "Valve — turn the valve, feed the flow"),
    long_description=_long_desc,
    long_description_content_type="text/markdown",
    url="https://github.com/goood2280/valve",
    python_requires=">=3.10",
    # app.py 는 최상위 모듈 (uvicorn app:app), backend 는 패키지
    py_modules=["app"],
    packages=find_packages(include=["backend", "backend.*"]),
    include_package_data=True,   # MANIFEST.in 의 frontend/config 포함
    install_requires=_req,
    entry_points={"console_scripts": ["valve=app:main"]},
    classifiers=[
        "Framework :: FastAPI",
        "Programming Language :: Python :: 3",
        "Private :: Do Not Upload",
    ],
)
