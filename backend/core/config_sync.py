"""Valve · config_sync
--------------------------
사내 운영 패턴: 여러 Valve 인스턴스가 같은 config 를 공유해야 함.
따라서 `settings.json / products.yaml / source_types.yaml` 을 **S3** 에 보관하고,
기동 시 거기서 pull → 로컬로 저장 → 실행. S3 가 다운되거나 내용이 손상되면
**직전 정상 설정** 으로 fallback 하고 알람.

원칙 (robust > fast):
1. S3 에서 받은 값이 JSON/YAML 파서를 통과해야만 "정상" 으로 인정.
2. 정상 확정 시 로컬 `*.last_good` 복사본 1개를 항상 유지 — 다음 기동 때 fallback.
3. fallback 이 발생한 모든 케이스는 alert_cb 로 통보 (sev=warn 또는 error).
4. 네트워크 실패는 조용히 넘어가되, 로컬에 아무것도 없으면 알람 + 번들 기본값.
5. 변경이 없으면 쓰기도 안 함 — 디스크 idempotent.
"""
from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Callable, Optional

import yaml


class ConfigSync:
    """S3 에서 Valve config 를 끌어오고 로컬 파일을 동기화.

    사용:
        cs = ConfigSync(s3, root=Path("./config"),
                        s3_prefix="valve-config",
                        alert_cb=on_config_alert)
        cs.sync("settings.json", parser=json.loads, kind="json")
        cs.sync("products.yaml", parser=yaml.safe_load, kind="yaml")

    S3 가 꺼져있거나 fake_local 모드면 S3 레이어만 skip 하고 로컬만 사용.
    """

    def __init__(self, s3_uploader, root: Path, s3_prefix: str = "valve-config",
                 alert_cb: Optional[Callable[[dict], None]] = None):
        self.s3 = s3_uploader
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.s3_prefix = s3_prefix.strip("/")
        self.alert = alert_cb or (lambda _evt: None)

    def _key(self, name: str) -> str:
        return f"{self.s3_prefix}/{name}" if self.s3_prefix else name

    def _local(self, name: str) -> Path:
        return self.root / name

    def _good(self, name: str) -> Path:
        return self.root / f"{name}.last_good"

    def sync(self, name: str, parser: Callable[[str], object], kind: str = "json") -> dict:
        """한 파일을 동기화. 리턴: {source: 's3'|'local'|'last_good'|'bundled',
                                      changed: bool, error?: str}.
        parser(content_str) → 파싱된 객체. 예외 내면 '손상' 으로 간주."""
        result = {"name": name, "source": None, "changed": False, "error": None}
        local_path = self._local(name)
        good_path = self._good(name)

        s3_text = self._fetch_s3(name)
        if s3_text is not None:
            try:
                parser(s3_text)  # validate
            except Exception as e:
                self._alert("config_s3_invalid", "warn",
                            f"{name} — S3 내용 파싱 실패, 로컬 fallback",
                            {"name": name, "error": str(e)[:300]})
                s3_text = None  # s3 버리고 로컬 진행

        if s3_text is not None:
            current = local_path.read_text(encoding="utf-8") if local_path.exists() else ""
            if current != s3_text:
                local_path.write_text(s3_text, encoding="utf-8")
                result["changed"] = True
            # 성공적으로 파싱된 s3 본은 good 으로 보존
            good_path.write_text(s3_text, encoding="utf-8")
            result["source"] = "s3"
            return result

        # S3 미 가용 — 로컬 파싱 시도
        if local_path.exists():
            try:
                parser(local_path.read_text(encoding="utf-8"))
                result["source"] = "local"
                return result
            except Exception as e:
                self._alert("config_local_corrupt", "error",
                            f"{name} — 로컬 파일 손상, last_good fallback 시도",
                            {"name": name, "error": str(e)[:300]})

        # 로컬도 손상 — last_good 복구
        if good_path.exists():
            try:
                parser(good_path.read_text(encoding="utf-8"))
                shutil.copy2(good_path, local_path)
                self._alert("config_fallback_last_good", "warn",
                            f"{name} — last_good 로 fallback 완료", {"name": name})
                result["source"] = "last_good"
                result["changed"] = True
                return result
            except Exception as e:
                self._alert("config_last_good_corrupt", "error",
                            f"{name} — last_good 도 손상", {"name": name, "error": str(e)[:300]})

        # 전부 없음 — 알람만, 호출자가 번들 기본값 채우게
        self._alert("config_missing", "error",
                    f"{name} — S3·로컬·last_good 모두 사용 불가, 번들 기본값으로 시작",
                    {"name": name})
        result["source"] = "bundled"
        result["error"] = "no source available"
        return result

    def _fetch_s3(self, name: str) -> Optional[str]:
        """S3 에서 config 파일 텍스트를 가져옴. 실패 시 None."""
        key = self._key(name)
        try:
            return self.s3.get_text(key) if hasattr(self.s3, "get_text") else None
        except Exception as e:
            self._alert("config_s3_unreachable", "warn",
                        f"{name} — S3 접근 실패, 로컬 fallback",
                        {"name": name, "key": key, "error": str(e)[:300]})
            return None

    def _alert(self, kind: str, severity: str, title: str, meta: dict):
        try:
            self.alert({
                "ts": time.time(),
                "source": "valve.config_sync",
                "kind": kind,
                "severity": severity,
                "title": title,
                "meta": meta,
            })
        except Exception:
            pass
