"""
Valve · s3_up
-------------
S3 업로드 어댑터.
  - 개발 모드: settings.s3.fake_local_path 가 있고 endpoint_url 이 비어있으면 로컬 폴더 모사
  - 실제 모드: boto3 로 업로드 (atomic: tmp key → copy → delete tmp)
"""
from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any


class S3Uploader:
    def __init__(self, settings: dict):
        self.settings = settings
        s3 = settings.get("s3", {})
        self.bucket = s3.get("bucket", "flow-datalake")
        self.prefix = (s3.get("prefix") or "").strip("/")
        self.endpoint_url = (s3.get("endpoint_url") or "").strip()
        self.fake_local = (s3.get("fake_local_path") or "").strip()
        self.access_key = s3.get("access_key") or None
        self.secret_key = s3.get("secret_key") or None

        self._s3_client = None
        if self._is_fake():
            Path(self.fake_local).mkdir(parents=True, exist_ok=True)
        else:
            self._init_boto()

    def _init_boto(self):
        try:
            import boto3  # lazy
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url or None,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        except Exception as e:
            self._s3_client = None
            self._boto_error = str(e)

    def _is_fake(self) -> bool:
        return bool(self.fake_local) and not self.endpoint_url

    def _full_key(self, key: str) -> str:
        key = key.lstrip("/")
        return f"{self.prefix}/{key}" if self.prefix else key

    def _fake_path(self, key: str) -> Path:
        root = Path(self.fake_local).resolve() / self.bucket / self._full_key(key)
        return root

    async def put_atomic(self, local_path: Path, key: str):
        full = self._full_key(key)
        if self._is_fake():
            dst = Path(self.fake_local).resolve() / self.bucket / full
            tmp = dst.with_suffix(dst.suffix + ".tmp")
            dst.parent.mkdir(parents=True, exist_ok=True)
            # tmp 에 복사 후 rename (원자성 흉내)
            shutil.copy2(local_path, tmp)
            tmp.replace(dst)
            return

        if self._s3_client is None:
            raise RuntimeError(f"boto3 client not initialized: {getattr(self, '_boto_error', 'unknown')}")

        tmp_key = full + ".tmp"
        await asyncio.to_thread(
            self._s3_client.upload_file, str(local_path), self.bucket, tmp_key
        )
        await asyncio.to_thread(
            self._s3_client.copy_object,
            Bucket=self.bucket,
            Key=full,
            CopySource={"Bucket": self.bucket, "Key": tmp_key},
        )
        await asyncio.to_thread(
            self._s3_client.delete_object, Bucket=self.bucket, Key=tmp_key
        )

    def list_objects(self, prefix: str = "") -> list[str]:
        """UI 진단/탐색용."""
        full_prefix = self._full_key(prefix) if prefix else (self.prefix or "")
        if self._is_fake():
            base = Path(self.fake_local).resolve() / self.bucket
            if full_prefix:
                base = base / full_prefix
            if not base.exists():
                return []
            out = []
            for f in base.rglob("*"):
                if f.is_file():
                    out.append(str(f.relative_to(Path(self.fake_local).resolve() / self.bucket)).replace("\\", "/"))
            return sorted(out)

        if self._s3_client is None:
            return []
        try:
            resp = self._s3_client.list_objects_v2(Bucket=self.bucket, Prefix=full_prefix)
            return [o["Key"] for o in resp.get("Contents", [])]
        except Exception:
            return []

    def reload(self, settings: dict):
        self.__init__(settings)
