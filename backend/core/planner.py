"""
Valve · planner
---------------
probe 결과 → chunk plan 생성.

전략:
  - sample_window: 1시간 샘플 조회 + custom_col=[shard_key] → row·shard 분포 추정
  - projection:    하루치 전체지만 shard_key 1~2 컬럼만 받아 정확 분포
  - none:          probe 생략 (실패 기반 adaptive 로만 작동)

캐시:
  - probe 결과는 (product, source) 키로 settings.probe.cache_days (기본 7일) TTL
  - config/probe_cache.json 에 저장, 서버 재시작 후에도 유지
  - 운영자가 UI 에서 "probe 재실행" 버튼으로 강제 무효화 가능
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass
class Chunk:
    chunk_id: str
    product: str
    source: str
    date: str
    shard_filters: dict = field(default_factory=dict)
    expected_rows: int = 0
    status: str = "pending"

    def to_dict(self) -> dict:
        return {
            "chunk_id": self.chunk_id,
            "product": self.product,
            "source": self.source,
            "date": self.date,
            "shard_filters": self.shard_filters,
            "expected_rows": self.expected_rows,
            "status": self.status,
        }


@dataclass
class ChunkPlan:
    plan_id: str
    product: str
    source: str
    date: str
    chunks: list[Chunk]
    probe_meta: dict

    def to_dict(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "product": self.product,
            "source": self.source,
            "date": self.date,
            "chunks": [c.to_dict() for c in self.chunks],
            "probe_meta": self.probe_meta,
        }


class Planner:
    def __init__(self, lake_api, settings: dict, cache_path: Path):
        self.api = lake_api
        self.settings = settings
        self.cache_path = cache_path
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache: dict[str, dict] = self._load_cache()

    # ─── cache ───
    def _load_cache(self) -> dict:
        if not self.cache_path.exists():
            return {}
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_cache(self):
        try:
            self.cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _cache_key(self, product: str, source: str) -> str:
        return f"{product}::{source}"

    def _is_fresh(self, entry: dict) -> bool:
        days = float(self.settings.get("probe", {}).get("cache_days", 7))
        ts = float(entry.get("ts", 0))
        return (time.time() - ts) < (days * 86400)

    def invalidate(self, product: str | None = None, source: str | None = None):
        if product is None:
            self._cache.clear()
        else:
            keys = [k for k in self._cache if k.startswith(f"{product}::")]
            if source:
                keys = [k for k in keys if k == f"{product}::{source}"]
            for k in keys:
                self._cache.pop(k, None)
        self._save_cache()

    # ─── main ───
    async def build_plan(self, product: str, source_cfg: dict, prod_cfg: dict, date: str) -> ChunkPlan:
        # 제품/소스별 probe skip — probe 가 상습 실패하는 소스에 대해 아예 안 걸고 단일 chunk 시도
        skip_probe = bool(source_cfg.get("probe_skip") or prod_cfg.get("probe_skip"))
        strategy = "none" if skip_probe else self.settings.get("probe", {}).get("strategy", "sample_window")
        ckey = self._cache_key(product, source_cfg["name"])
        cached = self._cache.get(ckey)

        if strategy == "none":
            probe_meta = {"strategy": "none", "skipped": bool(skip_probe),
                          "reason": "probe_skip=true" if skip_probe else None}
        elif cached and self._is_fresh(cached):
            probe_meta = dict(cached["meta"])
            probe_meta["_from_cache"] = True
            probe_meta["_cache_age_sec"] = int(time.time() - cached["ts"])
        else:
            if strategy == "projection":
                probe_meta = await self._probe_projection(product, source_cfg, prod_cfg, date)
            else:
                probe_meta = await self._probe_sample(product, source_cfg, prod_cfg, date)
            # probe 가 error 를 반환했더라도 캐시에 저장하면 매번 반복 실패 하게 되므로,
            # 성공한 결과만 캐시 (또는 fallback_on_timeout=False 일 때만). 기본은 실패 캐시 안 함.
            if not probe_meta.get("error"):
                self._cache[ckey] = {"ts": time.time(), "meta": probe_meta}
                self._save_cache()

        chunks = self._plan_chunks(product, source_cfg, date, probe_meta)

        return ChunkPlan(
            plan_id=f"{product}-{source_cfg['name']}-{date}",
            product=product,
            source=source_cfg["name"],
            date=date,
            chunks=chunks,
            probe_meta=probe_meta,
        )

    # ─── probe: sample window ───
    async def _probe_sample(self, product, source_cfg, prod_cfg, date) -> dict:
        hours = float(self.settings["probe"].get("window_hours", 1))
        shard_keys: list = source_cfg.get("shard_hierarchy") or []
        first_shard = shard_keys[0] if shard_keys else None

        t0 = datetime.fromisoformat(f"{date}T00:00:00")
        t1 = t0 + timedelta(hours=hours)

        params = {
            **prod_cfg.get("params_template", {}),
            "table": source_cfg["table"],
            "dateFrom": t0.isoformat(),
            "dateTo": t1.isoformat(),
        }
        custom_col = [first_shard] if first_shard else ["time"]

        try:
            df = await self.api.query(params, custom_col)
            n_sample = len(df)
            est = int(n_sample * (24.0 / max(hours, 1e-6)))
            shards: list = []
            if first_shard and first_shard in df.columns:
                shards = [str(s) for s in df[first_shard].unique().to_list()]
            return {
                "strategy": "sample_window",
                "sample_hours": hours,
                "sample_rows": n_sample,
                "estimated_rows": est,
                "shards": shards[:1000],
                "shard_count": len(shards),
            }
        except Exception as e:
            return {
                "strategy": "sample_window",
                "estimated_rows": 0,
                "shards": [],
                "shard_count": 0,
                "error": f"{type(e).__name__}: {e}",
            }

    # ─── probe: projection ───
    async def _probe_projection(self, product, source_cfg, prod_cfg, date) -> dict:
        shard_keys: list = source_cfg.get("shard_hierarchy") or []
        t0 = datetime.fromisoformat(f"{date}T00:00:00")
        t1 = t0 + timedelta(days=1)

        params = {
            **prod_cfg.get("params_template", {}),
            "table": source_cfg["table"],
            "dateFrom": t0.isoformat(),
            "dateTo": t1.isoformat(),
        }
        custom_col = (shard_keys[:2] if shard_keys else []) + ["time"]
        custom_col = list(dict.fromkeys(custom_col))  # dedupe preserve order

        try:
            df = await self.api.query(params, custom_col)
            first = shard_keys[0] if shard_keys else None
            shards = []
            if first and first in df.columns:
                shards = [str(s) for s in df[first].unique().to_list()]
            return {
                "strategy": "projection",
                "estimated_rows": len(df),
                "shards": shards,
                "shard_count": len(shards),
            }
        except Exception as e:
            return {
                "strategy": "projection",
                "estimated_rows": 0,
                "shards": [],
                "shard_count": 0,
                "error": f"{type(e).__name__}: {e}",
            }

    # ─── plan chunks ───
    def _plan_chunks(self, product: str, source_cfg: dict, date: str, probe_meta: dict) -> list[Chunk]:
        est_rows = int(probe_meta.get("estimated_rows") or 0)
        shards = list(probe_meta.get("shards") or [])
        target = int(source_cfg.get("target_chunk_rows") or 500_000)
        shard_keys = list(source_cfg.get("shard_hierarchy") or [])

        # case A: probe 실패 or shard 없음 → 단일 chunk 로 시도 (timeout 나면 executor 가 fallback)
        if not shard_keys or not shards:
            return [Chunk(
                chunk_id=f"{product}-{source_cfg['name']}-{date}-00",
                product=product,
                source=source_cfg["name"],
                date=date,
                expected_rows=est_rows,
            )]

        # case B: 크기가 작으면 단일 chunk
        if est_rows > 0 and est_rows <= target:
            return [Chunk(
                chunk_id=f"{product}-{source_cfg['name']}-{date}-00",
                product=product,
                source=source_cfg["name"],
                date=date,
                expected_rows=est_rows,
            )]

        # case C: shard 분할
        n_chunks = max(1, (est_rows + target - 1) // target)
        n_chunks = min(n_chunks, len(shards))
        groups: list[list] = [[] for _ in range(n_chunks)]
        for i, s in enumerate(shards):
            groups[i % n_chunks].append(s)

        first_shard = shard_keys[0]
        chunks = []
        for i, g in enumerate(groups):
            chunks.append(Chunk(
                chunk_id=f"{product}-{source_cfg['name']}-{date}-{i:02d}",
                product=product,
                source=source_cfg["name"],
                date=date,
                shard_filters={first_shard: g},
                expected_rows=est_rows // n_chunks if n_chunks else est_rows,
            ))
        return chunks
