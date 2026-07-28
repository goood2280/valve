"""Persistent retry queue for failed pipeline raw units.

The normal pipeline keeps producing event/feature/wide outputs from the best
available data.  This store separately guarantees that a failed
vehicle/source/date unit remains eligible for retry even after it falls out of
the rolling ``raw_days`` window.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import date
from pathlib import Path


class PipelineRetryStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._items: dict[str, dict] = {}
        self._load()

    @staticmethod
    def key(vehicle: str, source: str, start, end, split: str) -> str:
        return "|".join((str(vehicle), str(source), str(start), str(end), str(split)))

    def _load(self):
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self._items = dict(raw.get("items") or {})
        except Exception:
            self._items = {}

    def _save_locked(self):
        payload = {"version": 1, "updated_at": time.time(), "items": self._items}
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def record_failure(self, vehicle: str, source: str, start, end, split: str,
                       error: str, delays: list[int] | None = None,
                       now: float | None = None) -> dict:
        now = float(now or time.time())
        delays = [max(1, int(v)) for v in (delays or [300, 900, 1800, 3600])]
        key = self.key(vehicle, source, start, end, split)
        with self._lock:
            prev = self._items.get(key) or {}
            attempts = int(prev.get("attempts") or 0) + 1
            delay = delays[min(attempts - 1, len(delays) - 1)]
            item = {
                "key": key,
                "vehicle": str(vehicle), "source": str(source),
                "start": str(start), "end": str(end), "split": str(split),
                "status": "retry_wait", "attempts": attempts,
                "first_failed_at": float(prev.get("first_failed_at") or now),
                "last_failed_at": now, "next_retry_at": now + delay,
                "last_error": str(error)[:500],
            }
            self._items[key] = item
            self._save_locked()
            return dict(item)

    def mark_success(self, vehicle: str, source: str, start, end, split: str) -> dict | None:
        key = self.key(vehicle, source, start, end, split)
        with self._lock:
            item = self._items.pop(key, None)
            if item is not None:
                self._save_locked()
            return dict(item) if item else None

    def pending(self, vehicle: str | None = None) -> list[dict]:
        with self._lock:
            items = [dict(v) for v in self._items.values()
                     if not vehicle or v.get("vehicle") == vehicle]
        return sorted(items, key=lambda v: (float(v.get("next_retry_at") or 0), v.get("key") or ""))

    def due_units(self, vehicle: str, now: float | None = None) -> list[tuple]:
        now = float(now or time.time())
        out = []
        for item in self.pending(vehicle):
            if float(item.get("next_retry_at") or 0) > now:
                continue
            try:
                out.append((item["source"], date.fromisoformat(item["start"]),
                            date.fromisoformat(item["end"]), item["split"]))
            except (KeyError, TypeError, ValueError):
                continue
        return out

    def summary(self, vehicle: str | None = None, now: float | None = None) -> dict:
        now = float(now or time.time())
        items = self.pending(vehicle)
        first = min((float(v.get("first_failed_at") or now) for v in items), default=None)
        next_retry = min((float(v.get("next_retry_at") or now) for v in items), default=None)
        return {
            "pending": len(items),
            "due": sum(float(v.get("next_retry_at") or 0) <= now for v in items),
            "oldest_age_sec": round(now - first, 1) if first is not None else 0,
            "next_retry_at": next_retry,
            "max_attempts": max((int(v.get("attempts") or 0) for v in items), default=0),
            "items": items,
        }
