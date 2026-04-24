"""
Valve · state
-------------
Jobs/Chunks 상태 저장 + SSE broadcast.
  - append-only log (jobs.jsonl)
  - 메모리 snapshot (plans · chunks · partition_status)
  - crash 복구: in_progress 상태 chunk 는 재시작 시 pending 으로 되돌림
  - SSE listener queue 로 실시간 브로드캐스트

partition_status 는 Monitor 히트맵용 요약:
  key: "product/source/date" → {"status": ..., "last_ts": ..., "total_rows": ...}
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any


class StateStore:
    MAX_QUEUE = 500

    def __init__(self, log_path: Path, max_bytes: int = 50 * 1024 * 1024, keep: int = 5):
        """append-only jobs.jsonl 에 기록.
        max_bytes 초과 시 rotate: 현재 메모리 snapshot 을 새 파일 첫 줄에 기록하고,
        기존 파일은 .1 → .2 → ... → .keep 으로 밀어낸 뒤 가장 오래된 건 삭제.
        rotate 덕에 재기동 시에도 snapshot 라인 1줄만 읽으면 바로 복원.
        """
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max(1024, int(max_bytes))  # 하한 1 KB — 테스트 편의
        self.keep = max(1, int(keep))

        self._plans: dict[str, dict] = {}
        self._chunks: dict[str, dict] = {}
        self._partitions: dict[str, dict] = {}
        self._listeners: list[asyncio.Queue] = []

        self._load_from_log()

    # ─── replay ───
    def _load_from_log(self):
        if not self.log_path.exists():
            return
        try:
            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        evt = json.loads(line)
                        self._apply(evt, emit=False)
                    except Exception:
                        continue
        except Exception:
            pass

        # crash 복구: in_progress → pending
        recovered = 0
        for cid, ch in self._chunks.items():
            if ch.get("status") == "in_progress":
                ch["status"] = "pending"
                ch["recovered"] = True
                recovered += 1
        if recovered:
            self._append({"ts": time.time(), "kind": "recovery", "count": recovered})

    # ─── persistence ───
    def _append(self, evt: dict):
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(evt, ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass
        self._maybe_rotate()

    def _maybe_rotate(self):
        try:
            if not self.log_path.exists():
                return
            if self.log_path.stat().st_size < self.max_bytes:
                return
        except Exception:
            return
        try:
            # 오래된 것부터 끝에서 안쪽으로 한 단계씩 밀어냄. 맨 끝은 삭제.
            for i in range(self.keep, 0, -1):
                older = self.log_path.with_suffix(self.log_path.suffix + f".{i}")
                newer = self.log_path.with_suffix(self.log_path.suffix + f".{i-1}") if i > 1 else self.log_path
                if newer.exists():
                    if i == self.keep and older.exists():
                        older.unlink(missing_ok=True)
                    if older.exists():
                        older.unlink(missing_ok=True)
                    newer.rename(older)
            # 새 파일 첫 줄은 현재 메모리 snapshot — 재기동 시 이 한 줄로 바로 복원
            snap = {
                "ts": time.time(), "kind": "snapshot",
                "plans": self._plans, "chunks": self._chunks, "partitions": self._partitions,
            }
            with open(self.log_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(snap, ensure_ascii=False, default=str) + "\n")
        except Exception:
            # rotate 실패는 치명적이지 않음 — 다음 append 에서 재시도
            pass

    # ─── apply (memory only) ───
    def _apply(self, evt: dict, emit: bool = True):
        kind = evt.get("kind")
        if kind == "snapshot":
            # rotate 직후 남겨진 메모리 스냅샷 — replay 시작점.
            self._plans = dict(evt.get("plans") or {})
            self._chunks = dict(evt.get("chunks") or {})
            self._partitions = dict(evt.get("partitions") or {})
            return
        if kind == "plan":
            self._plans[evt["plan_id"]] = evt["plan"]
            # 플랜 접수 시 파티션 상태 초기화
            p = evt["plan"]
            pkey = f"{p['product']}/{p['source']}/{p['date']}"
            self._partitions[pkey] = {
                "product": p["product"],
                "source": p["source"],
                "date": p["date"],
                "status": "planned",
                "total_chunks": len(p.get("chunks", [])),
                "done_chunks": 0,
                "last_ts": evt.get("ts"),
            }
        elif kind == "chunk":
            cid = evt["chunk_id"]
            prev = self._chunks.get(cid, {})
            prev.update(evt.get("update") or {})
            prev["chunk_id"] = cid
            self._chunks[cid] = prev
            self._refresh_partition_from_chunk(prev)
        elif kind == "partition":
            pkey = evt["partition_key"]
            prev = self._partitions.get(pkey, {})
            prev.update(evt.get("update") or {})
            prev["last_ts"] = evt.get("ts")
            self._partitions[pkey] = prev
        elif kind == "recovery":
            pass
        if emit:
            self._emit(evt)

    def _refresh_partition_from_chunk(self, chunk: dict):
        product = chunk.get("product")
        source = chunk.get("source")
        date = chunk.get("date")
        if not (product and source and date):
            # chunk_id 파싱: "{product}-{source}-{YYYY-MM-DD}-{idx}"
            try:
                parts = chunk["chunk_id"].rsplit("-", 4)
                # ["{prod}-{src}", "YYYY", "MM", "DD", "idx"] → 주의: date 가 dash 포함
                # 안전: 앞의 두 dash 로 product/source 추출, 이후는 date
                head = chunk["chunk_id"]
                # 대안: chunk dict 의 product/source/date 에 executor 가 채워넣음 → 이 경로는 fallback
                return
            except Exception:
                return

        pkey = f"{product}/{source}/{date}"
        pstate = self._partitions.setdefault(pkey, {
            "product": product, "source": source, "date": date,
            "status": "planned", "total_chunks": 0, "done_chunks": 0,
        })
        # done_chunks 재계산(간단하게: 현재 plan 의 chunk 들 중 success 수)
        done = 0
        total = 0
        plan_id = f"{product}-{source}-{date}"
        plan = self._plans.get(plan_id, {})
        for c in plan.get("chunks", []):
            total += 1
            st = self._chunks.get(c["chunk_id"], {}).get("status")
            if st == "success":
                done += 1
        pstate["total_chunks"] = total or pstate.get("total_chunks", 0)
        pstate["done_chunks"] = done

        if total and done == total:
            pstate["status"] = "success"
        elif any(self._chunks.get(c["chunk_id"], {}).get("status") == "failed"
                 for c in plan.get("chunks", [])):
            pstate["status"] = "partial_failed"
        elif any(self._chunks.get(c["chunk_id"], {}).get("status") == "in_progress"
                 for c in plan.get("chunks", [])):
            pstate["status"] = "running"
        pstate["last_ts"] = time.time()

    def _emit(self, evt: dict):
        dead = []
        for q in self._listeners:
            try:
                q.put_nowait(evt)
            except asyncio.QueueFull:
                dead.append(q)
            except Exception:
                dead.append(q)
        for q in dead:
            if q in self._listeners:
                self._listeners.remove(q)

    # ─── public ───
    # _apply 를 먼저 호출해 메모리 상태를 최신화한 뒤 _append — 그래야 _maybe_rotate 가
    # 방금 처리된 이벤트까지 포함한 snapshot 으로 rotate 할 수 있음.
    def record_plan(self, plan: dict):
        evt = {"ts": time.time(), "kind": "plan", "plan_id": plan["plan_id"], "plan": plan}
        self._apply(evt)
        self._append(evt)

    def update_chunk(self, chunk_id: str, update: dict):
        evt = {"ts": time.time(), "kind": "chunk", "chunk_id": chunk_id, "update": update}
        self._apply(evt)
        self._append(evt)

    def update_partition(self, partition_key: str, update: dict):
        evt = {"ts": time.time(), "kind": "partition", "partition_key": partition_key, "update": update}
        self._apply(evt)
        self._append(evt)

    def snapshot(self) -> dict:
        return {
            "plans": self._plans,
            "chunks": self._chunks,
            "partitions": self._partitions,
            "ts": time.time(),
        }

    def get_plan(self, plan_id: str):
        return self._plans.get(plan_id)

    def get_chunk(self, chunk_id: str):
        return self._chunks.get(chunk_id)

    # ─── SSE ───
    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self.MAX_QUEUE)
        self._listeners.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._listeners:
            self._listeners.remove(q)
