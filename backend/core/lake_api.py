"""
Valve · lake_api
----------------
사내 DataLake 의 query(params, custom_col, user) 함수를 감싸는 어댑터.

Mock 모드:
  - HY000 에러 5% 확률 주입
  - 지연 시뮬레이션 (probe 0.3~1.5s · 실쿼리 3~15s · 가끔 6분 timeout)
  - custom_col 기반 가짜 DataFrame 생성 (실제 shard key 분포 흉내)

Real 모드:
  - settings.lake_api.module = "패키지.모듈:함수" 형태로 importlib 동적 로드
  - Valve 가 기대하는 시그니처:
        query(params: dict, custom_col: list, user: str) -> DataFrame
  - **인증 정보는 user_name 하나뿐이다.** 사내 DataLake query API 는 별도 키/토큰을
    받지 않는다 — 계정(user)만 넘기면 된다. (v0.3.9 이전에는 쓰지도 않는 api_key
    필드가 설정에 있었다. 지금은 없다.)
  - 사내 getData 는 keyword 시그니처라 그대로 못 꽂는다. 얇은 어댑터를 하나 두고
    그 경로를 module 에 적는다 (reference/Ref_raw_query.py 참조):

        # mycorp/valve_adapter.py  →  module: "mycorp.valve_adapter:query"
        from bigdataquery import getData

        def query(params, custom_col, user):
            if custom_col:                      # 빈 리스트면 전체 컬럼 (인자 자체를 뺀다)
                return getData(params, custom_columns=custom_col, user_name=user)
            return getData(params, user_name=user)

공통 보증:
  - rate limit (min_interval_sec, 전역 lock)
  - asyncio.wait_for timeout (기본 290s = 4분 50초, 5분 제한 안쪽)
  - exponential backoff 재시도 (기본 3회, HY000/Timeout/ConnectionError 만)
"""
from __future__ import annotations

import asyncio
import importlib
import random
import time
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd
import polars as pl


class HY000Error(Exception):
    """사내 ODBC 드라이버 간헐 장애 모사"""


# ─────────────────────────────────────────────
# Mock engine
# ─────────────────────────────────────────────
class _MockQueryEngine:
    """실제 query() 시그니처를 흉내낸 가짜 엔진. 개발/데모용."""

    HY000_RATE = 0.05
    SLOW_RATE = 0.01  # 1% 확률로 > timeout 시뮬
    SLOW_SECONDS = 360  # 6분

    def __call__(self, params: dict, custom_col: list, user: str) -> pd.DataFrame:
        try:
            t0 = datetime.fromisoformat(params["dateFrom"])
            t1 = datetime.fromisoformat(params["dateTo"])
            span = t1 - t0
        except Exception:
            span = timedelta(hours=24)

        is_probe = span <= timedelta(hours=1, minutes=10)

        # Slow (timeout 시뮬)
        if not is_probe and random.random() < self.SLOW_RATE:
            time.sleep(self.SLOW_SECONDS)

        # Normal delay
        if is_probe:
            time.sleep(random.uniform(0.3, 1.5))
        else:
            time.sleep(random.uniform(3.0, 15.0))

        # HY000
        if random.random() < self.HY000_RATE:
            raise HY000Error("[HY000] simulated ODBC driver error (Valve mock)")

        return self._build_df(params, custom_col, is_probe, span)

    def _build_df(self, params, custom_col, is_probe, span):
        # row count: probe 면 1시간치, 실쿼리면 하루치 총량
        hours = max(span.total_seconds() / 3600.0, 0.25)

        table = params.get("table", "")
        base_per_hour = {
            "RAW_FAB_DATA": 3000,
            "RAW_INLINE_DATA": 25000,
            "RAW_ET_DATA": 40000,
        }.get(table, 5000)

        # shard filter 가 걸리면 row 축소 — IN 연산자 걸린 모든 컬럼을 shard 로 간주
        shard_factor = 1.0
        for key, sv in params.items():
            if key in ("table", "dateFrom", "dateTo"):
                continue
            if isinstance(sv, dict) and sv.get("op") == "in":
                n = len(sv.get("value") or [])
                if n:
                    shard_factor *= min(1.0, n / 30.0)  # 30 shard 중 n 개 선택 가정

        row_count = int(base_per_hour * hours * shard_factor)
        row_count = max(10, min(row_count, 1_500_000))

        data = {}
        try:
            t_base = datetime.fromisoformat(params["dateFrom"])
        except Exception:
            t_base = datetime.now()

        for col in custom_col:
            if col == "root_lot_id":
                data[col] = [f"R{random.randint(0, 29):03d}" for _ in range(row_count)]
            elif col == "lot_id":
                data[col] = [f"L{random.randint(0, 499):04d}" for _ in range(row_count)]
            elif col == "wafer_id":
                data[col] = [random.randint(1, 25) for _ in range(row_count)]
            elif col == "item_id":
                data[col] = [f"ITEM_{random.randint(0, 99):03d}" for _ in range(row_count)]
            elif col == "time":
                data[col] = [t_base + timedelta(seconds=random.randint(0, int(span.total_seconds()) or 1))
                             for _ in range(row_count)]
            elif col == "value":
                data[col] = [round(random.gauss(100, 10), 4) for _ in range(row_count)]
            else:
                data[col] = [f"{col}_{random.randint(0, 9)}" for _ in range(row_count)]

        return pd.DataFrame(data)


_mock_engine = _MockQueryEngine()


def _get_real_query_fn(module_path: str) -> Callable:
    mod_str, _, fn_str = module_path.partition(":")
    if not mod_str or not fn_str:
        raise ValueError(f"invalid module path: {module_path!r} (expected 'pkg.mod:fn')")
    mod = importlib.import_module(mod_str)
    return getattr(mod, fn_str)


# ─────────────────────────────────────────────
# Adapter
# ─────────────────────────────────────────────
class LakeAPI:
    def __init__(self, settings: dict):
        self.settings = settings
        lk = settings["lake_api"]
        self.user = lk["user"]
        self.timeout_sec = int(lk["timeout_sec"])
        self.min_interval = float(lk["min_interval_sec"])
        self.retry_attempts = int(lk["retry"]["attempts"])
        self.backoff = list(lk["retry"]["backoff_sec"])
        self.retryable_tokens = tuple(lk.get("retryable_errors") or [])

        mode = lk.get("mode", "mock")
        if mode == "mock":
            self._fn: Callable[..., pd.DataFrame] = _mock_engine
        else:
            self._fn = _get_real_query_fn(lk["module"])

        self._last_call_time = 0.0
        self._rate_lock = asyncio.Lock()

    async def query(self, params: dict, custom_col: list, user: str | None = None) -> pl.DataFrame:
        """결과는 항상 polars.DataFrame. pandas 반환한 real 어댑터도 내부에서 변환.
        사내 query 시그니처: query(params, custom_col, user) — 인증은 user 하나뿐이다.

        user 를 주면 그 계정으로 호출한다 (사내 getData 의 user_name).
        None 이면 settings.lake_api.user 기본값."""
        last_err: Exception | None = None
        for attempt in range(self.retry_attempts):
            try:
                await self._wait_min_interval()
                df = await asyncio.wait_for(
                    asyncio.to_thread(self._invoke, params, custom_col, user),
                    timeout=self.timeout_sec,
                )
                return self._to_polars(df)
            except asyncio.TimeoutError as e:
                last_err = TimeoutError(f"query timeout after {self.timeout_sec}s")
                # Timeout 은 retryable 로 취급(사용자 요구: HY000/timeout 모두 재시도)
            except Exception as e:
                last_err = e
                if not self._is_retryable(e):
                    raise

            if attempt < self.retry_attempts - 1:
                delay = self.backoff[min(attempt, len(self.backoff) - 1)]
                await asyncio.sleep(delay)

        assert last_err is not None
        raise last_err

    def _invoke(self, params: dict, custom_col: list, user: str | None = None):
        """실제 함수 호출. user 가 None 이면 settings 기본 계정(user_name)."""
        return self._fn(params, custom_col, user or self.user)

    @staticmethod
    def _to_polars(df) -> pl.DataFrame:
        """pandas / polars 둘 다 polars 로 통일. pyarrow 버전 충돌 시 dict 경유 폴백.

        사내 어댑터는 '해당 조건 데이터 없음' 을 None(또는 빈 list)으로 주기도 한다 —
        조회 결과 없음은 에러가 아니라 빈 DataFrame 이다 (뒷단이 그대로 흘려보낸다)."""
        if df is None:
            return pl.DataFrame()
        if isinstance(df, list) and not df:
            return pl.DataFrame()
        if isinstance(df, pl.DataFrame):
            return df
        if isinstance(df, pd.DataFrame):
            try:
                return pl.from_pandas(df)
            except Exception:
                # 폴백: dict 경유 (datetime 은 파이썬 객체로 내려 polars 가 자동 감지)
                out = {}
                for c in df.columns:
                    s = df[c]
                    out[c] = s.tolist()
                return pl.DataFrame(out)
        raise TypeError(f"unsupported df type: {type(df).__name__}")

    async def _wait_min_interval(self):
        async with self._rate_lock:
            now = time.monotonic()
            gap = now - self._last_call_time
            if gap < self.min_interval:
                await asyncio.sleep(self.min_interval - gap)
            self._last_call_time = time.monotonic()

    def _is_retryable(self, e: Exception) -> bool:
        if not self.retryable_tokens:
            return False
        name = type(e).__name__
        msg = str(e)
        for tok in self.retryable_tokens:
            if tok in name or tok in msg:
                return True
        return False

    def reload(self, settings: dict):
        """settings 가 웹에서 바뀐 뒤 호출. mode/module 전환 지원."""
        self.__init__(settings)
