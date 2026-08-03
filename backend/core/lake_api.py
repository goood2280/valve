"""
Valve · lake_api
----------------
사내 DataLake 의 query(params, custom_col, user) 함수를 감싸는 실 API 전용 어댑터.

실 API:
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
            return getData(params, custom_columns=custom_col, user_name=user)

공통 보증:
  - rate limit (min_interval_sec, 전역 lock)
  - asyncio.wait_for timeout (기본 290s = 4분 50초, 5분 제한 안쪽)
  - exponential backoff 재시도 (기본 3회, HY000/Timeout/ConnectionError 만)
"""
from __future__ import annotations

import asyncio
import importlib
import threading
import time
from typing import Callable

import pandas as pd
import polars as pl


_PARAM_ALIASES = {
    "table": "table_name",
    "datefrom": "dateFrom",
    "date_from": "dateFrom",
    "datgeFrom": "dateFrom",
    "dateto": "dateTo",
    "date_to": "dateTo",
}


def normalize_query_params(params: dict | None) -> dict:
    """Return the flat parameter shape accepted by ``bigdataquery.getData``."""
    normalized: dict = {}
    for raw_key, raw_value in dict(params or {}).items():
        key = _PARAM_ALIASES.get(str(raw_key), str(raw_key))
        if key in {"op", "product_code"}:
            continue
        value = raw_value
        if isinstance(value, dict) and "value" in value:
            value = value.get("value")
        if value is None or value == "" or value == []:
            continue
        normalized[key] = value

    table_name = str(normalized.get("table_name") or "").strip()
    if table_name:
        normalized["table_name"] = table_name
    return normalized


class HY000Error(Exception):
    """사내 ODBC 계층의 HY000 오류를 분류할 때 사용하는 예외."""


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

        self._fn: Callable[..., pd.DataFrame] = _get_real_query_fn(lk["module"])

        self._last_call_time = 0.0
        # FeaturePipeline의 raw worker마다 별도 asyncio.run loop가 생긴다. asyncio.Lock은
        # 첫 loop에 귀속되어 다른 worker에서 "bound to a different event loop"가 날 수
        # 있으므로 프로세스 공용 rate limit은 threading.Lock으로 직렬화한다.
        self._rate_lock = threading.Lock()

    async def query(self, params: dict, custom_col: list, user: str | None = None) -> pl.DataFrame:
        """결과는 항상 polars.DataFrame. pandas 반환한 real 어댑터도 내부에서 변환.
        사내 query 시그니처: query(params, custom_col, user) — 인증은 user 하나뿐이다.

        user 를 주면 그 계정으로 호출한다 (사내 getData 의 user_name).
        None 이면 settings.lake_api.user 기본값."""
        # 모든 어댑터와 조회 경로가 동일한 사내 API 규약을 사용하게 한다.
        # 구 코드의 ``table``은 받아주되 외부 함수에는 ``table_name``만 전달한다.
        params = normalize_query_params(params)
        if not str(params.get("table_name") or "").strip():
            raise ValueError(
                "table_name could not be resolved; set a source name/table or use the "
                "RAW_{SOURCE}_DATA default"
            )

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
        await asyncio.to_thread(self._wait_min_interval_blocking)

    def _wait_min_interval_blocking(self):
        with self._rate_lock:
            now = time.monotonic()
            gap = now - self._last_call_time
            if gap < self.min_interval:
                time.sleep(self.min_interval - gap)
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
        """settings 가 웹에서 바뀐 뒤 실제 API 어댑터를 다시 로드."""
        self.__init__(settings)
