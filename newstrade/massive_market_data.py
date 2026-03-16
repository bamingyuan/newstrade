from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
import time

import requests


MASSIVE_GROUPED_DAILY_URL = "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{date}"


@dataclass
class MassiveGroupedDailyClient:
    api_key: str
    max_calls_per_minute: int = 5
    timeout_seconds: int = 20
    session: requests.Session | None = None
    _last_call_monotonic: float = 0.0

    def __post_init__(self) -> None:
        self._owns_session = self.session is None
        self._session = self.session or requests.Session()

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _wait_for_slot(self) -> None:
        interval = 60.0 / max(1, self.max_calls_per_minute)
        now = time.monotonic()
        wait_seconds = interval - (now - self._last_call_monotonic)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_call_monotonic = time.monotonic()

    def fetch_grouped_daily(self, trade_date: date) -> list[dict[str, Any]]:
        if not self.api_key.strip():
            raise RuntimeError("MASSIVE_API_KEY is missing. Set it in your .env file.")

        url = MASSIVE_GROUPED_DAILY_URL.format(date=trade_date.isoformat())
        params = {
            "adjusted": "true",
            "include_otc": "false",
            "apiKey": self.api_key,
        }
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; newstrade/1.0; +https://example.invalid/newstrade)",
        }

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                self._wait_for_slot()
                response = self._session.get(url, params=params, headers=headers, timeout=self.timeout_seconds)
                if response.status_code == 429 and attempt < 2:
                    time.sleep(1.0 * (2**attempt))
                    continue
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
                return [_normalize_grouped_daily_row(item) for item in payload.get("results", [])]
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < 2:
                    time.sleep(0.5 * (2**attempt))
                    continue
                break

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Massive grouped daily request failed unexpectedly")


def _normalize_grouped_daily_row(item: dict[str, Any]) -> dict[str, Any]:
    timestamp_ms = item.get("t")
    if timestamp_ms is None:
        price_as_of_ts_utc = ""
    else:
        price_as_of_ts_utc = datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat()

    return {
        "symbol": str(item.get("T", "")).strip().upper(),
        "open_price": _as_float(item.get("o")),
        "close_price": _as_float(item.get("c")),
        "high_price": _as_float(item.get("h")),
        "low_price": _as_float(item.get("l")),
        "volume": _as_float(item.get("v")),
        "vwap": _as_float(item.get("vw")),
        "transaction_count": _as_int(item.get("n")),
        "price_as_of_ts_utc": price_as_of_ts_utc,
    }


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
