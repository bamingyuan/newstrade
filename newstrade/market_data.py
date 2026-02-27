from __future__ import annotations

from typing import Any

from .config import AppConfig


def pct_change(start_value: float | None, end_value: float | None) -> float | None:
    if start_value is None or end_value is None or start_value == 0:
        return None
    return ((end_value - start_value) / start_value) * 100.0


def calculate_series_pct_change(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    return pct_change(values[0], values[-1])


def selected_move_pct(snapshot: dict[str, Any], scan_window: str) -> float | None:
    if scan_window == "intraday":
        return snapshot.get("pct_change_intraday")
    return snapshot.get("pct_change_1d")


def passes_symbol_filters(snapshot: dict[str, Any], config: AppConfig, scan_window: str) -> tuple[bool, str]:
    symbol = snapshot.get("symbol", "?")
    move_pct = selected_move_pct(snapshot, scan_window)

    if move_pct is None:
        return False, f"{symbol}: missing percent change"

    abs_move = abs(move_pct)
    if abs_move < config.min_pct_change:
        return False, f"{symbol}: abs move below minimum"
    if abs_move > config.max_pct_change:
        return False, f"{symbol}: abs move above maximum"

    last_price = snapshot.get("last_price")
    if config.min_price is not None and (last_price is None or last_price < config.min_price):
        return False, f"{symbol}: below MIN_PRICE"
    if config.max_price is not None and (last_price is None or last_price > config.max_price):
        return False, f"{symbol}: above MAX_PRICE"

    market_cap = snapshot.get("market_cap")
    if config.market_cap_filter_active and market_cap is None:
        return False, f"{symbol}: market cap unavailable"

    if config.min_market_cap is not None and market_cap is not None and market_cap < config.min_market_cap:
        return False, f"{symbol}: below MIN_MARKET_CAP"
    if config.max_market_cap is not None and market_cap is not None and market_cap > config.max_market_cap:
        return False, f"{symbol}: above MAX_MARKET_CAP"

    return True, "passed"
