from __future__ import annotations

from typing import Any

from .config import AppConfig


def pct_change(start_value: float | None, end_value: float | None) -> float | None:
    if start_value is None or end_value is None or start_value == 0:
        return None
    return ((end_value - start_value) / start_value) * 100.0


def passes_symbol_filters(snapshot: dict[str, Any], config: AppConfig) -> tuple[bool, str]:
    symbol = str(snapshot.get("symbol", "?"))
    move_pct = snapshot.get("pct_change")

    if move_pct is None:
        return False, f"{symbol}: missing percent change"

    abs_move = abs(float(move_pct))
    if abs_move < config.min_pct_change:
        return False, f"{symbol}: abs move below minimum"
    if abs_move > config.max_pct_change:
        return False, f"{symbol}: abs move above maximum"

    close_price = snapshot.get("close_price")
    if config.min_price is not None and (close_price is None or float(close_price) < config.min_price):
        return False, f"{symbol}: below MIN_PRICE"
    if config.max_price is not None and (close_price is None or float(close_price) > config.max_price):
        return False, f"{symbol}: above MAX_PRICE"

    volume = snapshot.get("volume")
    if (config.min_volume is not None or config.max_volume is not None) and volume is None:
        return False, f"{symbol}: volume unavailable"
    if config.min_volume is not None and volume is not None and float(volume) < config.min_volume:
        return False, f"{symbol}: below MIN_VOLUME"
    if config.max_volume is not None and volume is not None and float(volume) > config.max_volume:
        return False, f"{symbol}: above MAX_VOLUME"

    return True, "passed"
