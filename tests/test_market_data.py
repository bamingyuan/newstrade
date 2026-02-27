from __future__ import annotations

from newstrade.config import AppConfig
from newstrade.market_data import calculate_series_pct_change, passes_symbol_filters, pct_change


def test_pct_change_basic() -> None:
    assert round(pct_change(100, 110), 4) == 10.0
    assert round(pct_change(100, 90), 4) == -10.0


def test_calculate_series_pct_change() -> None:
    values = [10, 12, 15]
    assert round(calculate_series_pct_change(values), 4) == 50.0


def test_passes_symbol_filters_for_1d() -> None:
    cfg = AppConfig()
    snapshot = {
        "symbol": "AAPL",
        "last_price": 150,
        "pct_change_1d": 3.0,
        "pct_change_intraday": 1.0,
        "market_cap": 2_000_000_000_000,
    }
    passed, _ = passes_symbol_filters(snapshot, cfg, scan_window="1d")
    assert passed is True


def test_fails_when_market_cap_missing_and_filter_active() -> None:
    cfg = AppConfig()
    snapshot = {
        "symbol": "AAPL",
        "last_price": 150,
        "pct_change_1d": 3.0,
        "pct_change_intraday": 3.0,
        "market_cap": None,
    }
    passed, reason = passes_symbol_filters(snapshot, cfg, scan_window="1d")
    assert passed is False
    assert "market cap unavailable" in reason
