from __future__ import annotations

import pytest

from newstrade.config import ConfigError, build_config_from_mapping


def base_mapping() -> dict[str, str]:
    return {
        "SYMBOL_MODE": "both",
        "SYMBOLS": "AAPL,MSFT,NVDA",
        "SCAN_WINDOW_DEFAULT": "1d",
        "INTRADAY_LOOKBACK_DAYS": "5",
        "INTRADAY_BAR_SIZE": "4 hours",
        "MIN_PCT_CHANGE": "2.0",
        "MAX_PCT_CHANGE": "30.0",
        "MIN_PRICE": "3",
        "MAX_PRICE": "2000",
        "MIN_MARKET_CAP": "1000000000",
        "MAX_MARKET_CAP": "5000000000000",
        "NEWS_LOOKBACK_HOURS": "24",
        "MAX_NEWS_ARTICLES_PER_SYMBOL": "8",
        "YAHOO_RSS_REGION": "US",
        "YAHOO_RSS_LANG": "en-US",
        "OPENAI_API_KEY": "test-key",
        "OPENAI_MODEL": "gpt-4.1-mini",
        "OPENAI_TIMEOUT_SECONDS": "30",
        "OPENAI_TEMPERATURE": "0",
        "IBKR_HOST": "127.0.0.1",
        "IBKR_PORT": "4002",
        "IBKR_CLIENT_ID": "37",
        "IBKR_ACCOUNT_MODE": "paper",
        "TIMEZONE": "UTC",
        "DB_PATH": "./data/newstrade.db",
        "CSV_EXPORT_DIR": "./exports",
        "LOG_LEVEL": "INFO",
        "NEWS_DEDUP_MODE": "url",
    }


def test_config_loads_and_parses_symbols() -> None:
    mapping = base_mapping()
    cfg = build_config_from_mapping(mapping)

    assert cfg.symbol_mode == "both"
    assert cfg.symbols == ["AAPL", "MSFT", "NVDA"]
    assert cfg.scan_window_default == "1d"


def test_invalid_percent_range_raises() -> None:
    mapping = base_mapping()
    mapping["MIN_PCT_CHANGE"] = "10"
    mapping["MAX_PCT_CHANGE"] = "5"

    with pytest.raises(ConfigError):
        build_config_from_mapping(mapping)


def test_invalid_symbol_mode_raises() -> None:
    mapping = base_mapping()
    mapping["SYMBOL_MODE"] = "invalid"

    with pytest.raises(ConfigError):
        build_config_from_mapping(mapping)


def test_optional_market_cap_can_be_disabled() -> None:
    mapping = base_mapping()
    mapping["MIN_MARKET_CAP"] = ""
    mapping["MAX_MARKET_CAP"] = ""

    cfg = build_config_from_mapping(mapping)
    assert cfg.min_market_cap is None
    assert cfg.max_market_cap is None
    assert cfg.market_cap_filter_active is False
