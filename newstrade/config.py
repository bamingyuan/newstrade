from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping
import os

from dotenv import load_dotenv


VALID_SYMBOL_MODES = {"env", "ibkr", "both"}
VALID_SCAN_WINDOWS = {"1d", "intraday"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigError(ValueError):
    """Raised when .env values are invalid."""


@dataclass
class AppConfig:
    symbol_mode: str = "both"
    symbols: list[str] = field(default_factory=lambda: ["AAPL", "MSFT", "NVDA"])
    scan_window_default: str = "1d"
    intraday_lookback_days: int = 5
    intraday_bar_size: str = "4 hours"

    min_pct_change: float = 2.0
    max_pct_change: float = 30.0
    min_price: float | None = 3.0
    max_price: float | None = 2000.0
    market_cap_enabled: bool = True
    min_market_cap: float | None = 1_000_000_000.0
    max_market_cap: float | None = 5_000_000_000_000.0

    news_lookback_hours: int = 24
    max_news_articles_per_symbol: int = 8
    yahoo_rss_region: str = "US"
    yahoo_rss_lang: str = "en-US"

    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini"
    openai_timeout_seconds: int = 30
    openai_temperature: float = 0.0

    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4002
    ibkr_client_id: int = 37
    ibkr_account_mode: str = "paper"

    timezone: str = "UTC"
    db_path: str = "./data/newstrade.db"
    log_level: str = "INFO"
    news_dedup_mode: str = "url"
    csv_export_dir: str = "./exports"

    def validate(self) -> None:
        if self.symbol_mode not in VALID_SYMBOL_MODES:
            raise ConfigError(f"SYMBOL_MODE must be one of {sorted(VALID_SYMBOL_MODES)}")
        if self.scan_window_default not in VALID_SCAN_WINDOWS:
            raise ConfigError(f"SCAN_WINDOW_DEFAULT must be one of {sorted(VALID_SCAN_WINDOWS)}")
        if self.log_level.upper() not in VALID_LOG_LEVELS:
            raise ConfigError(f"LOG_LEVEL must be one of {sorted(VALID_LOG_LEVELS)}")

        if self.min_pct_change < 0:
            raise ConfigError("MIN_PCT_CHANGE must be >= 0")
        if self.min_pct_change > self.max_pct_change:
            raise ConfigError("MIN_PCT_CHANGE cannot be greater than MAX_PCT_CHANGE")
        if self.min_price is not None and self.max_price is not None and self.min_price > self.max_price:
            raise ConfigError("MIN_PRICE cannot be greater than MAX_PRICE")
        if (
            self.market_cap_enabled
            and self.min_market_cap is not None
            and self.max_market_cap is not None
            and self.min_market_cap > self.max_market_cap
        ):
            raise ConfigError("MIN_MARKET_CAP cannot be greater than MAX_MARKET_CAP")

        if self.news_lookback_hours <= 0:
            raise ConfigError("NEWS_LOOKBACK_HOURS must be > 0")
        if self.max_news_articles_per_symbol <= 0:
            raise ConfigError("MAX_NEWS_ARTICLES_PER_SYMBOL must be > 0")
        if self.intraday_lookback_days <= 0:
            raise ConfigError("INTRADAY_LOOKBACK_DAYS must be > 0")
        if self.openai_timeout_seconds <= 0:
            raise ConfigError("OPENAI_TIMEOUT_SECONDS must be > 0")

    @property
    def db_path_obj(self) -> Path:
        return Path(self.db_path)

    @property
    def csv_export_dir_obj(self) -> Path:
        return Path(self.csv_export_dir)

    @property
    def market_cap_filter_active(self) -> bool:
        return self.market_cap_enabled and (self.min_market_cap is not None or self.max_market_cap is not None)


def _parse_symbols(raw: str) -> list[str]:
    return [symbol.strip().upper() for symbol in raw.split(",") if symbol.strip()]


def _parse_optional_float(raw: str | None, default: float | None) -> float | None:
    if raw is None:
        return default
    text = raw.strip()
    if text == "":
        return None
    return float(text)


def _parse_optional_int(raw: str | None, default: int | None) -> int | None:
    if raw is None:
        return default
    text = raw.strip()
    if text == "":
        return None
    return int(text)


def _parse_binary_flag(raw: str | None, default: bool, name: str) -> bool:
    if raw is None:
        return default
    text = raw.strip()
    if text == "":
        return default
    if text == "1":
        return True
    if text == "0":
        return False
    raise ConfigError(f"{name} must be 0 or 1")


def build_config_from_mapping(mapping: Mapping[str, str]) -> AppConfig:
    symbol_mode = mapping.get("SYMBOL_MODE", "both").strip().lower()
    symbols = _parse_symbols(mapping.get("SYMBOLS", "AAPL,MSFT,NVDA"))
    scan_window_default = mapping.get("SCAN_WINDOW_DEFAULT", "1d").strip().lower()

    config = AppConfig(
        symbol_mode=symbol_mode,
        symbols=symbols,
        scan_window_default=scan_window_default,
        intraday_lookback_days=int(mapping.get("INTRADAY_LOOKBACK_DAYS", 5)),
        intraday_bar_size=mapping.get("INTRADAY_BAR_SIZE", "4 hours").strip(),
        min_pct_change=float(mapping.get("MIN_PCT_CHANGE", 2.0)),
        max_pct_change=float(mapping.get("MAX_PCT_CHANGE", 30.0)),
        min_price=_parse_optional_float(mapping.get("MIN_PRICE"), 3.0),
        max_price=_parse_optional_float(mapping.get("MAX_PRICE"), 2000.0),
        market_cap_enabled=_parse_binary_flag(mapping.get("MARKET_CAP"), True, "MARKET_CAP"),
        min_market_cap=_parse_optional_float(mapping.get("MIN_MARKET_CAP"), 1_000_000_000.0),
        max_market_cap=_parse_optional_float(mapping.get("MAX_MARKET_CAP"), 5_000_000_000_000.0),
        news_lookback_hours=int(mapping.get("NEWS_LOOKBACK_HOURS", 24)),
        max_news_articles_per_symbol=int(mapping.get("MAX_NEWS_ARTICLES_PER_SYMBOL", 8)),
        yahoo_rss_region=mapping.get("YAHOO_RSS_REGION", "US").strip(),
        yahoo_rss_lang=mapping.get("YAHOO_RSS_LANG", "en-US").strip(),
        openai_api_key=mapping.get("OPENAI_API_KEY", "").strip(),
        openai_model=mapping.get("OPENAI_MODEL", "gpt-4.1-mini").strip(),
        openai_timeout_seconds=int(mapping.get("OPENAI_TIMEOUT_SECONDS", 30)),
        openai_temperature=float(mapping.get("OPENAI_TEMPERATURE", 0.0)),
        ibkr_host=mapping.get("IBKR_HOST", "127.0.0.1").strip(),
        ibkr_port=int(mapping.get("IBKR_PORT", 4002)),
        ibkr_client_id=int(mapping.get("IBKR_CLIENT_ID", 37)),
        ibkr_account_mode=mapping.get("IBKR_ACCOUNT_MODE", "paper").strip().lower(),
        timezone=mapping.get("TIMEZONE", "UTC").strip(),
        db_path=mapping.get("DB_PATH", "./data/newstrade.db").strip(),
        log_level=mapping.get("LOG_LEVEL", "INFO").strip().upper(),
        news_dedup_mode=mapping.get("NEWS_DEDUP_MODE", "url").strip().lower(),
        csv_export_dir=mapping.get("CSV_EXPORT_DIR", "./exports").strip(),
    )
    config.validate()
    return config


def load_config(env_file: str = ".env") -> AppConfig:
    load_dotenv(env_file)
    mapping: dict[str, str] = dict(os.environ)
    return build_config_from_mapping(mapping)
