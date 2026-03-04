from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
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
    scan_time_travel_enabled: bool = False
    scan_as_of_date: date | None = None

    min_pct_change: float = 2.0
    max_pct_change: float = 30.0
    min_price: float | None = 3.0
    max_price: float | None = 2000.0
    min_volume: float | None = None
    max_volume: float | None = None
    market_cap_enabled: bool = True
    min_market_cap: float | None = 1_000_000_000.0
    max_market_cap: float | None = 5_000_000_000_000.0

    news_lookback_hours: int = 24
    max_news_articles_per_symbol: int = 8
    yahoo_rss_region: str = "US"
    yahoo_rss_lang: str = "en-US"
    massive_news_enabled: bool = True
    massive_api_key: str = ""
    massive_max_calls_per_minute: int = 5
    massive_news_max_pages_per_symbol: int = 3

    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini"
    openai_timeout_seconds: int = 30
    openai_temperature: float = 0.0
    openai_max_completion_tokens: int | None = 300
    openai_score_retries: int = 1

    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4002
    ibkr_client_id: int = 37
    ibkr_account_mode: str = "paper"
    ibkr_max_symbols: int = 100

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
        if self.scan_time_travel_enabled and self.scan_as_of_date is None:
            raise ConfigError("SCAN_AS_OF_DATE is required when SCAN_TIME_TRAVEL=1")
        if self.scan_as_of_date is not None and self.scan_as_of_date > date.today():
            raise ConfigError("SCAN_AS_OF_DATE cannot be in the future")

        if self.min_pct_change < 0:
            raise ConfigError("MIN_PCT_CHANGE must be >= 0")
        if self.min_pct_change > self.max_pct_change:
            raise ConfigError("MIN_PCT_CHANGE cannot be greater than MAX_PCT_CHANGE")
        if self.min_price is not None and self.max_price is not None and self.min_price > self.max_price:
            raise ConfigError("MIN_PRICE cannot be greater than MAX_PRICE")
        if self.min_volume is not None and self.max_volume is not None and self.min_volume > self.max_volume:
            raise ConfigError("MIN_VOLUME cannot be greater than MAX_VOLUME")
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
        if self.massive_max_calls_per_minute <= 0:
            raise ConfigError("MASSIVE_MAX_CALLS_PER_MINUTE must be > 0")
        if self.massive_news_max_pages_per_symbol <= 0:
            raise ConfigError("MASSIVE_NEWS_MAX_PAGES_PER_SYMBOL must be > 0")
        if self.intraday_lookback_days <= 0:
            raise ConfigError("INTRADAY_LOOKBACK_DAYS must be > 0")
        if self.openai_timeout_seconds <= 0:
            raise ConfigError("OPENAI_TIMEOUT_SECONDS must be > 0")
        if self.openai_max_completion_tokens is not None and self.openai_max_completion_tokens <= 0:
            raise ConfigError("OPENAI_MAX_COMPLETION_TOKENS must be > 0 when set")
        if self.openai_score_retries < 0:
            raise ConfigError("OPENAI_SCORE_RETRIES must be >= 0")
        if self.ibkr_max_symbols <= 0:
            raise ConfigError("IBKR_MAX_SYMBOLS must be > 0")

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


def _parse_optional_date(raw: str | None, default: date | None, name: str) -> date | None:
    if raw is None:
        return default
    text = raw.strip()
    if text == "":
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ConfigError(f"{name} must be in YYYY-MM-DD format") from exc


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
        scan_time_travel_enabled=_parse_binary_flag(mapping.get("SCAN_TIME_TRAVEL"), False, "SCAN_TIME_TRAVEL"),
        scan_as_of_date=_parse_optional_date(mapping.get("SCAN_AS_OF_DATE"), None, "SCAN_AS_OF_DATE"),
        min_pct_change=float(mapping.get("MIN_PCT_CHANGE", 2.0)),
        max_pct_change=float(mapping.get("MAX_PCT_CHANGE", 30.0)),
        min_price=_parse_optional_float(mapping.get("MIN_PRICE"), 3.0),
        max_price=_parse_optional_float(mapping.get("MAX_PRICE"), 2000.0),
        min_volume=_parse_optional_float(mapping.get("MIN_VOLUME"), None),
        max_volume=_parse_optional_float(mapping.get("MAX_VOLUME"), None),
        market_cap_enabled=_parse_binary_flag(mapping.get("MARKET_CAP"), True, "MARKET_CAP"),
        min_market_cap=_parse_optional_float(mapping.get("MIN_MARKET_CAP"), 1_000_000_000.0),
        max_market_cap=_parse_optional_float(mapping.get("MAX_MARKET_CAP"), 5_000_000_000_000.0),
        news_lookback_hours=int(mapping.get("NEWS_LOOKBACK_HOURS", 24)),
        max_news_articles_per_symbol=int(mapping.get("MAX_NEWS_ARTICLES_PER_SYMBOL", 8)),
        yahoo_rss_region=mapping.get("YAHOO_RSS_REGION", "US").strip(),
        yahoo_rss_lang=mapping.get("YAHOO_RSS_LANG", "en-US").strip(),
        massive_news_enabled=_parse_binary_flag(mapping.get("MASSIVE_NEWS"), True, "MASSIVE_NEWS"),
        massive_api_key=mapping.get("MASSIVE_API_KEY", "").strip(),
        massive_max_calls_per_minute=int(mapping.get("MASSIVE_MAX_CALLS_PER_MINUTE", 5)),
        massive_news_max_pages_per_symbol=int(mapping.get("MASSIVE_NEWS_MAX_PAGES_PER_SYMBOL", 3)),
        openai_api_key=mapping.get("OPENAI_API_KEY", "").strip(),
        openai_model=mapping.get("OPENAI_MODEL", "gpt-4.1-mini").strip(),
        openai_timeout_seconds=int(mapping.get("OPENAI_TIMEOUT_SECONDS", 30)),
        openai_temperature=float(mapping.get("OPENAI_TEMPERATURE", 0.0)),
        openai_max_completion_tokens=_parse_optional_int(mapping.get("OPENAI_MAX_COMPLETION_TOKENS"), 300),
        openai_score_retries=int(mapping.get("OPENAI_SCORE_RETRIES", 1)),
        ibkr_host=mapping.get("IBKR_HOST", "127.0.0.1").strip(),
        ibkr_port=int(mapping.get("IBKR_PORT", 4002)),
        ibkr_client_id=int(mapping.get("IBKR_CLIENT_ID", 37)),
        ibkr_account_mode=mapping.get("IBKR_ACCOUNT_MODE", "paper").strip().lower(),
        ibkr_max_symbols=int(mapping.get("IBKR_MAX_SYMBOLS", 100)),
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
