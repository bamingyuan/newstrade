from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Mapping
import os
import re

from dotenv import load_dotenv


VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigError(ValueError):
    """Raised when .env values are invalid."""


@dataclass
class AppConfig:
    scan_time_travel_enabled: bool = False
    scan_as_of_date: date | None = None

    min_pct_change: float = 2.0
    max_pct_change: float = 30.0
    min_price: float | None = 3.0
    max_price: float | None = 2000.0
    min_volume: float | None = None
    max_volume: float | None = None

    max_news_symbols_per_run: int = 30
    news_lookback_hours: int = 24
    max_news_articles_per_symbol: int = 8
    yahoo_rss_region: str = "US"
    yahoo_rss_lang: str = "en-US"
    yahoo_rss_allowed_domains: list[str] = field(default_factory=list)
    massive_api_key: str = ""
    massive_max_calls_per_minute: int = 5

    openai_api_key: str = ""
    openai_model: str = "gpt-4.1-mini"
    openai_timeout_seconds: int = 30
    openai_temperature: float = 0.0
    openai_max_completion_tokens: int | None = 300
    openai_score_retries: int = 1

    timezone: str = "UTC"
    db_path: str = "./data/newstrade.db"
    log_level: str = "INFO"
    news_dedup_mode: str = "url"
    csv_export_dir: str = "./exports"

    def validate(self) -> None:
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

        if self.max_news_symbols_per_run <= 0:
            raise ConfigError("MAX_NEWS_SYMBOLS_PER_RUN must be > 0")
        if self.news_lookback_hours <= 0:
            raise ConfigError("NEWS_LOOKBACK_HOURS must be > 0")
        if self.max_news_articles_per_symbol <= 0:
            raise ConfigError("MAX_NEWS_ARTICLES_PER_SYMBOL must be > 0")
        if self.massive_max_calls_per_minute <= 0:
            raise ConfigError("MASSIVE_MAX_CALLS_PER_MINUTE must be > 0")
        if self.openai_timeout_seconds <= 0:
            raise ConfigError("OPENAI_TIMEOUT_SECONDS must be > 0")
        if self.openai_max_completion_tokens is not None and self.openai_max_completion_tokens <= 0:
            raise ConfigError("OPENAI_MAX_COMPLETION_TOKENS must be > 0 when set")
        if self.openai_score_retries < 0:
            raise ConfigError("OPENAI_SCORE_RETRIES must be >= 0")

    @property
    def db_path_obj(self) -> Path:
        return Path(self.db_path)

    @property
    def csv_export_dir_obj(self) -> Path:
        return Path(self.csv_export_dir)


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


_DOMAIN_PATTERN = re.compile(r"^[a-z0-9-]+(\.[a-z0-9-]+)+$")


def _parse_domains(raw: str | None, name: str) -> list[str]:
    if raw is None:
        return []

    domains: list[str] = []
    seen: set[str] = set()
    for token in raw.split(","):
        domain = token.strip().lower().rstrip(".")
        if not domain:
            continue
        if "/" in domain or "?" in domain or "#" in domain or "://" in domain:
            raise ConfigError(
                f"{name} must be a comma-separated list of bare domains (example: finance.yahoo.com,fool.com)"
            )
        if ":" in domain:
            domain = domain.split(":", 1)[0].strip()
        if not _DOMAIN_PATTERN.fullmatch(domain):
            raise ConfigError(f"{name} contains invalid domain '{domain}'")
        if domain not in seen:
            seen.add(domain)
            domains.append(domain)
    return domains


def build_config_from_mapping(mapping: Mapping[str, str]) -> AppConfig:
    config = AppConfig(
        scan_time_travel_enabled=_parse_binary_flag(mapping.get("SCAN_TIME_TRAVEL"), False, "SCAN_TIME_TRAVEL"),
        scan_as_of_date=_parse_optional_date(mapping.get("SCAN_AS_OF_DATE"), None, "SCAN_AS_OF_DATE"),
        min_pct_change=float(mapping.get("MIN_PCT_CHANGE", 2.0)),
        max_pct_change=float(mapping.get("MAX_PCT_CHANGE", 30.0)),
        min_price=_parse_optional_float(mapping.get("MIN_PRICE"), 3.0),
        max_price=_parse_optional_float(mapping.get("MAX_PRICE"), 2000.0),
        min_volume=_parse_optional_float(mapping.get("MIN_VOLUME"), None),
        max_volume=_parse_optional_float(mapping.get("MAX_VOLUME"), None),
        max_news_symbols_per_run=int(mapping.get("MAX_NEWS_SYMBOLS_PER_RUN", 30)),
        news_lookback_hours=int(mapping.get("NEWS_LOOKBACK_HOURS", 24)),
        max_news_articles_per_symbol=int(mapping.get("MAX_NEWS_ARTICLES_PER_SYMBOL", 8)),
        yahoo_rss_region=mapping.get("YAHOO_RSS_REGION", "US").strip(),
        yahoo_rss_lang=mapping.get("YAHOO_RSS_LANG", "en-US").strip(),
        yahoo_rss_allowed_domains=_parse_domains(
            mapping.get("YAHOO_RSS_ALLOWED_DOMAINS"),
            "YAHOO_RSS_ALLOWED_DOMAINS",
        ),
        massive_api_key=mapping.get("MASSIVE_API_KEY", "").strip(),
        massive_max_calls_per_minute=int(mapping.get("MASSIVE_MAX_CALLS_PER_MINUTE", 5)),
        openai_api_key=mapping.get("OPENAI_API_KEY", "").strip(),
        openai_model=mapping.get("OPENAI_MODEL", "gpt-4.1-mini").strip(),
        openai_timeout_seconds=int(mapping.get("OPENAI_TIMEOUT_SECONDS", 30)),
        openai_temperature=float(mapping.get("OPENAI_TEMPERATURE", 0.0)),
        openai_max_completion_tokens=_parse_optional_int(mapping.get("OPENAI_MAX_COMPLETION_TOKENS"), 300),
        openai_score_retries=int(mapping.get("OPENAI_SCORE_RETRIES", 1)),
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
