from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SymbolSnapshot:
    scan_run_id: int
    symbol: str
    last_price: float
    pct_change_1d: float | None
    pct_change_intraday: float | None
    volume: float | None
    market_cap: float | None
    price_source_ts_utc: str
    price_as_of_ts_utc: str
    passed_filters: bool


@dataclass
class NewsArticle:
    scan_run_id: int
    symbol: str
    url: str
    title: str
    source: str
    published_ts_utc: str
    rss_fetched_ts_utc: str
    dedup_key: str
    summary: str | None = None
    provider: str = "yahoo_rss"
    provider_article_id: str | None = None


@dataclass
class ArticleScore:
    scan_run_id: int
    symbol: str
    article_id: int
    openai_model: str
    summary: str
    impact_score: int
    impact_direction: str
    seriousness_score: int
    confidence: int
    impact_horizon: str
    reason_tags_json: str
    is_material_news: bool
    scored_ts_utc: str
    main_symbol: str | None = None
    mentioned_symbols_json: str = "[]"
    relevance_score: int = 0


@dataclass
class SymbolScore:
    scan_run_id: int
    symbol: str
    article_count: int
    weighted_impact_score: float
    weighted_seriousness_score: float
    bullish_bearish_label: str
    score_ts_utc: str
