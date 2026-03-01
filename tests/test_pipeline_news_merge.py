from __future__ import annotations

from datetime import date
import sqlite3

from newstrade.config import AppConfig
from newstrade.db import connect_db, create_scan_run, init_db, insert_symbol_snapshots
from newstrade.pipeline import run_news
from newstrade.time_utils import utc_now_iso


def _create_run_with_symbol(db_path: str, symbol: str = "AAPL") -> int:
    conn = connect_db(db_path)
    init_db(conn)
    run_id = create_scan_run(
        conn=conn,
        run_ts_utc=utc_now_iso(),
        scan_window="1d",
        symbol_mode="env",
        min_pct_change=2.0,
        max_pct_change=30.0,
        status="completed",
        notes="test run",
    )
    insert_symbol_snapshots(
        conn,
        [
            {
                "scan_run_id": run_id,
                "symbol": symbol,
                "last_price": 100.0,
                "pct_change_1d": 3.0,
                "pct_change_intraday": 1.0,
                "market_cap": None,
                "price_source_ts_utc": utc_now_iso(),
                "price_as_of_ts_utc": "2026-02-23T21:00:00+00:00",
                "passed_filters": True,
            }
        ],
    )
    conn.close()
    return run_id


def test_run_news_merges_yahoo_and_massive_and_filters_future(tmp_path) -> None:
    db_path = str(tmp_path / "news_merge.db")
    run_id = _create_run_with_symbol(db_path)
    config = AppConfig(
        symbol_mode="env",
        symbols=["AAPL"],
        scan_time_travel_enabled=True,
        scan_as_of_date=date.fromisoformat("2026-02-23"),
        market_cap_enabled=False,
        db_path=db_path,
        massive_api_key="test-key",
        news_lookback_hours=24,
        max_news_articles_per_symbol=10,
    )

    def yahoo_fetcher(**kwargs):
        return [
            {
                "symbol": "AAPL",
                "url": "https://example.com/shared?utm=1",
                "title": "Shared Title",
                "source": "Yahoo Finance RSS",
                "published_ts_utc": "2026-02-23T19:00:00+00:00",
                "rss_fetched_ts_utc": utc_now_iso(),
                "dedup_key": "https://example.com/shared",
                "summary": "",
                "provider": "yahoo_rss",
                "provider_article_id": "",
            }
        ]

    def massive_fetcher(**kwargs):
        return [
            {
                "symbol": "AAPL",
                "url": "https://example.com/shared",
                "title": "Shared Title From Massive",
                "source": "Massive",
                "published_ts_utc": "2026-02-23T19:00:00+00:00",
                "rss_fetched_ts_utc": utc_now_iso(),
                "dedup_key": "https://example.com/shared",
                "summary": "Duplicate row",
                "provider": "massive",
                "provider_article_id": "m-dup",
            },
            {
                "symbol": "AAPL",
                "url": "https://example.com/massive-only",
                "title": "Massive Only",
                "source": "Massive",
                "published_ts_utc": "2026-02-23T18:00:00+00:00",
                "rss_fetched_ts_utc": utc_now_iso(),
                "dedup_key": "https://example.com/massive-only",
                "summary": "Massive summary",
                "provider": "massive",
                "provider_article_id": "m-1",
            },
            {
                "symbol": "AAPL",
                "url": "https://example.com/future",
                "title": "Future Article",
                "source": "Massive",
                "published_ts_utc": "2026-02-23T22:30:00+00:00",
                "rss_fetched_ts_utc": utc_now_iso(),
                "dedup_key": "https://example.com/future",
                "summary": "Should be filtered",
                "provider": "massive",
                "provider_article_id": "m-future",
            },
        ]

    inserted = run_news(
        config=config,
        scan_run_id=run_id,
        news_fetcher=yahoo_fetcher,
        massive_news_fetcher=massive_fetcher,
    )
    assert inserted == 2

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT url, provider, summary, provider_article_id
        FROM news_articles
        WHERE scan_run_id = ?
        ORDER BY url
        """,
        (run_id,),
    ).fetchall()
    conn.close()

    assert rows == [
        ("https://example.com/massive-only", "massive", "Massive summary", "m-1"),
        ("https://example.com/shared?utm=1", "yahoo_rss", None, None),
    ]


def test_run_news_skips_massive_when_disabled(tmp_path) -> None:
    db_path = str(tmp_path / "news_massive_off.db")
    run_id = _create_run_with_symbol(db_path)
    config = AppConfig(
        symbol_mode="env",
        symbols=["AAPL"],
        scan_time_travel_enabled=True,
        scan_as_of_date=date.fromisoformat("2026-02-23"),
        market_cap_enabled=False,
        db_path=db_path,
        massive_news_enabled=False,
        massive_api_key="test-key",
        news_lookback_hours=24,
        max_news_articles_per_symbol=10,
    )

    def yahoo_fetcher(**kwargs):
        return [
            {
                "symbol": "AAPL",
                "url": "https://example.com/yahoo-only",
                "title": "Yahoo Only",
                "source": "Yahoo Finance RSS",
                "published_ts_utc": "2026-02-23T19:00:00+00:00",
                "rss_fetched_ts_utc": utc_now_iso(),
                "dedup_key": "https://example.com/yahoo-only",
                "summary": "",
                "provider": "yahoo_rss",
                "provider_article_id": "",
            }
        ]

    def massive_fetcher(**kwargs):
        raise AssertionError("Massive fetcher should not be called when MASSIVE_NEWS=0")

    inserted = run_news(
        config=config,
        scan_run_id=run_id,
        news_fetcher=yahoo_fetcher,
        massive_news_fetcher=massive_fetcher,
    )
    assert inserted == 1

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT url, provider FROM news_articles WHERE scan_run_id = ? ORDER BY article_id",
        (run_id,),
    ).fetchall()
    conn.close()
    assert rows == [("https://example.com/yahoo-only", "yahoo_rss")]
