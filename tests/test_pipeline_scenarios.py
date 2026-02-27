from __future__ import annotations

from pathlib import Path
import sqlite3

import pandas as pd

from newstrade.config import AppConfig
from newstrade.db import connect_db, init_db
from newstrade.pipeline import run_news, run_scan, run_score
from newstrade.reporting import build_report_dataframe, export_report_csv
from newstrade.time_utils import utc_now_iso


class FakeIbkrClient:
    def __init__(self, snapshots: dict[str, dict[str, float]]) -> None:
        self.snapshots = snapshots

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def discover_symbols(self) -> list[str]:
        return list(self.snapshots.keys())

    def fetch_price_snapshot(self, symbol: str, intraday_lookback_days: int, intraday_bar_size: str):
        base = self.snapshots[symbol]
        return {
            "symbol": symbol,
            "last_price": base["last_price"],
            "pct_change_1d": base["pct_change_1d"],
            "pct_change_intraday": base["pct_change_intraday"],
            "price_source_ts_utc": utc_now_iso(),
        }


class FakeScorer:
    def __init__(self, fail_title: str | None = None) -> None:
        self.fail_title = fail_title

    def score_article(self, article, retries=2):
        if self.fail_title and self.fail_title in article["title"]:
            raise RuntimeError("simulated scorer failure")
        return {
            "summary": "Test summary",
            "impact_score": 20,
            "seriousness_score": 60,
            "confidence": 80,
            "impact_horizon": "short_term",
            "reason_tags": ["macro"],
            "is_material_news": True,
        }


def make_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        symbol_mode="env",
        symbols=["AAPL", "MSFT", "NVDA"],
        min_pct_change=1.0,
        max_pct_change=50.0,
        min_price=1,
        max_price=10000,
        min_market_cap=100,
        max_market_cap=10_000_000_000_000,
        db_path=str(tmp_path / "newstrade.db"),
        csv_export_dir=str(tmp_path / "exports"),
        openai_api_key="test",
    )


def test_pipeline_happy_path(tmp_path: Path) -> None:
    cfg = make_config(tmp_path)
    snapshots = {
        "AAPL": {"last_price": 190, "pct_change_1d": 3.1, "pct_change_intraday": 2.2},
        "MSFT": {"last_price": 420, "pct_change_1d": -2.5, "pct_change_intraday": -1.8},
        "NVDA": {"last_price": 870, "pct_change_1d": 4.9, "pct_change_intraday": 3.3},
    }
    fake_ibkr = FakeIbkrClient(snapshots)

    def fake_caps(symbols: list[str]) -> dict[str, float | None]:
        return {symbol: 2_000_000_000_000 for symbol in symbols}

    def fake_news(**kwargs):
        symbol = kwargs["symbol"]
        now = utc_now_iso()
        return [
            {
                "symbol": symbol,
                "url": f"https://example.com/{symbol}/1",
                "title": f"{symbol} headline",
                "source": "Yahoo Finance RSS",
                "published_ts_utc": now,
                "rss_fetched_ts_utc": now,
                "dedup_key": f"https://example.com/{symbol}/1",
            }
        ]

    scan_run_id = run_scan(
        config=cfg,
        mode="env",
        ibkr_factory=lambda h, p, c: fake_ibkr,
        market_cap_fetcher=fake_caps,
    )
    inserted_news = run_news(cfg, scan_run_id, news_fetcher=fake_news)
    scored, symbol_scores = run_score(cfg, scan_run_id, scorer=FakeScorer())

    conn = connect_db(cfg.db_path)
    init_db(conn)

    assert inserted_news == 3
    assert scored == 3
    assert symbol_scores == 3

    table_counts = {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ["scan_runs", "symbols_snapshot", "news_articles", "article_scores", "symbol_scores"]
    }
    assert table_counts["scan_runs"] == 1
    assert table_counts["symbols_snapshot"] == 3
    assert table_counts["news_articles"] == 3
    assert table_counts["article_scores"] == 3
    assert table_counts["symbol_scores"] == 3
    conn.close()


def test_no_news_creates_neutral_symbol_scores(tmp_path: Path) -> None:
    cfg = make_config(tmp_path)
    snapshots = {"AAPL": {"last_price": 100, "pct_change_1d": 3.0, "pct_change_intraday": 2.0}}
    fake_ibkr = FakeIbkrClient(snapshots)

    scan_run_id = run_scan(
        config=cfg,
        mode="env",
        ibkr_factory=lambda h, p, c: fake_ibkr,
        market_cap_fetcher=lambda symbols: {"AAPL": 1_000_000_000},
    )

    inserted = run_news(cfg, scan_run_id, news_fetcher=lambda **kwargs: [])
    assert inserted == 0

    scored, symbol_scores = run_score(cfg, scan_run_id, scorer=FakeScorer())
    assert scored == 0
    assert symbol_scores == 1

    conn = sqlite3.connect(cfg.db_path)
    row = conn.execute(
        "SELECT article_count, weighted_impact_score, weighted_seriousness_score, bullish_bearish_label "
        "FROM symbol_scores WHERE scan_run_id = ? AND symbol = 'AAPL'",
        (scan_run_id,),
    ).fetchone()
    conn.close()

    assert row[0] == 0
    assert row[1] == 0
    assert row[2] == 0
    assert row[3] == "neutral"


def test_mixed_failures_continue_scoring(tmp_path: Path) -> None:
    cfg = make_config(tmp_path)
    cfg.symbols = ["AAPL"]

    snapshots = {"AAPL": {"last_price": 100, "pct_change_1d": 3.0, "pct_change_intraday": 2.0}}
    fake_ibkr = FakeIbkrClient(snapshots)

    def fake_news(**kwargs):
        symbol = kwargs["symbol"]
        now = utc_now_iso()
        return [
            {
                "symbol": symbol,
                "url": "https://example.com/aapl/fail",
                "title": "AAPL fail article",
                "source": "Yahoo Finance RSS",
                "published_ts_utc": now,
                "rss_fetched_ts_utc": now,
                "dedup_key": "https://example.com/aapl/fail",
            },
            {
                "symbol": symbol,
                "url": "https://example.com/aapl/ok",
                "title": "AAPL ok article",
                "source": "Yahoo Finance RSS",
                "published_ts_utc": now,
                "rss_fetched_ts_utc": now,
                "dedup_key": "https://example.com/aapl/ok",
            },
        ]

    scan_run_id = run_scan(
        config=cfg,
        mode="env",
        ibkr_factory=lambda h, p, c: fake_ibkr,
        market_cap_fetcher=lambda symbols: {"AAPL": 1_000_000_000},
    )
    run_news(cfg, scan_run_id, news_fetcher=fake_news)
    run_score(cfg, scan_run_id, scorer=FakeScorer(fail_title="fail"))

    conn = sqlite3.connect(cfg.db_path)
    rows = conn.execute(
        "SELECT error_message FROM article_scores WHERE scan_run_id = ? ORDER BY article_score_id",
        (scan_run_id,),
    ).fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0][0] is not None
    assert rows[1][0] is None


def test_export_matches_report_rows(tmp_path: Path) -> None:
    cfg = make_config(tmp_path)
    snapshots = {"AAPL": {"last_price": 100, "pct_change_1d": 3.0, "pct_change_intraday": 2.0}}
    fake_ibkr = FakeIbkrClient(snapshots)

    scan_run_id = run_scan(
        config=cfg,
        mode="env",
        ibkr_factory=lambda h, p, c: fake_ibkr,
        market_cap_fetcher=lambda symbols: {"AAPL": 1_000_000_000},
    )

    def fake_news(**kwargs):
        symbol = kwargs["symbol"]
        now = utc_now_iso()
        return [
            {
                "symbol": symbol,
                "url": "https://example.com/aapl/ok",
                "title": "AAPL article",
                "source": "Yahoo Finance RSS",
                "published_ts_utc": now,
                "rss_fetched_ts_utc": now,
                "dedup_key": "https://example.com/aapl/ok",
            }
        ]

    run_news(cfg, scan_run_id, news_fetcher=fake_news)
    run_score(cfg, scan_run_id, scorer=FakeScorer())

    conn = connect_db(cfg.db_path)
    init_db(conn)
    report_df = build_report_dataframe(conn, scan_run_id)
    export_path = export_report_csv(conn, scan_run_id, cfg.csv_export_dir)
    conn.close()

    assert export_path.exists()
    exported_df = pd.read_csv(export_path)
    assert len(exported_df) == len(report_df)
