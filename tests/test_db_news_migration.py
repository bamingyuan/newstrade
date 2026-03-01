from __future__ import annotations

import sqlite3

from newstrade.db import init_db


def test_init_db_adds_news_provider_columns_for_existing_db(tmp_path) -> None:
    db_path = tmp_path / "migration.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE news_articles (
            article_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            published_ts_utc TEXT,
            rss_fetched_ts_utc TEXT NOT NULL,
            dedup_key TEXT NOT NULL
        )
        """
    )
    conn.commit()

    init_db(conn)
    columns = [row[1] for row in conn.execute("PRAGMA table_info(news_articles)").fetchall()]
    conn.close()

    assert "summary" in columns
    assert "provider" in columns
    assert "provider_article_id" in columns
