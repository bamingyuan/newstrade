from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable


def connect_db(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing_columns = {
        str(row["name"]) if isinstance(row, sqlite3.Row) else str(row[1])
        for row in rows
    }
    if column in existing_columns:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS scan_runs (
            scan_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts_utc TEXT NOT NULL,
            scan_window TEXT NOT NULL,
            symbol_mode TEXT NOT NULL,
            min_pct_change REAL NOT NULL,
            max_pct_change REAL NOT NULL,
            status TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS symbols_snapshot (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            last_price REAL,
            pct_change_1d REAL,
            pct_change_intraday REAL,
            volume REAL,
            market_cap REAL,
            price_source_ts_utc TEXT NOT NULL,
            price_as_of_ts_utc TEXT NOT NULL,
            passed_filters INTEGER NOT NULL,
            FOREIGN KEY(scan_run_id) REFERENCES scan_runs(scan_run_id)
        );

        CREATE TABLE IF NOT EXISTS news_articles (
            article_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            published_ts_utc TEXT,
            rss_fetched_ts_utc TEXT NOT NULL,
            dedup_key TEXT NOT NULL,
            summary TEXT,
            provider TEXT NOT NULL DEFAULT 'yahoo_rss',
            provider_article_id TEXT,
            FOREIGN KEY(scan_run_id) REFERENCES scan_runs(scan_run_id),
            UNIQUE(scan_run_id, symbol, dedup_key)
        );

        CREATE TABLE IF NOT EXISTS article_scores (
            article_score_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            article_id INTEGER NOT NULL,
            openai_model TEXT NOT NULL,
            summary TEXT NOT NULL,
            impact_score INTEGER NOT NULL,
            impact_direction TEXT NOT NULL CHECK (impact_direction IN ('bearish', 'neutral', 'bullish')),
            seriousness_score INTEGER NOT NULL,
            confidence INTEGER NOT NULL,
            impact_horizon TEXT NOT NULL,
            reason_tags_json TEXT NOT NULL,
            is_material_news INTEGER NOT NULL,
            scored_ts_utc TEXT NOT NULL,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            total_tokens INTEGER,
            reasoning_tokens INTEGER,
            error_message TEXT,
            FOREIGN KEY(scan_run_id) REFERENCES scan_runs(scan_run_id),
            FOREIGN KEY(article_id) REFERENCES news_articles(article_id),
            UNIQUE(article_id)
        );

        CREATE TABLE IF NOT EXISTS symbol_scores (
            symbol_score_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            article_count INTEGER NOT NULL,
            weighted_impact_score REAL NOT NULL,
            weighted_seriousness_score REAL NOT NULL,
            bullish_bearish_label TEXT NOT NULL,
            score_ts_utc TEXT NOT NULL,
            FOREIGN KEY(scan_run_id) REFERENCES scan_runs(scan_run_id),
            UNIQUE(scan_run_id, symbol)
        );

        CREATE TABLE IF NOT EXISTS exports_log (
            export_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            file_path TEXT NOT NULL,
            created_ts_utc TEXT NOT NULL,
            FOREIGN KEY(scan_run_id) REFERENCES scan_runs(scan_run_id)
        );
        """
    )

    _ensure_column(conn, "article_scores", "prompt_tokens", "INTEGER")
    _ensure_column(conn, "article_scores", "completion_tokens", "INTEGER")
    _ensure_column(conn, "article_scores", "total_tokens", "INTEGER")
    _ensure_column(conn, "article_scores", "reasoning_tokens", "INTEGER")
    _ensure_column(conn, "article_scores", "impact_direction", "TEXT NOT NULL DEFAULT 'neutral'")
    _ensure_column(conn, "symbols_snapshot", "price_as_of_ts_utc", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "symbols_snapshot", "volume", "REAL")
    _ensure_column(conn, "news_articles", "summary", "TEXT")
    _ensure_column(conn, "news_articles", "provider", "TEXT NOT NULL DEFAULT 'yahoo_rss'")
    _ensure_column(conn, "news_articles", "provider_article_id", "TEXT")

    conn.commit()


def create_scan_run(
    conn: sqlite3.Connection,
    run_ts_utc: str,
    scan_window: str,
    symbol_mode: str,
    min_pct_change: float,
    max_pct_change: float,
    status: str,
    notes: str = "",
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO scan_runs (
            run_ts_utc, scan_window, symbol_mode, min_pct_change, max_pct_change, status, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_ts_utc, scan_window, symbol_mode, min_pct_change, max_pct_change, status, notes),
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_scan_run_status(conn: sqlite3.Connection, scan_run_id: int, status: str, notes: str = "") -> None:
    conn.execute(
        "UPDATE scan_runs SET status = ?, notes = ? WHERE scan_run_id = ?",
        (status, notes, scan_run_id),
    )
    conn.commit()


def insert_symbol_snapshots(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> None:
    conn.executemany(
        """
        INSERT INTO symbols_snapshot (
            scan_run_id, symbol, last_price, pct_change_1d, pct_change_intraday, volume, market_cap,
            price_source_ts_utc, price_as_of_ts_utc, passed_filters
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["scan_run_id"],
                row["symbol"],
                row.get("last_price"),
                row.get("pct_change_1d"),
                row.get("pct_change_intraday"),
                row.get("volume"),
                row.get("market_cap"),
                row["price_source_ts_utc"],
                row["price_as_of_ts_utc"],
                int(bool(row.get("passed_filters", False))),
            )
            for row in rows
        ],
    )
    conn.commit()


def get_symbols_for_run(conn: sqlite3.Connection, scan_run_id: int, passed_only: bool = True) -> list[sqlite3.Row]:
    sql = "SELECT * FROM symbols_snapshot WHERE scan_run_id = ?"
    params: list[Any] = [scan_run_id]
    if passed_only:
        sql += " AND passed_filters = 1"
    sql += " ORDER BY symbol"
    return list(conn.execute(sql, params).fetchall())


def list_existing_article_dedup_keys(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> set[str]:
    rows = conn.execute(
        "SELECT dedup_key FROM news_articles WHERE scan_run_id = ? AND symbol = ?",
        (scan_run_id, symbol),
    ).fetchall()
    return {str(row["dedup_key"]) for row in rows}


def insert_news_articles(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> int:
    inserted = 0
    for row in rows:
        try:
            conn.execute(
                """
                INSERT INTO news_articles (
                    scan_run_id, symbol, url, title, source, published_ts_utc, rss_fetched_ts_utc, dedup_key,
                    summary, provider, provider_article_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["scan_run_id"],
                    row["symbol"],
                    row["url"],
                    row["title"],
                    row["source"],
                    row.get("published_ts_utc"),
                    row["rss_fetched_ts_utc"],
                    row["dedup_key"],
                    row.get("summary"),
                    row.get("provider", "yahoo_rss"),
                    row.get("provider_article_id"),
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            continue
    conn.commit()
    return inserted


def get_unscored_articles(conn: sqlite3.Connection, scan_run_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT na.*
            FROM news_articles na
            LEFT JOIN article_scores a ON a.article_id = na.article_id
            WHERE na.scan_run_id = ?
              AND (a.article_id IS NULL OR a.error_message IS NOT NULL)
            ORDER BY na.symbol, na.published_ts_utc DESC
            """,
            (scan_run_id,),
        ).fetchall()
    )


def insert_article_score(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO article_scores (
            scan_run_id, symbol, article_id, openai_model, summary, impact_score, impact_direction, seriousness_score,
            confidence, impact_horizon, reason_tags_json, is_material_news, scored_ts_utc,
            prompt_tokens, completion_tokens, total_tokens, reasoning_tokens, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["scan_run_id"],
            row["symbol"],
            row["article_id"],
            row["openai_model"],
            row["summary"],
            row["impact_score"],
            row["impact_direction"],
            row["seriousness_score"],
            row["confidence"],
            row["impact_horizon"],
            row["reason_tags_json"],
            int(bool(row["is_material_news"])),
            row["scored_ts_utc"],
            row.get("prompt_tokens"),
            row.get("completion_tokens"),
            row.get("total_tokens"),
            row.get("reasoning_tokens"),
            row.get("error_message"),
        ),
    )
    conn.commit()


def get_scored_articles_for_symbol(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT a.*, n.published_ts_utc
            FROM article_scores a
            JOIN news_articles n ON n.article_id = a.article_id
            WHERE a.scan_run_id = ? AND a.symbol = ?
            ORDER BY n.published_ts_utc DESC
            """,
            (scan_run_id, symbol),
        ).fetchall()
    )


def upsert_symbol_score(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO symbol_scores (
            scan_run_id, symbol, article_count, weighted_impact_score,
            weighted_seriousness_score, bullish_bearish_label, score_ts_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scan_run_id, symbol)
        DO UPDATE SET
            article_count = excluded.article_count,
            weighted_impact_score = excluded.weighted_impact_score,
            weighted_seriousness_score = excluded.weighted_seriousness_score,
            bullish_bearish_label = excluded.bullish_bearish_label,
            score_ts_utc = excluded.score_ts_utc
        """,
        (
            row["scan_run_id"],
            row["symbol"],
            row["article_count"],
            row["weighted_impact_score"],
            row["weighted_seriousness_score"],
            row["bullish_bearish_label"],
            row["score_ts_utc"],
        ),
    )
    conn.commit()


def get_scan_run(conn: sqlite3.Connection, scan_run_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM scan_runs WHERE scan_run_id = ?", (scan_run_id,)).fetchone()


def get_latest_scan_run_ids(conn: sqlite3.Connection, limit: int = 30) -> list[int]:
    rows = conn.execute(
        "SELECT scan_run_id FROM scan_runs ORDER BY scan_run_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [int(row["scan_run_id"]) for row in rows]


def get_symbol_scores_report(conn: sqlite3.Connection, scan_run_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT
                ss.symbol,
                ss.last_price,
                ss.pct_change_1d,
                ss.pct_change_intraday,
                ss.volume,
                ss.market_cap,
                ss.price_as_of_ts_utc,
                s.article_count,
                s.weighted_impact_score,
                s.weighted_seriousness_score,
                s.bullish_bearish_label,
                s.score_ts_utc
            FROM symbol_scores s
            JOIN symbols_snapshot ss
              ON ss.scan_run_id = s.scan_run_id AND ss.symbol = s.symbol
            WHERE s.scan_run_id = ?
            ORDER BY s.weighted_seriousness_score DESC, ABS(s.weighted_impact_score) DESC
            """,
            (scan_run_id,),
        ).fetchall()
    )


def get_reason_tags_for_symbol(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> list[str]:
    rows = conn.execute(
        "SELECT reason_tags_json FROM article_scores WHERE scan_run_id = ? AND symbol = ?",
        (scan_run_id, symbol),
    ).fetchall()
    return [str(row["reason_tags_json"]) for row in rows]


def log_export(conn: sqlite3.Connection, scan_run_id: int, symbol: str, file_path: str, created_ts_utc: str) -> None:
    conn.execute(
        "INSERT INTO exports_log (scan_run_id, symbol, file_path, created_ts_utc) VALUES (?, ?, ?, ?)",
        (scan_run_id, symbol, file_path, created_ts_utc),
    )
    conn.commit()
