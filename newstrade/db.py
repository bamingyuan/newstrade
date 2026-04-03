from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable


DB_SCHEMA_VERSION = 2


def connect_db(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    if _needs_schema_reset(conn):
        _drop_all_tables(conn)

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            scan_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts_utc TEXT NOT NULL,
            trade_date TEXT,
            previous_trade_date TEXT,
            status TEXT NOT NULL,
            notes TEXT,
            total_candidates INTEGER NOT NULL DEFAULT 0,
            passed_candidates INTEGER NOT NULL DEFAULT 0,
            selected_candidates INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS symbols_snapshot (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            trade_date TEXT,
            previous_trade_date TEXT,
            close_price REAL,
            previous_close_price REAL,
            pct_change REAL,
            volume REAL,
            vwap REAL,
            transaction_count INTEGER,
            price_as_of_ts_utc TEXT NOT NULL,
            passed_filters INTEGER NOT NULL,
            rank_abs_pct_change INTEGER,
            selected_for_news INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(scan_run_id) REFERENCES scan_runs(scan_run_id),
            UNIQUE(scan_run_id, symbol)
        );

        CREATE TABLE IF NOT EXISTS news_articles (
            article_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            published_ts_utc TEXT,
            fetched_ts_utc TEXT NOT NULL,
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
            main_symbol TEXT,
            mentioned_symbols_json TEXT NOT NULL DEFAULT '[]',
            relevance_score INTEGER NOT NULL DEFAULT 0,
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
    conn.execute(f"PRAGMA user_version = {DB_SCHEMA_VERSION}")
    conn.commit()


def create_scan_run(
    conn: sqlite3.Connection,
    run_ts_utc: str,
    status: str,
    notes: str = "",
    trade_date: str | None = None,
    previous_trade_date: str | None = None,
    total_candidates: int = 0,
    passed_candidates: int = 0,
    selected_candidates: int = 0,
    **_: Any,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO scan_runs (
            run_ts_utc, trade_date, previous_trade_date, status, notes,
            total_candidates, passed_candidates, selected_candidates
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_ts_utc,
            trade_date,
            previous_trade_date,
            status,
            notes,
            total_candidates,
            passed_candidates,
            selected_candidates,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_scan_run_status(
    conn: sqlite3.Connection,
    scan_run_id: int,
    status: str,
    notes: str = "",
    trade_date: str | None = None,
    previous_trade_date: str | None = None,
    total_candidates: int | None = None,
    passed_candidates: int | None = None,
    selected_candidates: int | None = None,
) -> None:
    existing = get_scan_run(conn, scan_run_id)
    if existing is None:
        return
    conn.execute(
        """
        UPDATE scan_runs
        SET status = ?,
            notes = ?,
            trade_date = ?,
            previous_trade_date = ?,
            total_candidates = ?,
            passed_candidates = ?,
            selected_candidates = ?
        WHERE scan_run_id = ?
        """,
        (
            status,
            notes,
            trade_date if trade_date is not None else existing["trade_date"],
            previous_trade_date if previous_trade_date is not None else existing["previous_trade_date"],
            total_candidates if total_candidates is not None else existing["total_candidates"],
            passed_candidates if passed_candidates is not None else existing["passed_candidates"],
            selected_candidates if selected_candidates is not None else existing["selected_candidates"],
            scan_run_id,
        ),
    )
    conn.commit()


def insert_symbol_snapshots(conn: sqlite3.Connection, rows: Iterable[dict[str, Any]]) -> None:
    conn.executemany(
        """
        INSERT INTO symbols_snapshot (
            scan_run_id, symbol, trade_date, previous_trade_date, close_price, previous_close_price,
            pct_change, volume, vwap, transaction_count, price_as_of_ts_utc, passed_filters,
            rank_abs_pct_change, selected_for_news
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scan_run_id, symbol)
        DO UPDATE SET
            trade_date = excluded.trade_date,
            previous_trade_date = excluded.previous_trade_date,
            close_price = excluded.close_price,
            previous_close_price = excluded.previous_close_price,
            pct_change = excluded.pct_change,
            volume = excluded.volume,
            vwap = excluded.vwap,
            transaction_count = excluded.transaction_count,
            price_as_of_ts_utc = excluded.price_as_of_ts_utc,
            passed_filters = excluded.passed_filters,
            rank_abs_pct_change = excluded.rank_abs_pct_change,
            selected_for_news = excluded.selected_for_news
        """,
        [
            (
                row["scan_run_id"],
                row["symbol"],
                row.get("trade_date"),
                row.get("previous_trade_date"),
                _pick_value(row, "close_price", "last_price"),
                row.get("previous_close_price"),
                _pick_value(row, "pct_change", "pct_change_1d"),
                row.get("volume"),
                row.get("vwap"),
                row.get("transaction_count"),
                row.get("price_as_of_ts_utc", ""),
                int(bool(row.get("passed_filters", False))),
                row.get("rank_abs_pct_change"),
                int(bool(row.get("selected_for_news", row.get("passed_filters", False)))),
            )
            for row in rows
        ],
    )
    conn.commit()


def get_symbols_for_run(
    conn: sqlite3.Connection,
    scan_run_id: int,
    selected_only: bool = True,
    passed_only: bool | None = None,
) -> list[sqlite3.Row]:
    if passed_only is not None:
        selected_only = passed_only

    sql = "SELECT * FROM symbols_snapshot WHERE scan_run_id = ?"
    params: list[Any] = [scan_run_id]
    if selected_only:
        sql += " AND selected_for_news = 1"
    sql += " ORDER BY COALESCE(rank_abs_pct_change, 999999), symbol"
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
        fetched_ts_utc = row.get("fetched_ts_utc", row.get("rss_fetched_ts_utc"))
        try:
            conn.execute(
                """
                INSERT INTO news_articles (
                    scan_run_id, symbol, url, title, source, published_ts_utc, fetched_ts_utc, dedup_key,
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
                    fetched_ts_utc,
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
            confidence, impact_horizon, reason_tags_json, is_material_news, main_symbol, mentioned_symbols_json,
            relevance_score, scored_ts_utc, prompt_tokens, completion_tokens, total_tokens, reasoning_tokens,
            error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            row.get("main_symbol"),
            row.get("mentioned_symbols_json", "[]"),
            row.get("relevance_score", 0),
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


def get_symbol_article_details(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT
                n.article_id,
                n.symbol,
                n.title,
                n.source,
                n.url,
                n.published_ts_utc,
                COALESCE(a.summary, n.summary) AS summary,
                n.provider,
                n.provider_article_id,
                a.openai_model,
                a.impact_score,
                a.impact_direction,
                a.seriousness_score,
                a.confidence,
                a.impact_horizon,
                a.is_material_news,
                a.main_symbol,
                a.mentioned_symbols_json,
                a.reason_tags_json,
                a.relevance_score,
                a.scored_ts_utc,
                a.error_message
            FROM news_articles n
            LEFT JOIN article_scores a
              ON a.article_id = n.article_id
            WHERE n.scan_run_id = ? AND n.symbol = ?
            ORDER BY n.published_ts_utc DESC, n.article_id DESC
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


def delete_symbol_score(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> None:
    conn.execute(
        "DELETE FROM symbol_scores WHERE scan_run_id = ? AND symbol = ?",
        (scan_run_id, symbol),
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
                ss.trade_date,
                ss.previous_trade_date,
                ss.close_price,
                ss.previous_close_price,
                ss.pct_change,
                ss.volume,
                ss.vwap,
                ss.transaction_count,
                ss.price_as_of_ts_utc,
                ss.rank_abs_pct_change,
                s.article_count,
                s.weighted_impact_score,
                s.weighted_seriousness_score,
                s.bullish_bearish_label,
                s.score_ts_utc
            FROM symbol_scores s
            JOIN symbols_snapshot ss
              ON ss.scan_run_id = s.scan_run_id AND ss.symbol = s.symbol
            WHERE s.scan_run_id = ?
            ORDER BY ss.rank_abs_pct_change ASC, s.weighted_seriousness_score DESC, ABS(s.weighted_impact_score) DESC
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


def _needs_schema_reset(conn: sqlite3.Connection) -> bool:
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    table_names = [
        str(row["name"])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    return bool(table_names) and version != DB_SCHEMA_VERSION


def _drop_all_tables(conn: sqlite3.Connection) -> None:
    table_names = [
        str(row["name"])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    ]
    for table_name in table_names:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute("PRAGMA user_version = 0")
    conn.commit()


def _pick_value(row: dict[str, Any], preferred_key: str, fallback_key: str) -> Any:
    if preferred_key in row:
        return row.get(preferred_key)
    return row.get(fallback_key)
