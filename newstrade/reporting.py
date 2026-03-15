from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pandas as pd

from .aggregate import top_reason_tags
from .db import get_scored_articles_for_symbol, get_symbol_scores_report, log_export


def build_report_dataframe(conn: sqlite3.Connection, scan_run_id: int) -> pd.DataFrame:
    rows = get_symbol_scores_report(conn, scan_run_id)
    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "last_price",
                "pct_change_1d",
                "pct_change_intraday",
                "volume",
                "market_cap",
                "price_as_of_ts_utc",
                "article_count",
                "weighted_impact_score",
                "weighted_seriousness_score",
                "bullish_bearish_label",
                "top_reason_tags",
                "avg_relevance_score",
                "score_ts_utc",
            ]
        )

    data: list[dict[str, object]] = []
    for row in rows:
        symbol = str(row["symbol"])
        scored_rows = [dict(item) for item in get_scored_articles_for_symbol(conn, scan_run_id, symbol)]
        data.append(
            {
                "symbol": symbol,
                "last_price": row["last_price"],
                "pct_change_1d": row["pct_change_1d"],
                "pct_change_intraday": row["pct_change_intraday"],
                "volume": row["volume"],
                "market_cap": row["market_cap"],
                "price_as_of_ts_utc": row["price_as_of_ts_utc"],
                "article_count": row["article_count"],
                "weighted_impact_score": row["weighted_impact_score"],
                "weighted_seriousness_score": row["weighted_seriousness_score"],
                "bullish_bearish_label": row["bullish_bearish_label"],
                "top_reason_tags": ", ".join(top_reason_tags(scored_rows)),
                "avg_relevance_score": _average_relevance(scored_rows),
                "score_ts_utc": row["score_ts_utc"],
            }
        )

    return pd.DataFrame(data)


def report_to_console(df: pd.DataFrame, top: int = 30) -> str:
    if df.empty:
        return "No scored symbols found for this run. Either scoring has not run yet or no articles were collected."

    ordered = df.sort_values(
        by=["weighted_seriousness_score", "weighted_impact_score"],
        ascending=[False, False],
        key=lambda s: s.abs() if s.name == "weighted_impact_score" else s,
    ).head(top)

    view = ordered[
        [
            "symbol",
            "pct_change_1d",
            "pct_change_intraday",
            "volume",
            "weighted_impact_score",
            "weighted_seriousness_score",
            "avg_relevance_score",
            "article_count",
            "bullish_bearish_label",
            "top_reason_tags",
        ]
    ].copy()

    return view.to_string(index=False)


def _average_relevance(rows: list[dict[str, object]]) -> float:
    values: list[float] = []
    for row in rows:
        raw = row.get("relevance_score")
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        values.append(value)
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def export_report_csv(
    conn: sqlite3.Connection,
    scan_run_id: int,
    export_dir: str,
    file_name: str | None = None,
) -> Path:
    df = build_report_dataframe(conn, scan_run_id)
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    if file_name is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_name = f"newstrade_report_run_{scan_run_id}_{timestamp}.csv"

    full_path = export_path / file_name
    df.to_csv(full_path, index=False)

    created_ts = datetime.now(timezone.utc).isoformat()
    if df.empty:
        log_export(conn, scan_run_id, "ALL", str(full_path), created_ts)
    else:
        for symbol in df["symbol"].tolist():
            log_export(conn, scan_run_id, str(symbol), str(full_path), created_ts)

    return full_path
