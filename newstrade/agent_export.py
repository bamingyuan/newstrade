from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from .aggregate import top_reason_tags
from .db import (
    connect_db,
    get_latest_scan_run_ids,
    get_scan_run,
    get_scored_articles_for_symbol,
    get_symbol_article_details,
    init_db,
)
from .reporting import build_report_dataframe


AGENT_EXPORT_FILE_NAME = "agent_latest.json"
AGENT_PAYLOAD_VERSION = 1


def build_latest_agent_payload(db_path: str) -> dict[str, Any]:
    conn = connect_db(db_path)
    init_db(conn)
    try:
        latest_ids = get_latest_scan_run_ids(conn, limit=1)
        if not latest_ids:
            raise ValueError("No scan runs found. Run `newstrade scan` first.")
        return build_agent_payload(conn, latest_ids[0])
    finally:
        conn.close()


def build_agent_payload(conn: sqlite3.Connection, scan_run_id: int) -> dict[str, Any]:
    run_row = get_scan_run(conn, scan_run_id)
    if run_row is None:
        raise ValueError(f"Scan run not found: {scan_run_id}")

    report_df = build_report_dataframe(conn, scan_run_id)
    symbols: list[dict[str, Any]] = []

    for _, row in report_df.iterrows():
        symbol = str(row["symbol"])
        scored_rows = [dict(item) for item in get_scored_articles_for_symbol(conn, scan_run_id, symbol)]
        symbols.append(
            {
                "symbol": symbol,
                "trade_date": row["trade_date"],
                "previous_trade_date": row["previous_trade_date"],
                "close_price": _json_safe_value(row["close_price"]),
                "previous_close_price": _json_safe_value(row["previous_close_price"]),
                "pct_change": _json_safe_value(row["pct_change"]),
                "volume": _json_safe_value(row["volume"]),
                "vwap": _json_safe_value(row["vwap"]),
                "transaction_count": _json_safe_value(row["transaction_count"]),
                "price_as_of_ts_utc": row["price_as_of_ts_utc"],
                "rank_abs_pct_change": _json_safe_value(row["rank_abs_pct_change"]),
                "article_count": _json_safe_value(row["article_count"]),
                "weighted_impact_score": _json_safe_value(row["weighted_impact_score"]),
                "weighted_seriousness_score": _json_safe_value(row["weighted_seriousness_score"]),
                "bullish_bearish_label": row["bullish_bearish_label"],
                "avg_relevance_score": _json_safe_value(row["avg_relevance_score"]),
                "score_ts_utc": row["score_ts_utc"],
                "top_reason_tags": top_reason_tags(scored_rows),
                "articles": [
                    _serialize_article_row(dict(article_row))
                    for article_row in get_symbol_article_details(conn, scan_run_id, symbol)
                ],
            }
        )

    return {
        "schema_version": AGENT_PAYLOAD_VERSION,
        "generated_ts_utc": datetime.now(timezone.utc).isoformat(),
        "run": {
            "scan_run_id": int(run_row["scan_run_id"]),
            "run_ts_utc": run_row["run_ts_utc"],
            "status": run_row["status"],
            "notes": run_row["notes"],
            "trade_date": run_row["trade_date"],
            "previous_trade_date": run_row["previous_trade_date"],
            "total_candidates": int(run_row["total_candidates"]),
            "passed_candidates": int(run_row["passed_candidates"]),
            "selected_candidates": int(run_row["selected_candidates"]),
            "symbol_count": len(symbols),
        },
        "symbols": symbols,
    }


def export_latest_agent_payload(db_path: str, output_path: str | Path) -> Path:
    payload = build_latest_agent_payload(db_path)
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return destination


def default_agent_export_path(export_dir: str) -> Path:
    return Path(export_dir) / AGENT_EXPORT_FILE_NAME


def _serialize_article_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "article_id": int(row["article_id"]),
        "symbol": row["symbol"],
        "title": row["title"],
        "source": row["source"],
        "url": row["url"],
        "published_ts_utc": row["published_ts_utc"],
        "summary": row["summary"],
        "provider": row["provider"],
        "provider_article_id": row["provider_article_id"],
        "openai_model": row["openai_model"],
        "impact_score": _json_safe_value(row["impact_score"]),
        "impact_direction": row["impact_direction"],
        "seriousness_score": _json_safe_value(row["seriousness_score"]),
        "confidence": _json_safe_value(row["confidence"]),
        "impact_horizon": row["impact_horizon"],
        "is_material_news": bool(row["is_material_news"]),
        "main_symbol": row["main_symbol"],
        "mentioned_symbols": _parse_json_list(row["mentioned_symbols_json"]),
        "reason_tags": _parse_json_list(row["reason_tags_json"]),
        "relevance_score": _json_safe_value(row["relevance_score"]),
        "scored_ts_utc": row["scored_ts_utc"],
        "error_message": row["error_message"],
    }


def _parse_json_list(raw: object) -> list[str]:
    if raw is None:
        return []
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _json_safe_value(value: object) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return value
    return value
