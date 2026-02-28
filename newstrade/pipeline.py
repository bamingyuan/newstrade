from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time
import json
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .aggregate import compute_symbol_aggregate
from .ai_scoring import AIScorer, AIScorerConfig, build_failed_score
from .config import AppConfig
from .db import (
    connect_db,
    create_scan_run,
    get_symbols_for_run,
    get_unscored_articles,
    init_db,
    insert_article_score,
    insert_news_articles,
    insert_symbol_snapshots,
    list_existing_article_dedup_keys,
    update_scan_run_status,
    upsert_symbol_score,
    get_scored_articles_for_symbol,
)
from .ibkr_client import create_ibkr_client
from .market_data import passes_symbol_filters
from .reporting import build_report_dataframe, report_to_console
from .time_utils import utc_now_iso
from .yahoo_news import fetch_market_caps, fetch_symbol_news


class PipelineError(RuntimeError):
    """Raised when a pipeline stage cannot complete."""


def _resolve_scan_window(config: AppConfig, window: str | None) -> str:
    return (window or config.scan_window_default).strip().lower()


def _resolve_symbol_mode(config: AppConfig, mode: str | None) -> str:
    return (mode or config.symbol_mode).strip().lower()


def _resolve_time_travel_end_datetime(config: AppConfig) -> datetime | None:
    if not config.scan_time_travel_enabled or config.scan_as_of_date is None:
        return None
    ny_tz = ZoneInfo("America/New_York")
    return datetime.combine(config.scan_as_of_date, time(hour=16, minute=0), tzinfo=ny_tz)


def _collect_symbols(config: AppConfig, symbol_mode: str, ibkr_client: Any) -> list[str]:
    symbols: set[str] = set()

    if symbol_mode in {"env", "both"}:
        symbols.update(symbol.upper() for symbol in config.symbols)

    if symbol_mode in {"ibkr", "both"}:
        discovered = ibkr_client.discover_symbols()
        symbols.update(symbol.upper() for symbol in discovered)

    return sorted(symbol for symbol in symbols if symbol)


def run_scan(
    config: AppConfig,
    window: str | None = None,
    mode: str | None = None,
    ibkr_factory: Callable[[str, int, int], Any] = create_ibkr_client,
    market_cap_fetcher: Callable[[list[str]], dict[str, float | None]] = fetch_market_caps,
) -> int:
    scan_window = _resolve_scan_window(config, window)
    symbol_mode = _resolve_symbol_mode(config, mode)
    end_datetime = _resolve_time_travel_end_datetime(config)

    conn = connect_db(config.db_path)
    init_db(conn)

    scan_run_id = create_scan_run(
        conn=conn,
        run_ts_utc=utc_now_iso(),
        scan_window=scan_window,
        symbol_mode=symbol_mode,
        min_pct_change=config.min_pct_change,
        max_pct_change=config.max_pct_change,
        status="running",
        notes="",
    )

    ibkr_client = ibkr_factory(config.ibkr_host, config.ibkr_port, config.ibkr_client_id)

    try:
        if config.scan_time_travel_enabled and symbol_mode != "env":
            raise ValueError("SCAN_TIME_TRAVEL=1 requires symbol mode 'env'. Use --mode env or SYMBOL_MODE=env.")

        ibkr_client.connect()
        symbols = _collect_symbols(config, symbol_mode, ibkr_client)

        if not symbols:
            update_scan_run_status(conn, scan_run_id, "completed", "No symbols resolved")
            return scan_run_id

        market_cap_unavailable_globally = False
        market_cap_disabled_by_config = not config.market_cap_enabled
        if config.market_cap_enabled:
            try:
                market_caps = market_cap_fetcher(symbols)
            except Exception:  # noqa: BLE001
                market_caps = {symbol: None for symbol in symbols}
        else:
            market_caps = {symbol: None for symbol in symbols}

        if market_cap_disabled_by_config:
            filter_config = replace(config, min_market_cap=None, max_market_cap=None)
        elif config.market_cap_filter_active and market_caps and all(value is None for value in market_caps.values()):
            market_cap_unavailable_globally = True
            filter_config = replace(config, min_market_cap=None, max_market_cap=None)
        else:
            filter_config = config

        snapshot_rows: list[dict[str, Any]] = []
        passed_count = 0
        failed_details: list[str] = []
        time_travel_warnings: list[str] = []

        for symbol in symbols:
            try:
                snapshot = ibkr_client.fetch_price_snapshot(
                    symbol=symbol,
                    intraday_lookback_days=config.intraday_lookback_days,
                    intraday_bar_size=config.intraday_bar_size,
                    end_datetime=end_datetime,
                )
            except Exception as exc:  # noqa: BLE001
                failed_details.append(f"{symbol}: {exc}")
                snapshot_rows.append(
                    {
                        "scan_run_id": scan_run_id,
                        "symbol": symbol,
                        "last_price": None,
                        "pct_change_1d": None,
                        "pct_change_intraday": None,
                        "market_cap": market_caps.get(symbol),
                        "price_source_ts_utc": utc_now_iso(),
                        "passed_filters": False,
                    }
                )
                continue

            if config.scan_time_travel_enabled and config.scan_as_of_date is not None:
                latest_daily_bar_date = str(snapshot.get("latest_daily_bar_date") or "").strip()
                requested_date = config.scan_as_of_date.isoformat()
                if latest_daily_bar_date and latest_daily_bar_date < requested_date:
                    time_travel_warnings.append(
                        f"{symbol}: requested {requested_date}, latest available {latest_daily_bar_date}"
                    )

            snapshot["market_cap"] = market_caps.get(symbol)
            passed, reason = passes_symbol_filters(snapshot, filter_config, scan_window)
            if passed:
                passed_count += 1
            else:
                failed_details.append(reason)

            snapshot_rows.append(
                {
                    "scan_run_id": scan_run_id,
                    "symbol": symbol,
                    "last_price": snapshot.get("last_price"),
                    "pct_change_1d": snapshot.get("pct_change_1d"),
                    "pct_change_intraday": snapshot.get("pct_change_intraday"),
                    "market_cap": snapshot.get("market_cap"),
                    "price_source_ts_utc": snapshot.get("price_source_ts_utc", utc_now_iso()),
                    "passed_filters": passed,
                }
            )

        insert_symbol_snapshots(conn, snapshot_rows)

        notes = f"Processed {len(symbols)} symbols, passed {passed_count}."
        if config.scan_time_travel_enabled and config.scan_as_of_date is not None:
            notes += f" Time-travel enabled for {config.scan_as_of_date.isoformat()} at US close."
        if time_travel_warnings:
            sample = "; ".join(time_travel_warnings[:3])
            remaining = len(time_travel_warnings) - 3
            if remaining > 0:
                sample += f"; +{remaining} more"
            notes += f" Some symbols returned prior sessions: {sample}."
        if market_cap_disabled_by_config:
            notes += " Market-cap fetching disabled via MARKET_CAP=0."
        if market_cap_unavailable_globally:
            notes += " Market-cap filtering temporarily disabled because Yahoo market-cap data was unavailable."
        if failed_details:
            notes += " Some symbols failed filters or data fetch."
        update_scan_run_status(conn, scan_run_id, "completed", notes)
        return scan_run_id
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, OSError):
            message = (
                "Scan failed. Ensure IB Gateway/TWS is running and reachable at "
                f"{config.ibkr_host}:{config.ibkr_port} with client id {config.ibkr_client_id}. Error: {exc}"
            )
        else:
            message = f"Scan failed due to an upstream data or pipeline error: {exc}"
        update_scan_run_status(conn, scan_run_id, "failed", message)
        raise PipelineError(message) from exc
    finally:
        try:
            ibkr_client.disconnect()
        except Exception:  # noqa: BLE001
            pass
        conn.close()


def run_news(
    config: AppConfig,
    scan_run_id: int,
    news_fetcher: Callable[..., list[dict[str, str]]] = fetch_symbol_news,
) -> int:
    conn = connect_db(config.db_path)
    init_db(conn)
    symbols = get_symbols_for_run(conn, scan_run_id, passed_only=True)

    if not symbols:
        conn.close()
        return 0

    article_rows: list[dict[str, Any]] = []

    for symbol_row in symbols:
        symbol = str(symbol_row["symbol"])
        existing_keys = list_existing_article_dedup_keys(conn, scan_run_id, symbol)

        try:
            fetched = news_fetcher(
                symbol=symbol,
                lookback_hours=config.news_lookback_hours,
                max_articles=config.max_news_articles_per_symbol,
                region=config.yahoo_rss_region,
                lang=config.yahoo_rss_lang,
            )
        except Exception:  # noqa: BLE001
            continue

        for item in fetched:
            dedup_key = item["dedup_key"]
            if dedup_key in existing_keys:
                continue
            existing_keys.add(dedup_key)
            article_rows.append(
                {
                    "scan_run_id": scan_run_id,
                    "symbol": symbol,
                    "url": item["url"],
                    "title": item["title"],
                    "source": item["source"],
                    "published_ts_utc": item.get("published_ts_utc") or "",
                    "rss_fetched_ts_utc": item["rss_fetched_ts_utc"],
                    "dedup_key": dedup_key,
                }
            )

    inserted = insert_news_articles(conn, article_rows)
    conn.close()
    return inserted


def run_score(
    config: AppConfig,
    scan_run_id: int,
    scorer: Any | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[int, int]:
    conn = connect_db(config.db_path)
    init_db(conn)

    if scorer is None:
        scorer = AIScorer(
            AIScorerConfig(
                api_key=config.openai_api_key,
                model=config.openai_model,
                timeout_seconds=config.openai_timeout_seconds,
                temperature=config.openai_temperature,
            )
        )

    articles = get_unscored_articles(conn, scan_run_id)

    scored_count = 0
    total_articles = len(articles)
    for article in articles:
        article_payload = {
            "symbol": article["symbol"],
            "title": article["title"],
            "source": article["source"],
            "published_ts_utc": article["published_ts_utc"],
            "url": article["url"],
        }

        try:
            scored = scorer.score_article(article_payload, retries=2)
            error_message = None
        except Exception as exc:  # noqa: BLE001
            scored = build_failed_score(str(exc))
            error_message = str(exc)

        insert_article_score(
            conn,
            {
                "scan_run_id": scan_run_id,
                "symbol": article["symbol"],
                "article_id": article["article_id"],
                "openai_model": config.openai_model,
                "summary": scored["summary"],
                "impact_score": scored["impact_score"],
                "seriousness_score": scored["seriousness_score"],
                "confidence": scored["confidence"],
                "impact_horizon": scored["impact_horizon"],
                "reason_tags_json": json.dumps(scored["reason_tags"]),
                "is_material_news": scored["is_material_news"],
                "scored_ts_utc": scored.get("scored_ts_utc", utc_now_iso()),
                "error_message": error_message,
            },
        )
        scored_count += 1
        if progress_callback is not None:
            progress_callback(
                {
                    "current": scored_count,
                    "total": total_articles,
                    "article_id": article["article_id"],
                    "symbol": article["symbol"],
                    "url": article["url"],
                    "title": article["title"],
                    "status": "error" if error_message else "ok",
                    "error_message": error_message,
                }
            )

    symbols = get_symbols_for_run(conn, scan_run_id, passed_only=True)
    for symbol_row in symbols:
        symbol = str(symbol_row["symbol"])
        scored_rows = [dict(row) for row in get_scored_articles_for_symbol(conn, scan_run_id, symbol)]
        aggregate = compute_symbol_aggregate(symbol=symbol, scan_run_id=scan_run_id, rows=scored_rows)
        upsert_symbol_score(conn, aggregate)

    symbol_score_count = len(symbols)
    conn.close()
    return scored_count, symbol_score_count


def run_report(config: AppConfig, scan_run_id: int, top: int = 30) -> str:
    conn = connect_db(config.db_path)
    init_db(conn)
    df = build_report_dataframe(conn, scan_run_id)
    conn.close()
    return report_to_console(df, top=top)


def run_all(config: AppConfig, window: str | None = None, mode: str | None = None, top: int = 30) -> tuple[int, str]:
    scan_run_id = run_scan(config=config, window=window, mode=mode)
    run_news(config=config, scan_run_id=scan_run_id)
    run_score(config=config, scan_run_id=scan_run_id)
    report_text = run_report(config=config, scan_run_id=scan_run_id, top=top)
    return scan_run_id, report_text
