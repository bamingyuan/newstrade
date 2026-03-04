from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timezone
import json
import inspect
import logging
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
from .ibkr_client import IbkrScannerFilters, create_ibkr_client
from .massive_news import MassiveRateLimiter, fetch_symbol_news_massive
from .market_data import passes_symbol_filters
from .reporting import build_report_dataframe, report_to_console
from .time_utils import parse_iso_utc, utc_now_iso
from .yahoo_news import fetch_symbol_news


logger = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Raised when a pipeline stage cannot complete."""


def _normalize_impact_from_direction(scored: dict[str, Any]) -> dict[str, Any]:
    direction = str(scored.get("impact_direction", "neutral")).strip().lower()
    raw_score = int(scored.get("impact_score", 0))
    magnitude = min(100, abs(raw_score))

    if direction == "bearish":
        normalized_score = -max(1, magnitude)
    elif direction == "bullish":
        normalized_score = max(1, magnitude)
    else:
        direction = "neutral"
        normalized_score = 0

    normalized = dict(scored)
    normalized["impact_direction"] = direction
    normalized["impact_score"] = normalized_score
    return normalized


def _resolve_scan_window(config: AppConfig, window: str | None) -> str:
    return (window or config.scan_window_default).strip().lower()


def _resolve_symbol_mode(config: AppConfig, mode: str | None) -> str:
    return (mode or config.symbol_mode).strip().lower()


def _resolve_time_travel_end_datetime(config: AppConfig) -> datetime | None:
    if not config.scan_time_travel_enabled or config.scan_as_of_date is None:
        return None
    ny_tz = ZoneInfo("America/New_York")
    return datetime.combine(config.scan_as_of_date, time(hour=16, minute=0), tzinfo=ny_tz)


def _resolve_price_as_of_ts_utc(snapshot: dict[str, Any], end_datetime: datetime | None) -> str:
    value = str(snapshot.get("price_as_of_ts_utc") or "").strip()
    if value:
        return value
    if end_datetime is not None:
        return end_datetime.astimezone(timezone.utc).isoformat()
    return utc_now_iso()


def _safe_signature_params(func: Callable[..., Any]) -> tuple[set[str], bool]:
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return set(), True
    params = set(signature.parameters)
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    return params, accepts_kwargs


def _call_provider(
    fetcher: Callable[..., list[dict[str, str]]],
    kwargs: dict[str, Any],
) -> list[dict[str, str]]:
    params, accepts_kwargs = _safe_signature_params(fetcher)
    if accepts_kwargs or not params:
        return fetcher(**kwargs)
    return fetcher(**{key: value for key, value in kwargs.items() if key in params})


def _build_scanner_filters(config: AppConfig) -> IbkrScannerFilters:
    min_market_cap = config.min_market_cap if config.market_cap_enabled else None
    max_market_cap = config.max_market_cap if config.market_cap_enabled else None
    return IbkrScannerFilters(
        min_price=config.min_price,
        max_price=config.max_price,
        min_volume=config.min_volume,
        min_market_cap=min_market_cap,
        max_market_cap=max_market_cap,
        stock_type_filter=config.ibkr_stock_type_filter,
    )


def _collect_symbols(
    config: AppConfig,
    symbol_mode: str,
    ibkr_client: Any,
    scanner_filters: IbkrScannerFilters | None = None,
) -> list[str]:
    symbols: set[str] = set()

    if symbol_mode in {"env", "both"}:
        symbols.update(symbol.upper() for symbol in config.symbols)

    if symbol_mode in {"ibkr", "both"}:
        discovered = ibkr_client.discover_symbols(
            max_symbols=config.ibkr_max_symbols,
            filters=scanner_filters,
        )
        symbols.update(symbol.upper() for symbol in discovered)

    return sorted(symbol for symbol in symbols if symbol)


def run_scan(
    config: AppConfig,
    window: str | None = None,
    mode: str | None = None,
    ibkr_factory: Callable[[str, int, int], Any] = create_ibkr_client,
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
    logger.info(
        "Starting scan run_id=%s window=%s mode=%s",
        scan_run_id,
        scan_window,
        symbol_mode,
    )
    logger.debug(
        "Scan run_id=%s config ibkr_host=%s ibkr_port=%s ibkr_client_id=%s ibkr_max_symbols=%s ibkr_stock_type_filter=%s intraday_lookback_days=%s intraday_bar_size=%s end_datetime=%s",
        scan_run_id,
        config.ibkr_host,
        config.ibkr_port,
        config.ibkr_client_id,
        config.ibkr_max_symbols,
        config.ibkr_stock_type_filter,
        config.intraday_lookback_days,
        config.intraday_bar_size,
        end_datetime.isoformat() if end_datetime is not None else None,
    )

    try:
        if config.scan_time_travel_enabled and symbol_mode != "env":
            raise ValueError("SCAN_TIME_TRAVEL=1 requires symbol mode 'env'. Use --mode env or SYMBOL_MODE=env.")

        ibkr_client.connect()
        scanner_filters = _build_scanner_filters(config)
        symbols = _collect_symbols(config, symbol_mode, ibkr_client, scanner_filters=scanner_filters)
        logger.info("Resolved %s symbols for scan run_id=%s", len(symbols), scan_run_id)
        logger.debug("Scan run_id=%s symbols=%s", scan_run_id, symbols)

        if not symbols:
            update_scan_run_status(conn, scan_run_id, "completed", "No symbols resolved")
            logger.info("Scan run_id=%s completed with no symbols", scan_run_id)
            return scan_run_id

        filter_config = replace(config, min_market_cap=None, max_market_cap=None)

        snapshot_rows: list[dict[str, Any]] = []
        passed_count = 0
        failed_details: list[str] = []
        time_travel_warnings: list[str] = []

        for symbol in symbols:
            logger.debug("Fetching snapshot run_id=%s symbol=%s", scan_run_id, symbol)
            try:
                snapshot = ibkr_client.fetch_price_snapshot(
                    symbol=symbol,
                    intraday_lookback_days=config.intraday_lookback_days,
                    intraday_bar_size=config.intraday_bar_size,
                    end_datetime=end_datetime,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Snapshot fetch failed run_id=%s symbol=%s", scan_run_id, symbol)
                failed_details.append(f"{symbol}: {exc}")
                snapshot_rows.append(
                    {
                        "scan_run_id": scan_run_id,
                        "symbol": symbol,
                        "last_price": None,
                        "pct_change_1d": None,
                        "pct_change_intraday": None,
                        "volume": None,
                        "market_cap": None,
                        "price_source_ts_utc": utc_now_iso(),
                        "price_as_of_ts_utc": (
                            end_datetime.astimezone(timezone.utc).isoformat()
                            if end_datetime is not None
                            else utc_now_iso()
                        ),
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

            snapshot["market_cap"] = None
            passed, reason = passes_symbol_filters(snapshot, filter_config, scan_window)
            if passed:
                passed_count += 1
            else:
                failed_details.append(reason)
            logger.debug(
                "Filter result run_id=%s symbol=%s passed=%s reason=%s last_price=%s pct_change_1d=%s pct_change_intraday=%s volume=%s market_cap=%s",
                scan_run_id,
                symbol,
                passed,
                reason,
                snapshot.get("last_price"),
                snapshot.get("pct_change_1d"),
                snapshot.get("pct_change_intraday"),
                snapshot.get("volume"),
                snapshot.get("market_cap"),
            )

            snapshot_rows.append(
                {
                    "scan_run_id": scan_run_id,
                    "symbol": symbol,
                    "last_price": snapshot.get("last_price"),
                    "pct_change_1d": snapshot.get("pct_change_1d"),
                    "pct_change_intraday": snapshot.get("pct_change_intraday"),
                    "volume": snapshot.get("volume"),
                    "market_cap": snapshot.get("market_cap"),
                    "price_source_ts_utc": snapshot.get("price_source_ts_utc", utc_now_iso()),
                    "price_as_of_ts_utc": _resolve_price_as_of_ts_utc(snapshot, end_datetime),
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
        if not config.market_cap_enabled:
            notes += " Market-cap scanner bounds disabled via MARKET_CAP=0."
        if config.market_cap_enabled and symbol_mode in {"env", "both"}:
            notes += " Market-cap bounds are scanner-only and are not applied to env symbols."
        if failed_details:
            notes += " Some symbols failed filters or data fetch."
        update_scan_run_status(conn, scan_run_id, "completed", notes)
        logger.info("Completed scan run_id=%s processed=%s passed=%s", scan_run_id, len(symbols), passed_count)
        logger.debug("Scan run_id=%s completion_notes=%s", scan_run_id, notes)
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
        logger.exception("Scan failed run_id=%s message=%s", scan_run_id, message)
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
    massive_news_fetcher: Callable[..., list[dict[str, str]]] = fetch_symbol_news_massive,
) -> int:
    conn = connect_db(config.db_path)
    init_db(conn)
    symbols = get_symbols_for_run(conn, scan_run_id, passed_only=True)

    if not symbols:
        conn.close()
        return 0

    article_rows: list[dict[str, Any]] = []
    as_of_datetime = _resolve_time_travel_end_datetime(config)
    as_of_datetime_utc = as_of_datetime.astimezone(timezone.utc) if as_of_datetime is not None else None
    massive_enabled = config.massive_news_enabled and bool(config.massive_api_key.strip())
    massive_limiter = (
        MassiveRateLimiter(max_calls_per_minute=config.massive_max_calls_per_minute) if massive_enabled else None
    )

    for symbol_row in symbols:
        symbol = str(symbol_row["symbol"])
        existing_keys = list_existing_article_dedup_keys(conn, scan_run_id, symbol)

        fetched_items: list[dict[str, str]] = []

        try:
            yahoo_kwargs: dict[str, Any] = {
                "symbol": symbol,
                "lookback_hours": config.news_lookback_hours,
                "max_articles": config.max_news_articles_per_symbol,
                "region": config.yahoo_rss_region,
                "lang": config.yahoo_rss_lang,
                "as_of_datetime": as_of_datetime,
            }
            fetched_items.extend(_call_provider(news_fetcher, yahoo_kwargs))
        except Exception:  # noqa: BLE001
            pass

        if massive_enabled:
            try:
                massive_kwargs: dict[str, Any] = {
                    "symbol": symbol,
                    "lookback_hours": config.news_lookback_hours,
                    "max_articles": config.max_news_articles_per_symbol,
                    "api_key": config.massive_api_key,
                    "as_of_datetime": as_of_datetime,
                    "max_pages_per_symbol": config.massive_news_max_pages_per_symbol,
                    "max_calls_per_minute": config.massive_max_calls_per_minute,
                    "rate_limiter": massive_limiter,
                }
                fetched_items.extend(_call_provider(massive_news_fetcher, massive_kwargs))
            except Exception:  # noqa: BLE001
                pass

        for item in fetched_items:
            published_ts_utc = str(item.get("published_ts_utc") or "").strip()
            if as_of_datetime_utc is not None and published_ts_utc:
                published_dt = parse_iso_utc(published_ts_utc)
                if published_dt is not None and published_dt > as_of_datetime_utc:
                    continue

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
                    "published_ts_utc": published_ts_utc,
                    "rss_fetched_ts_utc": item["rss_fetched_ts_utc"],
                    "dedup_key": dedup_key,
                    "summary": str(item.get("summary", "")).strip() or None,
                    "provider": str(item.get("provider", "yahoo_rss")).strip() or "yahoo_rss",
                    "provider_article_id": str(item.get("provider_article_id", "")).strip() or None,
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
                max_completion_tokens=config.openai_max_completion_tokens,
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
            "summary": article["summary"],
        }

        try:
            scored = scorer.score_article(article_payload, retries=config.openai_score_retries)
            error_message = None
        except Exception as exc:  # noqa: BLE001
            scored = build_failed_score(str(exc), usage=getattr(exc, "usage", None))
            error_message = str(exc)
        scored = _normalize_impact_from_direction(scored)

        insert_article_score(
            conn,
            {
                "scan_run_id": scan_run_id,
                "symbol": article["symbol"],
                "article_id": article["article_id"],
                "openai_model": config.openai_model,
                "summary": scored["summary"],
                "impact_score": scored["impact_score"],
                "impact_direction": scored["impact_direction"],
                "seriousness_score": scored["seriousness_score"],
                "confidence": scored["confidence"],
                "impact_horizon": scored["impact_horizon"],
                "reason_tags_json": json.dumps(scored["reason_tags"]),
                "is_material_news": scored["is_material_news"],
                "scored_ts_utc": scored.get("scored_ts_utc", utc_now_iso()),
                "error_message": error_message,
                "prompt_tokens": scored.get("prompt_tokens"),
                "completion_tokens": scored.get("completion_tokens"),
                "total_tokens": scored.get("total_tokens"),
                "reasoning_tokens": scored.get("reasoning_tokens"),
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
                    "impact_direction": scored["impact_direction"],
                    "status": "error" if error_message else "ok",
                    "error_message": error_message,
                    "prompt_tokens": scored.get("prompt_tokens"),
                    "completion_tokens": scored.get("completion_tokens"),
                    "total_tokens": scored.get("total_tokens"),
                    "reasoning_tokens": scored.get("reasoning_tokens"),
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
