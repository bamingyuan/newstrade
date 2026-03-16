from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import inspect
import json
import logging
from typing import Any, Callable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from .aggregate import compute_symbol_aggregate
from .ai_scoring import AIScorer, AIScorerConfig, build_failed_score
from .config import AppConfig
from .db import (
    connect_db,
    create_scan_run,
    delete_symbol_score,
    get_scored_articles_for_symbol,
    get_symbols_for_run,
    get_unscored_articles,
    init_db,
    insert_article_score,
    insert_news_articles,
    insert_symbol_snapshots,
    list_existing_article_dedup_keys,
    update_scan_run_status,
    upsert_symbol_score,
)
from .massive_market_data import MassiveGroupedDailyClient
from .massive_news import MassiveRateLimiter, fetch_symbol_news_massive
from .market_data import passes_symbol_filters, pct_change
from .reporting import build_report_dataframe, report_to_console
from .time_utils import parse_iso_utc, utc_now_iso
from .yahoo_news import fetch_symbol_news


logger = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Raised when a pipeline stage cannot complete."""


def _hostname_matches_allowed_domains(url: str, allowed_domains: set[str]) -> bool:
    if not allowed_domains:
        return True
    try:
        hostname = (urlparse(url).hostname or "").strip().lower().rstrip(".")
    except ValueError:
        return False
    if not hostname:
        return False
    return any(hostname == domain or hostname.endswith(f".{domain}") for domain in allowed_domains)


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


def _normalize_symbol_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch in {".", "-"})
    return cleaned[:24]


def _normalize_article_relevance(scored: dict[str, Any], expected_symbol: str) -> dict[str, Any]:
    expected = _normalize_symbol_code(expected_symbol)
    main_symbol = _normalize_symbol_code(scored.get("main_symbol", ""))

    mentioned_symbols: list[str] = []
    seen: set[str] = set()
    raw_mentioned = scored.get("mentioned_symbols", [])
    iterable = raw_mentioned if isinstance(raw_mentioned, list) else [raw_mentioned]
    for value in iterable:
        symbol = _normalize_symbol_code(value)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        mentioned_symbols.append(symbol)
        if len(mentioned_symbols) >= 12:
            break

    if main_symbol and main_symbol not in seen:
        mentioned_symbols.insert(0, main_symbol)
        seen.add(main_symbol)
    if len(mentioned_symbols) > 12:
        mentioned_symbols = mentioned_symbols[:12]

    try:
        raw_score = int(scored.get("relevance_score", 0))
    except (TypeError, ValueError):
        raw_score = 0
    relevance_score = max(0, min(100, raw_score))

    if expected and main_symbol and main_symbol != expected:
        relevance_score = min(relevance_score, 20)
    elif expected and not main_symbol:
        relevance_score = min(relevance_score, 35)

    if len(mentioned_symbols) > 1:
        penalty = min(50, 12 * (len(mentioned_symbols) - 1))
        relevance_score = max(0, relevance_score - penalty)

    if expected and expected not in seen:
        relevance_score = min(relevance_score, 25)

    normalized = dict(scored)
    normalized["main_symbol"] = main_symbol
    normalized["mentioned_symbols"] = mentioned_symbols
    normalized["relevance_score"] = relevance_score
    return normalized


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


def _call_provider(fetcher: Callable[..., list[dict[str, str]]], kwargs: dict[str, Any]) -> list[dict[str, str]]:
    params, accepts_kwargs = _safe_signature_params(fetcher)
    if accepts_kwargs or not params:
        return fetcher(**kwargs)
    return fetcher(**{key: value for key, value in kwargs.items() if key in params})


def _resolve_requested_trade_date(config: AppConfig, reference_now: datetime | None) -> date:
    if config.scan_time_travel_enabled and config.scan_as_of_date is not None:
        return config.scan_as_of_date

    if reference_now is None:
        now_ny = datetime.now(ZoneInfo("America/New_York"))
    elif reference_now.tzinfo is None:
        now_ny = reference_now.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("America/New_York"))
    else:
        now_ny = reference_now.astimezone(ZoneInfo("America/New_York"))
    return now_ny.date() - timedelta(days=1)


def _find_available_trade_date(
    client: MassiveGroupedDailyClient,
    start_date: date,
    max_backtrack_days: int = 10,
) -> tuple[date, list[dict[str, Any]]]:
    for offset in range(max_backtrack_days + 1):
        candidate_date = start_date - timedelta(days=offset)
        rows = client.fetch_grouped_daily(candidate_date)
        if rows:
            return candidate_date, rows
    raise PipelineError(f"No Massive daily market summary data found on or before {start_date.isoformat()}.")


def _join_market_summaries(
    trade_date: date,
    previous_trade_date: date,
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    previous_by_symbol = {
        str(row["symbol"]): row
        for row in previous_rows
        if row.get("symbol") and row.get("close_price") is not None
    }

    joined_rows: list[dict[str, Any]] = []
    for current_row in current_rows:
        symbol = str(current_row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        close_price = current_row.get("close_price")
        previous_row = previous_by_symbol.get(symbol)
        if previous_row is None or close_price is None:
            continue
        previous_close_price = previous_row.get("close_price")
        if previous_close_price is None:
            continue

        joined_rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date.isoformat(),
                "previous_trade_date": previous_trade_date.isoformat(),
                "close_price": close_price,
                "previous_close_price": previous_close_price,
                "pct_change": pct_change(previous_close_price, close_price),
                "volume": current_row.get("volume"),
                "vwap": current_row.get("vwap"),
                "transaction_count": current_row.get("transaction_count"),
                "price_as_of_ts_utc": current_row.get("price_as_of_ts_utc") or utc_now_iso(),
            }
        )
    return joined_rows


def _rank_selected_symbols(rows: list[dict[str, Any]], max_selected: int) -> tuple[int, int]:
    passing_rows = [row for row in rows if row.get("passed_filters")]
    ordered = sorted(
        passing_rows,
        key=lambda row: (-abs(float(row.get("pct_change", 0.0))), str(row.get("symbol", ""))),
    )
    selected_symbols = {str(row["symbol"]) for row in ordered[:max_selected]}

    for rank, row in enumerate(ordered, start=1):
        row["rank_abs_pct_change"] = rank
        row["selected_for_news"] = str(row["symbol"]) in selected_symbols
    for row in rows:
        row.setdefault("rank_abs_pct_change", None)
        row.setdefault("selected_for_news", False)
    return len(passing_rows), len(selected_symbols)


def run_scan(
    config: AppConfig,
    window: str | None = None,
    mode: str | None = None,
    market_data_client: MassiveGroupedDailyClient | None = None,
    reference_now: datetime | None = None,
) -> int:
    del window, mode

    conn = connect_db(config.db_path)
    init_db(conn)
    scan_run_id = create_scan_run(conn=conn, run_ts_utc=utc_now_iso(), status="running", notes="")

    client = market_data_client or MassiveGroupedDailyClient(
        api_key=config.massive_api_key,
        max_calls_per_minute=config.massive_max_calls_per_minute,
    )

    try:
        requested_trade_date = _resolve_requested_trade_date(config, reference_now)
        trade_date, current_rows = _find_available_trade_date(client, requested_trade_date)
        previous_trade_date, previous_rows = _find_available_trade_date(client, trade_date - timedelta(days=1))
        joined_rows = _join_market_summaries(trade_date, previous_trade_date, current_rows, previous_rows)

        snapshot_rows: list[dict[str, Any]] = []
        for row in joined_rows:
            passed, reason = passes_symbol_filters(row, config)
            snapshot_rows.append(
                {
                    "scan_run_id": scan_run_id,
                    **row,
                    "passed_filters": passed,
                    "filter_reason": reason,
                }
            )

        passed_count, selected_count = _rank_selected_symbols(
            snapshot_rows,
            max_selected=config.max_news_symbols_per_run,
        )
        insert_symbol_snapshots(conn, snapshot_rows)

        notes = (
            f"Processed {len(snapshot_rows)} symbols from Massive grouped daily summaries. "
            f"Passed {passed_count}; selected top {selected_count} for news."
        )
        if trade_date != requested_trade_date:
            notes += f" Requested {requested_trade_date.isoformat()}, used prior trading session {trade_date.isoformat()}."
        if config.scan_time_travel_enabled:
            notes += " Time-travel enabled."

        update_scan_run_status(
            conn,
            scan_run_id,
            "completed",
            notes,
            trade_date=trade_date.isoformat(),
            previous_trade_date=previous_trade_date.isoformat(),
            total_candidates=len(snapshot_rows),
            passed_candidates=passed_count,
            selected_candidates=selected_count,
        )
        return scan_run_id
    except Exception as exc:  # noqa: BLE001
        message = f"Scan failed due to an upstream data or pipeline error: {exc}"
        update_scan_run_status(conn, scan_run_id, "failed", message)
        logger.exception("Scan failed run_id=%s message=%s", scan_run_id, message)
        raise PipelineError(message) from exc
    finally:
        try:
            client.close()
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
    symbols = get_symbols_for_run(conn, scan_run_id, selected_only=True)

    if not symbols:
        conn.close()
        return 0

    article_rows: list[dict[str, Any]] = []
    as_of_date = config.scan_as_of_date if config.scan_time_travel_enabled else None
    as_of_datetime = (
        datetime.combine(as_of_date, datetime.min.time(), tzinfo=ZoneInfo("America/New_York")).replace(hour=23, minute=59)
        if as_of_date is not None
        else None
    )
    as_of_datetime_utc = as_of_datetime.astimezone(timezone.utc) if as_of_datetime is not None else None
    yahoo_rss_allowed_domains = {domain.lower() for domain in config.yahoo_rss_allowed_domains}
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
            logger.exception("Yahoo news fetch failed for symbol=%s", symbol)

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
                logger.exception("Massive news fetch failed for symbol=%s", symbol)

        for item in fetched_items:
            provider = str(item.get("provider", "yahoo_rss")).strip().lower() or "yahoo_rss"
            url = str(item.get("url", "")).strip()
            if not url:
                continue
            if provider == "yahoo_rss" and yahoo_rss_allowed_domains:
                if not _hostname_matches_allowed_domains(url, yahoo_rss_allowed_domains):
                    continue

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
                    "url": url,
                    "title": item["title"],
                    "source": item["source"],
                    "published_ts_utc": published_ts_utc,
                    "fetched_ts_utc": str(
                        item.get("fetched_ts_utc") or item.get("rss_fetched_ts_utc") or utc_now_iso()
                    ),
                    "dedup_key": dedup_key,
                    "summary": str(item.get("summary", "")).strip() or None,
                    "provider": provider,
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
        scored = _normalize_article_relevance(scored, expected_symbol=str(article["symbol"]))

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
                "main_symbol": scored["main_symbol"],
                "mentioned_symbols_json": json.dumps(scored["mentioned_symbols"]),
                "relevance_score": scored["relevance_score"],
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
                    "main_symbol": scored["main_symbol"],
                    "relevance_score": scored["relevance_score"],
                    "status": "error" if error_message else "ok",
                    "error_message": error_message,
                    "prompt_tokens": scored.get("prompt_tokens"),
                    "completion_tokens": scored.get("completion_tokens"),
                    "total_tokens": scored.get("total_tokens"),
                    "reasoning_tokens": scored.get("reasoning_tokens"),
                }
            )

    selected_symbols = get_symbols_for_run(conn, scan_run_id, selected_only=True)
    all_symbols = get_symbols_for_run(conn, scan_run_id, selected_only=False)
    selected_symbol_codes = {str(row["symbol"]) for row in selected_symbols}

    symbol_score_count = 0
    for symbol_row in all_symbols:
        symbol = str(symbol_row["symbol"])
        if symbol not in selected_symbol_codes:
            delete_symbol_score(conn, scan_run_id, symbol)
            continue
        scored_rows = [dict(row) for row in get_scored_articles_for_symbol(conn, scan_run_id, symbol)]
        if not scored_rows:
            delete_symbol_score(conn, scan_run_id, symbol)
            continue
        aggregate = compute_symbol_aggregate(symbol=symbol, scan_run_id=scan_run_id, rows=scored_rows)
        upsert_symbol_score(conn, aggregate)
        symbol_score_count += 1

    conn.close()
    return scored_count, symbol_score_count


def run_report(config: AppConfig, scan_run_id: int, top: int = 30) -> str:
    conn = connect_db(config.db_path)
    init_db(conn)
    df = build_report_dataframe(conn, scan_run_id)
    conn.close()
    return report_to_console(df, top=top)


def run_all(
    config: AppConfig,
    window: str | None = None,
    mode: str | None = None,
    top: int = 30,
) -> tuple[int, str]:
    del window, mode
    scan_run_id = run_scan(config=config)
    run_news(config=config, scan_run_id=scan_run_id)
    run_score(config=config, scan_run_id=scan_run_id)
    report_text = run_report(config=config, scan_run_id=scan_run_id, top=top)
    return scan_run_id, report_text
