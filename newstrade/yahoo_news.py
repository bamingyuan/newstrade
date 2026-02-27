from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote, urlparse, urlunparse
import time
import xml.etree.ElementTree as ET

import requests

from .time_utils import utc_now


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    cleaned = parsed._replace(query="", fragment="")
    return urlunparse(cleaned)


def _parse_published(entry: Any) -> datetime | None:
    raw = entry.get("published") or entry.get("updated")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fetch_symbol_news(
    symbol: str,
    lookback_hours: int,
    max_articles: int,
    region: str,
    lang: str,
    timeout_seconds: int = 15,
) -> list[dict[str, str]]:
    rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region={region}&lang={lang}"
    now = utc_now()
    cutoff = now - timedelta(hours=lookback_hours)

    headers = {
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (compatible; newstrade/1.0; +https://example.invalid/newstrade)",
    }

    response: requests.Response | None = None
    for attempt in range(3):
        try:
            candidate = requests.get(rss_url, timeout=timeout_seconds, headers=headers)
            if candidate.status_code == 429:
                if attempt < 2:
                    time.sleep(1.0 * (2**attempt))
                    continue
                candidate.raise_for_status()
            candidate.raise_for_status()
            response = candidate
            break
        except requests.RequestException:
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            raise

    if response is None:
        return []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    root = ET.fromstring(response.text)
    items = root.findall(".//item")

    for item in items:
        url = str(item.findtext("link", default="")).strip()
        title = str(item.findtext("title", default="")).strip()
        if not url or not title:
            continue

        dedup_key = canonicalize_url(url)
        if dedup_key in seen:
            continue

        published_dt = _parse_published(
            {
                "published": item.findtext("pubDate", default=""),
                "updated": item.findtext("pubDate", default=""),
            }
        )
        if published_dt and published_dt < cutoff:
            continue

        seen.add(dedup_key)
        rows.append(
            {
                "symbol": symbol,
                "url": url,
                "title": title,
                "source": "Yahoo Finance RSS",
                "published_ts_utc": published_dt.isoformat() if published_dt else "",
                "rss_fetched_ts_utc": now.isoformat(),
                "dedup_key": dedup_key,
            }
        )

        if len(rows) >= max_articles:
            break

    return rows


def fetch_market_caps(symbols: list[str], timeout_seconds: int = 15) -> dict[str, float | None]:
    result: dict[str, float | None] = {symbol: None for symbol in symbols}
    if not symbols:
        return result

    chunk_size = 10
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; newstrade/1.0; +https://example.invalid/newstrade)",
    }

    for start in range(0, len(symbols), chunk_size):
        chunk = symbols[start : start + chunk_size]
        query = ",".join(chunk)
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={query}"
        payload: dict[str, Any] | None = None

        for attempt in range(3):
            try:
                response = requests.get(url, timeout=timeout_seconds, headers=headers)
                if response.status_code == 429:
                    if attempt < 2:
                        time.sleep(1.0 * (2**attempt))
                        continue
                    break
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(0.5 * (2**attempt))
                    continue
                break

        if payload is None:
            continue

        for row in payload.get("quoteResponse", {}).get("result", []):
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                result[symbol] = _as_float(row.get("marketCap"))

        # Keep request pacing low to reduce Yahoo throttling.
        time.sleep(0.15)

    missing_symbols = [symbol for symbol, value in result.items() if value is None]
    if not missing_symbols:
        return result

    now_ts = int(time.time())
    period1 = now_ts - 365 * 24 * 60 * 60

    for symbol in missing_symbols:
        encoded_symbol = quote(symbol, safe="")
        timeseries_url = (
            "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/"
            f"{encoded_symbol}?merge=false&padTimeSeries=true&period1={period1}&period2={now_ts}"
            "&type=trailingMarketCap,quarterlyMarketCap"
        )
        payload: dict[str, Any] | None = None

        for attempt in range(3):
            try:
                response = requests.get(timeseries_url, timeout=timeout_seconds, headers=headers)
                if response.status_code == 429:
                    if attempt < 2:
                        time.sleep(1.0 * (2**attempt))
                        continue
                    break
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(0.5 * (2**attempt))
                    continue
                break

        if payload is None:
            continue

        result[symbol] = _extract_market_cap_from_timeseries(payload)
        time.sleep(0.1)
    return result


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _extract_market_cap_from_timeseries(payload: dict[str, Any]) -> float | None:
    entries = payload.get("timeseries", {}).get("result", [])
    if not isinstance(entries, list):
        return None

    candidates: list[tuple[str, int, float]] = []
    for series_name, priority in (("trailingMarketCap", 1), ("quarterlyMarketCap", 0)):
        for item in entries:
            points = item.get(series_name)
            if not isinstance(points, list):
                continue
            for point in points:
                if not isinstance(point, dict):
                    continue
                reported_value = point.get("reportedValue")
                if not isinstance(reported_value, dict):
                    continue
                raw = _as_float(reported_value.get("raw"))
                if raw is None:
                    continue
                as_of_date = str(point.get("asOfDate", "")).strip()
                candidates.append((as_of_date, priority, raw))

    if not candidates:
        return None

    # Prefer the newest date; when tied, prefer trailing over quarterly.
    best = max(candidates, key=lambda item: (item[0], item[1]))
    return best[2]
