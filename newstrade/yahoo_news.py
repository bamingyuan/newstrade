from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, urlunparse
import time
import xml.etree.ElementTree as ET

import requests

from .time_utils import utc_now


def canonicalize_url(url: str) -> str:
    parsed = urlparse(url)
    cleaned = parsed._replace(query="", fragment="")
    return urlunparse(cleaned)


def _parse_published(raw: str) -> datetime | None:
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
    as_of_datetime: datetime | None = None,
    timeout_seconds: int = 15,
) -> list[dict[str, str]]:
    rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region={region}&lang={lang}"
    if as_of_datetime is None:
        now = utc_now()
    elif as_of_datetime.tzinfo is None:
        now = as_of_datetime.replace(tzinfo=timezone.utc)
    else:
        now = as_of_datetime.astimezone(timezone.utc)
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

        published_dt = _parse_published(str(item.findtext("pubDate", default="")).strip())
        if published_dt and (published_dt < cutoff or published_dt > now):
            continue

        seen.add(dedup_key)
        rows.append(
            {
                "symbol": symbol,
                "url": url,
                "title": title,
                "source": "Yahoo Finance RSS",
                "published_ts_utc": published_dt.isoformat() if published_dt else "",
                "fetched_ts_utc": now.isoformat(),
                "dedup_key": dedup_key,
                "summary": "",
                "provider": "yahoo_rss",
                "provider_article_id": "",
            }
        )
        if len(rows) >= max_articles:
            break

    return rows
