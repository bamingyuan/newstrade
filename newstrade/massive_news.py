from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
import time

import requests

from .time_utils import parse_iso_utc, utc_now
from .yahoo_news import canonicalize_url

MASSIVE_NEWS_URL = "https://api.massive.com/v2/reference/news"

@dataclass
class MassiveRateLimiter:
    max_calls_per_minute: int
    _last_call_monotonic: float = 0.0

    def wait_for_slot(self) -> None:
        interval = 60.0 / max(1, self.max_calls_per_minute)
        now = time.monotonic()
        wait_seconds = interval - (now - self._last_call_monotonic)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
        self._last_call_monotonic = time.monotonic()


def _to_utc(dt: datetime | None) -> datetime:
    if dt is None:
        return utc_now()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_retry_after_seconds(raw: str | None) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return float(max(0, int(text)))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (dt.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())


def _request_with_retries(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: int,
    rate_limiter: MassiveRateLimiter,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            rate_limiter.wait_for_slot()
            response = session.get(url, params=params, headers=headers, timeout=timeout_seconds)
            if response.status_code == 429:
                if attempt < 2:
                    retry_after = _parse_retry_after_seconds(response.headers.get("Retry-After"))
                    time.sleep(retry_after if retry_after is not None else (1.0 * (2**attempt)))
                    continue
                response.raise_for_status()
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            break
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Massive request failed unexpectedly")


def fetch_symbol_news_massive(
    symbol: str,
    lookback_hours: int,
    max_articles: int,
    api_key: str,
    as_of_datetime: datetime | None = None,
    max_pages_per_symbol: int = 3,
    max_calls_per_minute: int = 5,
    timeout_seconds: int = 20,
    rate_limiter: MassiveRateLimiter | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    if not api_key.strip():
        return []
    if max_articles <= 0 or lookback_hours <= 0:
        return []

    now = _to_utc(as_of_datetime)
    cutoff = now - timedelta(hours=lookback_hours)
    page_limit = min(1000, max(1, max_articles))

    owns_session = session is None
    client = session or requests.Session()
    limiter = rate_limiter or MassiveRateLimiter(max_calls_per_minute=max_calls_per_minute)
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; newstrade/1.0; +https://example.invalid/newstrade)",
    }

    params: dict[str, Any] = {
        "ticker": symbol,
        "sort": "published_utc",
        "order": "desc",
        "limit": page_limit,
        "published_utc.gte": _to_iso_z(cutoff),
        "published_utc.lte": _to_iso_z(now),
        "apiKey": api_key,
    }
    next_url: str | None = MASSIVE_NEWS_URL

    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    page_count = 0
    try:
        while next_url and page_count < max_pages_per_symbol and len(rows) < max_articles:
            request_params = params if next_url == MASSIVE_NEWS_URL else {"apiKey": api_key}
            response = _request_with_retries(
                session=client,
                url=next_url,
                params=request_params,
                headers=headers,
                timeout_seconds=timeout_seconds,
                rate_limiter=limiter,
            )
            payload: dict[str, Any] = response.json()

            fetched_ts_utc = utc_now().isoformat()
            for item in payload.get("results", []):
                url = str(item.get("article_url", "")).strip()
                title = str(item.get("title", "")).strip()
                if not url or not title:
                    continue

                dedup_key = canonicalize_url(url)
                if dedup_key in seen:
                    continue

                published_text = str(item.get("published_utc", "")).strip()
                published_dt = parse_iso_utc(published_text) if published_text else None
                if published_dt and (published_dt < cutoff or published_dt > now):
                    continue

                seen.add(dedup_key)
                publisher = item.get("publisher", {})
                source_name = str((publisher or {}).get("name", "")).strip() or "Massive News"
                rows.append(
                    {
                        "symbol": symbol,
                        "url": url,
                        "title": title,
                        "source": source_name,
                        "published_ts_utc": published_dt.isoformat() if published_dt else published_text,
                        "rss_fetched_ts_utc": fetched_ts_utc,
                        "dedup_key": dedup_key,
                        "summary": str(item.get("description", "")).strip(),
                        "provider": "massive",
                        "provider_article_id": str(item.get("id", "")).strip(),
                    }
                )
                if len(rows) >= max_articles:
                    break

            page_count += 1
            next_url = str(payload.get("next_url", "")).strip() or None
    finally:
        if owns_session:
            client.close()

    return rows[:max_articles]
