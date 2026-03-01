from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
import requests

from newstrade.massive_news import MASSIVE_NEWS_URL, MassiveRateLimiter, fetch_symbol_news_massive


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any], headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}", response=self)


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str], timeout: int) -> FakeResponse:
        self.calls.append((url, dict(params)))
        return self._responses.pop(0)

    def close(self) -> None:  # pragma: no cover - called only when function owns session
        return None


def test_fetch_massive_news_parses_and_paginates() -> None:
    session = FakeSession(
        responses=[
            FakeResponse(
                200,
                {
                    "results": [
                        {
                            "id": "m1",
                            "article_url": "https://example.com/a?ref=123",
                            "title": "A title",
                            "description": "A summary",
                            "published_utc": "2026-02-23T19:00:00Z",
                            "publisher": {"name": "Publisher A"},
                        }
                    ],
                    "next_url": "https://api.massive.com/v2/reference/news?cursor=abc",
                },
            ),
            FakeResponse(
                200,
                {
                    "results": [
                        {
                            "id": "m2",
                            "article_url": "https://example.com/b",
                            "title": "B title",
                            "description": "B summary",
                            "published_utc": "2026-02-23T18:00:00Z",
                            "publisher": {"name": "Publisher B"},
                        }
                    ],
                },
            ),
        ]
    )

    rows = fetch_symbol_news_massive(
        symbol="AAPL",
        lookback_hours=24,
        max_articles=10,
        api_key="test-key",
        as_of_datetime=datetime(2026, 2, 23, 21, 0, 0, tzinfo=timezone.utc),
        max_pages_per_symbol=3,
        max_calls_per_minute=1000,
        rate_limiter=MassiveRateLimiter(max_calls_per_minute=1000),
        session=session,
    )

    assert len(rows) == 2
    assert rows[0]["provider"] == "massive"
    assert rows[0]["summary"] == "A summary"
    assert rows[0]["provider_article_id"] == "m1"
    assert rows[0]["dedup_key"] == "https://example.com/a"

    first_url, first_params = session.calls[0]
    second_url, second_params = session.calls[1]
    assert first_url == MASSIVE_NEWS_URL
    assert first_params["ticker"] == "AAPL"
    assert first_params["apiKey"] == "test-key"
    assert "published_utc.gte" in first_params
    assert "published_utc.lte" in first_params
    assert second_url.startswith("https://api.massive.com/v2/reference/news?cursor=")
    assert second_params == {"apiKey": "test-key"}


def test_fetch_massive_news_retries_on_429(monkeypatch: pytest.MonkeyPatch) -> None:
    session = FakeSession(
        responses=[
            FakeResponse(429, {}, headers={"Retry-After": "0"}),
            FakeResponse(
                200,
                {
                    "results": [
                        {
                            "id": "m1",
                            "article_url": "https://example.com/a",
                            "title": "A title",
                            "description": "A summary",
                            "published_utc": "2026-02-23T19:00:00Z",
                            "publisher": {"name": "Publisher A"},
                        }
                    ],
                },
            ),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr("newstrade.massive_news.time.sleep", lambda seconds: sleeps.append(seconds))

    rows = fetch_symbol_news_massive(
        symbol="AAPL",
        lookback_hours=24,
        max_articles=10,
        api_key="test-key",
        as_of_datetime=datetime(2026, 2, 23, 21, 0, 0, tzinfo=timezone.utc),
        max_pages_per_symbol=1,
        max_calls_per_minute=1000,
        rate_limiter=MassiveRateLimiter(max_calls_per_minute=1000),
        session=session,
    )

    assert len(rows) == 1
    assert len(session.calls) == 2
    assert sleeps
