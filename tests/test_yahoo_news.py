from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests

from newstrade.yahoo_news import canonicalize_url, fetch_symbol_news


def test_canonicalize_url_removes_query_and_fragment() -> None:
    raw = "https://example.com/path?a=1&utm_source=x#section"
    assert canonicalize_url(raw) == "https://example.com/path"


def test_fetch_symbol_news_deduplicates_entries(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(hours=1)).strftime("%a, %d %b %Y %H:%M:%S +0000")

    rss = f"""
    <rss><channel>
      <item>
        <title>Title 1</title>
        <link>https://example.com/article?a=1</link>
        <pubDate>{recent}</pubDate>
      </item>
      <item>
        <title>Title 1 Duplicate</title>
        <link>https://example.com/article?a=2</link>
        <pubDate>{recent}</pubDate>
      </item>
    </channel></rss>
    """

    class FakeResponse:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    def fake_get(*args, **kwargs):
        return FakeResponse(rss)

    monkeypatch.setattr(requests, "get", fake_get)

    rows = fetch_symbol_news(
        symbol="AAPL",
        lookback_hours=24,
        max_articles=10,
        region="US",
        lang="en-US",
    )

    assert len(rows) == 1
    assert rows[0]["dedup_key"] == "https://example.com/article"
