from __future__ import annotations

from datetime import datetime, timedelta, timezone

from newstrade.aggregate import classify_direction_label, compute_symbol_aggregate


def test_classify_direction_label() -> None:
    assert classify_direction_label(20) == "bullish"
    assert classify_direction_label(-20) == "bearish"
    assert classify_direction_label(5) == "neutral"


def test_compute_symbol_aggregate_weighted() -> None:
    now = datetime.now(timezone.utc)
    rows = [
        {
            "impact_score": 50,
            "seriousness_score": 80,
            "published_ts_utc": (now - timedelta(hours=1)).isoformat(),
        },
        {
            "impact_score": -10,
            "seriousness_score": 20,
            "published_ts_utc": (now - timedelta(hours=24)).isoformat(),
        },
    ]

    agg = compute_symbol_aggregate(symbol="AAPL", scan_run_id=1, rows=rows, now_utc=now)

    assert agg["article_count"] == 2
    assert agg["weighted_seriousness_score"] > 20
    assert agg["weighted_impact_score"] > 0
