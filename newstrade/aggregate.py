from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .time_utils import parse_iso_utc


def classify_direction_label(weighted_impact_score: float) -> str:
    if weighted_impact_score > 10:
        return "bullish"
    if weighted_impact_score < -10:
        return "bearish"
    return "neutral"


def compute_symbol_aggregate(
    symbol: str,
    scan_run_id: int,
    rows: list[dict[str, Any]],
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    if not rows:
        return {
            "scan_run_id": scan_run_id,
            "symbol": symbol,
            "article_count": 0,
            "weighted_impact_score": 0.0,
            "weighted_seriousness_score": 0.0,
            "bullish_bearish_label": "neutral",
            "score_ts_utc": now_utc.isoformat(),
        }

    weighted_impact_sum = 0.0
    weighted_seriousness_sum = 0.0
    weight_sum = 0.0

    for row in rows:
        published = parse_iso_utc(row.get("published_ts_utc"))
        if published is None:
            age_hours = 24.0
        else:
            age_seconds = max((now_utc - published).total_seconds(), 0)
            age_hours = age_seconds / 3600.0

        weight = math.exp(-age_hours / 24.0)
        weighted_impact_sum += weight * float(row["impact_score"])
        weighted_seriousness_sum += weight * float(row["seriousness_score"])
        weight_sum += weight

    if weight_sum == 0:
        weighted_impact = 0.0
        weighted_seriousness = 0.0
    else:
        weighted_impact = weighted_impact_sum / weight_sum
        weighted_seriousness = weighted_seriousness_sum / weight_sum

    return {
        "scan_run_id": scan_run_id,
        "symbol": symbol,
        "article_count": len(rows),
        "weighted_impact_score": float(round(weighted_impact, 4)),
        "weighted_seriousness_score": float(round(weighted_seriousness, 4)),
        "bullish_bearish_label": classify_direction_label(weighted_impact),
        "score_ts_utc": now_utc.isoformat(),
    }


def top_reason_tags(rows: list[dict[str, Any]], limit: int = 3) -> list[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        raw = row.get("reason_tags_json")
        if not raw:
            continue
        try:
            tags = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(tags, list):
            for tag in tags:
                counter[str(tag)] += 1

    return [tag for tag, _ in counter.most_common(limit)]
