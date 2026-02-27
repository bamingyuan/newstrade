from __future__ import annotations

import pytest

from newstrade.ai_scoring import AIScoringError, validate_scoring_payload


def test_validate_scoring_payload_valid() -> None:
    payload = {
        "summary": "Earnings beat expectations.",
        "impact_score": 35,
        "seriousness_score": 70,
        "confidence": 82,
        "impact_horizon": "short_term",
        "reason_tags": ["earnings", "guidance"],
        "is_material_news": True,
    }

    validated = validate_scoring_payload(payload)
    assert validated["impact_score"] == 35
    assert validated["seriousness_score"] == 70


def test_validate_scoring_payload_invalid_range() -> None:
    payload = {
        "summary": "Too strong",
        "impact_score": 200,
        "seriousness_score": 70,
        "confidence": 82,
        "impact_horizon": "short_term",
        "reason_tags": ["earnings"],
        "is_material_news": True,
    }

    with pytest.raises(AIScoringError):
        validate_scoring_payload(payload)
