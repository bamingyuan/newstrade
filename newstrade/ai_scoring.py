from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from jsonschema import ValidationError, validate

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None


ALLOWED_REASON_TAGS = [
    "earnings",
    "guidance",
    "m&a",
    "regulation",
    "litigation",
    "macro",
    "product",
    "management",
    "analyst_action",
    "other",
]

SCORING_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "summary",
        "impact_score",
        "seriousness_score",
        "confidence",
        "impact_horizon",
        "reason_tags",
        "is_material_news",
    ],
    "properties": {
        "summary": {"type": "string", "maxLength": 400},
        "impact_score": {"type": "integer", "minimum": -100, "maximum": 100},
        "seriousness_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        "impact_horizon": {
            "type": "string",
            "enum": ["immediate", "short_term", "medium_term"],
        },
        "reason_tags": {
            "type": "array",
            "items": {"type": "string", "enum": ALLOWED_REASON_TAGS},
            "minItems": 1,
            "maxItems": 4,
            "uniqueItems": True,
        },
        "is_material_news": {"type": "boolean"},
    },
}


class AIScoringError(RuntimeError):
    """Raised when AI scoring fails after retries."""


@dataclass
class AIScorerConfig:
    api_key: str
    model: str
    timeout_seconds: int
    temperature: float


class AIScorer:
    def __init__(self, cfg: AIScorerConfig) -> None:
        if OpenAI is None:
            raise RuntimeError("openai package is not installed.")
        if not cfg.api_key:
            raise RuntimeError("OPENAI_API_KEY is missing. Set it in your .env file.")
        self.cfg = cfg
        self.client = OpenAI(api_key=cfg.api_key, timeout=cfg.timeout_seconds)

    def _build_messages(self, article: Mapping[str, Any]) -> list[dict[str, str]]:
        system = (
            "You are a financial news analyst. Return only JSON matching the schema. "
            "Keep summary concise and factual."
        )
        user = (
            "Score this stock news item.\n"
            f"Symbol: {article['symbol']}\n"
            f"Title: {article['title']}\n"
            f"Source: {article['source']}\n"
            f"Published UTC: {article.get('published_ts_utc', '')}\n"
            f"URL: {article['url']}\n\n"
            "Interpret likely directional effect for the stock only."
        )
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    def score_article(self, article: Mapping[str, Any], retries: int = 2) -> dict[str, Any]:
        messages = self._build_messages(article)
        last_error: Exception | None = None

        for attempt in range(retries + 1):
            try:
                completion = self.client.chat.completions.create(
                    model=self.cfg.model,
                    temperature=self.cfg.temperature,
                    response_format={"type": "json_object"},
                    messages=messages,
                )
                content = completion.choices[0].message.content or "{}"
                payload = json.loads(content)
                validated = validate_scoring_payload(payload)
                return validated
            except Exception as exc:  # intentionally broad for external API errors
                last_error = exc
                if attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                break

        raise AIScoringError(str(last_error))


def validate_scoring_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        validate(instance=payload, schema=SCORING_JSON_SCHEMA)
    except ValidationError as exc:
        raise AIScoringError(f"AI payload validation failed: {exc.message}") from exc

    summary = str(payload["summary"]).strip()
    if len(summary) > 400:
        summary = summary[:400]

    return {
        "summary": summary,
        "impact_score": int(payload["impact_score"]),
        "seriousness_score": int(payload["seriousness_score"]),
        "confidence": int(payload["confidence"]),
        "impact_horizon": str(payload["impact_horizon"]),
        "reason_tags": list(payload["reason_tags"]),
        "is_material_news": bool(payload["is_material_news"]),
    }


def build_failed_score(error_message: str) -> dict[str, Any]:
    return {
        "summary": "",
        "impact_score": 0,
        "seriousness_score": 0,
        "confidence": 0,
        "impact_horizon": "short_term",
        "reason_tags": ["other"],
        "is_material_news": False,
        "error_message": error_message,
        "scored_ts_utc": datetime.now(timezone.utc).isoformat(),
    }
