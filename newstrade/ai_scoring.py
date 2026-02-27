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

# OpenAI's structured-output schema validator supports a subset of JSON Schema.
# Keep this in sync with SCORING_JSON_SCHEMA while avoiding unsupported keywords.
OPENAI_SCORING_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": SCORING_JSON_SCHEMA["required"],
    "properties": {
        "summary": {"type": "string"},
        "impact_score": {"type": "integer", "minimum": -100, "maximum": 100},
        "seriousness_score": {"type": "integer", "minimum": 0, "maximum": 100},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        "impact_horizon": {"type": "string", "enum": ["immediate", "short_term", "medium_term"]},
        "reason_tags": {
            "type": "array",
            "items": {"type": "string", "enum": ALLOWED_REASON_TAGS},
            "minItems": 1,
            "maxItems": 4,
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

    def _model_supports_custom_temperature(self) -> bool:
        # gpt-5* currently only accepts its default temperature behavior.
        return not self.cfg.model.strip().lower().startswith("gpt-5")

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
        send_temperature = self._model_supports_custom_temperature()
        use_json_schema = True

        for attempt in range(retries + 1):
            try:
                request_args: dict[str, Any] = {
                    "model": self.cfg.model,
                    "messages": messages,
                }
                if use_json_schema:
                    request_args["response_format"] = {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "news_score",
                            "strict": True,
                            "schema": OPENAI_SCORING_JSON_SCHEMA,
                        },
                    }
                else:
                    request_args["response_format"] = {"type": "json_object"}
                if send_temperature:
                    request_args["temperature"] = self.cfg.temperature

                completion = self.client.chat.completions.create(
                    **request_args,
                )
                content = completion.choices[0].message.content or "{}"
                payload = json.loads(content)
                validated = validate_scoring_payload(payload)
                return validated
            except Exception as exc:  # intentionally broad for external API errors
                message = str(exc)
                if use_json_schema and "response_format" in message:
                    # Fallback for models/endpoints that do not support strict schema mode.
                    use_json_schema = False
                    continue
                if send_temperature and "temperature" in message and "unsupported" in message.lower():
                    # Defensive fallback for models that reject explicit temperature.
                    send_temperature = False
                    continue
                last_error = exc
                if attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                break

        raise AIScoringError(str(last_error))


def validate_scoring_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    # Accept extra model-provided fields and validate only the contract we need.
    normalized_payload = {key: payload[key] for key in SCORING_JSON_SCHEMA["required"] if key in payload}

    try:
        validate(instance=normalized_payload, schema=SCORING_JSON_SCHEMA)
    except ValidationError as exc:
        raise AIScoringError(f"AI payload validation failed: {exc.message}") from exc

    summary = str(normalized_payload["summary"]).strip()
    if len(summary) > 400:
        summary = summary[:400]

    return {
        "summary": summary,
        "impact_score": int(normalized_payload["impact_score"]),
        "seriousness_score": int(normalized_payload["seriousness_score"]),
        "confidence": int(normalized_payload["confidence"]),
        "impact_horizon": str(normalized_payload["impact_horizon"]),
        "reason_tags": list(normalized_payload["reason_tags"]),
        "is_material_news": bool(normalized_payload["is_material_news"]),
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
