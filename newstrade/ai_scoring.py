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
    "capital_allocation",
    "partnership_or_contract",
    "operations",
    "other",
]

ALLOWED_IMPACT_DIRECTIONS = [
    "bearish",
    "neutral",
    "bullish",
]

SCORING_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "summary",
        "impact_score",
        "impact_direction",
        "seriousness_score",
        "confidence",
        "impact_horizon",
        "reason_tags",
        "is_material_news",
        "main_symbol",
        "mentioned_symbols",
        "relevance_score",
    ],
    "properties": {
        "summary": {"type": "string", "maxLength": 400},
        "impact_score": {"type": "integer", "minimum": -100, "maximum": 100},
        "impact_direction": {"type": "string", "enum": ALLOWED_IMPACT_DIRECTIONS},
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
        "main_symbol": {"type": "string", "maxLength": 24},
        "mentioned_symbols": {
            "type": "array",
            "items": {"type": "string", "maxLength": 24},
            "maxItems": 12,
        },
        "relevance_score": {"type": "integer", "minimum": 0, "maximum": 100},
    },
}

# OpenAI's structured-output schema validator supports a subset of JSON Schema.
# Keep this in sync with SCORING_JSON_SCHEMA while avoiding unsupported keywords.
OPENAI_SCORING_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": SCORING_JSON_SCHEMA["required"],
    "properties": {
        "summary": {"type": "string", "maxLength": 400},
        "impact_score": {"type": "integer", "minimum": -100, "maximum": 100},
        "impact_direction": {"type": "string", "enum": ALLOWED_IMPACT_DIRECTIONS},
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
        "main_symbol": {"type": "string", "maxLength": 24},
        "mentioned_symbols": {
            "type": "array",
            "items": {"type": "string", "maxLength": 24},
            "maxItems": 12,
        },
        "relevance_score": {"type": "integer", "minimum": 0, "maximum": 100},
    },
}


class AIScoringError(RuntimeError):
    """Raised when AI scoring fails after retries."""

    def __init__(self, message: str, usage: Mapping[str, int | None] | None = None) -> None:
        super().__init__(message)
        self.usage = dict(usage or {})


@dataclass
class AIScorerConfig:
    api_key: str
    model: str
    timeout_seconds: int
    temperature: float
    max_completion_tokens: int | None


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
            "Keep summary concise and factual. "
            "Impact scoring rubric: -100 very bearish, 0 neutral, +100 very bullish. "
            "Direction rule: bearish must have negative impact_score, neutral must be 0, bullish must be positive. "
            "Magnitude guide: 1-20 mild, 21-50 moderate, 51-80 strong, 81-100 extreme. "
            "Also identify the article's main ticker symbol and list any mentioned ticker symbols. "
            "Relevance scoring rubric for the provided symbol: 90-100 directly and primarily about it, "
            "60-89 meaningfully about it but with some focus on peers/sector, "
            "20-59 only peripheral mention, 0-19 mostly about a different symbol."
        )
        user = (
            "Score this stock news item.\n"
            f"Symbol: {article['symbol']}\n"
            f"Title: {article['title']}\n"
            f"Feed Summary: {article.get('summary') or ''}\n"
            f"Source: {article['source']}\n"
            f"Published UTC: {article.get('published_ts_utc', '')}\n"
            f"URL: {article['url']}\n\n"
            "Interpret likely directional effect for the stock only. "
            "Return impact_direction as one of bearish/neutral/bullish. "
            "Use uppercase ticker codes in main_symbol and mentioned_symbols when possible."
        )
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    @staticmethod
    def _extract_usage(completion: Any) -> dict[str, int | None]:
        usage = getattr(completion, "usage", None)

        def _read(container: Any, key: str) -> Any:
            if container is None:
                return None
            if isinstance(container, Mapping):
                return container.get(key)
            return getattr(container, key, None)

        def _as_int(value: Any) -> int | None:
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        completion_details = _read(usage, "completion_tokens_details")
        reasoning_tokens = _read(completion_details, "reasoning_tokens")

        return {
            "prompt_tokens": _as_int(_read(usage, "prompt_tokens")),
            "completion_tokens": _as_int(_read(usage, "completion_tokens")),
            "total_tokens": _as_int(_read(usage, "total_tokens")),
            "reasoning_tokens": _as_int(reasoning_tokens),
        }

    def score_article(self, article: Mapping[str, Any], retries: int = 2) -> dict[str, Any]:
        messages = self._build_messages(article)
        last_error: Exception | None = None
        last_usage: dict[str, int | None] | None = None
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
                if self.cfg.max_completion_tokens is not None:
                    request_args["max_completion_tokens"] = self.cfg.max_completion_tokens

                completion = self.client.chat.completions.create(
                    **request_args,
                )
                usage = self._extract_usage(completion)
                last_usage = usage
                content = (completion.choices[0].message.content or "").strip()
                if not content:
                    raise AIScoringError(
                        "AI returned empty content. OPENAI_MAX_COMPLETION_TOKENS may be too low for this model.",
                        usage=usage,
                    )
                try:
                    payload = json.loads(content)
                except json.JSONDecodeError as exc:
                    raise AIScoringError(
                        "AI returned invalid JSON. Response may be truncated by OPENAI_MAX_COMPLETION_TOKENS.",
                        usage=usage,
                    ) from exc
                validated = validate_scoring_payload(payload)
                validated.update(usage)
                return validated
            except Exception as exc:  # intentionally broad for external API errors
                if isinstance(exc, AIScoringError) and last_usage:
                    exc = AIScoringError(str(exc), usage=last_usage)
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

        if isinstance(last_error, AIScoringError):
            raise last_error
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

    def _normalize_symbol(value: Any) -> str:
        text = str(value or "").strip().upper()
        allowed = "".join(ch for ch in text if ch.isalnum() or ch in {".", "-"})
        return allowed[:24]

    mentioned_symbols: list[str] = []
    seen_symbols: set[str] = set()
    raw_mentioned = normalized_payload.get("mentioned_symbols")
    if isinstance(raw_mentioned, list):
        for value in raw_mentioned:
            token = _normalize_symbol(value)
            if not token or token in seen_symbols:
                continue
            seen_symbols.add(token)
            mentioned_symbols.append(token)
            if len(mentioned_symbols) >= 12:
                break

    main_symbol = _normalize_symbol(normalized_payload.get("main_symbol", ""))
    if main_symbol and main_symbol not in seen_symbols:
        mentioned_symbols.insert(0, main_symbol)
    if len(mentioned_symbols) > 12:
        mentioned_symbols = mentioned_symbols[:12]

    relevance_score = int(normalized_payload["relevance_score"])
    relevance_score = max(0, min(100, relevance_score))

    return {
        "summary": summary,
        "impact_score": int(normalized_payload["impact_score"]),
        "impact_direction": str(normalized_payload["impact_direction"]).strip().lower(),
        "seriousness_score": int(normalized_payload["seriousness_score"]),
        "confidence": int(normalized_payload["confidence"]),
        "impact_horizon": str(normalized_payload["impact_horizon"]),
        "reason_tags": list(normalized_payload["reason_tags"]),
        "is_material_news": bool(normalized_payload["is_material_news"]),
        "main_symbol": main_symbol,
        "mentioned_symbols": mentioned_symbols,
        "relevance_score": relevance_score,
    }


def build_failed_score(
    error_message: str,
    usage: Mapping[str, int | None] | None = None,
) -> dict[str, Any]:
    usage_map = dict(usage or {})
    return {
        "summary": "",
        "impact_score": 0,
        "impact_direction": "neutral",
        "seriousness_score": 0,
        "confidence": 0,
        "impact_horizon": "short_term",
        "reason_tags": ["other"],
        "is_material_news": False,
        "main_symbol": "",
        "mentioned_symbols": [],
        "relevance_score": 0,
        "error_message": error_message,
        "scored_ts_utc": datetime.now(timezone.utc).isoformat(),
        "prompt_tokens": usage_map.get("prompt_tokens"),
        "completion_tokens": usage_map.get("completion_tokens"),
        "total_tokens": usage_map.get("total_tokens"),
        "reasoning_tokens": usage_map.get("reasoning_tokens"),
    }
