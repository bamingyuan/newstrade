# Newstrade v1

Newstrade is a beginner-friendly Python app that answers:

- Which stocks moved abnormally today?
- What likely caused it?
- How serious is the news?

It uses:

- IBKR for price movement scanning
- Yahoo Finance RSS + Massive (Polygon) for symbol news
- OpenAI for structured sentiment/severity scoring
- SQLite for storage
- Streamlit for a mobile-friendly dashboard

## 1) Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

Copy `.env.example` to `.env` and set your values:

```bash
copy .env.example .env
```

## 2) CLI workflow

Run one stage at a time:

```bash
newstrade scan --window 1d --mode both
newstrade news
newstrade score
newstrade report --top 30
newstrade export --format csv
```

`news`, `score`, `report`, and `export` automatically use the latest `scan_run_id` when `--scan-run-id` is omitted.

Or run the whole pipeline:

```bash
newstrade run-all --window 1d --mode both --top 30
```

## 3) Dashboard

```bash
python -m streamlit run newstrade/dashboard/app.py
```

The dashboard is read-only and shows:

- ranked symbols by seriousness
- impact and seriousness charts
- article-level details and AI summaries
- filtered CSV download

## 4) Database tables

- `scan_runs`
- `symbols_snapshot`
- `news_articles`
- `article_scores`
- `symbol_scores`
- `exports_log`

## 5) Notes

- Timestamps are stored in UTC.
- If OpenAI fails for an article, the pipeline stores a fallback neutral score and continues.
- Impact contract is deterministic: `impact_direction` is one of `bearish|neutral|bullish`, and stored `impact_score` is normalized to match direction (`bearish<0`, `neutral=0`, `bullish>0`) on a `-100..100` scale.
- Magnitude guide used in scoring prompt: `1-20 mild`, `21-50 moderate`, `51-80 strong`, `81-100 extreme`.
- `newstrade score ...` now prints per-article token usage (`prompt`, `completion`, `total`, `reasoning`) for hotspot analysis.
- Tune OpenAI output/cost behavior with `.env`: `OPENAI_MAX_COMPLETION_TOKENS` and `OPENAI_SCORE_RETRIES`.
- For reasoning-heavy models (for example `gpt-5-mini`), `OPENAI_MAX_COMPLETION_TOKENS` also covers reasoning tokens. If set too low, the model may return empty content.
- Set `SCAN_TIME_TRAVEL=1` and `SCAN_AS_OF_DATE=YYYY-MM-DD` to test scans/news against a past date (interpreted as US close, 16:00 `America/New_York`).
- When time travel is enabled, scan mode must be `env` (`newstrade scan --mode env ...`), otherwise the scan fails with a clear message.
- `newstrade news` merges Yahoo RSS and Massive results when `MASSIVE_API_KEY` is set, and deduplicates by canonical article URL.
- Set `MASSIVE_NEWS=0` to disable Massive API calls entirely, even if `MASSIVE_API_KEY` is present.
- Massive is optional; if `MASSIVE_API_KEY` is empty, the pipeline uses Yahoo only.
- For Massive free-tier usage, keep `MASSIVE_MAX_CALLS_PER_MINUTE=5`.
- For 2-week historical backfill, set `NEWS_LOOKBACK_HOURS=336`.
- `symbols_snapshot.price_source_ts_utc` records when price data was fetched; `symbols_snapshot.price_as_of_ts_utc` records the market as-of timestamp used for that snapshot.
- `news_articles.summary` stores provider summaries when available (for example Massive `description`), and is included in AI scoring context.
- On weekends/holidays, scan continues and may use the previous trading session for some symbols. A warning is added to the run notes.
- Set `MARKET_CAP=0` in `.env` to skip market-cap API calls entirely. In that mode, market-cap filters are disabled and `symbols_snapshot.market_cap` stays empty.
- If market-cap filters are active and cap data is unavailable for a symbol, that symbol is filtered out.

## 6) Tests

```bash
pytest -q
```
