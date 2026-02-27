# Newstrade v1

Newstrade is a beginner-friendly Python app that answers:

- Which stocks moved abnormally today?
- What likely caused it?
- How serious is the news?

It uses:

- IBKR for price movement scanning
- Yahoo Finance RSS for symbol news
- OpenAI for structured sentiment/severity scoring
- SQLite for storage
- Streamlit for a mobile-friendly dashboard

## 1) Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and set your values:

```bash
copy .env.example .env
```

## 2) CLI workflow

Run one stage at a time:

```bash
python -m newstrade.cli scan --window 1d --mode both
python -m newstrade.cli news --scan-run-id 1
python -m newstrade.cli score --scan-run-id 1
python -m newstrade.cli report --scan-run-id 1 --top 30
python -m newstrade.cli export --scan-run-id 1 --format csv
```

Or run the whole pipeline:

```bash
python -m newstrade.cli run-all --window 1d --mode both --top 30
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
- If market-cap filters are active and cap data is unavailable for a symbol, that symbol is filtered out.

## 6) Tests

```bash
pytest -q
```
