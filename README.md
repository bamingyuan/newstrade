# Newstrade

Newstrade is a Python app for investigating unusual stock moves.

It scans US stocks with IBKR market data, collects recent news from Yahoo Finance RSS and optionally Massive (Polygon), scores each article with OpenAI, stores the results in SQLite, and lets you review the run in the CLI or a small Streamlit dashboard.

## What It Does

- finds symbols that moved enough to be interesting
- collects recent news for those symbols
- scores each article for impact, seriousness, confidence, and relevance
- aggregates article scores into symbol-level rankings
- exports results to CSV and shows them in a read-only dashboard

## Setup

Requirements:

- Python 3.10+
- IB Gateway or TWS running locally for the scan step
- an OpenAI API key for the `score` step
- optionally, a Massive API key if you want a second news source

After cloning the repository:

```bash
python -m venv .venv
```

Activate the virtual environment:

```bash
# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

Install the app with `pip`:

```bash
python -m pip install --upgrade pip
python -m pip install .
```

If you also want the test dependency:

```bash
python -m pip install ".[dev]"
```

Create your local config file:

```bash
copy .env.example .env
```

On macOS or Linux:

```bash
cp .env.example .env
```

## CLI Workflow

The pipeline is designed as separate stages:

```bash
newstrade scan --window 1d --mode both
newstrade news
newstrade score
newstrade report --top 30
newstrade export --format csv
```

What each command does:

- `scan` resolves symbols and stores price snapshots
- `news` fetches recent articles for the passed symbols
- `score` sends articles to OpenAI and stores structured scores
- `report` prints the ranked symbol summary in the terminal
- `export` writes a CSV report to `CSV_EXPORT_DIR`

`news`, `score`, `report`, and `export` automatically use the latest `scan_run_id` if you do not pass `--scan-run-id`.

You can also run the whole pipeline in one command:

```bash
newstrade run-all --window 1d --mode both --top 30
```

Useful scan options:

- `--window 1d` uses daily percentage change
- `--window intraday` uses intraday percentage change
- `--mode env` uses only the symbols from `.env`
- `--mode ibkr` uses only the IBKR scanner
- `--mode both` combines `.env` symbols and IBKR scanner results

## Dashboard

Start the Streamlit dashboard with:

```bash
python -m streamlit run newstrade/dashboard/app.py
```

The dashboard is read-only and lets you browse recent runs, symbol-level scores, and article details.

## Important `.env` Settings

You do not need to tweak every variable. These are the ones to understand first:

- `SYMBOL_MODE` chooses whether symbols come from `.env`, IBKR, or both.
- `SYMBOLS` is the comma-separated fallback/watchlist used when `SYMBOL_MODE` includes `env`.
- `OPENAI_API_KEY` is required for `newstrade score`.
- `OPENAI_MODEL` controls which model scores the articles.
- `IBKR_HOST`, `IBKR_PORT`, and `IBKR_CLIENT_ID` control the connection to IB Gateway or TWS.
- `DB_PATH` is the SQLite database path.
- `CSV_EXPORT_DIR` is where CSV exports are written.

Helpful optional settings:

- `MASSIVE_API_KEY` enables Massive news in addition to Yahoo RSS.
- `MASSIVE_NEWS=0` disables Massive completely, even if a key is present.
- `YAHOO_RSS_ALLOWED_DOMAINS` limits accepted Yahoo RSS article domains.
- `MIN_PCT_CHANGE`, `MIN_PRICE`, `MAX_PRICE`, `MIN_VOLUME`, and market-cap settings control scan filters.
- `SCAN_TIME_TRAVEL=1` with `SCAN_AS_OF_DATE=YYYY-MM-DD` lets you replay a past date. Time travel only works with `--mode env` or `SYMBOL_MODE=env`.

A few practical notes:

- leaving optional numeric filters empty disables them
- if `MASSIVE_API_KEY` is empty, the app uses Yahoo RSS only
- timestamps are stored in UTC
- the app writes local data to `./data` and exports to `./exports` by default

## Tests

Run the test suite with:

```bash
pytest -q
```
