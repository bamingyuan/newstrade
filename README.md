# Newstrade

Newstrade is a Python app for investigating unusual stock moves.

It uses Massive daily market summaries to rank the biggest prior-close to close movers across the U.S. stock market, collects recent news from Yahoo Finance RSS, scores each article with OpenAI, stores the results in SQLite, and lets you review the run in the CLI or a small Streamlit dashboard.

## What It Does

- pulls the full U.S. stock daily market summary from Massive
- compares the latest completed session close to the previous trading session close
- filters and ranks symbols by absolute daily move
- fetches recent news only for the top selected movers
- scores each article for impact, seriousness, confidence, and relevance
- aggregates article scores into symbol-level rankings
- exports results to CSV and shows them in a read-only dashboard

## Setup

Requirements:

- Python 3.10+
- a Massive API key for market data [https://massive.com/](https://massive.com/)
- an OpenAI API key for the `score` step [https://platform.openai.com/api-keys](https://platform.openai.com/api-keys)

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
cp .env.example .env
```

## CLI Workflow

The pipeline is designed as separate stages:

```bash
newstrade scan
newstrade news
newstrade score
newstrade report --top 30 (Optional)
newstrade export --format csv (Optional)
```

What each command does:

- `scan` loads the latest completed Massive daily market summary, compares it to the previous trading session, filters the universe, and selects the top movers for news
- `news` fetches recent articles for the selected symbols
- `score` sends articles to OpenAI and stores structured scores
- `report` prints the ranked symbol summary in the terminal
- `export` writes a CSV report to `CSV_EXPORT_DIR`

`news`, `score`, `report`, and `export` automatically use the latest `scan_run_id` if you do not pass `--scan-run-id`.

You can also run the whole pipeline in one command:

```bash
newstrade run-all --top 30
```

To export the latest agent JSON as part of the same run:

```bash
newstrade run-all --top 30 --export-agent-json
```

If you want one machine-readable file for an AI agent, export the latest run as JSON:

```bash
newstrade export-agent-json
```

By default this writes:

```text
exports/agent_latest.json
```

You can also choose a custom file path:

```bash
newstrade export-agent-json --output /path/to/agent_latest.json
```

## Dashboard

Start the Streamlit dashboard with:

```bash
python -m streamlit run newstrade/dashboard/app.py
```

The dashboard is read-only and lets you browse recent runs, symbol-level scores, and article details.

## Important `.env` Settings

You do not need to tweak every variable. These are the ones to understand first:

- `MASSIVE_API_KEY` is required for `newstrade scan`.
- `MAX_NEWS_SYMBOLS_PER_RUN` controls how many filtered movers continue into `news` and `score`.
- `OPENAI_API_KEY` is required for `newstrade score`.
- `OPENAI_MODEL` controls which model scores the articles.
- `DB_PATH` is the SQLite database path.
- `CSV_EXPORT_DIR` is where CSV exports are written.

Helpful optional settings:

- `YAHOO_RSS_ALLOWED_DOMAINS` limits accepted Yahoo RSS article domains.
- `MIN_PCT_CHANGE`, `MIN_PRICE`, `MAX_PRICE`, `MIN_VOLUME`, and `MAX_VOLUME` control mover filters.
- `SCAN_TIME_TRAVEL=1` with `SCAN_AS_OF_DATE=YYYY-MM-DD` lets you replay a past session date.

