from __future__ import annotations

import html
import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from newstrade.config import load_config
from newstrade.db import connect_db, get_latest_scan_run_ids, init_db
from newstrade.reporting import build_report_dataframe


st.set_page_config(page_title="Newstrade Dashboard", page_icon="N", layout="centered")

st.markdown(
    """
    <style>
    .block-container {
        max-width: 720px;
        padding-top: 1.25rem;
        padding-bottom: 7rem;
    }
    .app-shell {
        display: flex;
        flex-direction: column;
        gap: 0.9rem;
    }
    .symbol-card,
    .article-card {
        background: #ffffff;
        border: 1px solid #d7dfeb;
        border-radius: 18px;
        padding: 1rem;
        box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
    }
    .card-title {
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #667085;
        margin-bottom: 0.9rem;
    }
    .metric-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 1rem;
        padding: 0.68rem 0;
        border-top: 1px solid #eef2f7;
    }
    .metric-row:first-of-type {
        border-top: none;
        padding-top: 0;
    }
    .metric-label {
        font-size: 0.95rem;
        color: #475467;
    }
    .metric-value {
        font-size: 1rem;
        font-weight: 700;
        color: #101828;
        text-align: right;
        word-break: break-word;
    }
    .symbol-value {
        font-size: 1.3rem;
        letter-spacing: 0.02em;
    }
    .pct-pill {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 999px;
        padding: 0.35rem 0.75rem;
        border: 1px solid transparent;
        min-width: 6.5rem;
    }
    .article-title {
        font-size: 1.05rem;
        font-weight: 700;
        color: #101828;
        margin-bottom: 0.8rem;
        line-height: 1.4;
    }
    .article-row {
        margin-top: 0.55rem;
        color: #475467;
        line-height: 1.45;
        word-break: break-word;
    }
    .article-label {
        font-weight: 700;
        color: #344054;
    }
    .article-link a {
        color: #175cd3;
        text-decoration: none;
    }
    .article-link a:hover {
        text-decoration: underline;
    }
    .nav-caption {
        color: #667085;
        font-size: 0.9rem;
        text-align: center;
        margin-top: 0.15rem;
        margin-bottom: 0.85rem;
    }
    .sticky-footer-nav {
        position: fixed;
        left: 50%;
        bottom: 0.75rem;
        transform: translateX(-50%);
        width: min(720px, calc(100vw - 1.7rem));
        z-index: 20;
        padding: 0.7rem;
        border: 1px solid rgba(215, 223, 235, 0.95);
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.94);
        box-shadow: 0 10px 28px rgba(15, 23, 42, 0.12);
        backdrop-filter: blur(10px);
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.75rem;
    }
    .sticky-footer-nav form {
        margin: 0;
    }
    .sticky-footer-nav-button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 100%;
        min-height: 3rem;
        border-radius: 14px;
        border: 1px solid #d0d5dd;
        background: #ffffff;
        color: #101828;
        text-decoration: none;
        font-weight: 600;
    }
    .sticky-footer-nav-button:hover {
        border-color: #98a2b3;
        background: #f8fafc;
    }
    .sticky-footer-nav-button.is-disabled {
        color: #98a2b3;
        background: #f9fafb;
        pointer-events: none;
        cursor: default;
    }
    .sticky-footer-spacer {
        min-height: 3rem;
    }
    @media (max-width: 640px) {
        .block-container {
            padding-left: 0.85rem;
            padding-right: 0.85rem;
            padding-bottom: 7.5rem;
        }
        .symbol-card,
        .article-card {
            border-radius: 16px;
            padding: 0.9rem;
        }
        .metric-row {
            align-items: flex-start;
            flex-direction: column;
            gap: 0.35rem;
        }
        .metric-value {
            text-align: left;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_symbol_detail(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> pd.DataFrame:
    sql = """
        SELECT
            published_ts_utc,
            title,
            url
        FROM news_articles
        WHERE scan_run_id = ? AND symbol = ?
        ORDER BY published_ts_utc DESC, article_id DESC
    """
    return pd.read_sql_query(sql, conn, params=(scan_run_id, symbol))


def _coerce_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_decimal(value: object, digits: int = 1) -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:.{digits}f}".rstrip("0").rstrip(".")


def _format_pct(value: object) -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return "n/a"
    return f"{numeric:+.2f}%".replace("+", "")


def _format_timestamp(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "Unknown"

    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return str(value)

    if hasattr(parsed, "to_pydatetime"):
        dt_value = parsed.to_pydatetime()
    elif isinstance(parsed, datetime):
        dt_value = parsed
    else:
        return str(value)

    return dt_value.strftime("%Y-%m-%d %H:%M")


def _pct_change_style(value: object, max_abs_change: float) -> tuple[str, str, str]:
    numeric = _coerce_float(value)
    if numeric is None:
        return "#f5f7fa", "#d0d5dd", "#344054"

    scale_base = max(max_abs_change, 1.0)
    intensity = min(abs(numeric) / scale_base, 1.0)
    alpha = 0.12 + (0.34 * intensity)

    if numeric < 0:
        return f"rgba(220, 38, 38, {alpha:.2f})", "#dc2626", "#7f1d1d"
    if numeric > 0:
        return f"rgba(22, 163, 74, {alpha:.2f})", "#16a34a", "#14532d"
    return "#f5f7fa", "#d0d5dd", "#344054"


def _get_query_param_int(name: str) -> int | None:
    raw = st.query_params.get(name)
    if raw is None or raw == "":
        return None
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if raw is None:
        return None
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return None


def _clamp_index(value: int | None, size: int) -> int:
    if size <= 0:
        return 0
    if value is None:
        return 0
    return max(0, min(value, size - 1))


def _render_symbol_card(row: pd.Series, max_abs_change: float) -> None:
    pct_background, pct_border, pct_text = _pct_change_style(row.get("pct_change_1d"), max_abs_change)
    reason_tags = str(row.get("top_reason_tags") or "").strip() or "n/a"

    card_html = f"""
    <div class="symbol-card">
        <div class="card-title">Symbol Snapshot</div>
        <div class="metric-row">
            <div class="metric-label">Symbol</div>
            <div class="metric-value symbol-value">{html.escape(str(row.get("symbol", "n/a")))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">pct_change_1d</div>
            <div class="metric-value pct-pill" style="background:{pct_background};border-color:{pct_border};color:{pct_text};">
                {html.escape(_format_pct(row.get("pct_change_1d")))}
            </div>
        </div>
        <div class="metric-row">
            <div class="metric-label">weighted_impact_score</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("weighted_impact_score"), 1))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">weighted_seriousness_score</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("weighted_seriousness_score"), 1))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">avg_relevance_score</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("avg_relevance_score"), 1))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">reason_tags</div>
            <div class="metric-value">{html.escape(reason_tags)}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">article_count</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("article_count"), 0))}</div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)


def _render_articles(detail_df: pd.DataFrame) -> None:
    if detail_df.empty:
        st.info("No news articles found for this symbol.")
        return

    for _, article in detail_df.iterrows():
        url = str(article.get("url") or "").strip()
        safe_url = html.escape(url, quote=True)
        link_html = (
            f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer">{safe_url}</a>'
            if safe_url
            else "n/a"
        )

        article_html = f"""
        <div class="article-card">
            <div class="card-title">News</div>
            <div class="article-title">{html.escape(str(article.get("title") or "Untitled"))}</div>
            <div class="article-row">
                <span class="article-label">Published:</span>
                {html.escape(_format_timestamp(article.get("published_ts_utc")))}
            </div>
            <div class="article-row article-link">
                <span class="article-label">URL:</span>
                {link_html}
            </div>
        </div>
        """
        st.markdown(article_html, unsafe_allow_html=True)


def main() -> None:
    st.title("Newstrade")
    st.caption("Mobile-first view of abnormal movers and the news driving them.")

    config = load_config()
    conn = connect_db(config.db_path)
    init_db(conn)

    run_ids = get_latest_scan_run_ids(conn)
    if not run_ids:
        st.info("No scan runs found yet. Use CLI `scan` then `news` and `score`.")
        conn.close()
        return

    query_run = _get_query_param_int("run")
    initial_run = query_run if query_run in run_ids else run_ids[0]
    selected_run = st.selectbox("Run", run_ids, index=run_ids.index(initial_run))

    df = build_report_dataframe(conn, selected_run)
    if df.empty:
        st.warning("No symbol scores for this run yet. Execute CLI `score` command.")
        conn.close()
        return

    pct_change_numeric = pd.to_numeric(df["pct_change_1d"], errors="coerce")
    filtered = (
        df.assign(
            _abs_pct_change=pct_change_numeric.abs().fillna(0),
            _pct_change_raw=pct_change_numeric.fillna(0),
        )
        .sort_values(by=["_abs_pct_change", "_pct_change_raw"], ascending=[False, True])
        .drop(columns=["_abs_pct_change", "_pct_change_raw"])
        .reset_index(drop=True)
    )

    symbols = filtered["symbol"].astype(str).tolist()
    query_symbol_index = _get_query_param_int("symbol_index")
    selected_index = _clamp_index(query_symbol_index, len(symbols))

    if len(symbols) == 1:
        selected_symbol = symbols[0]
        selected_index = 0
        st.caption("Browse symbols")
        st.markdown(
            f'<div class="nav-caption">Only symbol in this run: {html.escape(selected_symbol)}</div>',
            unsafe_allow_html=True,
        )
    else:
        selected_symbol = st.select_slider("Browse symbols", options=symbols, value=symbols[selected_index])
        selected_index = symbols.index(selected_symbol)
        st.markdown(
            f'<div class="nav-caption">Symbol {selected_index + 1} of {len(symbols)}</div>',
            unsafe_allow_html=True,
        )

    if query_run != selected_run or query_symbol_index != selected_index:
        st.query_params["run"] = str(selected_run)
        st.query_params["symbol_index"] = str(selected_index)

    selected_row = filtered.iloc[selected_index]
    max_abs_change = float(pd.to_numeric(filtered["pct_change_1d"], errors="coerce").abs().fillna(0).max())

    _render_symbol_card(selected_row, max_abs_change)
    detail_df = load_symbol_detail(conn, selected_run, selected_symbol)
    _render_articles(detail_df)

    previous_index = max(selected_index - 1, 0)
    next_index = min(selected_index + 1, len(symbols) - 1)
    previous_class = "sticky-footer-nav-button is-disabled" if selected_index == 0 else "sticky-footer-nav-button"
    next_class = "sticky-footer-nav-button is-disabled" if selected_index == len(symbols) - 1 else "sticky-footer-nav-button"
    previous_disabled = "disabled" if selected_index == 0 else ""
    next_disabled = "disabled" if selected_index == len(symbols) - 1 else ""
    footer_html = f"""
    <div class="sticky-footer-nav" aria-label="Symbol navigation">
        <form method="get">
            <input type="hidden" name="run" value="{selected_run}">
            <input type="hidden" name="symbol_index" value="{previous_index}">
            <button type="submit" class="{previous_class}" {previous_disabled}>Previous</button>
        </form>
        <form method="get">
            <input type="hidden" name="run" value="{selected_run}">
            <input type="hidden" name="symbol_index" value="{next_index}">
            <button type="submit" class="{next_class}" {next_disabled}>Next</button>
        </form>
    </div>
    <div class="sticky-footer-spacer" aria-hidden="true"></div>
    """
    st.markdown(footer_html, unsafe_allow_html=True)

    conn.close()


if __name__ == "__main__":
    main()
