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
    [data-testid="stAppDeployButton"] {
        display: none;
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
    .metric-label-with-help {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        flex-wrap: wrap;
    }
    .metric-help {
        position: relative;
        display: inline-block;
    }
    .metric-help summary {
        list-style: none;
        width: 1.1rem;
        height: 1.1rem;
        border-radius: 999px;
        border: 1px solid #98a2b3;
        color: #667085;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.72rem;
        font-weight: 700;
        cursor: pointer;
        background: #ffffff;
    }
    .metric-help summary::-webkit-details-marker {
        display: none;
    }
    .metric-help[open] summary {
        border-color: #475467;
        color: #344054;
    }
    .metric-help-box {
        position: absolute;
        top: 1.55rem;
        left: 0;
        width: min(16rem, 72vw);
        padding: 0.65rem 0.75rem;
        border-radius: 12px;
        border: 1px solid #d0d5dd;
        background: #ffffff;
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.12);
        color: #344054;
        font-size: 0.82rem;
        line-height: 1.45;
        z-index: 5;
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
    .article-meta {
        margin-bottom: 0.75rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.45rem;
    }
    .article-score-pill {
        display: inline-flex;
        align-items: center;
        padding: 0.32rem 0.6rem;
        border-radius: 999px;
        border: 1px solid #d0d5dd;
        background: #f2f4f7;
        color: #344054;
        font-size: 0.84rem;
        font-weight: 600;
        line-height: 1.2;
    }
    .article-summary {
        margin-bottom: 0.8rem;
        color: #344054;
        line-height: 1.55;
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
    .sticky-footer-shell {
        position: fixed;
        left: 50%;
        bottom: 0.75rem;
        transform: translateX(-50%);
        width: min(720px, calc(100vw - 1.7rem));
        z-index: 20;
        padding: 0.7rem;
        border: 1px solid #d7dfeb;
        border-radius: 18px;
        background: #ffffff;
        box-shadow: 0 10px 28px rgba(15, 23, 42, 0.12);
    }
    div.st-key-sticky_nav_prev,
    div.st-key-sticky_nav_next {
        position: fixed;
        bottom: 1.45rem;
        width: calc(min(720px, calc(100vw - 1.7rem)) / 2 - 1.075rem);
        z-index: 21;
    }
    div.st-key-sticky_nav_prev {
        left: calc(50% - min(720px, calc(100vw - 1.7rem)) / 2 + 0.7rem);
    }
    div.st-key-sticky_nav_next {
        right: calc(50% - min(720px, calc(100vw - 1.7rem)) / 2 + 0.7rem);
    }
    div.st-key-sticky_nav_prev button,
    div.st-key-sticky_nav_next button {
        min-height: 3rem;
        border-radius: 14px;
        border: 1px solid #d0d5dd;
        background: #ffffff;
        color: #101828;
        box-shadow: none;
        opacity: 1;
    }
    div.st-key-sticky_nav_prev button:hover,
    div.st-key-sticky_nav_next button:hover {
        border-color: #98a2b3;
        background: #f8fafc;
        color: #101828;
    }
    div.st-key-sticky_nav_prev button:disabled,
    div.st-key-sticky_nav_next button:disabled {
        border-color: #d0d5dd;
        background: #f9fafb;
        color: #98a2b3;
        opacity: 1;
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
        div.st-key-sticky_nav_prev,
        div.st-key-sticky_nav_next {
            width: calc((100vw - 1.7rem - 1.4rem - 0.75rem) / 2);
        }
        div.st-key-sticky_nav_prev {
            left: 1.55rem;
        }
        div.st-key-sticky_nav_next {
            right: 1.55rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False, ttl=30)
def load_run_ids_cached(db_path: str) -> list[int]:
    conn = connect_db(db_path)
    init_db(conn)
    try:
        return get_latest_scan_run_ids(conn)
    finally:
        conn.close()


@st.cache_data(show_spinner=False, ttl=30)
def load_report_dataframe_cached(db_path: str, scan_run_id: int) -> pd.DataFrame:
    conn = connect_db(db_path)
    init_db(conn)
    try:
        return build_report_dataframe(conn, scan_run_id)
    finally:
        conn.close()


@st.cache_data(show_spinner=False, ttl=30)
def load_symbol_detail_cached(db_path: str, scan_run_id: int, symbol: str) -> pd.DataFrame:
    conn = connect_db(db_path)
    init_db(conn)
    try:
        return load_symbol_detail(conn, scan_run_id, symbol)
    finally:
        conn.close()


def load_symbol_detail(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> pd.DataFrame:
    sql = """
        SELECT
            n.published_ts_utc,
            n.title,
            n.url,
            COALESCE(a.summary, n.summary) AS summary,
            a.relevance_score,
            a.seriousness_score,
            a.confidence
        FROM news_articles n
        LEFT JOIN article_scores a
          ON a.article_id = n.article_id
        WHERE n.scan_run_id = ? AND n.symbol = ?
        ORDER BY n.published_ts_utc DESC, n.article_id DESC
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


def _score_pill(label: str, value: object, max_value: float = 100.0) -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        background = "#f2f4f7"
        border = "#d0d5dd"
        color = "#344054"
        rendered_value = "n/a"
    else:
        normalized = max(0.0, min(numeric, max_value))
        rendered_value = str(int(round(normalized)))
        if normalized < 34:
            background = "#f2f4f7"
            border = "#d0d5dd"
            color = "#344054"
        elif normalized < 67:
            background = "#ffedd5"
            border = "#fb923c"
            color = "#9a3412"
        else:
            background = "#dcfce7"
            border = "#4ade80"
            color = "#166534"

    return (
        '<span class="article-score-pill" '
        f'style="background:{background};border-color:{border};color:{color};">'
        f"{html.escape(label)}: {html.escape(rendered_value)}"
        "</span>"
    )


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


def _sync_selected_index(index_state_key: str, slider_state_key: str, symbols: list[str]) -> None:
    current_symbol = st.session_state.get(slider_state_key)
    if current_symbol in symbols:
        st.session_state[index_state_key] = symbols.index(current_symbol)


def _step_selected_index(index_state_key: str, slider_state_key: str, symbols: list[str], delta: int) -> None:
    if not symbols:
        return
    current_index = st.session_state.get(index_state_key, 0)
    if not isinstance(current_index, int):
        current_index = 0
    next_index = max(0, min(current_index + delta, len(symbols) - 1))
    st.session_state[index_state_key] = next_index
    st.session_state[slider_state_key] = symbols[next_index]


def _metric_label_with_help(label: str, help_text: str) -> str:
    safe_label = html.escape(label)
    safe_help_text = html.escape(help_text)
    return (
        '<span class="metric-label-with-help">'
        f"{safe_label}"
        '<details class="metric-help">'
        f'<summary aria-label="About {safe_label}">i</summary>'
        f'<div class="metric-help-box">{safe_help_text}</div>'
        "</details>"
        "</span>"
    )


def _render_symbol_card(row: pd.Series, max_abs_change: float) -> None:
    pct_background, pct_border, pct_text = _pct_change_style(row.get("pct_change_1d"), max_abs_change)
    reason_tags = str(row.get("top_reason_tags") or "").strip() or "n/a"
    impact_label = _metric_label_with_help(
        "Weighted Impact Score",
        "A weighted view of how strong the expected market effect is across this symbol's news.",
    )
    seriousness_label = _metric_label_with_help(
        "Weighted Seriousness Score",
        "A weighted view of how important and material the recent news appears to be.",
    )
    relevance_label = _metric_label_with_help(
        "Average Relevance Score",
        "The average score for how directly the recent articles relate to this symbol.",
    )

    card_html = f"""
    <div class="symbol-card">
        <div class="card-title">Symbol Snapshot</div>
        <div class="metric-row">
            <div class="metric-label">Symbol</div>
            <div class="metric-value symbol-value">{html.escape(str(row.get("symbol", "n/a")))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">Change 1d</div>
            <div class="metric-value pct-pill" style="background:{pct_background};border-color:{pct_border};color:{pct_text};">
                {html.escape(_format_pct(row.get("pct_change_1d")))}
            </div>
        </div>
        <div class="metric-row">
            <div class="metric-label">{impact_label}</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("weighted_impact_score"), 1))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">{seriousness_label}</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("weighted_seriousness_score"), 1))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">{relevance_label}</div>
            <div class="metric-value">{html.escape(_format_decimal(row.get("avg_relevance_score"), 1))}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">Reason Tags</div>
            <div class="metric-value">{html.escape(reason_tags)}</div>
        </div>
        <div class="metric-row">
            <div class="metric-label">Article Count</div>
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
        summary = str(article.get("summary") or "").strip()
        relevance_raw = article.get("relevance_score")
        seriousness_raw = article.get("seriousness_score")
        confidence_raw = article.get("confidence")
        safe_url = html.escape(url, quote=True)
        link_html = (
            f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer">{safe_url}</a>'
            if safe_url
            else "n/a"
        )
        summary_html = f'<div class="article-summary">{html.escape(summary)}</div>' if summary else ""
        meta_html = (
            f'<div class="article-meta">'
            f'{_score_pill("Relevance", relevance_raw)}'
            f'{_score_pill("Seriousness", seriousness_raw)}'
            f'{_score_pill("Confidence", confidence_raw)}'
            f"</div>"
        )

        article_html = f"""
        <div class="article-card">
            <div class="article-title">{html.escape(str(article.get("title") or "Untitled"))}</div>
            {meta_html}
            {summary_html}
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

    config = load_config()
    run_ids = load_run_ids_cached(config.db_path)
    if not run_ids:
        st.info("No scan runs found yet. Use CLI `scan` then `news` and `score`.")
        return

    selected_run = st.selectbox("Run", run_ids)

    df = load_report_dataframe_cached(config.db_path, selected_run)
    if df.empty:
        st.warning("No scored symbols found for this run. Run `news` and `score`, or check whether any articles were collected.")
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
    index_state_key = f"selected_symbol_index_{selected_run}"
    slider_state_key = f"selected_symbol_slider_{selected_run}"
    selected_index = st.session_state.get(index_state_key, 0)
    if not isinstance(selected_index, int) or selected_index < 0 or selected_index >= len(symbols):
        selected_index = 0
    st.session_state[index_state_key] = selected_index

    if len(symbols) == 1:
        selected_symbol = symbols[0]
        selected_index = 0
        st.caption("Browse symbols")
        st.markdown(
            f'<div class="nav-caption">Only symbol in this run: {html.escape(selected_symbol)}</div>',
            unsafe_allow_html=True,
        )
    else:
        if st.session_state.get(slider_state_key) not in symbols:
            st.session_state[slider_state_key] = symbols[selected_index]
        selected_symbol = st.select_slider(
            "Browse symbols",
            options=symbols,
            key=slider_state_key,
            on_change=_sync_selected_index,
            args=(index_state_key, slider_state_key, symbols),
        )
        selected_index = symbols.index(selected_symbol)
        st.markdown(
            f'<div class="nav-caption">Symbol {selected_index + 1} of {len(symbols)}</div>',
            unsafe_allow_html=True,
        )

    selected_row = filtered.iloc[selected_index]
    max_abs_change = float(pd.to_numeric(filtered["pct_change_1d"], errors="coerce").abs().fillna(0).max())

    _render_symbol_card(selected_row, max_abs_change)
    detail_df = load_symbol_detail_cached(config.db_path, selected_run, selected_symbol)
    _render_articles(detail_df)

    st.markdown('<div class="sticky-footer-shell" aria-hidden="true"></div>', unsafe_allow_html=True)
    st.button(
        "Previous",
        key="sticky_nav_prev",
        use_container_width=True,
        disabled=selected_index == 0,
        on_click=_step_selected_index,
        args=(index_state_key, slider_state_key, symbols, -1),
    )
    st.button(
        "Next",
        key="sticky_nav_next",
        use_container_width=True,
        disabled=selected_index == len(symbols) - 1,
        on_click=_step_selected_index,
        args=(index_state_key, slider_state_key, symbols, 1),
    )


if __name__ == "__main__":
    main()
