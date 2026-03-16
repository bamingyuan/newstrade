from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

from newstrade.config import load_config
from newstrade.db import connect_db, get_latest_scan_run_ids, init_db
from newstrade.reporting import build_report_dataframe


st.set_page_config(page_title="Newstrade Dashboard", page_icon="N", layout="centered")


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
            a.confidence,
            n.provider
        FROM news_articles n
        LEFT JOIN article_scores a
          ON a.article_id = n.article_id
        WHERE n.scan_run_id = ? AND n.symbol = ?
        ORDER BY n.published_ts_utc DESC, n.article_id DESC
    """
    return pd.read_sql_query(sql, conn, params=(scan_run_id, symbol))


def _format_pct(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric):+.2f}%"


def _format_price(value: object) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "n/a"
    return f"{float(numeric):.2f}"


def _format_timestamp(value: object) -> str:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return "Unknown"
    if hasattr(parsed, "to_pydatetime"):
        dt_value = parsed.to_pydatetime()
    elif isinstance(parsed, datetime):
        dt_value = parsed
    else:
        return str(value)
    return dt_value.strftime("%Y-%m-%d %H:%M")


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
        st.warning("No scored symbols found for this run. Run `news` and `score` for the selected run.")
        return

    ordered = df.sort_values(by=["rank_abs_pct_change", "weighted_seriousness_score"], ascending=[True, False]).reset_index(drop=True)
    symbols = ordered["symbol"].astype(str).tolist()
    selected_symbol = st.selectbox("Symbol", symbols)
    row = ordered[ordered["symbol"] == selected_symbol].iloc[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Previous Close", _format_price(row.get("previous_close_price")))
    col2.metric("Close", _format_price(row.get("close_price")))
    col3.metric("Move", _format_pct(row.get("pct_change")))

    col4, col5, col6 = st.columns(3)
    col4.metric("Article Count", str(int(row.get("article_count", 0))))
    col5.metric("Impact", _format_price(row.get("weighted_impact_score")))
    col6.metric("Seriousness", _format_price(row.get("weighted_seriousness_score")))

    st.caption(
        f"Trade date {row.get('trade_date')} vs {row.get('previous_trade_date')} | "
        f"Rank #{int(row.get('rank_abs_pct_change', 0) or 0)}"
    )
    st.write(f"Top reason tags: {row.get('top_reason_tags') or 'n/a'}")

    detail_df = load_symbol_detail_cached(config.db_path, selected_run, selected_symbol)
    if detail_df.empty:
        st.info("No news articles found for this symbol.")
        return

    for _, article in detail_df.iterrows():
        with st.container(border=True):
            st.subheader(str(article.get("title") or "Untitled"))
            st.caption(
                f"{article.get('provider', 'unknown')} | "
                f"{_format_timestamp(article.get('published_ts_utc'))}"
            )
            if article.get("summary"):
                st.write(str(article.get("summary")))
            st.write(
                f"Relevance: {article.get('relevance_score', 'n/a')} | "
                f"Seriousness: {article.get('seriousness_score', 'n/a')} | "
                f"Confidence: {article.get('confidence', 'n/a')}"
            )
            url = str(article.get("url") or "").strip()
            if url:
                st.link_button("Open article", url, use_container_width=False)


if __name__ == "__main__":
    main()
