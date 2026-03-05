from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from newstrade.config import load_config
from newstrade.db import connect_db, get_latest_scan_run_ids, init_db
from newstrade.reporting import build_report_dataframe


st.set_page_config(page_title="Newstrade Dashboard", page_icon="??", layout="wide")

st.markdown(
    """
    <style>
    .kpi-card {
        background: #f6f8fa;
        border: 1px solid #d0d7de;
        border-radius: 10px;
        padding: 10px 12px;
    }
    @media (max-width: 900px) {
        .block-container {
            padding-left: 1rem;
            padding-right: 1rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_symbol_detail(conn: sqlite3.Connection, scan_run_id: int, symbol: str) -> pd.DataFrame:
    sql = """
        SELECT
            n.published_ts_utc,
            n.title,
            n.source,
            n.url,
            a.summary,
            a.impact_score,
            a.impact_direction,
            a.seriousness_score,
            a.confidence,
            a.reason_tags_json,
            a.main_symbol,
            a.mentioned_symbols_json,
            a.relevance_score,
            a.error_message
        FROM news_articles n
        LEFT JOIN article_scores a
          ON a.article_id = n.article_id
        WHERE n.scan_run_id = ? AND n.symbol = ?
        ORDER BY n.published_ts_utc DESC
    """
    return pd.read_sql_query(sql, conn, params=(scan_run_id, symbol))


def main() -> None:
    st.title("Newstrade")
    st.caption("Abnormal movers + likely news impact and seriousness")

    config = load_config()
    conn = connect_db(config.db_path)
    init_db(conn)

    run_ids = get_latest_scan_run_ids(conn)
    if not run_ids:
        st.info("No scan runs found yet. Use CLI `scan` then `news` and `score`.")
        conn.close()
        return

    selected_run = st.selectbox("Scan Run", run_ids)

    df = build_report_dataframe(conn, selected_run)
    if df.empty:
        st.warning("No symbol scores for this run yet. Execute CLI `score` command.")
        conn.close()
        return

    col1, col2, col3, col4 = st.columns(4)
    seriousness_min = col1.slider("Min Seriousness", 0, 100, 0)
    direction = col2.selectbox("Impact Direction", ["all", "bullish", "bearish", "neutral"], index=0)
    window_view = col3.selectbox("Window", ["1d", "intraday"], index=0)
    symbol_search = col4.text_input("Symbol Search", "").strip().upper()

    filtered = df.copy()
    filtered = filtered[filtered["weighted_seriousness_score"] >= seriousness_min]

    if direction != "all":
        filtered = filtered[filtered["bullish_bearish_label"] == direction]

    if symbol_search:
        filtered = filtered[filtered["symbol"].str.contains(symbol_search, na=False)]

    filtered = filtered.sort_values(
        by=["weighted_seriousness_score", "weighted_impact_score"],
        ascending=[False, False],
        key=lambda series: series.abs() if series.name == "weighted_impact_score" else series,
    )

    move_column = "pct_change_1d" if window_view == "1d" else "pct_change_intraday"

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Movers", int(filtered.shape[0]))
    k2.metric("Avg Seriousness", round(float(filtered["weighted_seriousness_score"].mean()), 2) if not filtered.empty else 0)
    k3.metric("Bullish", int((filtered["bullish_bearish_label"] == "bullish").sum()))
    k4.metric("Bearish", int((filtered["bullish_bearish_label"] == "bearish").sum()))

    st.subheader("Ranked Symbols")
    st.dataframe(
        filtered[
            [
                "symbol",
                move_column,
                "weighted_impact_score",
                "weighted_seriousness_score",
                "avg_relevance_score",
                "article_count",
                "bullish_bearish_label",
                "top_reason_tags",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    csv_bytes = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download Filtered CSV",
        csv_bytes,
        file_name=f"newstrade_dashboard_run_{selected_run}.csv",
        mime="text/csv",
    )

    st.subheader("Visuals")
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        seriousness_chart = filtered.set_index("symbol")[["weighted_seriousness_score"]]
        st.bar_chart(seriousness_chart)
    with chart_col2:
        impact_chart = filtered.set_index("symbol")[["weighted_impact_score"]]
        st.bar_chart(impact_chart)

    st.subheader("Symbol Details")
    symbols = filtered["symbol"].tolist()
    if symbols:
        selected_symbol = st.selectbox("Select Symbol", symbols)
        detail_df = load_symbol_detail(conn, selected_run, selected_symbol)
        st.dataframe(detail_df, use_container_width=True, hide_index=True)
    else:
        st.info("No symbols match current filters.")

    conn.close()


if __name__ == "__main__":
    main()
