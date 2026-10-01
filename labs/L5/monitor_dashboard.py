"""Live dashboard for the progress CSV written by pyspark_demo.streaming.FileMetricsListener.

    uv run streamlit run labs/L5/monitor_dashboard.py
    uv run streamlit run labs/L5/monitor_dashboard.py -- --csv path/to/progress.csv

Start it before the live pipeline cell of L5_streaming.ipynb (section 8); it rereads the CSV every 2 seconds.
What every chart shows: labs/L5/stream_monitor.md.
"""

import argparse
from pathlib import Path

import pandas as pd
import streamlit as st

REPO = next(p for p in [Path(__file__).resolve(), *Path(__file__).resolve().parents] if (p / "pyproject.toml").exists())
DEFAULT_CSV = REPO / "data" / "l5" / "metrics" / "progress.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--csv", default=str(DEFAULT_CSV), help="progress CSV written by the listener")
args = parser.parse_args()

st.set_page_config(page_title="L5 stream monitor", layout="wide")
st.title("Structured Streaming monitor")

csv_path = Path(st.sidebar.text_input("Progress CSV", args.csv))
refresh = st.sidebar.toggle("Auto-refresh (2 s)", value=True)


def chart(df: pd.DataFrame, value: str, title: str) -> None:
    st.caption(title)
    st.line_chart(df, x="time", y=value, color="query_name")


@st.fragment(run_every="2s" if refresh else None)
def dashboard() -> None:
    if not csv_path.exists():
        st.info(f"Waiting for the notebook to write {csv_path}")
        return

    df = pd.read_csv(csv_path)
    # Plot against time, not batch_id: every query numbers its own batches, from wherever its
    # checkpoint left off, so two queries' batch ids do not line up.
    df["time"] = pd.to_datetime(df["batch_timestamp"])
    queries = sorted(df["query_name"].unique())
    selected = st.multiselect("Queries", queries, default=queries)
    df = df[df["query_name"].isin(selected)]
    if df.empty:
        return

    last = df.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Batches", len(df))
    c2.metric("Input rows", f"{int(df['num_input_rows'].sum()):,}")
    c3.metric("Last batch", f"{int(last['trigger_ms']):,} ms", help=f"{last['query_name']}, batch {last['batch_id']}")
    c4.metric("Dropped by watermark", f"{int(df['rows_dropped_by_watermark'].sum()):,}")

    # Q9c: a query that takes in rows faster than it processes them builds a backlog.
    for name, g in df.groupby("query_name"):
        recent = g.tail(5)
        if recent["input_rows_per_sec"].mean() > recent["processed_rows_per_sec"].mean():
            st.warning(f"{name}: input rate is above processing rate over the last 5 batches — falling behind")

    # Full width: six series (input and processed per query) need room for their legend.
    rates = df.melt(
        id_vars=["time", "query_name"],
        value_vars=["input_rows_per_sec", "processed_rows_per_sec"],
        var_name="rate",
    )
    rates["series"] = rates["query_name"] + " · " + rates["rate"].str.replace("_rows_per_sec", "")
    st.caption("Rows per second (input vs processed)")
    st.line_chart(rates, x="time", y="value", color="series")

    # Only stateful queries have a watermark, and before their first batch Spark reports the epoch.
    # Blank those values instead of dropping the rows: every chart then has the same queries, in the
    # same order, so each query keeps its colour across charts.
    wm = pd.to_datetime(df["watermark"], errors="coerce", utc=True)
    df = df.assign(watermark_time=wm.where(wm > pd.Timestamp("1971-01-01", tz="UTC")))

    left, right = st.columns(2)
    with left:
        chart(df, "trigger_ms", "Batch duration (ms)")
        chart(df, "state_rows", "State rows held")
        if "duplicates_dropped" in df:  # absent in CSVs written before the column was added
            chart(df, "duplicates_dropped", "Duplicates dropped")
    with right:
        chart(df, "watermark_time", "Watermark (event time the query has declared complete)")
        chart(df, "rows_dropped_by_watermark", "Rows dropped by watermark")

    st.caption("Last 20 batches")
    st.dataframe(df.drop(columns=["time", "watermark_time"]).tail(20).iloc[::-1], hide_index=True, width="stretch")


dashboard()
