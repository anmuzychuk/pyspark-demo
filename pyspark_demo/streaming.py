"""Structured Streaming helpers: stop_query, and progress listeners that print (SimpleMetricsListener)
or log every micro-batch to CSV (FileMetricsListener)."""

import csv
import datetime as dt
import math
import threading
from pathlib import Path
from typing import override

from pyspark.sql import SparkSession
from pyspark.sql.streaming.listener import StreamingQueryListener


def stop_query(spark: SparkSession, *names: str) -> None:
    """Stop the active queries with these names, if any.

    A query name must be unique among a session's *active* queries, so re-running a cell whose
    previous run was interrupted fails with "a query with that name is already active".
    """
    for q in spark.streams.active:
        if q.name in names:
            print(f"stopping leftover query {q.name}")
            q.stop()


CSV_COLUMNS = [
    "logged_at",
    "query_name",
    "query_id",
    "run_id",
    "batch_id",
    "batch_timestamp",
    "num_input_rows",
    "input_rows_per_sec",
    "processed_rows_per_sec",
    "trigger_ms",
    "add_batch_ms",
    "watermark",
    "state_rows",
    "rows_dropped_by_watermark",
    "duplicates_dropped",
]


def _rate(value: float | None) -> float | None:
    """Spark reports a rate as NaN (or leaves it out) when there is nothing to divide by."""
    return None if value is None or math.isnan(value) else value


def _progress_row(p) -> dict:
    """One CSV_COLUMNS row from a StreamingQueryProgress."""
    return {
        "logged_at": dt.datetime.now().isoformat(timespec="seconds"),
        "query_name": _label(p.name, p.id),
        "query_id": str(p.id),
        "run_id": str(p.runId),
        "batch_id": p.batchId,
        "batch_timestamp": p.timestamp,
        "num_input_rows": p.numInputRows,
        "input_rows_per_sec": _rate(p.inputRowsPerSecond),
        "processed_rows_per_sec": _rate(p.processedRowsPerSecond),
        "trigger_ms": p.durationMs.get("triggerExecution", 0),
        "add_batch_ms": p.durationMs.get("addBatch"),
        "watermark": p.eventTime.get("watermark"),
        "state_rows": sum(op.numRowsTotal for op in p.stateOperators),
        "rows_dropped_by_watermark": sum(op.numRowsDroppedByWatermark for op in p.stateOperators),
        "duplicates_dropped": sum(op.customMetrics.get("numDroppedDuplicateRows", 0) for op in p.stateOperators),
    }


def _label(name: str | None, query_id) -> str:
    return name or f"unnamed-{str(query_id)[:8]}"


def _format_msg(msg: str) -> str:
    return f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"


class SimpleMetricsListener(StreamingQueryListener):
    """Prints one line per stream event: start, every micro-batch, stop or failure.

    One listener sees every query in the session, so register it once:
    `spark.streams.addListener(SimpleMetricsListener())`.

    The callbacks run on the driver after each batch, on Spark's listener thread. Keep them fast
    and never let them raise: appending a line to a local file is fine, a slow HTTP call to a
    monitoring system is not — hand that to a queue and a background thread instead.
    """

    @override
    def onQueryStarted(self, event):
        print(_format_msg(f"[Stream started]: {_label(event.name, event.id)}"))

    @override
    def onQueryProgress(self, event):
        r = _progress_row(event.progress)
        dropped, duplicates = r["rows_dropped_by_watermark"], r["duplicates_dropped"]
        print(
            _format_msg(
                f"* {r['query_name']} | batch {r['batch_id']} | {r['num_input_rows']} rows | "
                f"in {r['input_rows_per_sec'] or 0:.1f} rows/s | out {r['processed_rows_per_sec'] or 0:.1f} rows/s | "
                f"{r['trigger_ms']} ms"
                + (f" | dropped by watermark: {dropped}" if dropped else "")
                + (f" | duplicates dropped: {duplicates}" if duplicates else "")
            )
        )

    @override
    def onQueryIdle(self, event):
        # Spark 3.5+: fired instead of onQueryProgress when a trigger found no new data.
        pass

    @override
    def onQueryTerminated(self, event):
        if event.exception:
            print(_format_msg(f"[Stream failed]: {event.id} | {event.exception}"))
        else:
            print(_format_msg(f"[Stream stopped]: {event.id}"))


class FileMetricsListener(SimpleMetricsListener):
    """Appends one CSV row per micro-batch of every query to `csv_path` (columns: CSV_COLUMNS).

    Start, stop and failure lines are still printed; per-batch lines are not, so a notebook running
    several queries stays readable. `labs/L5/monitor_dashboard.py` plots the file live.
    """

    def __init__(self, csv_path: str | Path):
        self.csv_path = Path(csv_path)
        # Two queries finishing a batch at the same moment would otherwise interleave their rows.
        self._lock = threading.Lock()

    @override
    def onQueryProgress(self, event):
        row = _progress_row(event.progress)
        with self._lock:
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            is_new = not self.csv_path.exists()
            with self.csv_path.open("a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                if is_new:
                    writer.writeheader()
                writer.writerow(row)
