from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_tbl_partitions_summary(table: str, bar_width: int = 32) -> dict:
    """Bytes and rows per file on disk, and rows per partition when `table` is read back.

    Both halves come from one scan. Grouping by `spark_partition_id()` *and* the `_metadata`
    file columns together also records which file each partition read — the piece neither
    figure shows on its own, and the one that matters, because a table's file count and its
    partition count are set by different mechanisms and routinely disagree:

      * fewer partitions than files — small files packed into one split (8 files -> 4 tasks);
      * fewer populated partitions than tasks — one atomic row group (2 files -> 1 of 4).

    Prints the report and returns the same numbers for programmatic use.
    """
    spark = SparkSession.active()

    location = next(r[1] for r in spark.sql(f"DESCRIBE EXTENDED {table}").collect() if r[0] == "Location")
    directory = Path(location.replace("file:", ""))
    df = spark.table(table)
    n_partitions = df.rdd.getNumPartitions()

    spark.sparkContext.setJobDescription(f"Inspect: partitions and files of {table}")
    scanned = (
        df.groupBy(
            F.spark_partition_id().alias("partition"),
            F.col("_metadata.file_path").alias("path"),
            F.col("_metadata.file_size").alias("file_bytes"),
        )
        .agg(F.count(F.lit(1)).alias("rows"))
        .collect()
    )

    # Files are listed from disk, not taken from the scan: a zero-row file produces no group
    # above and would otherwise vanish from the report — the one you most want to see.
    files = {f.name: {"bytes": f.stat().st_size, "rows": 0} for f in sorted(directory.glob("part-*.parquet"))}
    # Partitions come from range(n) for the same reason: an empty partition emits no group,
    # but it is still a task that will be scheduled and will do nothing.
    partitions = {pid: {"rows": 0, "files": []} for pid in range(n_partitions)}

    for r in scanned:
        name = r["path"].rsplit("/", 1)[-1]
        files.setdefault(name, {"bytes": r["file_bytes"], "rows": 0})["rows"] += r["rows"]
        partition = partitions.setdefault(r["partition"], {"rows": 0, "files": []})
        partition["rows"] += r["rows"]
        partition["files"].append(name)

    total_rows = sum(p["rows"] for p in partitions.values())
    total_bytes = sum(f["bytes"] for f in files.values())
    # Every part file in a table shares one uuid, so "part-NNNNN" is the distinguishing part.
    short = lambda name: "-".join(name.split("-", 2)[:2])  # noqa: E731
    reads_width = max((len(", ".join(sorted({short(n) for n in p["files"]}))) for p in partitions.values()), default=5)

    print(f"\n{table}  ({directory})")
    print(f"  on disk: {len(files)} file(s), {total_bytes:,} bytes")
    print(f"    {'file':<13}{'bytes':>14}{'rows':>14}")
    for name, f in files.items():
        print(f"    {short(name):<13}{f['bytes']:>14,}{f['rows']:>14,}")

    print(f"\n  read back: {n_partitions} partition(s), {total_rows:,} rows")
    print(f"    {'partition':<11}{'rows':>14}   {'reads':<{reads_width}}")
    for pid in sorted(partitions):
        p = partitions[pid]
        reads = ", ".join(sorted({short(n) for n in p["files"]})) or "-"
        bar = "#" * round(bar_width * p["rows"] / max(total_rows, 1))
        print(f"    {pid:<11}{p['rows']:>14,}   {reads:<{reads_width}} {bar}")

    empty = sum(1 for p in partitions.values() if p["rows"] == 0)
    if empty:
        print(f"    -> {empty} empty partition(s): slots that will be scheduled and do no work")

    return {
        "table": table,
        "location": str(directory),
        "files": files,
        "partitions": partitions,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
    }
