# Delta table folder: data files, `_delta_log` and checksums

Notes for L3. Companion to [parquet.md](parquet.md) (plain Parquet output) and [avro.md](avro.md)
(how Spark splits `data/users_big.avro`).

Two ways into this material:

- **[delta_history.ipynb](delta_history.ipynb)** builds a throwaway table and steps through it live:
  the log files and commit actions (section 2), appends, deletes and updates, time travel, `RESTORE`,
  `OPTIMIZE` and `VACUUM`. Start there.
- **`labs/L3/delta_example.py`** writes `data/users_big.delta` in one go, the table this file
  describes:

  ```bash
  uv run python labs/L3/delta_example.py
  ```

Delta is a Spark package, loaded like spark-avro. The artifact name includes the Spark version it's
built for: `io.delta:delta-spark_4.2_2.13:4.4.0` is Delta 4.4.0 for Spark 4.2 and Scala 2.13. Two
settings switch it on:

```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

Since `delta-spark` is a project dependency, the notebook instead lets
`configure_spark_with_delta_pip(builder)` fill in the matching jar. Note that it *replaces*
`spark.jars.packages`, so other packages go in its `extra_packages` argument.

A freshly written table looks like this:

```
data/users_big.delta/
├── part-00000-…-c000.snappy.parquet      83985
├── .part-00000-…-c000.snappy.parquet.crc   668
├── part-00001-…-c000.snappy.parquet      76261
├── .part-00001-…-c000.snappy.parquet.crc   604
├── part-00002-…-c000.snappy.parquet      22391
├── .part-00002-…-c000.snappy.parquet.crc   184
└── _delta_log/
    ├── 00000000000000000000.json           1871
    ├── .00000000000000000000.json.crc         24
    ├── 00000000000000000000.crc            3004
    ├── .00000000000000000000.crc.crc          32
    └── _staged_commits/                 (empty)
```

**A Delta table is a folder of ordinary Parquet files plus a transaction log (`_delta_log/`) that
says which of those files make up the table.**

## 1. The data files are plain Parquet

One file per Spark partition, exactly as in [parquet.md](parquet.md). Written from
`users_big.avro` with `maxPartitionBytes = 100000`, that's 3 files:

| File | Bytes | Rows | ids | Parquet row groups |
|---|---|---|---|---|
| `part-00000-…` | 83985 | 9084 | 0–9083 | 1 |
| `part-00001-…` | 76261 | 8534 | 9084–17617 | 1 |
| `part-00002-…` | 22391 | 2382 | 17618–19999 | 1 |

They carry the same rows, sizes and Spark footer metadata as the plain Parquet output of
`spark_avro_read_parquet_write.py`. They aren't byte-identical: a couple of bytes inside the Parquet
footer are swapped (`00 06` vs `06 00`), which doesn't change the data.

Differences from a plain Parquet folder:

- **No `_SUCCESS` file.** A write is committed exactly when its `_delta_log/NNN.json` appears. If
  that file is missing the write never happened, even though its data files are on disk.
- **Each file has its own UUID.** In plain Parquet output every file from one `save()` shares a job
  UUID; Delta gives each file a random one.
- **The folder listing is not the table.** Only files `add`ed and not later `remove`d in the log
  count. After a few operations there are routinely more Parquet files on disk than the table uses,
  and `DESCRIBE DETAIL` reports the smaller number.

## 2. The log: commits, actions and checksums

The notebook prints all of this for a live table in section 2; the summary:

- **`NNN.json` is one commit**, named after the version, zero-padded to 20 digits. Each line is one
  action: `commitInfo` (audit record), `metaData` (schema and properties), `protocol` (minimum
  reader/writer versions), `add` and `remove` (which files are in the table).
- **`add` carries stats** — row count, min/max per column, null counts — which is what lets Spark
  skip files that can't match a filter, and answer `count()` without opening any Parquet.
- **`NNN.crc` is Delta's own summary** of the whole table state at that version: totals, schema,
  protocol, a file-size histogram and (for small tables) every live file. It's a cache; the `.json`
  commits are the source of truth.
- **`.something.crc` files are Hadoop's**, added for every file written on a local filesystem: an
  8-byte header plus a CRC32 per 512 bytes, as in [parquet.md](parquet.md). That's why you see
  `.NNN.crc.crc`, the Hadoop checksum of Delta's checksum file. They don't appear on HDFS or S3.
- **`_staged_commits/`** is for coordinated commits, where an external coordinator such as Unity
  Catalog decides which commit wins: a writer stages its commit there and it is "backfilled" into
  `NNN.json` once accepted. Path-based tables commit directly, so the folder stays empty.

## 3. Checkpoints

Rebuilding the table state means replaying every commit, which gets slow after thousands of them. By
default Delta writes a **checkpoint** every 10 commits: the whole state collapsed into one Parquet
file, `00000000000000000010.checkpoint.parquet`, next to a small `_last_checkpoint` pointer
(`{"version": 10, "size": 17, "numOfAddFiles": 14, ...}`). Readers start from the checkpoint and
replay only what came after.

A checkpoint also keeps `remove` entries as **tombstones**, so `VACUUM` knows which files are
deletable and when they stopped being used.

## 4. Plain Parquet vs Delta folder

| | Parquet (`users_big.parquet`) | Delta (`users_big.delta`) |
|---|---|---|
| Data files | 3 × `part-*.snappy.parquet` | The same 3 Parquet files |
| File name UUID | One per write job | One per file |
| "Write finished" marker | `_SUCCESS` | `_delta_log/NNN.json` |
| Schema | Inside each Parquet footer | Also in the log (`metaData`) |
| Which files belong to the table | Every file in the folder | Only files `add`ed and not `remove`d |
| Per-file stats | Parquet footers (must open each file) | In the log (no need to open files) |
| History / time travel | None: overwrite replaces the files | Every version until `VACUUM` |
| Hadoop `.crc` sidecars | Per data file and `_SUCCESS` | Per data file and per log file |

## 5. Gotchas

- **`delta.\`path\`` in SQL needs an absolute path.** ``DESCRIBE HISTORY delta.`data/users_big.delta` ``
  fails with `TABLE_OR_VIEW_NOT_FOUND`, which is why `delta_example.py` resolves the path first. The
  DataFrame API (`.load("data/...")`) accepts relative paths.
- **Rerunning `delta_example.py` doesn't reset the table.** `mode("overwrite")` adds a new version
  that removes the old files and adds new ones; the old files stay on disk until `VACUUM`. Delete the
  folder to start from version 0 again.
- **`count()` can hide missing files.** It is answered from the log's row counts, so it still succeeds
  for a version whose files `VACUUM` has deleted. Read actual rows to check that an old version is
  still readable.
- **Don't edit the folder by hand.** Deleting a file the log references breaks reads, and adding one
  changes nothing, because the log doesn't know about it.
- **`DeltaTable.toDF().inputFiles()` goes stale.** It keeps listing the files from when `forPath()`
  was called. A fresh `spark.read.format("delta").load(path)` gives the current ones.
