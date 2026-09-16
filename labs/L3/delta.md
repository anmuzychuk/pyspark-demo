# Delta table folder: data files, `_delta_log` and checksums

Notes for L3. Companion to [parquet.md](parquet.md) (plain Parquet output) and [avro.md](avro.md) (how Spark splits `data/users_big.avro`).

Script: `labs/L3/delta_example.py` reads `data/users_big.avro` with `spark.sql.files.maxPartitionBytes = 100000` (3 partitions) and writes a Delta table:

```bash
uv run python labs/L3/delta_example.py
```

```python
df = spark.read.format("avro").load("data/users_big.avro")
df.write.format("delta").mode("overwrite").save(str(table_path))
```

Delta is a Spark package, loaded like spark-avro. The artifact name includes the Spark version it's built for: `io.delta:delta-spark_4.2_2.13:4.4.0` is Delta 4.4.0 for Spark 4.2 and Scala 2.13. Two settings switch it on:

```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

The result:

```
data/users_big.delta/
├── part-00000-a901150b-3c1e-4798-9193-88bb4a46e058-c000.snappy.parquet      83985
├── .part-00000-a901150b-…-c000.snappy.parquet.crc                            668
├── part-00001-f2af3356-1321-4840-a8cb-b2bc0e0c5429-c000.snappy.parquet      76261
├── .part-00001-f2af3356-…-c000.snappy.parquet.crc                            604
├── part-00002-cb64cc21-7bf2-4232-8d87-191d29873ccc-c000.snappy.parquet      22391
├── .part-00002-cb64cc21-…-c000.snappy.parquet.crc                            184
└── _delta_log/
    ├── 00000000000000000000.json                                             1871
    ├── .00000000000000000000.json.crc                                          24
    ├── 00000000000000000000.crc                                              3004
    ├── .00000000000000000000.crc.crc                                           32
    └── _staged_commits/                                                    (empty)
```

**A Delta table is a folder of ordinary Parquet files plus a transaction log (`_delta_log/`) that says which of those files make up the table.**

## 1. The data files: plain Parquet, same as before

The 3 data files are regular Parquet files, one per Spark partition, exactly like in [parquet.md](parquet.md). They have the same sizes, the same rows and the same Spark footer metadata as the plain Parquet output. They aren't byte-identical: a couple of bytes inside the Parquet footer are swapped (`00 06` vs `06 00`), which doesn't change the data.

| File | Bytes | Rows | ids | Parquet row groups |
|---|---|---|---|---|
| `part-00000-a901150b…` | 83985 | 9084 | 0–9083 | 1 |
| `part-00001-f2af3356…` | 76261 | 8534 | 9084–17617 | 1 |
| `part-00002-cb64cc21…` | 22391 | 2382 | 17618–19999 | 1 |

Any Parquet reader (pyarrow, DuckDB, `spark.read.parquet`) can open them. But reading the folder as plain Parquet is wrong once the table has history: it would also pick up files the log has already removed (see section 6).

Differences from plain Parquet output:

- **No `_SUCCESS` file.** Delta doesn't need it. A write is committed exactly when its `_delta_log/NNN.json` file appears. If that file is missing, the write never happened, even if its data files are on disk.
- **Each file has its own UUID.** In plain Parquet output all files from one `save()` share a job UUID. Delta gives every file a random one (`a901150b…`, `f2af3356…`, `cb64cc21…`).

## 2. `_delta_log/00000000000000000000.json`: the commit

Every successful write creates one commit file. Its name is the table **version**, zero-padded to 20 digits: `…000.json` is version 0, `…001.json` is version 1, and so on. The file holds one JSON **action** per line.

Version 0 has 6 lines:

```json
{"commitInfo": {"timestamp": 1789399082817, "operation": "WRITE",
                "operationParameters": {"mode": "Overwrite", "partitionBy": "[]"},
                "isolationLevel": "Serializable", "isBlindAppend": false,
                "operationMetrics": {"numFiles": "3", "numOutputRows": "20000", "numOutputBytes": "182637", ...},
                "engineInfo": "Apache-Spark/4.2.0 Delta-Lake/4.4.0", "txnId": "0f2170a4-…"}}
{"metaData": {"id": "9ce91304-…", "format": {"provider": "parquet", "options": {}},
              "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",...},{\"name\":\"name\",\"type\":\"string\",...}]}",
              "partitionColumns": [], "configuration": {}, "createdTime": 1789399081409}}
{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
{"add": {"path": "part-00000-a901150b-…-c000.snappy.parquet", "partitionValues": {}, "size": 83985,
         "modificationTime": 1789399082294, "dataChange": true,
         "stats": "{\"numRecords\":9084,\"minValues\":{\"id\":0,\"name\":\"user_000000\"},\"maxValues\":{\"id\":9083,\"name\":\"user_009083\"},\"nullCount\":{\"id\":0,\"name\":0}}"}}
{"add": {"path": "part-00001-f2af3356-…", "size": 76261, "stats": "{\"numRecords\":8534,\"minValues\":{\"id\":9084,...},\"maxValues\":{\"id\":17617,...}}", ...}}
{"add": {"path": "part-00002-cb64cc21-…", "size": 22391, "stats": "{\"numRecords\":2382,\"minValues\":{\"id\":17618,...},\"maxValues\":{\"id\":19999,...}}", ...}}
```

| Action | Meaning |
|---|---|
| `commitInfo` | Audit record: when, which operation, mode, metrics, engine version. This is what `DESCRIBE HISTORY` shows. |
| `metaData` | Table ID, file format, **schema** (as a Spark struct in JSON), partition columns, table properties (`configuration`). Written on the first commit and again whenever any of these change. |
| `protocol` | Minimum reader/writer versions a client needs. `1` / `2` means no advanced features. Enabling e.g. deletion vectors raises them and adds `readerFeatures` / `writerFeatures`. |
| `add` | "This file is now part of the table": its path, size, and **stats** (row count, min/max per column, null counts). |
| `remove` | "This file is no longer part of the table" (appears in later commits, see section 6). |

### Why the stats matter

- **Data skipping.** For `WHERE id > 18000`, Spark compares the filter with each file's min/max and skips `part-00000` (max 9083) and `part-00001` (max 17617) without opening them.
- **Metadata-only answers.** `count()` on a table can be answered from `numRecords` without reading any Parquet. Section 6 shows how that can hide missing files.

To print a commit file:

```python
import json

for line in open("data/users_big.delta/_delta_log/00000000000000000000.json"):
    action = json.loads(line)
    print(json.dumps(action, indent=2))
```

## 3. `_delta_log/00000000000000000000.crc`: Delta's version checksum

**This `.crc` is not the Hadoop checksum format from parquet.md.** It's a JSON summary of the table's full state at that version:

```json
{
  "txnId": "0f2170a4-…",
  "tableSizeBytes": 182637,
  "numFiles": 3,
  "numMetadata": 1,
  "numProtocol": 1,
  "setTransactions": [],
  "domainMetadata": [],
  "metadata": { "...same as the metaData action..." },
  "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
  "fileSizeHistogram": {
    "sortedBinBoundaries": [0, 8192, 16384, 32768, 65536, 131072, "..."],
    "fileCounts":          [0, 0,    1,     0,     2,     0,      "..."],
    "totalBytes":          [0, 0,    22391, 0,     160246, 0,     "..."]
  },
  "allFiles": [ "...the 3 add entries..." ]
}
```

- **What it summarizes.** Totals (size, file count), the current schema and protocol, and a histogram of file sizes. Here that's 1 file in the 16–32 KB bin (`part-00002`) and 2 files in the 64–128 KB bin. For a small table like this one it also lists every live file (`allFiles`).
- **Why it exists.** It lets Delta check the state it rebuilt from the commit files, and answer questions like "how big is this table?" without replaying the whole log.
- **It's optional.** If it's missing, the table still works. It's a cache, not the source of truth: the `.json` commits are.

## 4. Hidden `.crc` files: Hadoop checksums again

Every file Spark writes on the local filesystem gets a hidden Hadoop checksum sidecar, as explained in [parquet.md](parquet.md): an 8-byte header plus a 4-byte CRC32 per 512 bytes. That includes the log files:

| Sidecar | Protects | Size |
|---|---|---|
| `.part-0000N-….parquet.crc` | the data files | 668 / 604 / 184 B, identical to the Parquet output |
| `.00000000000000000000.json.crc` | the commit, 1871 B → 4 chunks | 8 + 4 × 4 = **24 B** |
| `.00000000000000000000.crc.crc` | Delta's version checksum, 3004 B → 6 chunks | 8 + 6 × 4 = **32 B** |

So `.crc.crc` means "the Hadoop checksum of Delta's checksum file". As before, these only appear on the local filesystem, not on HDFS or S3.

## 5. `_delta_log/_staged_commits/`: empty for this table

The folder is used by **coordinated commits**, where an external coordinator, such as Unity Catalog, decides which commit wins. A writer first puts its commit into `_staged_commits/`. After the coordinator accepts it, the commit is "backfilled", meaning copied into the normal `_delta_log/NNN.json`. The Delta 4.4 jars confirm this: `CoordinatedCommitsUtils` and the Unity Catalog commit coordinator reference `_staged_commits`, together with names like `generateUnbackfilledDeltaFilePath`.

A table written straight to a path, like this one, commits directly to `NNN.json`, so the folder stays empty.

## 6. How the folder changes on later writes

The steps below ran on a **copy** of the table, so `data/users_big.delta` is still at version 0.

### Version 1: append 5 rows

```python
spark.createDataFrame([(20000 + i, f"user_{20000 + i:06d}") for i in range(5)], "id long, name string") \
    .write.format("delta").mode("append").save(path)
```

- **4 new Parquet files for 5 rows.** With `local[4]`, `createDataFrame` spreads 5 rows over 4 partitions, giving files of 1, 1, 1 and 2 rows. That's the "small files problem" in miniature.
- **`00000000000000000001.json` holds only `commitInfo` + 4 × `add`.** No `metaData` or `protocol`, because they didn't change. Older files aren't touched.

### Version 2: `DELETE FROM … WHERE id < 9084`

```
DESCRIBE HISTORY
version  operation  operationMetrics
2        DELETE     numRemovedFiles -> 1, numDeletedRows -> 9084, numCopiedRows -> 0, numAddedFiles -> 0, ...
1        WRITE      numFiles -> 4, numOutputRows -> 5
0        WRITE      numFiles -> 3, numOutputRows -> 20000
```

- **A whole file was removed.** Every row in `part-00000-a901150b…` matched (its ids are 0–9083), so Delta didn't rewrite anything (`numCopiedRows 0`). It just logged `{"remove": {"path": "part-00000-a901150b…", ...}}`.
- **The removed file is still on disk.** `remove` only takes it out of the *current* version. Older versions still point to it:

  ```python
  spark.read.format("delta").load(path).count()                          # 10921 = 20000 + 5 - 9084
  spark.read.format("delta").option("versionAsOf", 0).load(path).count() # 20000  (time travel)
  ```

- **One extra 0-row Parquet file (379 B) appeared that no commit references.** `DELETE` wrote it for the (empty) remaining rows but left it out of the commit. **Delta ignores any file not listed in the log**, so it's invisible to readers.

At this point the folder has 8 data files on disk, while `DESCRIBE DETAIL` reports `numFiles = 6` (2 left from version 0 + 4 from version 1). **The log defines the table, not the folder listing.**

### Version 10: checkpoint

After 8 more single-row appends (versions 3–10), `_delta_log/` contains:

```
00000000000000000000.json … 00000000000000000010.json   (+ a .crc for each)
00000000000000000010.checkpoint.parquet
_last_checkpoint
_staged_commits/
```

- **Why a checkpoint.** Rebuilding the table state means replaying every commit, which gets slow after thousands of commits. By default Delta writes a **checkpoint** every 10 commits: the whole state collapsed into one Parquet file.
- **This checkpoint** has 17 rows: 14 `add` (live files), 1 `remove` (the file deleted in version 2, kept as a "tombstone" so `VACUUM` knows about it), 1 `metaData` and 1 `protocol`.
- **`_last_checkpoint`** is a small JSON pointer (`{"version": 10, "size": 17, "numOfAddFiles": 14, ...}`), so readers start from the checkpoint and replay only the commits after it.

### `VACUUM`: actually deleting files

```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")  # demo only
spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS")
```

- **What it deleted.** `VACUUM` removes files that are no longer part of the table and are older than the retention period (7 days by default). Here it deleted 2 files: the one removed in version 2 and the unreferenced 0-row file. That took the folder from 16 to 14 Parquet files.
- **After that, time travel to version 0 is broken, but `count()` hides it.**

  ```python
  v0 = spark.read.format("delta").option("versionAsOf", 0).load(path)
  v0.count()                  # 20000  ← answered from log stats, no files opened
  v0.agg(F.sum("id")).first() # fails: part-00000-a901150b-….snappy.parquet. File does not exist.
  ```

  Don't use `count()` to check whether an old version is still readable.

- **Why the retention check exists.** Delta refuses `RETAIN 0 HOURS` unless you disable it, because a running query or a writer that's still working could need the files you delete.

## 7. Plain Parquet vs Delta folder

| | Parquet (`users_big.parquet`) | Delta (`users_big.delta`) |
|---|---|---|
| Data files | 3 × `part-*.snappy.parquet` | Same 3 Parquet files (identical sizes and rows) |
| File name UUID | One per write job | One per file |
| "Write finished" marker | `_SUCCESS` | `_delta_log/NNN.json` |
| Schema | Inside each Parquet footer | Also in the log (`metaData`) |
| Which files belong to the table | Every file in the folder | Only files `add`ed and not `remove`d in the log |
| Per-file stats | Parquet footers (must open each file) | In the log (no need to open files) |
| History / time travel | None: overwrite replaces the files | Every version kept until `VACUUM` |
| Hadoop `.crc` sidecars | For each data file and `_SUCCESS` | For each data file and each log file |

## 8. Gotchas

- **`delta.\`path\`` in SQL needs an absolute path.** `DESCRIBE HISTORY delta.\`data/users_big.delta\`` fails with `TABLE_OR_VIEW_NOT_FOUND`, which is why `delta_example.py` resolves the path first. `.load("data/...")` in the DataFrame API accepts relative paths.
- **Rerunning `delta_example.py` doesn't reset the table.** `mode("overwrite")` creates a new version (1, 2, …) that `remove`s the old files and `add`s new ones. The old files stay on disk until `VACUUM`. Delete the folder to start from version 0.
- **Don't edit or delete files inside the folder by hand.** Removing a file the log still references breaks reads, and adding a file changes nothing, because the log doesn't know about it.
