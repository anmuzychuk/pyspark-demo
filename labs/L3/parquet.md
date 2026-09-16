# Parquet output folder: part files, `_SUCCESS` and `.crc`

Notes for L3. Companion to [avro.md](avro.md), which covers how Spark splits `data/users_big.avro`.

Scripts:

- `labs/L3/avro_write_synthetic_data.py` writes `data/users_big.avro` (20,000 records, 5 blocks)
- `labs/L3/spark_avro_read_parquet_write.py` reads it with `spark.sql.files.maxPartitionBytes = 100000` and writes `data/users_big.parquet`

```python
df = spark.read.format("avro").load("data/users_big.avro")
df.write.format("parquet").mode("overwrite").save("data/users_big.parquet")
```

The result is a folder with 8 files:

```
_SUCCESS                                                          0
._SUCCESS.crc                                                     8
part-00000-0aa0c47b-4adf-4163-80f8-4f1b00b14cb1-c000.snappy.parquet   83985
.part-00000-0aa0c47b-4adf-4163-80f8-4f1b00b14cb1-c000.snappy.parquet.crc  668
part-00001-0aa0c47b-4adf-4163-80f8-4f1b00b14cb1-c000.snappy.parquet   76261
.part-00001-0aa0c47b-4adf-4163-80f8-4f1b00b14cb1-c000.snappy.parquet.crc  604
part-00002-0aa0c47b-4adf-4163-80f8-4f1b00b14cb1-c000.snappy.parquet   22391
.part-00002-0aa0c47b-4adf-4163-80f8-4f1b00b14cb1-c000.snappy.parquet.crc  184
```

## 1. The 3 Parquet files: one per Spark partition, not per Avro block

The 3 files correspond to the **3 Spark partitions**, not to Avro blocks. `users_big.avro` actually has **5** blocks. With `maxPartitionBytes = 100000`, Spark grouped them 2 + 2 + 1 into 3 splits. Spark writes **one file per task**, so 3 partitions give 3 files.

Reading the files back matches the partition table exactly:

| File | Rows | ids | Came from Avro blocks | Parquet row groups |
|---|---|---|---|---|
| `part-00000` | 9084 | 0–9083 | 1 + 2 (4576 + 4508) | 1 |
| `part-00001` | 8534 | 9084–17617 | 3 + 4 (4267 + 4267) | 1 |
| `part-00002` | 2382 | 17618–19999 | 5 | 1 |

- **The partitioning carries straight through.** Reading and writing involve no shuffle, so the partitions Spark created while reading are the ones it writes out. The `groupBy(...).show()` in the script runs as a separate job and doesn't change `df`.
- **Without the `maxPartitionBytes` setting, you'd get 1 file.** At the default 128 MB split size the whole 292 KB file is a single partition.
- **Parquet's version of an Avro block is the row group.** Each file here holds just one, because the default row-group size is 128 MB.

To inspect the files:

```python
import glob

import pyarrow.compute as pc
import pyarrow.parquet as pq

for f in sorted(glob.glob("data/users_big.parquet/part-*.parquet")):
    meta = pq.ParquetFile(f).metadata
    ids = pq.read_table(f, columns=["id"]).column("id")
    print(f.split("/")[-1][:10], meta.num_rows, meta.num_row_groups, pc.min(ids).as_py(), pc.max(ids).as_py())
```

## 2. Why 8 files

| Files | Written by | Purpose |
|---|---|---|
| 3 × `part-*.snappy.parquet` | Spark tasks | The data |
| 3 × `.part-*.parquet.crc` | Hadoop local filesystem | Checksums for each data file |
| `_SUCCESS` (0 bytes) | Job commit | Marks that the whole write finished |
| `._SUCCESS.crc` | Hadoop local filesystem | Checksum of the empty `_SUCCESS` |

Plain `ls` shows only 4 of them, because the `.crc` files start with a dot. Use `ls -la`.

### Parts of a data file name

`part-00000-0aa0c47b-…-c000.snappy.parquet`

- **`00000`** is the task (partition) number.
- **`0aa0c47b-…`** is the ID of this write job. It's the same for every file from one `save()`, so a later `mode("append")` can't overwrite existing files.
- **`c000`** counts files within one task. It goes up to `c001` and beyond if you set `spark.sql.files.maxRecordsPerFile`.
- **`snappy`** is the compression codec, Spark's default for Parquet. The 3 files total 182 KB, against 292 KB for the Avro file.

### `_SUCCESS`

- **What writes it.** Tasks first write into a `_temporary/` folder. When the job commits, Hadoop moves the files into place and creates the empty `_SUCCESS` file.
- **What it means.** It exists only if every task succeeded. If it's missing, the folder may hold a partial write. Tools like Airflow often wait for this file before starting the next step.
- **Readers skip it.** Spark ignores files whose names start with `_` or `.`, so neither `_SUCCESS` nor the `.crc` files get read as data.

## 3. What `.crc` files are

They're **checksum sidecar files** created by Hadoop's local filesystem layer, which Spark uses whenever it writes to `file://` paths.

```
63 72 63 00   "crc\0"   magic header
00 00 02 00   512       bytes per checksum
61 5e 90 fd   CRC32 of bytes 0–511
d8 c6 6d b1   CRC32 of bytes 512–1023
...
```

- **Format.** An 8-byte header, then one 4-byte CRC32 for every 512 bytes of data, so a `.crc` file is 8 + 4 × ⌈size / 512⌉ bytes. `part-00000` is 83,985 bytes, which makes 165 chunks and a 668-byte `.crc`. The empty `_SUCCESS` gets a header only (8 bytes).
- **Verified.** Recomputing every chunk's CRC32 with `zlib` matched all 4 `.crc` files.
- **Purpose.** When Spark reads the file back through Hadoop, it checks each chunk. A corrupted byte raises a `ChecksumException` instead of silently returning bad data.
- **You only see them locally.** HDFS keeps checksums hidden on its storage nodes, and S3 doesn't use these files at all, so a cluster won't show `.crc` files.
- **Practical tip.** Deleting them is harmless. But if you change a `.parquet` file with another tool and leave the old `.crc` next to it, Spark will fail with a checksum error. Delete the stale `.crc` and the read works again.

To decode and verify them:

```python
import glob
import os
import struct
import zlib

for crc_path in sorted(glob.glob("data/users_big.parquet/.*.crc")):
    raw = open(crc_path, "rb").read()
    data_path = os.path.join(os.path.dirname(crc_path), os.path.basename(crc_path)[1:-4])
    data = open(data_path, "rb").read()

    magic = raw[:4]                                    # b"crc\x00"
    bytes_per_checksum = struct.unpack(">i", raw[4:8])[0]  # 512
    stored = [struct.unpack(">I", raw[i:i + 4])[0] for i in range(8, len(raw), 4)]
    computed = [zlib.crc32(data[i:i + bytes_per_checksum]) for i in range(0, len(data), bytes_per_checksum)]

    print(os.path.basename(data_path)[:14], magic, bytes_per_checksum, len(stored), stored == computed)
```
