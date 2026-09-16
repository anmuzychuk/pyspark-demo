# Avro: file anatomy, schemas and sync markers

Notes for L3, based on `labs/L3/avro_example.py`, which writes `data/user.avro` and reads it back.

## 1. What's in `data/user.avro` (301 bytes)

`avro_example.py` writes 3 records (Alice, Bob, Charlie) with this schema:

```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "favorite_number", "type": ["int", "null"]},
    {"name": "favorite_color", "type": ["string", "null"]}
  ]
}
```

An Avro data file has two parts: a **header**, then one or more **data blocks**.

```
┌─ HEADER ───────────────────────────────────────────────────────────┐
│ 4f 62 6a 01            "Obj" + version 1   ← magic bytes            │
│ 04                     metadata map: 2 entries                      │
│   14 "avro.codec"      08 "null"           ← no compression         │
│   16 "avro.schema"     80 03 {"type": "record", ...}  (192 bytes)   │
│ 00                     end of metadata map                          │
│ 319d185e...9bf5782e    16-byte random sync marker                   │
├─ DATA BLOCK ───────────────────────────────────────────────────────┤
│ 06                     record count = 3                             │
│ 4e                     block size   = 39 bytes                      │
│   0a "Alice"  00 f6 01  00 08 "blue"                                │
│   06 "Bob"    02        00 0a "green"                               │
│   0e "Charlie" 00 aa 0c 02                                          │
│ 319d185e...9bf5782e    sync marker again (marks the end of a block) │
└────────────────────────────────────────────────────────────────────┘
```

Decoded records:

```
{'name': 'Alice',   'favorite_number': 123,  'favorite_color': 'blue'}
{'name': 'Bob',     'favorite_number': None, 'favorite_color': 'green'}
{'name': 'Charlie', 'favorite_number': 789,  'favorite_color': None}
```

To inspect it yourself:

```bash
xxd data/user.avro
```

```python
from avro.datafile import DataFileReader
from avro.io import DatumReader

reader = DataFileReader(open("data/user.avro", "rb"), DatumReader())
print(reader.meta)                # avro.codec, avro.schema
print(reader.sync_marker.hex())   # the 16-byte sync marker
for record in reader:
    print(record)
```

### What to take from this

- **The full schema is stored in the file as JSON.** Any reader can decode the file without being given the schema separately. That's why Avro files "describe themselves".
- **The data itself has no field names, tags or separators.** Values are written one after another in the order the schema lists the fields. Without the schema the bytes are meaningless, and that's why they're so compact: 39 bytes for 3 records.
- **Numbers use zigzag varints.** `f6 01` is 123 and `aa 0c` is 789. Strings are a length followed by UTF-8 bytes, so `0a` (length 5) comes before `Alice`.
- **Union values start with a branch index.** In `["int", "null"]`, `00` means "the int branch follows" and `02` (index 1) means null, which takes no further bytes. That's how Bob's missing number costs only 1 byte.
- **The sync marker repeats after every block.** That lets Spark or Hadoop split a large file and start reading from the middle (see section 6).

## 2. A minimal Avro setup

The smallest valid schema isn't even a record. A primitive type works on its own:

```json
"string"
```

The smallest record needs just `type`, `name` and `fields`:

```json
{"type": "record", "name": "User", "fields": []}
```

A realistic minimal schema looks like this:

```json
{
  "type": "record",
  "name": "User",
  "namespace": "ua.ucu.demo",
  "doc": "A user of the demo app",
  "fields": [
    {"name": "name",            "type": "string"},
    {"name": "favorite_number", "type": ["null", "int"],    "default": null},
    {"name": "favorite_color",  "type": ["null", "string"], "default": null}
  ]
}
```

## 3. Schema-level attributes (named types: `record`, `enum`, `fixed`)

| Attribute | Required | Meaning |
|---|---|---|
| `type` | yes | `"record"`, `"enum"` or `"fixed"` |
| `name` | yes | Type name, e.g. `User` |
| `namespace` | no | Qualifies the name: `ua.ucu.demo` + `User` gives the full name `ua.ucu.demo.User` |
| `doc` | no | Documentation only; ignored when encoding |
| `aliases` | no | Old names for this type, used when a type gets renamed |
| `fields` | yes (record) | List of field definitions (see section 4) |
| `symbols` | yes (enum) | e.g. `["RED", "GREEN", "BLUE"]` |
| `size` | yes (fixed) | Number of bytes, e.g. `16` for an MD5 hash |

### What `namespace` is for

It works like a Java package or a Python module path. It doesn't change the bytes on disk at all, but it matters in these cases:

- **Name clashes.** Two different `Address` records in one schema (for example `billing.Address` and `shipping.Address`) are only allowed if their full names differ.
- **Schema resolution.** When reading data written with an older schema, the writer's and reader's record full names must match (or be listed in `aliases`). A namespace keeps unrelated schemas from matching by accident.
- **Code generation.** Java and C# tools turn the namespace into a package or namespace name.
- **Schema registries** (Confluent, Kafka) identify schemas by their full name.
- **Nested records inherit it.** A record defined inside `ua.ucu.demo.User` without its own namespace is automatically in `ua.ucu.demo`. You can also write the full name directly: `"name": "ua.ucu.demo.User"`.

For a local one-off file like `user.avro`, leaving the namespace out is fine.

## 4. Field attributes

| Attribute | Required | Meaning |
|---|---|---|
| `name` | yes | Field name |
| `type` | yes | Any type: primitive, complex, union, or a named type's name |
| `default` | no | Value a reader uses when the data lacks this field. **This is what makes schema evolution work.** |
| `doc` | no | Documentation |
| `aliases` | no | Old names for the field, used when it gets renamed |
| `order` | no | `ascending` (default), `descending` or `ignore`; only affects sorting |

### Types you can use in `type`

- **Primitives:** `null`, `boolean`, `int` (32-bit), `long` (64-bit), `float`, `double`, `bytes`, `string`
- **Complex types:**
  - `{"type": "array", "items": "string"}`
  - `{"type": "map", "values": "long"}` (map keys are always strings)
  - `{"type": "enum", "name": "Color", "symbols": ["RED", "BLUE"]}`
  - `{"type": "fixed", "name": "Md5", "size": 16}`
  - a nested `record`
- **Unions:** a JSON list such as `["null", "string"]`, which is how Avro expresses "optional"
- **Logical types** (an annotation on top of a primitive):
  - `{"type": "int", "logicalType": "date"}`
  - `{"type": "long", "logicalType": "timestamp-millis"}`
  - `{"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}`
  - `{"type": "string", "logicalType": "uuid"}`


## 5. The sync marker

A **sync marker** is 16 random bytes the writer picks once per file. It's stored at the end of the header and written again after every data block:

```
[header … SYNC] [count|size|records… SYNC] [count|size|records… SYNC] …
```

- **It's random, so no escaping is needed.** CSV has to quote newlines inside values. Avro just relies on odds: a 16-byte random value turning up by chance in your data has a probability of about 2⁻¹²⁸ at any given position.
- **It's different in every file.** That's why you can't simply join two `.avro` files byte by byte.
- **Blocks are about 64 KB.** The Python `avro` library closes a block once it holds 64,000 bytes (`SYNC_INTERVAL = 4000 * 16` in `avro/datafile.py`).

`user.avro` holds only 3 records, so it has just one block and the marker isn't doing anything useful there. A file with 20,000 records (292,015 bytes) has a marker every ~64 KB:

```
marker offsets: 150, 64171, 128196, 192222, 256248, 291999
```

That file and those offsets come from `labs/L3/avro_write_synthetic_data.py`, which writes the
20,000 records and then scans the bytes for the marker:

```bash
uv run python labs/L3/avro_write_synthetic_data.py
```

The marker is random per file, so your offsets will be close to these but not identical, and the
byte dump in section 1 will show a different marker after you rewrite `data/user.avro`.

## 7. How Spark uses sync markers to split files

For a large file, Spark cuts the byte range into **splits** of `spark.sql.files.maxPartitionBytes` (default 128 MB) and gives each split to its own task. A split boundary is just a byte offset, so it will usually land in the middle of a record. Without markers, a task starting at byte 100,000 would have no idea where the next record begins.

Spark's Avro reader handles this with two calls on the Avro file reader:

1. **`sync(start)`** jumps to the split's start offset and scans forward to the first sync marker. Whatever comes before that marker belongs to the previous split, so it's skipped.
2. **`pastSync(end)`** is checked before reading each block. The task keeps reading whole blocks as long as the marker before the block started before `end`.

So the rule is: **a split owns every block whose preceding sync marker starts inside `[start, end)`**. A task may read past its `end` to finish its last block, and the next task skips that same partial data. Every block is read exactly once: no gaps, no duplicates.

### Checked with Spark

Reading the 20,000-record file with Spark 4.2.0 and `maxPartitionBytes = 100000`:

```
split [     0, 100000)  owns blocks after markers 150, 64171      ← reads past 100000 to finish block 2
split [100000, 200000)  skips to marker 128196; owns 128196, 192222
split [200000, 292015)  skips to marker 256248; owns 256248
```

| partition | rows | min_id | max_id |
|---|---|---|---|
| 0 | 9084 | 0 | 9083 |
| 1 | 8534 | 9084 | 17617 |
| 2 | 2382 | 17618 | 19999 |

A pure-Python simulation of the rule above predicted exactly these row counts (9084 / 8534 / 2382). In total Spark read 20,000 rows and 20,000 distinct ids, so nothing was lost or read twice.

The table comes from `labs/L3/spark_avro_read_parquet_write.py`, which groups by
`spark_partition_id()` to count the rows each task read. It then writes the Parquet folder that
[parquet.md](parquet.md) picks apart:

```bash
uv run python labs/L3/spark_avro_read_parquet_write.py
```

### What Spark gains from this

- **One large file is read in parallel.** A 10 GB Avro file becomes about 80 tasks at the default split size, not one.
- **Compressed files stay splittable.** Codecs like `snappy`, `deflate` or `zstd` compress each block separately, and the markers sit between blocks, uncompressed. Compare `data.csv.gz`: gzip can't be split, so Spark reads the whole file in a single task.
- **Data locality on HDFS or S3.** Splits line up with storage blocks, so executors can read bytes stored near them.
- **Cheap retries.** A failed task rereads only its own split, not the whole file.
- **It stays correct even when a block is bigger than a split.** If no marker starts inside a split, that task returns 0 rows, and the neighbouring task reads the whole block.

### Other formats

- **CSV and JSON Lines** split on newlines, which works like a poor man's sync marker. It breaks as soon as a value contains a newline, which is why `multiLine=true` makes them unsplittable.
- **Parquet** has no markers. It keeps the byte offsets of its row groups in a footer, and Spark reads the footer to plan splits.

## 8. Using Avro in PySpark

PySpark doesn't include the Avro data source. You need the external package, matching your Spark and Scala versions:

```python
SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.2.0")
```

It's about 3 jars, downloaded on first use. After that:

```python
df = spark.read.format("avro").load("data/user.avro")
df.write.format("avro").save("data/user_out")
```
