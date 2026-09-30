import datetime as dt
import json
import random
import time

from pyspark_demo import ROOT_PATH

STREAMING_BASE = ROOT_PATH / "data" / "l5"

LANDING = STREAMING_BASE / "landing"
CKPT = STREAMING_BASE / "checkpoints"
DEVICES = [f"device_{i:02d}" for i in range(10)]


def drop_batch(
    n_records: int = 200, late_fraction: float = 0.0, late_minutes: int = 45, extra_field: bool = False
) -> str:
    """Write one JSON file of readings into LANDING. Returns the path."""
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    lines = []
    for _ in range(n_records):
        is_late = random.random() < late_fraction
        offset = (
            dt.timedelta(minutes=random.uniform(0, late_minutes))
            if is_late
            else dt.timedelta(seconds=random.uniform(0, 30))
        )
        rec = {
            "device_id": random.choice(DEVICES),
            "event_time": (now - offset).isoformat(),
            "temperature": round(random.gauss(20, 4), 2),
            "humidity": round(random.uniform(30, 80), 1),
        }
        if extra_field:
            rec["battery_pct"] = random.randint(1, 100)
        lines.append(json.dumps(rec))

    # Databricks: dbutils.fs.put(path, "\n".join(lines), overwrite=True)
    # Locally: write under a hidden name, then rename. The file source may list the folder
    # while a file is half-written; a rename is atomic, and Spark skips names starting with ".".
    path = LANDING / f"batch_{time.time_ns()}.json"
    tmp = LANDING / f".{path.name}.tmp"
    tmp.write_text("\n".join(lines))
    tmp.rename(path)
    return str(path)
