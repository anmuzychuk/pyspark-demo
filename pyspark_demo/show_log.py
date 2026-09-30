import json


def show_log(log_dir, last=2):
    """Print the newest entries of a checkpoint log: offsets/, commits/ or sources/0/."""
    # Skip Hadoop's binary checksum sidecars (.0.crc, .1.crc, ...): they are not the log.
    entries = [p for p in log_dir.iterdir() if p.is_file() and not p.name.startswith(".")]
    entries.sort(key=lambda p: int(p.name.split(".")[0]))  # 0, 1, ..., 9.compact, 10, ...
    print(f"== {log_dir}/  ({len(entries)} entries, showing the last {last})")
    for p in entries[-last:]:
        version, *records = p.read_text().splitlines()
        print(f"-- {p.name}  (format {version})")
        for line in records:
            record = json.loads(line)
            if "conf" in record:  # the session confs the query must keep; long, so summarise
                record["conf"] = f"<{len(record['conf'])} settings>"
            print("  ", json.dumps(record))
    print()
