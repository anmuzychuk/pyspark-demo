import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

path = "data/users_big.avro"
schema = avro.schema.parse(
    '{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]}'
)
with open(path, "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), schema)
    for i in range(20_000):
        writer.append({"id": i, "name": f"user_{i:06d}"})
    writer.close()

data = open(path, "rb").read()
sync = DataFileReader(open(path, "rb"), DatumReader()).sync_marker
offsets, pos = [], 0
while (pos := data.find(sync, pos)) != -1:
    offsets.append(pos)
    pos += len(sync)
print(offsets)
