from pathlib import Path

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

DATA_DIR = Path(__file__).resolve().parents[2] / "data"

schema_json = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "favorite_number", "type": ["int", "null"]},
    {"name": "favorite_color", "type": ["string", "null"]}
  ]
}
"""
schema = avro.schema.parse(schema_json)

user_data = [
    {"name": "Alice", "favorite_number": 123, "favorite_color": "blue"},
    {"name": "Bob", "favorite_color": "green"},
    {"name": "Charlie", "favorite_number": 789},
]


DATA_DIR.mkdir(exist_ok=True)

# writing the data to an Avro file
destination_path = DATA_DIR / "user.avro"
with open(destination_path, "wb") as out_file:
    writer = DataFileWriter(out_file, DatumWriter(), schema)
    for user in user_data:
        writer.append(user)
    writer.close()


# read the data back from the Avro file
source_path = DATA_DIR / "user.avro"

with open(source_path, "rb") as in_file:
    reader = DataFileReader(in_file, DatumReader())
    for user in reader:
        print(user)
    reader.close()
