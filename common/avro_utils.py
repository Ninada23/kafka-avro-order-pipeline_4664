"""Schemaless Avro (de)serialization helpers.

No Schema Registry is used for this assignment: the producer and consumer
both load the same .avsc file from disk and agree on the schema out of band,
which keeps the setup simple while still satisfying the "Avro serialization"
requirement.
"""
import io
import json

from fastavro import parse_schema, schemaless_reader, schemaless_writer


def load_schema(path: str):
    with open(path, "r") as f:
        return parse_schema(json.load(f))


def serialize(schema, record: dict) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()


def deserialize(schema, data: bytes) -> dict:
    buf = io.BytesIO(data)
    return schemaless_reader(buf, schema)
