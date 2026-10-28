"""Versioned transport contract; binary values never pass through text decoding."""
import base64
import json

import pyarrow as pa

EXACT_TIME = pa.struct([
    pa.field("seconds", pa.int64(), nullable=False),
    pa.field("fraction", pa.string(), nullable=False),
])
SCHEMA = pa.schema([
    pa.field("schema_version", pa.uint16(), nullable=False),
    pa.field("is_deleted", pa.bool_()), pa.field("bucket_disabled", pa.bool_()),
    pa.field("bucket_context_json", pa.string(), nullable=False),
    pa.field("raw", pa.binary(), nullable=False),
    pa.field("display_text", pa.string(), nullable=False),
    pa.field("event_time", EXACT_TIME),
    pa.field("index_time", EXACT_TIME),
    pa.field("host", pa.binary()), pa.field("source", pa.binary()),
    pa.field("sourcetype", pa.binary()), pa.field("index", pa.string()),
    pa.field("bucket_id", pa.string()), pa.field("object_uri", pa.string()),
    pa.field("source_sha256", pa.string(), nullable=False),
    pa.field("record_ordinal", pa.uint64(), nullable=False),
    pa.field("decompressed_offset", pa.uint64(), nullable=False),
    pa.field("record_wire", pa.binary(), nullable=False),
    pa.field("metadata_json", pa.string(), nullable=False),
    pa.field("decoder_state_json", pa.string(), nullable=False),
    pa.field("decoder_warnings", pa.list_(pa.field("element", pa.string())), nullable=False),
    pa.field("decoder", pa.string(), nullable=False),
], metadata={b"obs_migrate.schema": b"3", b"time_policy": b"unix_seconds_plus_exact_decimal_fraction;observed_v1_align5"})
BINARY_FIELDS = tuple(f.name for f in SCHEMA if pa.types.is_binary(f.type))


def json_bytes(event):
    row = dict(event)
    if row.get("schema_version") != 3 or set(row) != set(SCHEMA.names):
        raise ValueError("export schema mismatch")
    for key in BINARY_FIELDS:
        if row[key] is not None:
            row[key] = base64.b64encode(row[key]).decode("ascii")
    return (json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def from_json(line):
    row = json.loads(line)
    if row.get("schema_version") != 3 or set(row) != set(SCHEMA.names):
        raise ValueError("export schema mismatch")
    for key in BINARY_FIELDS:
        if row[key] is not None:
            row[key] = base64.b64decode(row[key], validate=True)
    return row


def batches(events, max_rows=1024, max_bytes=8 * 1024 * 1024):
    """At most max_rows and max_bytes serialized bytes, plus one bounded event."""
    if max_rows < 1 or max_bytes < 1:
        raise ValueError("batch limits must be positive")
    rows, size = [], 0
    for event in events:
        event_size = len(json_bytes(event))
        if rows and (len(rows) >= max_rows or size + event_size > max_bytes):
            yield pa.RecordBatch.from_pylist(rows, schema=SCHEMA)
            rows, size = [], 0
        rows.append(event)
        size += event_size
    if rows:
        yield pa.RecordBatch.from_pylist(rows, schema=SCHEMA)
