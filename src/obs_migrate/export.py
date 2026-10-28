"""Streaming local exporters with no overwrite and failure cleanup."""
import hashlib
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import time

import pyarrow.parquet as pq

from .events import SCHEMA, batches, from_json, json_bytes


@contextmanager
def atomic_output(output):
    """Private adjacent temporary file; publish complete content without overwrite."""
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise FileExistsError('output already exists; overwrite disabled')
    fd, temp = tempfile.mkstemp(prefix='.obs-migrate-', dir=output.parent)
    try:
        with os.fdopen(fd,'wb') as file:
            yield file
            file.flush()
            os.fsync(file.fileno())
        os.link(temp,output)
    finally:
        os.unlink(temp)


def export(events, output, format="parquet", *, stats=None, max_rows=1024, max_bytes=8*1024*1024):
    if format not in ("ndjson", "parquet"):
        raise ValueError("format must be ndjson or parquet")
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise FileExistsError("output already exists; overwrite disabled")
    start = time.monotonic()
    counts = {"events": 0, "raw_bytes": 0, "warning_count": 0, "missing_event_time": 0,
              "missing_index_time": 0, "missing_host": 0, "missing_source": 0,
              "missing_sourcetype": 0, "decode_errors": 0}
    digest = hashlib.sha256()

    def counted():
        for event in events:
            digest.update(json_bytes(event))
            counts["events"] += 1
            counts["raw_bytes"] += len(event["raw"])
            counts["warning_count"] += len(event["decoder_warnings"])
            for key in ("event_time", "index_time", "host", "source", "sourcetype"):
                counts[f"missing_{key}"] += event[key] is None
            yield event

    try:
        with atomic_output(output) as file:
            if format == "ndjson":
                for event in counted():
                    file.write(json_bytes(event))
            else:
                with pq.ParquetWriter(file, SCHEMA, compression="zstd", write_statistics=False) as writer:
                    for batch in batches(counted(), max_rows, max_bytes):
                        writer.write_batch(batch)
        counts.update(output_bytes=output.stat().st_size, canonical_sha256=digest.hexdigest(),
                      elapsed_seconds=time.monotonic() - start)
        counts["events_per_second"] = counts["events"] / max(counts["elapsed_seconds"], 1e-9)
        if stats is not None:
            stats.update(counts)
        return counts
    finally:
        close = getattr(events, "close", None)
        if close is not None:
            close()


def read_export(path, format):
    if format == "parquet-dataset":
        from .dataset import read_dataset
        yield from read_dataset(path)
    elif format == "ndjson":
        with open(path, "rb") as file:
            for line in file:
                yield from_json(line)
    elif format == "parquet":
        with pq.ParquetFile(path) as file:
            if not file.schema_arrow.equals(SCHEMA, check_metadata=True):
                raise ValueError("export schema mismatch")
            # Preserve exporter byte bounds: do not merge many large row groups.
            for group in range(file.num_row_groups):
                for batch in file.iter_batches(batch_size=1024, row_groups=[group]):
                    yield from batch.to_pylist()
    else:
        raise ValueError("unknown format")


def verify(events, path, format):
    from itertools import zip_longest
    sentinel = object()
    actual_rows = read_export(path, format)
    count = 0
    try:
        for expected, actual in zip_longest(events, actual_rows, fillvalue=sentinel):
            if expected is sentinel or actual is sentinel or expected != actual:
                raise ValueError(f"export mismatch at record {count}")
            count += 1
        return {"verified_events": count, "comparison": "all canonical fields, ordered", "independent_format_validation": False}
    finally:
        for iterator in (events, actual_rows):
            close = getattr(iterator, "close", None)
            if close is not None:
                close()
