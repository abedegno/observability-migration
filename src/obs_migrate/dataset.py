"""Partitioned local Parquet with provisional checkpoints and replay-based resume.

Only a complete manifest publishes a dataset. Resume scans from the start: gzip
offsets and decoder state are not safe restart tokens. Existing partitions are
validated against both their hashes and newly decoded canonical rows.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import itertools
import json
import os
from pathlib import Path
import stat
import tempfile
import time
import uuid

from .events import SCHEMA, json_bytes
from .export import export, read_export

SCHEMA_SHA = hashlib.sha256(SCHEMA.serialize().to_pybytes()).hexdigest()
BINDING = ("schema_version", "source_sha256", "decoder", "bucket_context_json",
           "index", "bucket_id", "object_uri")
COUNTERS = ("events", "raw_bytes", "output_bytes", "warning_count", "missing_event_time",
            "missing_index_time", "missing_host", "missing_source", "missing_sourcetype")


def _regular(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("dataset files must be regular files, without symlinks")


def _sha(path):
    _regular(path)
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def _sync(directory):
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _save(directory, manifest):
    content = json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    if len(content.encode()) > 16 * 1024 * 1024:
        raise ValueError("dataset manifest exceeds 16 MiB")
    fd, temp = tempfile.mkstemp(prefix=".checkpoint-", dir=directory)
    try:
        with os.fdopen(fd, "w") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp, directory / "manifest.json")
        _sync(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


@contextmanager
def _lock(directory):
    fd = os.open(directory / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("invalid dataset lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("dataset writer already active") from None
        yield
    finally:
        os.close(fd)


def _load(directory):
    path = directory / "manifest.json"
    _regular(path)
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("dataset manifest exceeds 16 MiB")
    manifest = json.loads(path.read_text())
    if (not isinstance(manifest, dict) or manifest.get("dataset_version") != 1 or manifest.get("schema_sha256") != SCHEMA_SHA
            or not isinstance(manifest.get("complete"), bool)):
        raise ValueError("dataset manifest/schema mismatch")
    try:
        if (set(manifest["binding"]) != set(BINDING) or not isinstance(manifest["parts"], list)
                or set(manifest["limits"]) != {"rows", "canonical_bytes"}
                or any(type(v) is not int or v < 1 for v in manifest["limits"].values())):
            raise ValueError("invalid dataset binding/limits")
        if manifest["complete"] and (not manifest["parts"] or len(manifest["canonical_sha256"]) != 64):
            raise ValueError("invalid complete dataset")
        for n, part in enumerate(manifest["parts"]):
            if (part["path"] != f"part-{n:06d}.parquet"
                    or any(type(part[key]) is not int or part[key] < 0 for key in COUNTERS)
                    or part["events"] < 1 or len(part["sha256"]) != 64
                    or len(part["canonical_sha256"]) != 64
                    or part["last_record_ordinal"] < part["first_record_ordinal"]):
                raise ValueError("invalid dataset partition entry")
    except (KeyError, TypeError, AttributeError):
        raise ValueError("malformed dataset manifest") from None
    return manifest


def _part_rows(directory, part):
    path = directory / part["path"]
    if _sha(path) != part["sha256"] or path.stat().st_size != part["output_bytes"]:
        raise ValueError("dataset partition checksum mismatch")
    digest, count, raw_bytes = hashlib.sha256(), 0, 0
    first = last = None
    for row in read_export(path, "parquet"):
        digest.update(json_bytes(row))
        count += 1
        raw_bytes += len(row["raw"])
        if first is None:
            first = row["record_ordinal"]
        last = row["record_ordinal"]
        yield row
    if (count != part["events"] or digest.hexdigest() != part["canonical_sha256"]
            or raw_bytes != part["raw_bytes"] or first != part["first_record_ordinal"]
            or last != part["last_record_ordinal"]):
        raise ValueError("dataset partition content mismatch")


def read_dataset(directory):
    """Yield only a manifest-complete dataset; consume fully to validate integrity."""
    directory = Path(directory)
    manifest = _load(directory)
    if not manifest["complete"]:
        raise ValueError("dataset incomplete; checkpoints are provisional")
    before = _sha(directory / "manifest.json")
    previous = -1
    digest = hashlib.sha256()
    for part in manifest["parts"]:
        for row in _part_rows(directory, part):
            if ({key: row[key] for key in BINDING} != manifest["binding"]
                    or row["record_ordinal"] <= previous):
                raise ValueError("dataset binding/order mismatch")
            previous = row["record_ordinal"]
            digest.update(json_bytes(row))
            yield row
    if digest.hexdigest() != manifest["canonical_sha256"]:
        raise ValueError("dataset canonical checksum mismatch")
    if _sha(directory / "manifest.json") != before:
        raise ValueError("dataset manifest changed while reading")


def export_dataset(rows, directory, *, resume=False, partition_rows=100_000,
                   partition_bytes=64 * 1024 * 1024, stats=None):
    """Export bounded Parquet partitions. Resume validates files and replays prefix.

    The bytes limit is canonical NDJSON size, plus at most one bounded row, not a
    bound on compressed file size or RSS. Each partition reuses the normal writer.
    """
    directory = Path(directory)
    start = time.monotonic()
    if partition_rows < 1 or partition_bytes < 1:
        raise ValueError("partition limits must be positive")
    limits = {"rows": partition_rows, "canonical_bytes": partition_bytes}
    try:
        if resume:
            if directory.is_symlink() or not directory.is_dir():
                raise ValueError("resume requires an existing dataset directory")
        else:
            directory.mkdir(mode=0o700)
        with _lock(directory):
            if resume and not (directory / "manifest.json").exists() and not (directory / "manifest.json").is_symlink():
                if {p.name for p in directory.iterdir()} != {".lock"}:
                    raise ValueError("resume directory has data but no valid checkpoint")
                manifest = None
            else:
                manifest = _load(directory) if resume else None
            if manifest is not None and manifest["limits"] != limits:
                raise ValueError("resume partition limits mismatch")
            iterator = iter(rows)
            first = next(iterator, None)
            if first is None:
                raise ValueError("no events; refusing an unbound empty dataset")
            binding = {key: first[key] for key in BINDING}
            if manifest is None:
                manifest = {"dataset_version": 1, "schema_sha256": SCHEMA_SHA,
                            "binding": binding, "limits": limits, "complete": False, "parts": []}
                _save(directory, manifest)
            elif manifest["binding"] != binding:
                raise ValueError("resume source/context/decoder binding mismatch")

            whole_digest = hashlib.sha256()
            def checked():
                previous = -1
                for row in itertools.chain([first], iterator):
                    if {key: row[key] for key in BINDING} != binding:
                        raise ValueError("dataset source/context binding changed")
                    if row["record_ordinal"] <= previous:
                        raise ValueError("dataset physical ordinals must increase")
                    previous = row["record_ordinal"]
                    whole_digest.update(json_bytes(row))
                    yield row

            source = iter(checked())
            reused = 0
            for part in manifest["parts"]:
                # Validate saved data independently before comparing fresh input.
                digest = hashlib.sha256()
                for _ in _part_rows(directory, part):
                    row = next(source, None)
                    if row is None:
                        raise ValueError("resume source shorter than checkpoint")
                    digest.update(json_bytes(row))
                if digest.hexdigest() != part["canonical_sha256"]:
                    raise ValueError("resume replay mismatch")
                reused += 1
            pending = next(source, None)
            if manifest["complete"]:
                if pending is not None:
                    raise ValueError("completed dataset has fewer events than source")
                if whole_digest.hexdigest() != manifest["canonical_sha256"]:
                    raise ValueError("completed dataset canonical checksum mismatch")
            else:
                while pending is not None:
                    initial, pending = pending, None
                    first_ordinal = last_ordinal = initial["record_ordinal"]

                    def partition():
                        nonlocal pending, last_ordinal
                        count, size = 0, 0
                        for row in itertools.chain([initial], source):
                            row_size = len(json_bytes(row))
                            if count and size + row_size > partition_bytes:
                                pending = row
                                return
                            last_ordinal = row["record_ordinal"]
                            count += 1
                            size += row_size
                            yield row
                            if count >= partition_rows:
                                return

                    candidate = directory / (".candidate-" + uuid.uuid4().hex + ".parquet")
                    target = directory / f"part-{len(manifest['parts']):06d}.parquet"
                    try:
                        counts = export(partition(), candidate)
                        entry = {key: counts[key] for key in (*COUNTERS, "canonical_sha256")}
                        entry.update(path=target.name, sha256=_sha(candidate),
                                     first_record_ordinal=first_ordinal, last_record_ordinal=last_ordinal)
                        if target.exists() or target.is_symlink():
                            # Crash after part publication but before checkpoint: adopt
                            # only a byte-identical orphan. Never replace existing data.
                            if _sha(target) != entry["sha256"]:
                                raise ValueError("uncheckpointed partition differs; refusing overwrite")
                        else:
                            os.link(candidate, target)
                            _sync(directory)
                        manifest["parts"].append(entry)
                        _save(directory, manifest)
                    finally:
                        candidate.unlink(missing_ok=True)
                    if pending is None:
                        pending = next(source, None)
                # Exhaustion above runs journal integrity/sidecar reconciliation.
                manifest["complete"] = True
                manifest["canonical_sha256"] = whole_digest.hexdigest()
                _save(directory, manifest)
            result = {key: sum(p[key] for p in manifest["parts"]) for key in COUNTERS}
            result.update(partitions=len(manifest["parts"]), reused_partitions=reused,
                          canonical_sha256=whole_digest.hexdigest(), elapsed_seconds=time.monotonic()-start,
                          complete=True, resume_method="validated_full_prefix_replay")
            if stats is not None:
                stats.update(result)
            return result
    finally:
        close = getattr(rows, "close", None)
        if close is not None:
            close()
