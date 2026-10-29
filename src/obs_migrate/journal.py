"""Experimental strict adapter to the pinned upstream Python decoder.

Small, original BOTS v3 fixtures exercise a limited journal subset. General
Splunk/DDSS compatibility remains unverified. Upstream is imported, never copied.
"""
from contextlib import contextmanager
import gzip
import base64
import hashlib
import io
import json
import math
import os
from pathlib import Path
import struct

import zstandard

from .wire import uvarint, signed32, event_components, exact_time
from .zstd_integrity import is_zstd_magic
from .lz4_integrity import MAGIC as LZ4_MAGIC
from .source_identity import SourceIdentityError,normalize_sha256,require_sha256

PIN = "d2885c63b36f115189f810eb0f43a888cc73c967"
DECODER = f"splunk-ddss-python@{PIN}+obs-strict-5"
EXTENDED_EVENT_DECODER = DECODER + "+event-float64-1"
MAX_RECORD = 16 * 1024 * 1024
MAX_DICTIONARY = 16 * 1024 * 1024
MAX_STRINGS = 100_000


class JournalError(ValueError):
    pass


class UnsupportedIndexType(JournalError):
    pass


def validate_index_type(index_type):
    if index_type not in ('unknown','event','metric'):
        raise ValueError('index_type must be unknown, event or metric')
    if index_type == 'metric':
        raise UnsupportedIndexType('the event adapter does not extract metric points; use experimental metrics journal-export for supported retained journals. Metric stubs require the full bucket, including TSIDX files')


def resolve(path):
    path = Path(path).resolve(strict=True)
    if path.is_dir():
        found = [p for name in ("journal", "journal.gz", "journal.zst", "journal.lz4")
                 if (p := path / "rawdata" / name).is_file()]
        if len(found) != 1:
            raise JournalError("bucket must contain exactly one rawdata/journal[.gz|.zst|.lz4]")
        path = found[0].resolve(strict=True)
    if not path.is_file():
        raise JournalError("source must be a regular local file")
    return path


def fingerprint(file):
    h = hashlib.sha256()
    while chunk := file.read(1024 * 1024):
        h.update(chunk)
    return h.hexdigest()


class HashReader(io.RawIOBase):
    def __init__(self, file):
        self.file, self.digest, self.count = file, hashlib.sha256(), 0

    def readable(self):
        return True

    def readinto(self, buffer):
        n = self.file.readinto(buffer)
        self.digest.update(memoryview(buffer)[:n])
        self.count += n
        return n


def _source_stamp(value):
    return (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)


def _check_source_identity(file,path,expected):
    try:
        descriptor=_source_stamp(os.fstat(file.fileno()))
        current_path=_source_stamp(os.stat(path))
    except OSError as error:raise JournalError('source changed during scan: descriptor/path unavailable') from error
    if descriptor!=expected or current_path!=expected:
        raise JournalError('source changed during scan: descriptor/path identity or stat differs')


@contextmanager
def open_source(path,*,max_decompressed_bytes=None,expected_sha256=None):
    """Hash original bytes twice and check descriptor/path stability around reads."""
    expected_sha256=normalize_sha256(expected_sha256)
    if max_decompressed_bytes is not None and (type(max_decompressed_bytes) is not int or max_decompressed_bytes<1):raise JournalError('decompressed byte limit must be a positive integer')
    with open(path, "rb", buffering=0) as file:
        stamp=_source_stamp(os.fstat(file.fileno()))
        _check_source_identity(file,path,stamp)
        sha = fingerprint(file)
        _check_source_identity(file,path,stamp)
        require_sha256(sha,expected_sha256,'journal source')
        file.seek(0)
        magic = file.read(4)
        file.seek(0)
        if is_zstd_magic(magic):
            from .zstd_integrity import preflight,FrameIntegrityError
            try:preflight(file,max_output_bytes=max_decompressed_bytes)
            except FrameIntegrityError as error:raise JournalError('zstd frame-integrity preflight failed: '+str(error)) from error
            file.seek(0)
        if magic==LZ4_MAGIC:
            from .lz4_integrity import preflight,FrameIntegrityError
            try:preflight(file,max_output_bytes=max_decompressed_bytes)
            except FrameIntegrityError as error:raise JournalError('lz4 frame-integrity preflight failed: '+str(error)) from error
            file.seek(0)
        _check_source_identity(file,path,stamp)
        reader = HashReader(file)
        with io.BufferedReader(reader) as buffered:
            magic = buffered.peek(4)[:4]
            if magic.startswith(b"\x1f\x8b"):
                stream = gzip.GzipFile(fileobj=buffered)
                compression = "gzip"
            elif is_zstd_magic(magic):
                stream = zstandard.ZstdDecompressor(max_window_size=32 * 1024 * 1024).stream_reader(buffered, read_across_frames=True,closefd=False)
                compression = "zstd"
            elif magic==LZ4_MAGIC:
                from lz4.frame import LZ4FrameFile
                stream = LZ4FrameFile(buffered, mode='rb')
                compression = 'lz4'
            else:
                stream, compression = buffered, "none"
            try:
                yield stream, sha, compression, reader
                _check_source_identity(file,path,stamp)
                # Require entire original stream consumption, including decompressor trailer.
                if buffered.read(1):
                    raise JournalError("unconsumed source bytes")
                if reader.digest.hexdigest() != sha:
                    raise JournalError("source changed during scan")
                _check_source_identity(file,path,stamp)
            finally:
                if stream is not buffered:
                    stream.close()


def decoder_class(*, metric_storage=False, max_decompressed_bytes=None, extended_events=False):
    if type(extended_events) is not bool or metric_storage and extended_events:
        raise ValueError("extended event admission requires an event-only decoder")
    if max_decompressed_bytes is not None and (type(max_decompressed_bytes) is not int or max_decompressed_bytes<1):
        raise ValueError('decompressed byte limit must be a positive integer')
    try:
        from .decoder_source import load
        JournalDecoder,JournalStream=load()
    except (ImportError,OSError,ValueError) as e:
        raise JournalError('pinned upstream Python decoder unavailable or differs from audited pin; register the audited source with obsmigrate setup-decoder --source /path/to/pinned-decoder') from e

    class GuardedStream(JournalStream):
        def __init__(self, reader):
            super().__init__(reader)
            self.capture = None
            self.end = None

        def _check(self, n, peek=False):
            if n < 0 or n > MAX_RECORD:
                raise JournalError("read exceeds record limit or has negative length")
            if max_decompressed_bytes is not None and self.pos+n>max_decompressed_bytes:
                raise JournalError('journal decompressed byte limit exceeded')
            if self.end is not None and self.pos + n > self.end:
                if peek:
                    return max(0, self.end - self.pos)
                raise JournalError("read crosses event boundary")
            return n

        def read(self, n):
            n = self._check(n)
            value = super().read(n)
            if self.capture is not None:
                self.capture.extend(value)
            return value

        def read_byte(self):
            return self.read(1)[0]

        def skip(self, n):
            self.read(n)
            return n

        def peek(self, n):
            return super().peek(self._check(n, peek=True))

        def read_uvarint(self):
            value = 0
            for i in range(10):
                b = self.read_byte()
                if i == 9 and b > 1:
                    raise JournalError("varint overflow")
                value |= (b & 127) << (i * 7)
                if not b & 128:
                    return value
            raise JournalError("unterminated varint")

    class StrictDecoder(JournalDecoder):
        def __init__(self, reader):
            super().__init__(reader)
            self.reader = GuardedStream(reader)
            self.dictionary_bytes = self.string_count = 0
            self.header = None
            self.record_offset = 0
            self.record_wire = b""
            self.private_seen = False
            self.state_time_seen = False
            self.metadata_entries = []

        def _read_string_field(self):
            value = self.reader.read(self.reader.read_uvarint())
            self.dictionary_bytes += len(value)
            self.string_count += 1
            if self.dictionary_bytes > MAX_DICTIONARY or self.string_count > MAX_STRINGS:
                raise JournalError("dictionary limit exceeded")
            return value.decode("utf-8", "surrogateescape")

        def _decode_header(self):
            if self.header is not None or self.reader.pos != 1:
                raise JournalError("repeated or misplaced header")
            data = self.reader.peek(6)
            if len(data) != 6:
                raise JournalError("truncated header")
            self.header = {"version": data[0], "align_bits": data[1],
                           "base_index_time": struct.unpack("<i", data[2:])[0]}
            if data[1] != 0 and (data[0], data[1]) != (1, 5):
                raise JournalError("unsupported journal version/alignment pair")
            super()._decode_header()

        def _decode_new_state(self):
            super()._decode_new_state()
            if self.opcode & 1:
                self.state_time_seen = True

        def decode_metadata(self, buffer):
            # Upstream owns journal framing. Replace only its incorrect conversion
            # of every metadata operand into a string dictionary reference.
            if self.event.metadata_count > 4096:
                raise JournalError("metadata entry count exceeds limit")
            offset = expanded = 0
            entries = []
            strings = self.fields.get(6, [])
            for _ in range(self.event.metadata_count):
                start = offset
                key, n = uvarint(buffer, offset)
                offset += n
                combined = key << 2 if self.opcode < 36 else key
                representation, key_index = combined & 15, combined >> 4
                supported = (0, 10) if metric_storage or extended_events and self.opcode == 39 else (0, 8)
                if representation not in supported:
                    raise JournalError(f"unsupported metadata representation {representation}")
                if not 1 <= key_index <= len(strings):
                    raise JournalError("metadata key dictionary reference out of bounds")
                operand, n = uvarint(buffer, offset)
                offset += n
                key_bytes = strings[key_index-1].encode("utf-8", "surrogateescape")
                expanded += len(key_bytes)
                entry = {"key": base64.b64encode(key_bytes).decode("ascii"),
                         "representation": representation, "operand": str(operand),
                         "wire": base64.b64encode(buffer[start:offset]).decode("ascii")}
                if representation == 0:
                    if not 1 <= operand <= len(strings):
                        raise JournalError("metadata value dictionary reference out of bounds")
                    value = strings[operand-1].encode("utf-8", "surrogateescape")
                    expanded += len(value)
                    entry["value_base64"] = base64.b64encode(value).decode("ascii")
                elif representation == 8:
                    entry["value_uint64"] = str(operand)
                else:
                    # In observed extended metric records this is a byte offset
                    # into the retained storage block, not the Float64 bits.
                    entry["storage_offset"] = str(operand)
                if expanded > MAX_RECORD:
                    raise JournalError("expanded metadata exceeds record limit")
                entries.append(entry)
            self.metadata_entries = entries
            self.event.metadata_fields = {}
            return offset

        def decode_field(self, key, value):
            if key <= 0 or value <= 0:
                raise JournalError("zero dictionary reference")
            return super().decode_field(key, value)

        def _decode_next(self):
            if self.header is None and self.opcode != 10:
                raise JournalError("journal header required")
            if self.opcode == 9:
                self.private_seen = True
            self.record_offset = self.reader.pos - 1
            self.reader.capture = bytearray([self.opcode])
            try:
                super()._decode_next()
                self.record_wire = bytes(self.reader.capture)
            finally:
                self.reader.capture = None

        def _decode_event(self):
            self.metadata_entries = []
            if self.header["align_bits"] and (self.reader.pos - 1) % (1 << self.header["align_bits"]):
                raise JournalError("event opcode violates declared alignment")
            if metric_storage and self.opcode != 39:
                raise JournalError("unverified metric journal opcode; observed profile requires opcode 39")
            if not metric_storage and (self.opcode <= 2 or self.opcode & 4):
                if not (extended_events and self.opcode == 39 and
                        (self.header['version'], self.header['align_bits']) == (1, 5)):
                    raise JournalError("oldstyle/extended storage events unsupported; extended profile requires declared event index")
            start = self.reader.pos
            length = self.reader.read_uvarint()
            prefix_end = self.reader.pos
            if length > MAX_RECORD - 11:
                raise JournalError("event exceeds record limit")
            # Restore consumed length prefix so upstream still owns event parsing.
            prefix = bytes(self.reader.capture[1:])
            self.reader.buffer[:0] = prefix
            self.reader.pos = start
            del self.reader.capture[1:]
            self.reader.end = prefix_end + length
            try:
                super()._decode_event()
                if self.reader.pos != self.reader.end:
                    raise JournalError("event length mismatch")
                if self.total_metadata_errors or self.event.metadata_fields.get("__extraction_errors__"):
                    raise JournalError("metadata extraction failed; recovery disabled")
            finally:
                self.reader.end = None

    return StrictDecoder



def _decoder_warnings(time_profile,index_within_bounds,private_seen):
    warnings=["experimental_journal_decoder", "timestamp_profile_limited" if time_profile else "timestamp_semantics_unverified",
              "physical_extraction_not_splunk_search"]
    if index_within_bounds is False:warnings.append("index_candidate_outside_bucket_info")
    if private_seen:warnings.append("private_data_not_interpreted_retain_source_archive")
    return warnings



def _extended_event_metadata(entries, extension):
    """Observed aligned Float64 offsets; retain every operand and exact bit."""
    if not extension or len(extension) % 8 or len(extension) > MAX_RECORD:
        raise JournalError("unverified extended event storage length")
    offsets = set()
    result = []
    for original in entries:
        entry = dict(original)
        if entry['representation'] == 10:
            offset = int(entry['storage_offset'])
            if offset < 0 or offset % 8 or offset + 8 > len(extension) or offset in offsets:
                raise JournalError("invalid or overlapping extended event Float64 offset")
            offsets.add(offset)
            bits = extension[offset:offset+8]
            value = struct.unpack('<d', bits)[0]
            if not math.isfinite(value):
                raise JournalError("nonfinite extended event Float64 is unverified")
            entry['value_float64_bits_le_base64'] = base64.b64encode(bits).decode('ascii')
            entry['value_float64_hex'] = value.hex()
        elif entry['representation'] != 0:
            raise JournalError("unverified extended event metadata representation")
        result.append(entry)
    if len(offsets) != len(extension) // 8:
        raise JournalError("unreferenced extended event storage bytes")
    return result


def _event_row(decoder, context, sha, ordinal, *, index=None, bucket_id=None, object_uri=None, visibility="physical"):
    """One canonical row builder, shared by full scanning and addressed reads."""
    e = decoder.get_event()
    def binary(s):
        return s.encode("utf-8", "surrogateescape")
    metadata = decoder.metadata_entries
    extended = decoder.record_wire[0] == 39
    components = (event_components(decoder.record_wire, allow_extended=True) if extended
                  else event_components(decoder.record_wire))
    extension = None
    if extended:
        length = components['extended_storage_length']
        end = len(decoder.record_wire) - len(e.message)
        if length != e.extended_storage_len or length > end:
            raise JournalError("invalid extended event block length")
        extension = decoder.record_wire[end-length:end]
        metadata = _extended_event_metadata(metadata, extension)
    index_candidate = decoder.base_index_time + signed32(components["index_delta_wire"])
    time_profile = (decoder.header.get("version") == 1 and
                    decoder.header.get("align_bits") == 5 and decoder.state_time_seen)
    event_time = exact_time(decoder.base_event_time, components["subsecond_wire"]) if time_profile else None
    index_time = exact_time(index_candidate) if time_profile else None
    state = {"header": decoder.header, "base_event_time": decoder.base_event_time,
             "base_event_time_seen": decoder.state_time_seen,
             "index_time_diff": e.index_time_diff, "time_sub_seconds_shifted": e.time_sub_seconds,
             "upstream_event_time_candidate_ms": e.event_time,
             "upstream_index_time_candidate_s": e.index_time,
             "stream_id": e.stream_id, "stream_offset": e.stream_offset,
             "stream_sub_offset": e.stream_sub_offset, "has_hash": e.has_hash,
             "wire_components": components,
             "index_time_candidate_s": index_candidate,
             "event_time_second_candidate": decoder.base_event_time,
             "subsecond_interpretation": "decimal_marker_v1_align5" if time_profile else "unverified",
             "active_host": decoder.active_host, "active_source": decoder.active_source,
             "active_sourcetype": decoder.active_source_type,
             "hash_hex": e.hash.hex(), "include_punctuation": e.include_punctuation}
    if extended:
        state['extended_storage_base64'] = base64.b64encode(extension).decode('ascii')
        state['extended_event_profile'] = 'v1-align5-opcode39-float64-offsets'
    deleted = context.classify(decoder.record_offset, state)
    if visibility == "undeleted" and deleted:
        return None
    warnings = _decoder_warnings(time_profile,state["index_candidate_within_bucket_info"],decoder.private_seen)
    return {"schema_version": 3, "is_deleted": deleted, "bucket_disabled": context.disabled,
           "bucket_context_json": context.context_json, "raw": e.message, "display_text": e.message.decode("utf-8", "replace"),
           "event_time": event_time, "index_time": index_time,
           "host": binary(e.host) if decoder.active_host else None,
           "source": binary(e.source) if decoder.active_source else None,
           "sourcetype": binary(e.sourcetype) if decoder.active_source_type else None,
           "index": index, "bucket_id": bucket_id, "object_uri": object_uri,
           "source_sha256": sha, "record_ordinal": ordinal,
           "decompressed_offset": decoder.record_offset, "record_wire": decoder.record_wire,
           "metadata_json": json.dumps(metadata, sort_keys=True, separators=(",", ":")),
           "decoder_state_json": json.dumps(state, sort_keys=True, separators=(",", ":")),
           "decoder_warnings": warnings, "decoder": EXTENDED_EVENT_DECODER if extended else DECODER}

def events(path, *, experimental=False, index=None, bucket_id=None, object_uri=None, stats=None,
           visibility="physical", include_disabled=False, index_type="unknown",max_decompressed_bytes=None,_observer=None,expected_source_sha256=None):
    validate_index_type(index_type)
    if not experimental:
        raise JournalError("journal decoder remains experimental; pass --experimental to run the journal harness")
    path = resolve(path)
    options = {'extended_events': True} if index_type == 'event' else {}
    if max_decompressed_bytes is not None:
        options['max_decompressed_bytes'] = max_decompressed_bytes
    cls = decoder_class(**options)
    ordinal = 0
    try:
        from .buckets import BucketContext
        context = BucketContext(path, visibility, include_disabled)
        source_context=(open_source(path) if max_decompressed_bytes is None and expected_source_sha256 is None
            else open_source(path,max_decompressed_bytes=max_decompressed_bytes,expected_sha256=expected_source_sha256))
        with source_context as (stream, sha, compression, reader):
            decoder = cls(stream if _observer is None else _observer.wrap(stream))
            while decoder.scan():
                row = _event_row(decoder, context, sha, ordinal, index=index,
                    bucket_id=bucket_id, object_uri=object_uri, visibility=visibility)
                if row is not None:
                    context.emitted_count += 1
                    if _observer is not None:_observer.event(decoder, row)
                    yield row
                ordinal += 1
            if decoder.err():
                raise JournalError(f"decode failed at decompressed byte {decoder.reader.pos}: {decoder.err()}")
            if not decoder.reader._eof or decoder.reader.buffer:
                raise JournalError(f"incomplete source at decompressed byte {decoder.reader.pos}")
            if decoder.header is None or ordinal == 0:
                raise JournalError("no events decoded; this may be an empty journal or a metric stub, not proof of an empty archive. Preserve the full bucket and TSIDX files")
            context.finish(stats)
            if _observer is not None:_observer.finish(decoder, context, sha)
            if stats is not None:
                stats.update(source_bytes=reader.count,
                             source_bytes_read=reader.count * (3 if compression in ("zstd","lz4") else 2),
                             total_source_bytes_read=reader.count * (3 if compression in ("zstd","lz4") else 2) + stats.get("sidecar_bytes_read", 0),
                             decompressed_bytes=decoder.reader.pos,
                             source_sha256=sha, compression=compression)
    except (SourceIdentityError,ModuleNotFoundError):raise
    except Exception as e:
        raise JournalError(f"{path.name}: record {ordinal}: {e}") from e


def inspect(path, *, index_type="unknown"):
    if index_type not in ('unknown','event','metric'):
        raise ValueError('index_type must be unknown, event or metric')
    path = resolve(path)
    with open(path, "rb") as file:
        sha = fingerprint(file)
        size = file.tell()
        file.seek(0)
        magic = file.read(4)
    return {"source_sha256": sha, "bytes": size,
            "compression": "gzip" if magic.startswith(b"\x1f\x8b") else "zstd" if magic == b"\x28\xb5\x2f\xfd" else "lz4" if magic==LZ4_MAGIC else "none",
            "journal_compatibility": "unverified", "indexed_search": False,
            "declared_index_type": index_type, "metric_point_extraction": False,
            "metric_point_extraction_scope": "event_adapter",
            "retained_metric_journal_reader": "experimental; use metrics journal-export with full profile validation"}
