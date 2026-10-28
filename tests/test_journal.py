import gzip
import io
from types import SimpleNamespace

import pytest
import zstandard as zstd

from obs_migrate import journal
from obs_migrate.cli import main


@pytest.mark.parametrize("compression", ["none", "gzip", "zstd"])
def test_stream_transport_magic_not_extension(tmp_path, compression):
    # Ordinary bytes only: tests compression transport, never journal compatibility.
    data = b"open transport test\x00\xff"*10000
    encoded = {"none": lambda x: x, "gzip": gzip.compress,
               "zstd": zstd.ZstdCompressor(write_checksum=True).compress}[compression](data)
    path = tmp_path / "misleading.extension"
    path.write_bytes(encoded)
    with journal.open_source(path) as (stream, sha, detected, stats):
        assert detected == compression
        result = bytearray()
        while chunk := stream.read(4096):
            result.extend(chunk)
        assert bytes(result) == data
    assert path.read_bytes() == encoded


@pytest.mark.parametrize("compress", [gzip.compress, zstd.ZstdCompressor().compress])
def test_truncated_compression_fails(tmp_path, compress):
    path = tmp_path / "truncated"
    path.write_bytes(compress(b"transport data")[:-2])
    with pytest.raises((ValueError, EOFError, OSError)):
        with journal.open_source(path) as (stream, *_):
            while stream.read(8):
                pass


@pytest.mark.parametrize("data", [b"", b"\x0a", b"\x0a\x01", b"not a journal", b"\x00"])
def test_malformed_input_never_publishes(tmp_path, data):
    path, out = tmp_path / "bad", tmp_path / "output"
    path.write_bytes(data)
    assert main(["buckets", "export", str(path), "--out", str(out), "--experimental"]) == 1
    assert path.read_bytes() == data
    assert not out.exists()
    assert not list(tmp_path.glob(".obs-migrate-*"))


def test_no_claim_without_explicit_experimental_flag(tmp_path):
    path = tmp_path / "journal"
    path.write_bytes(b"ordinary bytes")
    with pytest.raises(journal.JournalError, match="remains experimental"):
        list(journal.events(path))
    assert journal.inspect(path)["journal_compatibility"] == "unverified"


def test_ambiguous_bucket_and_source_protection(tmp_path):
    bucket = tmp_path / "bucket"
    rawdata = bucket / "rawdata"
    rawdata.mkdir(parents=True)
    (rawdata / "journal").write_bytes(b"ordinary bytes")
    assert main(["buckets", "export", str(bucket), "--out", str(bucket / "export"), "--experimental"]) == 1
    (rawdata / "journal.gz").touch()
    with pytest.raises(ValueError, match="exactly one"):
        journal.resolve(bucket)


def test_strict_string_bytes_and_limits():
    cls = journal.decoder_class()
    # A length-prefixed string primitive, not a fabricated journal.
    decoder = cls(io.BytesIO(b"\x02\xff\x00"))
    assert decoder._read_string_field().encode("utf-8", "surrogateescape") == b"\xff\x00"
    decoder = cls(io.BytesIO(b"\x80"*11))
    with pytest.raises(ValueError, match="overflow"):
        decoder.reader.read_uvarint()
    for n in (-1, journal.MAX_RECORD+1):
        with pytest.raises(ValueError):
            decoder.reader.skip(n)


def test_adapter_preserves_fields_without_promoting_timestamp_claims(tmp_path, monkeypatch):
    # Stubbed decoder event proves adapter mapping only, no journal format claim.
    class Stub:
        def __init__(self, reader):
            self.input = reader
            self.reader = SimpleNamespace(_eof=True, buffer=b"", pos=123)
            self.header = {"version": 1}
            self.base_event_time = 9
            self.base_index_time = 15
            self.metadata_entries = []
            self.state_time_seen = True
            self.private_seen = True
            self.active_host = self.active_source = self.active_source_type = 1
            self.record_offset = 42
            self.record_wire = b"opaque record bytes"
            self.done = False
        def scan(self):
            self.input.read()
            if self.done:
                return False
            self.done = True
            return True
        def get_event(self):
            return SimpleNamespace(message=b"\xff\x00", host="h\udcff", source="", sourcetype="t",
                metadata_fields={"k\udcff": ["v\udcff", ""]}, index_time_diff=2,
                time_sub_seconds=123456789, event_time=123465789, index_time=17,
                stream_id=1, stream_offset=2, stream_sub_offset=3, has_hash=False,
                hash=b"\0"*20, include_punctuation=False)
        def err(self):
            return None
    monkeypatch.setattr(journal, "decoder_class", lambda: Stub)
    monkeypatch.setattr(journal, "event_components", lambda _: {"index_delta_wire": 2})
    path = tmp_path / "stub"
    path.write_bytes(b"ordinary transport bytes")
    rows = list(journal.events(path, experimental=True))
    assert rows[0]["raw"] == b"\xff\x00"
    assert rows[0]["host"] == b"h\xff"
    assert rows[0]["source"] == b""
    assert rows[0]["event_time"] is None and rows[0]["index_time"] is None
    assert '123456789' in rows[0]["decoder_state_json"]
    assert rows[0]["object_uri"] is None
    assert rows[0]["record_wire"] == b"opaque record bytes"


def test_source_change_detected(tmp_path):
    path = tmp_path / "changing"
    path.write_bytes(b"a"*100000)
    with pytest.raises(ValueError, match="source changed"):
        with journal.open_source(path) as (stream, *_):
            with path.open("r+b") as changed:
                changed.seek(90000)
                changed.write(b"changed")
            while stream.read(4096):
                pass


def test_zstd_integrity_does_not_require_external_cli(tmp_path, monkeypatch):
    import shutil,subprocess
    path=tmp_path/'compressed';path.write_bytes(zstd.ZstdCompressor().compress(b'transport test'))
    monkeypatch.setattr(shutil,'which',lambda _:None)
    def denied(*args,**kwargs):raise AssertionError('no external validator should run')
    monkeypatch.setattr(subprocess,'run',denied)
    with journal.open_source(path) as (stream,_,compression,_):
        assert compression=='zstd' and stream.read()==b'transport test'


def test_dictionary_budget_fails():
    cls = journal.decoder_class()
    decoder = cls(io.BytesIO(b"\x01x"))
    decoder.dictionary_bytes = journal.MAX_DICTIONARY
    with pytest.raises(ValueError, match="dictionary limit"):
        decoder._read_string_field()
