"""Real Splunk BOTS v3 archive bytes, never synthesized journal structures."""
from collections import Counter
import csv
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from obs_migrate.journal import JournalError, events
from obs_migrate.export import export, verify

FIXTURES = Path(__file__).parent / "fixtures" / "botsv3"

# Manually transcribed from gzip-decoded public one-event archive, not generated
# by the decoder. This checks parser framing/byte preservation, not an external
# Splunk search oracle. Host/source counts are independently stored by Splunk.
ONE_RAW = b"<error><message>Splunkd daemon is not responding: ('Error connecting to /services/shcluster/config/config: [Errno 111] Connection refused',)</message></error>"


def table(bucket, name):
    with (bucket / name).open() as file:
        rows = list(csv.reader(file, delimiter="\t"))
    return int(rows[0][2]), {r[1].encode(): int(r[2]) for r in rows[1:]}


def test_original_fixture_checksums():
    manifest = json.loads((FIXTURES / "provenance.json").read_text())
    assert manifest["md5"] == "d7ccca99a01cff070dff3c139cdc10eb"
    assert manifest["license"] == "CC0-1.0"
    for file in manifest["files"]:
        data = (FIXTURES / file["fixture_path"]).read_bytes()
        assert len(data) == file["bytes"]
        assert hashlib.sha256(data).hexdigest() == file["sha256"]


@pytest.mark.parametrize("name,count", [("one-event", 1), ("stream-29", 29), ("typed-metadata", 1)])
def test_real_journal_counts_match_splunk_metadata(name, count):
    bucket = FIXTURES / name
    rows = list(events(bucket, experimental=True))
    assert len(rows) == count
    for field, filename in (("host", "Hosts.data"), ("source", "Sources.data"),
                            ("sourcetype", "SourceTypes.data")):
        expected_count, expected_values = table(bucket, filename)
        assert len(rows) == expected_count
        assert Counter(row[field] for row in rows) == expected_values
    with gzip.open(bucket / "rawdata/journal.gz", "rb") as file:
        wire = file.read()
    for ordinal, row in enumerate(rows):
        assert row["record_ordinal"] == ordinal
        assert row["decompressed_offset"] % 32 == 0
        offset = row["decompressed_offset"]
        assert wire[offset:offset+len(row["record_wire"])] == row["record_wire"]
        assert row["record_wire"].endswith(row["raw"])
        assert row["event_time"] is not None and row["index_time"] is not None
        assert json.loads(row["decoder_state_json"])["header"]["align_bits"] == 5


def test_one_event_manually_checked_payload_and_metadata():
    row, = events(FIXTURES / "one-event", experimental=True)
    assert row["raw"] == ONE_RAW
    assert row["host"] == b"host::OD-FM-NA-i-0ad2d665d4bdace22.amazonaws.com"
    assert row["source"] == b"source::ess_content_importer"
    assert row["sourcetype"] == b"sourcetype::ess_content_importer"
    assert json.loads(row["metadata_json"]) == [{"key": "dGltZXN0YW1w", "representation": 0,
        "operand": "2", "value_base64": "bm9uZQ==", "wire": "EAI="}]
    state = json.loads(row["decoder_state_json"])
    assert state["index_time_diff"] == state["time_sub_seconds_shifted"] == 0
    assert state["upstream_index_time_candidate_s"] == 1568916650


@pytest.mark.parametrize("format", ["ndjson", "parquet"])
@pytest.mark.parametrize("name", ["one-event", "stream-29", "typed-metadata"])
def test_real_journal_export_roundtrip(tmp_path, format, name):
    bucket = FIXTURES / name
    path = tmp_path / format
    before = hashlib.sha256((bucket / "rawdata/journal.gz").read_bytes()).hexdigest()
    stats = export(events(bucket, experimental=True), path, format)
    result = verify(events(bucket, experimental=True), path, format)
    assert result["verified_events"] == stats["events"] == table(bucket, "Hosts.data")[0]
    assert hashlib.sha256((bucket / "rawdata/journal.gz").read_bytes()).hexdigest() == before


def test_real_unsigned_metadata_and_index_time_candidate():
    import base64
    row, = events(FIXTURES / "typed-metadata", experimental=True)
    entries = {base64.b64decode(e["key"]): e for e in json.loads(row["metadata_json"])}
    assert entries[b"date_year"]["representation"] == 8
    assert entries[b"date_year"]["value_uint64"] == "2018"
    assert entries[b"date_zone"]["value_uint64"] == "0"
    assert entries[b"timestartpos"]["value_uint64"] == "42"
    assert entries[b"timeendpos"]["value_uint64"] == "52"
    state = json.loads(row["decoder_state_json"])
    assert state["index_time_candidate_s"] == 1534756550
    assert state["wire_components"]["index_delta_wire"] == 4293086949
    assert state["upstream_index_time_candidate_s"] - state["index_time_candidate_s"] == 1 << 32
    assert row["index_time"] == {"seconds": 1534756550, "fraction": "0"}


def test_unknown_alignment_rejected_from_real_fixture(tmp_path):
    # Deliberate corruption of an authentic seed, not a fabricated supported format.
    data = bytearray(gzip.decompress((FIXTURES / "one-event/rawdata/journal.gz").read_bytes()))
    data[2] = 6
    path = tmp_path / "corrupt"
    path.write_bytes(data)
    with pytest.raises(JournalError, match="unsupported journal version/alignment"):
        list(events(path, experimental=True))


def test_truncated_original_gzip_never_publishes(tmp_path):
    path, out = tmp_path / "truncated.gz", tmp_path / "events.parquet"
    path.write_bytes((FIXTURES / "one-event/rawdata/journal.gz").read_bytes()[:-8])
    with pytest.raises(JournalError):
        export(events(path, experimental=True), out)
    assert not out.exists()
    assert not list(tmp_path.glob(".obs-migrate-*"))
