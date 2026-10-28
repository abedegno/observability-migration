import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from obs_migrate.events import SCHEMA, batches, from_json, json_bytes
from obs_migrate.export import export, read_export, verify


@pytest.mark.parametrize("format", ["ndjson", "parquet"])
def test_roundtrip_all_binary_and_provenance(tmp_path, row, format):
    path = tmp_path / format
    stats = export(iter([row]), path, format)
    assert list(read_export(path, format)) == [row]
    assert verify(iter([row]), path, format)["verified_events"] == 1
    assert stats["events"] == 1 and stats["raw_bytes"] == len(row["raw"])
    if format == "parquet":
        assert pq.read_schema(path).equals(SCHEMA, check_metadata=True)
        assert pq.read_table(path)["raw"][0].as_py() == row["raw"]
    else:
        assert b'"raw":"/wBoZWxsbwo="' in path.read_bytes()


@pytest.mark.parametrize("format", ["ndjson", "parquet"])
def test_no_partial_export_on_decoder_failure(tmp_path, row, format):
    def broken():
        yield row
        raise ValueError("truncated journal")
    path = tmp_path / format
    with pytest.raises(ValueError, match="truncated"):
        export(broken(), path, format, max_rows=1)
    assert list(tmp_path.iterdir()) == []


def test_no_overwrite(tmp_path, row):
    path = tmp_path / "keep"
    path.write_bytes(b"original")
    with pytest.raises(FileExistsError):
        export(iter([row]), path)
    assert path.read_bytes() == b"original"


def test_batch_bounds_and_order(row):
    rows = (dict(row, record_ordinal=i) for i in range(50))
    result = list(batches(rows, max_rows=3, max_bytes=len(json_bytes(row))*2))
    assert all(b.num_rows <= 2 for b in result)
    assert [r["record_ordinal"] for b in result for r in b.to_pylist()] == list(range(50))


@pytest.mark.parametrize("format", ["ndjson", "parquet"])
def test_verify_detects_value_and_count_changes(tmp_path, row, format):
    path = tmp_path / "rows"
    export(iter([row]), path, format)
    for expected in ([], [row, row], [dict(row, raw=b"changed")],
                     [dict(row, event_time={"seconds": 1534776480, "fraction": "0.1234567891"})],
                     [dict(row, metadata_json="[]")],
                     [dict(row, source_sha256="b"*64)]):
        with pytest.raises(ValueError, match="record"):
            verify(iter(expected), path, format)


def test_schema_rejection(tmp_path):
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"raw": ["lossy text"]}), path)
    with pytest.raises(ValueError, match="schema"):
        list(read_export(path, "parquet"))


def test_invalid_base64_rejected(row):
    with pytest.raises(ValueError):
        from_json(json_bytes(row).replace(b'/wBoZWxsbwo=', b'***'))


def test_cli_export_and_verify_contract(tmp_path, row, monkeypatch, capsys):
    # Exercises CLI/transport against canonical records, not journal compatibility.
    from obs_migrate import journal
    from obs_migrate.cli import main
    source = tmp_path / "source"
    source.write_bytes(b"ordinary contract input")
    monkeypatch.setattr(journal, "events", lambda *a, **k: iter([row]))
    for format in ("ndjson", "parquet"):
        path = tmp_path / format
        assert main(["buckets", "export", str(source), "--out", str(path), "--format", format]) == 0
        assert main(["buckets", "verify", str(source), "--against", str(path), "--format", format]) == 0
    assert '"verified_events": 1' in capsys.readouterr().out
    assert source.read_bytes() == b"ordinary contract input"


def test_reader_respects_export_row_groups(tmp_path, row):
    path = tmp_path / "groups.parquet"
    export((dict(row, record_ordinal=i) for i in range(5)), path, max_bytes=1)
    assert pq.ParquetFile(path).num_row_groups == 5
    assert [r["record_ordinal"] for r in read_export(path, "parquet")] == list(range(5))


def test_verify_failure_closes_source(tmp_path, row):
    path = tmp_path / "rows"
    export(iter([row]), path, "ndjson")
    closed = []
    def source():
        try:
            yield dict(row, raw=b"changed")
            yield row
        finally:
            closed.append(True)
    with pytest.raises(ValueError, match="mismatch"):
        verify(source(), path, "ndjson")
    assert closed == [True]
