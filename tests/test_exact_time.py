"""Timestamp contract arithmetic, precision and schema boundaries (not journal fixtures)."""
from decimal import Decimal

import pytest

from obs_migrate.events import from_json, json_bytes
from obs_migrate.export import export, read_export
from obs_migrate.wire import exact_time


@pytest.mark.parametrize("format", ["ndjson", "parquet"])
def test_exact_subnanosecond_and_negative_epoch_roundtrip(row, tmp_path, format):
    time = exact_time(-1, 6380000115)
    assert time == {"seconds": -1, "fraction": "0.3190000057"}
    assert Decimal(time["seconds"]) + Decimal(time["fraction"]) == Decimal("-0.6809999943")
    row.update(event_time=time, index_time=exact_time(1534756550))
    output = tmp_path / format
    export(iter([row]), output, format)
    assert list(read_export(output, format)) == [row]


def test_exact_time_range_and_no_float():
    assert exact_time(0) == {"seconds": 0, "fraction": "0"}
    assert exact_time(0, 2) == {"seconds": 0, "fraction": "0"}
    assert exact_time((1 << 63)-1)["seconds"] == (1 << 63)-1
    for seconds in (1 << 63, -(1 << 63)-1, 1.5):
        with pytest.raises(ValueError, match="int64"):
            exact_time(seconds)
    for wire in (-1, 1 << 64):
        with pytest.raises(ValueError, match="uint64"):
            exact_time(0, wire)


@pytest.mark.parametrize("old_version", [1, 2])
def test_old_timestamp_contract_rejected(row, old_version):
    with pytest.raises(ValueError, match="schema"):
        from_json(json_bytes(row).replace(b'"schema_version":3', f'"schema_version":{old_version}'.encode()))
