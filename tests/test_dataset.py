"""Checkpoints must survive interruption and reject corruption or changed inputs."""
import fcntl
import hashlib
import json
from pathlib import Path

import pytest

from obs_migrate import dataset
from obs_migrate.cli import main
from obs_migrate.events import json_bytes
from obs_migrate.export import verify
from obs_migrate.journal import events, JournalError

ROOT = Path(__file__).parent / "fixtures/botsv3"


def rows(row, count=7):
    return (dict(row, record_ordinal=i) for i in range(count))


def test_partition_bounds_and_complete_replay(tmp_path, row):
    out = tmp_path / "data"
    result = dataset.export_dataset(rows(row), out, partition_rows=3, partition_bytes=1)
    assert result["partitions"] == result["events"] == 7
    assert result["canonical_sha256"] == hashlib.sha256(b"".join(json_bytes(r) for r in rows(row))).hexdigest()
    assert list(dataset.read_dataset(out)) == list(rows(row))
    manifest_before = (out / "manifest.json").read_bytes()
    replay = dataset.export_dataset(rows(row), out, resume=True, partition_rows=3, partition_bytes=1)
    assert replay["reused_partitions"] == 7
    assert (out / "manifest.json").read_bytes() == manifest_before


def test_interrupt_resume_and_provisional_reader(tmp_path, row):
    out = tmp_path / "data"
    closed = []
    def broken():
        try:
            yield from rows(row, 5)
            raise KeyboardInterrupt()
        finally:
            closed.append(True)
    with pytest.raises(KeyboardInterrupt):
        dataset.export_dataset(broken(), out, partition_rows=2)
    assert closed == [True]
    manifest = json.loads((out / "manifest.json").read_text())
    assert not manifest["complete"] and len(manifest["parts"]) == 2
    with pytest.raises(ValueError, match="incomplete"):
        list(dataset.read_dataset(out))
    before = [(out/p["path"]).stat().st_mtime_ns for p in manifest["parts"]]
    result = dataset.export_dataset(rows(row), out, resume=True, partition_rows=2)
    assert result["events"] == 7 and result["reused_partitions"] == 2
    assert [(out/p["path"]).stat().st_mtime_ns for p in manifest["parts"]] == before
    assert verify(rows(row), out, "parquet-dataset")["verified_events"] == 7


@pytest.mark.parametrize("change", ["source_sha256", "bucket_context_json", "decoder", "index", "raw"])
def test_resume_rejects_changed_source_context_or_payload(tmp_path, row, change):
    out = tmp_path / "data"
    dataset.export_dataset(rows(row), out, partition_rows=2)
    changed = dict(row, **{change: b"changed" if change == "raw" else "changed"})
    with pytest.raises(ValueError, match="binding|replay"):
        dataset.export_dataset(rows(changed), out, resume=True, partition_rows=2)


def test_corrupt_partition_and_changed_limits_rejected(tmp_path, row):
    out = tmp_path / "data"
    dataset.export_dataset(rows(row), out, partition_rows=2)
    with pytest.raises(ValueError, match="limits"):
        dataset.export_dataset(rows(row), out, resume=True, partition_rows=3)
    part = out / "part-000000.parquet"
    part.write_bytes(part.read_bytes()[:-1])
    with pytest.raises(ValueError, match="checksum"):
        dataset.export_dataset(rows(row), out, resume=True, partition_rows=2)
    with pytest.raises(ValueError, match="checksum"):
        list(dataset.read_dataset(out))


def test_crash_between_partition_and_checkpoint_recovers(tmp_path, row, monkeypatch):
    out = tmp_path / "data"
    save = dataset._save
    def crash(directory, manifest):
        if manifest["parts"]:
            raise OSError("simulated checkpoint interruption")
        save(directory, manifest)
    monkeypatch.setattr(dataset, "_save", crash)
    with pytest.raises(OSError, match="interruption"):
        dataset.export_dataset(rows(row), out, partition_rows=2)
    assert (out/"part-000000.parquet").is_file()
    monkeypatch.setattr(dataset, "_save", save)
    result = dataset.export_dataset(rows(row), out, resume=True, partition_rows=2)
    assert result["events"] == 7
    assert list(dataset.read_dataset(out)) == list(rows(row))


def test_conflicting_orphan_and_symlink_never_overwritten(tmp_path, row):
    out = tmp_path / "data"
    def broken():
        yield from rows(row, 2)
        raise ValueError("stop")
    with pytest.raises(ValueError, match="stop"):
        dataset.export_dataset(broken(), out, partition_rows=2)
    victim = tmp_path / "untouched"
    victim.write_bytes(b"keep")
    (out/"part-000001.parquet").symlink_to(victim)
    with pytest.raises(ValueError, match="regular"):
        dataset.export_dataset(rows(row), out, resume=True, partition_rows=2)
    assert victim.read_bytes() == b"keep"


def test_exclusive_writer_and_no_overwrite(tmp_path, row):
    out = tmp_path / "data"
    dataset.export_dataset(rows(row), out)
    with pytest.raises(FileExistsError):
        dataset.export_dataset(rows(row), out)
    with (out/".lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="writer already active"):
            dataset.export_dataset(rows(row), out, resume=True)


def test_resume_before_first_checkpoint_and_malformed_manifest(tmp_path, row):
    out = tmp_path/"data"
    with pytest.raises(ValueError, match="no events"):
        dataset.export_dataset(iter([]), out)
    assert dataset.export_dataset(rows(row), out, resume=True)["events"] == 7
    (out/"manifest.json").write_text('[]')
    with pytest.raises(ValueError, match="manifest"):
        list(dataset.read_dataset(out))


def test_cli_rejects_partition_options_for_single_file_and_source_output(tmp_path, capsys):
    source = ROOT/"stream-29"
    assert main(["buckets", "export", str(source), "--experimental", "--out", str(tmp_path/"bad"), "--resume"]) == 1
    assert "require --format" in capsys.readouterr().err
    assert main(["buckets", "export", str(source), "--experimental", "--format", "parquet-dataset",
                 "--out", str(source/"forbidden.dataset")]) == 1
    assert "outside" in capsys.readouterr().err


def test_real_bucket_cli_resume_and_verify(tmp_path, capsys):
    source = ROOT/"stream-29"
    out = tmp_path/"data"
    command = ["buckets", "export", str(source), "--experimental", "--visibility", "undeleted",
               "--format", "parquet-dataset", "--out", str(out), "--partition-rows", "5"]
    assert main(command) == 0
    assert json.loads(capsys.readouterr().out)["events"] == 16
    assert main(command+["--resume"]) == 0
    assert json.loads(capsys.readouterr().out)["reused_partitions"] == 4
    assert main(["buckets", "verify", str(source), "--experimental", "--visibility", "undeleted",
                 "--format", "parquet-dataset", "--against", str(out)]) == 0
    assert json.loads(capsys.readouterr().out)["verified_events"] == 16


def test_late_source_failure_never_completes_dataset(tmp_path):
    source = ROOT/"stream-29"
    def broken():
        yield from events(source, experimental=True)
        raise JournalError("late integrity failure")
    out = tmp_path/"data"
    with pytest.raises(JournalError, match="late"):
        dataset.export_dataset(broken(), out, partition_rows=5)
    assert not json.loads((out/"manifest.json").read_text())["complete"]
    with pytest.raises(ValueError, match="incomplete"):
        list(dataset.read_dataset(out))
    result = dataset.export_dataset(events(source, experimental=True), out, resume=True, partition_rows=5)
    assert result["reused_partitions"] == 5 and result["events"] == 29
    assert verify(events(source, experimental=True), out, "parquet-dataset")["verified_events"] == 29
