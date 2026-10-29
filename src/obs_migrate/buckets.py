"""Read-only BOTS-observed bucket context and delete-mask reconciliation.

Undeleted extraction is not Splunk search: active index configuration, extraction
rules and distributed bucket selection remain outside this module.
"""
import csv
import io
import json
from pathlib import Path

MAX_DELETE_ROWS = 100_000
MAX_DELETE_FILES = 256
MAX_DELETE_BYTES = 64 * 1024 * 1024
DELETE_COLUMNS = ["timestamp", "event_address", "type_id", "host_id", "source_id", "sourcetype_id"]


def _lines(file):
    while line := file.readline(4097):
        if len(line) > 4096:
            raise ValueError("bucket CSV line exceeds limit")
        yield line


def _manifest(root):
    from .journal import open_source
    path = root / ".bucketManifest"
    if not path.exists():
        return None, None
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024*1024:
        raise ValueError("invalid bucket manifest")
    names = set()
    with open_source(path) as (stream, sha, *_):
        text = io.TextIOWrapper(stream, encoding="utf-8", newline="")
        try:
            reader = csv.DictReader(_lines(text))
            if "path" not in (reader.fieldnames or []):
                raise ValueError("unsupported bucket manifest schema")
            for row in reader:
                name = row["path"]
                if not name or Path(name).name != name or name in (".", "..") or name in names:
                    raise ValueError("invalid or duplicate bucket manifest path")
                names.add(name)
                if len(names) > 10000:
                    raise ValueError("bucket manifest entry limit exceeded")
        finally:
            text.detach()
    return names, (path, sha)


class BucketContext:
    def __init__(self, journal, visibility="physical", include_disabled=False):
        from .journal import open_source
        if visibility not in ("physical", "undeleted"):
            raise ValueError("visibility must be physical or undeleted")
        self.visibility = visibility
        self.bucket = journal.parent.parent if journal.parent.name == "rawdata" else None
        self.disabled = self.bucket.name.startswith("DISABLED-") if self.bucket else None
        self.files = []
        self.deletes = {}
        self.matched = set()
        self.physical_count = self.deleted_count = self.emitted_count = 0
        self.index_bounds = None
        self.index_min = self.index_max = None
        self.event_second_min = self.event_second_max = None
        self.index_outside_bounds = 0
        self.mask_names = []
        self.info_present = False
        self.manifest_present = False
        self.manifest_listed = None
        if visibility == "undeleted" and self.bucket is None:
            raise ValueError("undeleted extraction requires bucket rawdata context")
        if visibility == "undeleted" and self.disabled and not include_disabled:
            raise ValueError("disabled-name bucket excluded; explicit include_disabled required")
        if self.bucket:
            names, manifest_file = _manifest(self.bucket.parent)
            self.manifest_present = names is not None
            self.manifest_listed = self.bucket.name in names if names is not None else None
            if manifest_file:
                self.files.append(manifest_file)
            info = self.bucket / "bucket_info.csv"
            self.info_present = info.exists()
            if self.info_present:
                if info.is_symlink() or not info.is_file() or info.stat().st_size > 4096:
                    raise ValueError("invalid bucket_info.csv")
                with open_source(info) as (stream, sha, *_):
                    reader = csv.DictReader(io.StringIO(stream.read(4097).decode("utf-8")))
                    rows = list(reader)
                    if len(rows) != 1 or not {"indextime_et", "indextime_lt"} <= set(reader.fieldnames or []):
                        raise ValueError("unsupported bucket_info.csv schema")
                    # Native sidecars can leave an endpoint empty. Retain it as
                    # unknown; never invent zero, an extremum or an open range.
                    self.index_bounds = tuple(None if rows[0][key] == "" else int(rows[0][key])
                                              for key in ("indextime_et", "indextime_lt"))
                    if None not in self.index_bounds and self.index_bounds[0] > self.index_bounds[1]:
                        raise ValueError("invalid index-time bounds")
                self.files.append((info, sha))
            directory = journal.parent / "deletes"
            if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
                raise ValueError("invalid deletes directory")
            masks = sorted(directory.iterdir()) if directory.exists() else []
            self.mask_names = [p.name for p in masks]
            if len(masks) > MAX_DELETE_FILES:
                raise ValueError("delete-file limit exceeded")
            if any(p.is_symlink() or not p.is_file() for p in masks):
                raise ValueError("delete masks must be regular local files")
            if sum(p.stat().st_size for p in masks) > MAX_DELETE_BYTES:
                raise ValueError("delete-byte limit exceeded")
            total = 0
            for path in masks:
                with open_source(path) as (stream, sha, *_):
                    text = io.TextIOWrapper(stream, encoding="utf-8", newline="")
                    try:
                        reader = csv.DictReader(_lines(text))
                        if reader.fieldnames != DELETE_COLUMNS:
                            raise ValueError("unsupported delete-mask columns")
                        for row in reader:
                            total += 1
                            if total > MAX_DELETE_ROWS or None in row or any(v is None for v in row.values()):
                                raise ValueError("delete row count/shape exceeds supported limits")
                            values = tuple(int(row[key]) for key in DELETE_COLUMNS)
                            timestamp, address, kind, host, source, sourcetype = values
                            if kind != 0 or address < 0 or min(host, source, sourcetype) < 1:
                                raise ValueError("unsupported delete-mask entry")
                            if address in self.deletes and self.deletes[address] != values:
                                raise ValueError("conflicting delete-mask entries")
                            self.deletes[address] = values
                    finally:
                        text.detach()
                self.files.append((path, sha))
        self.context_json = json.dumps({
            "visibility": visibility, "disabled_name": self.disabled,
            "disabled_evidence": "directory_name_only" if self.bucket else None,
            "index_time_bounds_s": self.index_bounds,
            "manifest_listed": self.manifest_listed,
            "delete_unique_addresses": len(self.deletes),
            "sidecars": [{"name": str(p.relative_to(self.bucket)) if p.is_relative_to(self.bucket) else ".bucketManifest@index", "sha256": sha} for p, sha in self.files],
            "splunk_search_equivalence": "unverified",
        }, sort_keys=True, separators=(",", ":"))

    def classify(self, offset, state):
        self.physical_count += 1
        candidate = state["index_time_candidate_s"]
        self.index_min = candidate if self.index_min is None else min(self.index_min, candidate)
        self.index_max = candidate if self.index_max is None else max(self.index_max, candidate)
        inside = None
        if self.index_bounds:
            lower, upper = self.index_bounds
            if ((lower is not None and candidate < lower) or
                    (upper is not None and candidate > upper)):
                inside = False
            elif lower is not None and upper is not None:
                inside = True
        state["index_candidate_within_bucket_info"] = inside
        self.index_outside_bounds += inside is False
        second = state["event_time_second_candidate"]
        self.event_second_min = second if self.event_second_min is None else min(self.event_second_min, second)
        self.event_second_max = second if self.event_second_max is None else max(self.event_second_max, second)
        if self.bucket is None:
            return None
        if not self.deletes:
            return False
        if (state["header"]["version"], state["header"]["align_bits"]) != (1, 5):
            raise ValueError("delete mapping is supported only for observed version 1/alignment 5")
        address = offset >> 5
        deletion = self.deletes.get(address)
        if deletion is None:
            return False
        actual = (state["event_time_second_candidate"], address, 0, state["active_host"],
                  state["active_source"], state["active_sourcetype"])
        if deletion != actual:
            raise ValueError("delete-mask timestamp/metadata IDs disagree with addressed event")
        self.matched.add(address)
        self.deleted_count += 1
        return True

    def finish(self, stats=None):
        from .journal import fingerprint
        if self.matched != self.deletes.keys():
            raise ValueError("delete masks reference unmatched journal addresses")
        if self.bucket:
            directory = self.bucket / "rawdata/deletes"
            current_masks = sorted(p.name for p in directory.iterdir()) if directory.exists() else []
            if directory.is_symlink() or current_masks != self.mask_names:
                raise ValueError("delete-mask inventory changed during scan")
            if (self.bucket / "bucket_info.csv").exists() != self.info_present:
                raise ValueError("bucket-info inventory changed during scan")
            if (self.bucket.parent / ".bucketManifest").exists() != self.manifest_present:
                raise ValueError("bucket-manifest inventory changed during scan")
        for path, expected in self.files:
            with path.open("rb") as file:
                if fingerprint(file) != expected:
                    raise ValueError("bucket sidecar changed during scan")
        if stats is not None:
            stats.update(physical_events=self.physical_count, deleted_events=self.deleted_count,
                         emitted_events=self.emitted_count, visibility=self.visibility,
                         index_candidate_bounds_match=((self.index_min, self.index_max) == self.index_bounds
                             if self.index_bounds and None not in self.index_bounds else None),
                         index_candidates_outside_bounds=self.index_outside_bounds,
                         index_candidate_min_s=self.index_min, index_candidate_max_s=self.index_max,
                         event_second_candidate_min=self.event_second_min,
                         event_second_candidate_max=self.event_second_max,
                         delete_addresses_matched=len(self.matched), bucket_disabled=self.disabled,
                         manifest_listed=self.manifest_listed,
                         sidecar_bytes_read=sum(p.stat().st_size * 3 for p, _ in self.files))


def inventory(root, include_disabled=False):
    """List immediate bucket dirs; never aggregate disabled-name buckets by default."""
    root = Path(root).resolve(strict=True)
    result = []
    names, _ = _manifest(root)
    for bucket in sorted(root.iterdir()):
        if not bucket.is_dir():
            continue
        disabled = bucket.name.startswith("DISABLED-")
        journals = [p for name in ("journal", "journal.gz", "journal.zst", "journal.lz4")
                    if (p := bucket / "rawdata" / name).is_file()]
        listed = bucket.name in names if names is not None else None
        if not journals or (not include_disabled and (disabled or listed is False)):
            continue
        result.append({"bucket_name": bucket.name, "disabled_name": disabled,
                       "journal_candidates": len(journals), "manifest_listed": listed,
                       "delete_files": len(list((bucket / "rawdata/deletes").glob("*"))),
                       "search_visibility": "unverified"})
    return result
