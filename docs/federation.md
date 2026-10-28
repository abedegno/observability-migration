# Optional federation

The Python readers expose Arrow batches for downstream engines. Install the
optional Python DataFusion dependency with `pip install '.[federation]'`.
Providers operate on validated immutable physical sources; SQL engine residuals
remain responsible for full predicate evaluation. This is not SPL or general
native indexed-search equivalence.

The original Rust/PyO3 adapter lives in [native/obs-native](../native/obs-native).
It bridges existing Python readers to DataFusion using Arrow interfaces; it does
not independently implement a proprietary bucket decoder. Optional projection
and necessary-condition filters retain engine residuals and lifecycle checks.

Build in an activated virtual environment with Rust/Cargo installed:

```sh
python -m pip install -e '.[federation]'
python scripts/build_native_provider.py --release
```

`Cargo.lock` fixes the Rust dependency graph. This is an interpreter-specific
experimental extension, not a production binary wheel. The portable journal
path does not require it. Source validation and initial scans are not removed
by lazy registration, and decompressed addressing is not compressed random seek.

## Bounded metadata projections

For a journal-backed event snapshot, `cache_projections=True` also retains exact
canonical `metadata_json` strings within a bounded cache:

```python
with EventBucket(path, experimental=True, offline_snapshot=True,
                 backend="journal", cache_projections=True,
                 metadata_cache_bytes=32 * 1024**2) as source:
    for row in source.projected_records(("record_ordinal", "metadata_json")):
        consume(row)
```

The budget defaults to 32 MiB and accepts 0–64 MiB. CLI snapshot commands expose
`--cache-projections --metadata-cache-bytes BYTES`. A zero budget preserves the
scalar/raw cache while metadata uses full canonical decoding. An entry that
cannot fit also falls back; budget exhaustion never drops rows or metadata.
`decoder_state_json` always retains its full-record decoding/digest check.

The cache is populated only by the complete validated initial scan, preserves
JSON strings exactly, and is not persisted. The source report's
`metadata_projection_cache` records payload/reference/descriptor bytes, charged
bytes, unique values and cached-record coverage. Charging includes packed bytes
plus 128 bytes per unique entry as a construction allowance; it is not a process
RSS limit. The temporary exact-string lookup is discarded after capture.

Native scan counters distinguish full record decoding from bytes copied from
cached metadata/native spans. Initial source validation/decompression, source
identity, query byte caps, cancellation handling and engine residual filters
remain required. This optimization adds no metadata predicate pushdown or
compressed seeking. Optional native-engine tests compare projections, filters,
limits, aggregates and joins with an ordinary materialized DataFusion reference.

Optional qualification covers zero, partial and full metadata caches on genuine
gzip/Zstd sources, routing/time candidates with SQL residuals, cross-bucket
UNION/join provenance and same-session recovery after byte-cap failures. The
[developer SQL gate](../scripts/benchmark_event_federation.py) measures actual
engine execution separately from initial validation and reference construction.
It does not establish a stable native API or promote new pruning/search support.
