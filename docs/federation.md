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

### Explicit larger metric captures

`capture.create(..., limits=LARGE_METRICS)` and
`CapturedBucket(..., limits=LARGE_METRICS)` reuse the finite MSIDX4 profile from
`obs_migrate.metric_limits`. CLI equivalents are
`metrics capture-create ... --resource-profile large` and
`metrics capture-sql ... --resource-profile large`. Supply the profile explicitly
on both creation and reopening; the artifact never grants itself larger limits.
Defaults and legacy MSIDX1/2 behaviour remain unchanged; explicit profiles refuse
legacy formats. Full physical preflight remains mandatory on every new open,
including queries that later select no rows.

The policy stays outside the capture manifest, original member bytes, source
snapshot and logical query identity. Explicit receipts and source reports record
it separately. Cursors retain at most the profile's encoded-state allowance per
reader and are released after preflight, scans, early close and capture close.
If another iterator owns the capture lock, native close returns without blocking
and defers shared cursor cleanup until that owner releases the lock.
This is not a process RSS limit. Capture file/point, Arrow input/result and engine
budgets remain independent and unchanged. Multiple slices/readers have separate
cursor budgets.

The direct native provider can count or project bounded columns without encoding
all canonical fields. The DatasetTable provider still materializes its explicit
selection through bounded Parquet; a broad query can refuse the materialization
budget even if its final result is only a count. Select a bounded source predicate
or register explicit dataset columns as below. A qualified native projection is
another option. Budgets never increase implicitly.
Both retain SQL residual filters, physical-data semantics and provenance. Direct file/bucket selection accepts its own explicit profile as described in
the [data contract](data-format.md); explicit snapshot views are described below.


### Explicit capture DatasetTable columns

The portable DatasetTable bridge can expose a caller-selected canonical schema:

```python
session.register_capture("points", captured,
    columns=["source_sha256", "point_ordinal", "host", "indexed_time"])
result = session.sql("select count(*) as n from points")
```

`columns=None` retains all fields. An explicit value must be a nonempty list or
tuple of unique canonical names; its order and exact field types are preserved.
For count-only work, `columns=["point_ordinal"]` preserves physical cardinality.
The registered table cannot reference omitted fields. Source predicates still
validate their dependencies even when those fields are omitted. Full source
preflight still occurs on every capture open.

CLI equivalent (repeat the option for each field):

```sh
obsmigrate metrics capture-sql capture.zip --capture-sha256 "$CAPTURE_SHA256" \
  --experimental --provider dataset --resource-profile large \
  --dataset-column point_ordinal --sql 'select count(*) as n from points' \
  --out count.parquet
```

`--dataset-column` refuses with the native provider. Registration columns are an
explicit source schema, not automatic SQL projection or predicate pushdown.
The first file access materializes all selected rows/columns to bounded in-memory
Parquet. Cumulative Arrow bytes, encoded Parquet bytes, shared session input,
result and engine budgets remain independent. A small SQL result does not bypass
them. Repeated queries reuse the completed buffer; failure commits no partial
buffer or input reservation.

SQL query identity includes explicit dataset columns; result provenance records
them in `query.dataset_columns` and `capture_datasets`. Capture/selection identity
and original members stay unchanged. Projected `storage_json` retains the same
logical first-row manifest and slice/query envelope as full canonical records.
Keep the original capture: a projected table is a derived view, not a lossless
replacement for the source archive. Direct library callers may also use
`captured.select(...).records(columns=[...])`.


### Bounded larger offline bucket SQL

`BucketSession.register(..., limits=LARGE_METRICS)` reuses direct physical
selection under the explicit MSIDX4 resource policy. The CLI exposes the same path:

```sh
obsmigrate metrics bucket-sql /offline/bucket --experimental --offline-snapshot \
  --resource-profile large --source 'exact-original-source' \
  --sql 'select count(*) as n from points' --out count.parquet
```

Choose actual exact source predicates (`--metric-name`, routing fields or
`--dimension`); the Python predicate also supports exact stored-time bounds.
Every slice still receives complete preflight, including no-match slices. The
selected canonical rows are then eagerly materialized into a MemTable. SQL
residuals remain in DataFusion, and SQL projection/count/LIMIT cannot bypass
preparation of that explicit selection. Full input, shared session, result and
engine budgets remain unchanged at256MiB by default. A broad selection can still
refuse. Choose a bounded source predicate or an existing qualified capture provider
rather than assuming SQL text reduces source preparation.

Explicit source policy is retained in result provenance outside query/snapshot
identity. Original byte fields, numeric bits, timestamp structs and row envelopes
remain canonical. Defaults and legacy behavior are unchanged; explicit larger
profiles refuse MSIDX1/2. Failed registration commits no table/input charge;
retained exceptions and cancelled query streams permit subsequent reuse.

The bucket path is separate from the explicit projected snapshot path below.
Full-schema snapshot preparation still obeys the same admission and Arrow limits;
SQL text does not automatically reduce that input.


### Explicit projected full-snapshot SQL

Choose the complete physical snapshot table schema before SQL preparation:

```python
session.register_metric_snapshot("points", path,
    experimental=True, offline_snapshot=True,
    expected_snapshot_sha256=trusted_snapshot_sha256,
    limits=LARGE_METRICS,
    columns=["source_sha256", "point_ordinal", "host", "indexed_time"])
result = session.sql("select count(*) as n from points")
```

CLI equivalent:

```sh
obsmigrate metrics snapshot-sql /offline/bucket --experimental --offline-snapshot \
  --resource-profile large --expected-snapshot-sha256 "$SNAPSHOT_SHA256" \
  --snapshot-column source_sha256 --snapshot-column point_ordinal \
  --snapshot-column host --snapshot-column indexed_time \
  --sql 'select count(*) as n from points' --out count.parquet
```

Use `columns=["point_ordinal"]` for count-only work. Explicit columns must be a
nonempty list/tuple of unique canonical names; order, nullable types and schema
metadata are preserved. SQL cannot access omitted fields. Every original field,
including omitted numeric values, is validated in full per-file preflight before
projected emission. All slices and final source/tree/parent checks must succeed
before the table is registered. No source predicate, automatic SQL projection,
search-visibility interpretation or indexed-search behavior is introduced.

`columns=None` retains the original full-schema preparation; `limits=None`
retains default admission. Explicit larger policies still refuse legacy formats;
default-policy projections reuse the verified MSIDX1/2 readers. Full-schema input
can still exceed the unchanged256MiB cap even when explicit admission succeeds.
Selected columns are eagerly retained in a MemTable; cumulative input, shared
session, result and engine budgets remain independent and unchanged.

The selected schema is in `sources.<table>.query.columns` and its query identity.
Explicit resource policy remains outside that identity in the source report.
Snapshot/original-member identities do not change with either setting. Projected
`storage_json` equals the original bulk snapshot envelope, including exactly one
first-row manifest across slices; it adds no selection-query envelope. Result-level
provenance remains available when SQL projects row envelopes away.

Library consumers can also use `Snapshot.records(..., columns=[...])`. Finish or
close owned iterators; only successful exhaustion proves complete validation.
Preparation failure commits no table/input charge and closes owned rows/readers;
retained error tracebacks and cancelled queries permit session reuse. These are
representation/engine byte limits, not a total process RSS guarantee.
