# Supported formats and limitations

Readers are experimental and accept only validated physical profiles. A producer
version or recognised header does not imply that every bucket from that producer
is supported. Complete preflight, source consistency and resource checks remain
mandatory. The current command's `--help` lists applicable options and limits.

| Data | Implemented boundary |
|---|---|
| Event journals | Observed version 1/alignment 5; gzip, Zstd and plain transport; string/unsigned event metadata representations 0/8. |
| Linked event leaves | Observed legacy/TSIDX7/8/9 profiles, including supported TSIDX9 flags 0/19/27; complete journal/index linkage, strict unknown-tail refusal. |
| Retained metric journals | Observed opcode 39 and metadata representations 0/10; original value/time/wire components retained. Indexed time and search visibility are distinct. |
| Metric index leaves | Observed MSIDX1/MSIDX2 and bounded MSIDX4 plain/compressed profiles; flags 0/5/96/101 where recognised by preflight. Unknown compression or mask contexts refuse. |
| Event jobs | Explicit offline multi-bucket event selection, per-source identity, duplicate retention, complete manifest publication and verified resume. Metric inputs refuse event jobs. |
| Acquired SmartStore/S3 | Explicit acquired inventories and complete masks; bounded read-only conditional/version-bound acquisition. Caller-asserted quiescence is not an atomic live snapshot. |
| Federation | Optional Arrow/DataFusion and Rust FFI adapters; experimental, bounded physical data access with engine residuals. |

The portable runtime uses pinned PyArrow and Zstandard dependencies. Python 3.13
and 3.14 are the current target interpreters on macOS arm64 and Linux amd64.
The `>=3.11` package metadata is not a claim that every interpreter/platform has
been qualified. See the tests for the genuine samples included in this repository.

The test fixture set is deliberately small: original public BOTS gzip events,
controlled native Zstd events and small genuine MSIDX1/2/4 metric controls. A
passing sample is not a certification of all settings, auxiliary arrays or
producer versions. Fixtures preserve native bytes and source/oracle provenance;
negative corruption tests do not establish new supported layouts.

Not implemented: SPL equivalence, general native indexed search, search-time
knowledge objects, universal DDSS packaging, live bucket snapshots, distributed
copy ownership, metric deletion semantics, compressed random seeking or LZ4
journal support. Original pre-ingestion input transformations cannot be reversed
by preserving the stored journal bytes. Keep the complete original source.
