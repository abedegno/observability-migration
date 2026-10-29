# Supported formats and limitations

Readers are experimental and accept only validated physical profiles. A producer
version or recognised header does not imply that every bucket from that producer
is supported. Complete preflight, source consistency and resource checks remain
mandatory. The current command's `--help` lists applicable options and limits.

| Data | Implemented boundary |
|---|---|
| Event journals | Observed version 1/alignment 5; gzip, Zstd, standard-frame LZ4 and plain transport; string/unsigned event metadata representations 0/8. |
| Linked event leaves | Observed legacy/TSIDX7/8/9 profiles, including supported TSIDX9 flags 0/19/27; complete journal/index linkage, strict unknown-tail refusal. |
| Retained metric journals | Observed opcode 39 and metadata representations 0/10; original value/time/wire components retained. Indexed time and search visibility are distinct. |
| Metric index leaves | Observed MSIDX1/MSIDX2 and bounded MSIDX4 plain/compressed profiles; flags 0/5/96/101 where recognised by preflight. Unknown compression or mask contexts refuse. |
| Event jobs | Explicit offline multi-bucket event selection, per-source identity, duplicate retention, complete manifest publication and verified resume. Metric inputs refuse event jobs. |
| Acquired SmartStore/S3 | Explicit acquired inventories and complete masks; bounded read-only conditional/version-bound acquisition. Caller-asserted quiescence is not an atomic live snapshot. |
| Federation | Optional Arrow/DataFusion and Rust FFI adapters; experimental, bounded physical data access with engine residuals. |

The portable runtime uses pinned PyArrow, Zstandard and Python LZ4 dependencies. Python 3.13
and 3.14 are the current target interpreters on macOS arm64 and Linux amd64.
The `>=3.11` package metadata is not a claim that every interpreter/platform has
been qualified. See the tests for the genuine samples included in this repository.

The test fixture set is deliberately small: original public BOTS gzip events,
controlled native Zstd/LZ4 events and small genuine MSIDX1/2/4 metric controls. A
passing sample is not a certification of all settings, auxiliary arrays or
producer versions. Fixtures preserve native bytes and source/oracle provenance;
negative corruption tests do not establish new supported layouts.

Not implemented: SPL equivalence, general native indexed search, search-time
knowledge objects, universal DDSS packaging, live bucket snapshots, distributed
copy ownership, metric deletion semantics, compressed random seeking. Original pre-ingestion input transformations cannot be reversed
by preserving the stored journal bytes. Keep the complete original source.

## Native LZ4 journals

Standard LZ4 frames are detected by magic bytes. Genuine isolated Splunk 7.0.0
and 10.6.0.5 controls cover eight stored events and sixteen retained metric
points (seventeen measurements), with original compressed buckets and independent
native references. Discovery accepts `rawdata/journal.lz4`; both linked event
backends and local event jobs use the existing journal path. Metric stubs still
refuse and require the indexed bucket.

The separately installed `lz4==4.4.5` codec owns frame decoding and checksums.
A bounded complete-frame preflight rejects truncation, malformed frames and
trailing garbage before rows are emitted. It applies the caller's decompressed
byte bound with output chunks of at most 64 KiB. This is not a total process
memory guarantee. Original compressed SHA, record wire and decompressed offsets
remain authoritative; event/metric schemas and existing codecs are unchanged.

Ordinary-byte tests additionally cover standard concatenated frames and checksum
refusal; they do not certify another native producer profile. Legacy/skippable LZ4
frames and native LZ4 SmartStore/DDSS packaging are not qualified. A valid frame
without content checksums cannot authenticate its payload; use an independently
trusted expected source SHA where authenticity is required.
