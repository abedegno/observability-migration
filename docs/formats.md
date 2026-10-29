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

## Immutable SmartStore deletion bases

Acquired event buckets can apply complete receipt-bound CSV deletion masks to the
observed all-zero, one-bit TSIDX8 flags0 and TSIDX9 flags0 bases. Original index
bytes remain unchanged. The TSIDX9 control uses native Splunk 10.6.0.5 writing
level4 with Zstd: fourteen stored events, three duplicate events deleted, eleven
selected events, and native eviction/restart/search rehydration. Both event
backends and Arrow/Parquet retain original bytes, stored times and metadata.

Only these finite base profiles are admitted for this overlay. TSIDX9 flags19/27
support in the local physical reader does not qualify them as SmartStore deletion
bases. Complete journal/mask/address reconciliation and acquisition identity checks
remain mandatory, including empty selections. Ordinary local undeleted reads still
require the native bitmap to agree with the masks; a remote zero base cannot bypass
that check. Unused bits inside the original delete array must also be zero.

This extends experimental physical extraction only. It does not establish native
search visibility, distributed ownership, concurrent upload semantics, live atomic
snapshots or DDSS packaging. The genuine regression control and native references
are in `tests/fixtures/smartstore-tsidx9-10.6`.

## Distinct SmartStore origin and uploader

A native failover can retain the bucket origin GUID while a different peer uploads
its objects. The reader validates these separately: manifest ID, native bucket
path and receipt key retain the origin; object members use the declared uploader.
The observed Zstd/TSIDX8 flags0 control retains all fourteen events and matches
native before/after search and CSV references. Both Arrow and journal backends
preserve original stored fields and Arrow/Parquet transports.

Distinct copies expose `copy_identity` with both GUIDs and ownership `UNVERIFIED`.
This is explicit receipt-bound physical-copy access; it does not elect a cluster
owner or choose among generations. Multiple receipts for one bucket, unlisted
siblings and inconsistent identities refuse. Distinct-uploader deletion contexts
remain unqualified and refuse; existing same-origin deletion behavior is unchanged.
The control is in `tests/fixtures/smartstore-foreign-uploader-10.6`. No production
licence material is bundled or needed by the offline reader.
