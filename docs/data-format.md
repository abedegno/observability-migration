# Event data contract

Journal exports use schema version 3. Linked event exports add an `index_link`
struct and retain their own format metadata. Metrics have separate profile-labelled
schemas; they are not interchangeable with journal events.

| Field | Meaning |
|---|---|
| `raw` | Exact stored event payload bytes. |
| `record_wire` | Event opcode through payload, preserving original encoding. |
| `event_time`, `index_time` | Nullable structs: signed Int64 `seconds` plus exact decimal-string `fraction`. No float conversion or nanosecond rounding. |
| `host`, `source`, `sourcetype` | Nullable original dictionary bytes, including stored prefixes; empty differs from absent. |
| `metadata_json` | Ordered typed entries with original keys, operands and wire encodings; binary values use Base64, unsigned values use decimal strings. |
| `decoder_state_json`, `decoder_warnings` | Original components and labelled diagnostic interpretations. |
| `source_sha256` | Hash of the original compressed source bytes. |
| `record_ordinal`, `decompressed_offset` | Physical ordinal and original decompressed opcode address. Filtering preserves physical ordinal gaps. |
| `index`, `bucket_id`, `object_uri` | Trusted caller-supplied provenance, omitted when unknown. |
| `is_deleted`, `bucket_disabled`, `bucket_context_json` | Observed source/mask context; not complete search visibility. |
| `display_text` | Derived UTF-8 replacement decoding; never a substitute for `raw`. |

Parquet/Arrow retain binary fields and timestamp structs. NDJSON uses Base64 for
binary fields and objects for timestamps. The instant is the arithmetic sum of
seconds and fraction: `seconds=-1`, `fraction="0.25"` denotes -0.75 seconds.
Promoted timestamp semantics are restricted to observed version 1/alignment 5;
wire components remain available for verification and future interpretation.

Verification compares ordered canonical fields, not display text or row counts
alone. A source hash binds observed bytes; authenticity requires an independently
trusted expected identity. Reconciled `undeleted` selection does not emulate SPL,
permissions, search-time fields or distributed bucket selection.

Retain original archives. Event rows do not reconstruct complete journals,
dictionaries, index files or all source sidecars. Consumers of library iterators
must finish successfully before committing their output.

An optional `bucket_info.csv` index-time endpoint may be empty. In
`bucket_context_json.index_time_bounds_s`, each missing endpoint is `null`; known
values and the original sidecar hash are retained. A partial range cannot confirm
containment or exact extrema. A violation of a known endpoint can still produce
the existing diagnostic warning; it never removes an event. Malformed nonempty
values and reversed complete bounds refuse. Journal timestamp decoding remains
independent of these sidecar annotations.


### Experimental extended event metadata

An explicitly declared event index (`--index-type event` for a journal scan;
event jobs already declare their inputs) can read the observed journal
v1/alignment-5/opcode-39 Float64 extension profile. Unknown index types continue
to refuse extended records; the event adapter still refuses metric index types.
This does not identify an index type from opcode 39 alone.

For this profile, metadata representation 10 is a byte offset into an original
extended storage block. The adapter retains its key, operand, original wire and
storage offset, adding `value_float64_bits_le_base64` and exact `value_float64_hex`
to that entry in `metadata_json`. `decoder_state_json.extended_storage_base64`
retains the complete block. Original `record_wire`, source identity, times and
routing remain available. The existing schema-3 envelope is unchanged; extended
rows identify decoder suffix `+event-float64-1`, and classic rows stay unchanged.

Only finite Float64 values at unique aligned eight-byte offsets with complete
block coverage are admitted, alongside representation-0 strings. Other extended
opcodes, journal profiles, representations, overlaps, unused bytes and nonfinite
values refuse. These are stored event fields, not metric measurements. Native
physical export comparisons do not establish search-time extraction, knowledge
objects, indexed search, or whole-index/snapshot equivalence.


### Explicit larger MSIDX4 bulk resource profile

`metric_limits.MetricLimits` is a frozen per-reader profile. The predefined
`LARGE_METRICS` permits 1,000,000 value/time entries and column values per slice;
terms, dimension sets, column/group counts, metadata and source caps stay unchanged.
The default `None` retains existing count limits and decoded-block cache behaviour.

Explicit profiles use an LRU of original-block incremental XOR cursors. A cursor
holds one bounded encoded block and decoder state. Forward accesses advance that
state; repeated positions reuse it; backward accesses restart the bounded block
prefix. Eviction releases the cursor. Charge is encoded block bytes plus 1,024 bytes
per cursor, with an 8 MiB maximum; an individual cursor that cannot fit refuses
before its block is read. This is logical accounting, not an RSS guarantee, and
does not imply compressed-file random seeking or indexed search.

Rows, numeric provenance, snapshot identities and transport schemas are unchanged.
Export/verify receipts identify explicit limits separately. Optional
`msidx.records(..., limits=profile, stats=dict)` reports numeric work and peak cursor
charge; `complete` becomes true only after full source exhaustion and final source
checks. Closing a partial iterator releases cursor buffers and records incomplete
validation. Larger profiles apply to MSIDX4 file/bucket bulk export and verification,
explicitly opted-in immutable capture creation, reopening and capture SQL, and
explicit direct file/bucket selection. Default limits remain unchanged; snapshot
SQL qualification and legacy-reader policies remain separate. Capture policy, provider budgets and scope are described in the
[federation guide](federation.md#explicit-larger-metric-captures).


Metric Parquet traversal validates the complete Arrow schema and reads one row
group at a time in batches of at most 1,024 rows, with column decoding performed
without parallel workers. This local reader policy avoids the higher peak memory
observed for wide metric rows; it changes neither global Arrow thread settings,
canonical fields, source verification nor output schemas. It is not a hard RSS
limit or a guarantee for arbitrary Parquet files and platforms.


An explicitly projected immutable metric capture dataset preserves selected field
bytes, nullable types, timestamp structs and row provenance, while omitting other
fields from its registered schema. SQL provenance binds the requested columns;
source capture/selection identities remain unchanged. The default remains the full
canonical schema. See [DatasetTable columns](federation.md#explicit-capture-datasettable-columns).


Direct MSIDX4 physical selection accepts the same explicit finite metric profile:
`metric_select.records|batches|explain|export|verify(..., limits=LARGE_METRICS)`
and `bucket_select.Selection(..., limits=LARGE_METRICS)` (or its convenience
functions). CLI `metrics select-*` and `metrics bucket-select-*` expose
`--resource-profile large`. Supply the policy on each operation; defaults remain
unchanged and explicit profiles refuse MSIDX1/2.

Selection still validates every physical source point, including no-match slices,
and rechecks regular-file/bucket identities. Original canonical rows and logical
query/snapshot hashes do not change with the policy. Explicit reports and receipts
record `metric_resource_profile`. Bucket-selection bundle manifests retain this
policy outside query identity; verification requires the same explicit policy and
never derives its admission budget from the artifact. A default-generated bundle
continues to verify under its original default contract.

Source contexts and partial bucket iterators release numeric cursors on close,
including errors whose tracebacks are retained. Library consumers must close
partially consumed iterators and exhaust successfully before committing results.
These are exact physical predicates; larger snapshot SQL, indexed-search visibility
and new storage profiles require separate qualification.


Bounded offline `bucket-sql` can explicitly reuse the same MSIDX4 profile through
`--resource-profile large` or `BucketSession.register(..., limits=profile)`.
Exact source predicates bound the eager canonical input; SQL filters remain engine
residuals. See [bucket SQL](federation.md#bounded-larger-offline-bucket-sql) for the
separate input/result/engine bounds and the full-snapshot distinction.
