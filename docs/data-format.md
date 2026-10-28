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
