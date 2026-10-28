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
