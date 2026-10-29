# Changelog

## Unreleased

- Read the observed SmartStore failover copy with distinct origin/uploader GUIDs,
  preserving both identities and original events; keep ambiguous copies and
  unqualified distinct-uploader deletion contexts refused.

- Accept the observed immutable TSIDX9 flags0 SmartStore deletion base after
  complete receipt-bound CSV mask and journal reconciliation. Retain original
  index bytes and strict local bitmap agreement; add genuine native deletion,
  rehydration and lossless Arrow/Parquet regression controls.

## 1.1.0rc3 — 2026-10-29

- Read genuine standard-frame LZ4 event/retained-metric journals through the
  existing decoder, local bucket discovery, linked access and event jobs; preserve
  original source identity and reject incomplete/corrupt/trailing frames.
- Add pinned Python LZ4 dependency and native 7.0/10.6 fixtures with independent
  exports, exact metadata/time/value checks and Parquet/job recovery coverage.

- Add explicit full-snapshot field schemas and bounded MSIDX4 policies for metric
  snapshot SQL, validating all source fields before projected preparation.
- Run eager SQL coverage whenever DataFusion is installed, without requiring the
  optional native bridge.

- Expose existing explicit MSIDX4 resource profiles in bounded offline bucket SQL;
  preserve default admission, exact source validation and SQL byte budgets.

- Extend explicit bounded MSIDX4 profiles to direct file/bucket physical selection
  and its CLI; preserve default limits, query identity and original row bytes.
- Retain storage-only multi-slice provenance regressions using unchanged native
  fixture bytes, and release owned selection cursors on early close and failure.

- Add explicit canonical column selection for immutable capture DatasetTables,
  library records and capture SQL; preserve full-schema defaults and byte limits.

- Extend explicit bounded MSIDX4 resource profiles to immutable capture creation,
  reopening and capture SQL, preserving defaults, identities and source checks.
  Release incremental numeric cursors on preflight/scan completion and interruption.

## 1.1.0rc2 — 2026-10-28

- Reduce metric Parquet read/verification peak memory by decoding row-group batches
  without parallel column workers; preserve exact rows and global Arrow settings.

- Add opt-in bounded larger MSIDX4 bulk extraction with incremental XOR cursors,
  preserving default limits, exact numeric provenance and source validation.

- Read the observed extended Float64 event metadata profile for explicitly declared
  event inputs, retaining exact numeric and extension bytes with strict layout checks.

- Preserve empty bucket-info index-time endpoints as unknown, allowing native
  buckets with partial annotations while retaining source checks and diagnostics.

## 1.1.0rc1 — 2026-10-28

- Bound capture/source file reads by their observed size, avoiding transient
  cap-sized allocations and refusing incomplete reads.

- Reduce posting preflight call overhead while retaining integer decoding,
  bounds and cardinality checks.

- Add opt-in JSON member/stage progress on stderr for event job planning, export,
  verification and resume, preserving stdout results and validation semantics.

- Verify native query recovery on the same session after cancellation while
  the caller retains the original exception traceback, across cache budgets.

## 1.0.0 — 2026-10-28

- Define the portable CLI/API support and maintenance contract; proprietary
  profiles and optional native federation retain their explicit experimental flags.
- Extend genuine gzip/Zstd native SQL residual, partial-cache, cross-bucket
  provenance and failure/reuse coverage, with an optional reproducible SQL gate.

## 0.59.0rc5 — 2026-10-28

- Add bounded exact metadata JSON projections to the existing opt-in native
  projection cache, with canonical decoding when an entry cannot be retained.
- Expose the metadata cache budget and retained-byte/coverage accounting.
- Release native query streams before raising result-limit errors, avoiding
  a crash during error-path teardown.
- Detach native stream ownership before propagating interruptions, including
  when callers retain the exception traceback.

## 0.59.0rc4 — 2026-10-28

- Reduce worker-pool memory overhead during Parquet export verification and
  manifest-managed dataset reads without changing schema or field comparisons.
- Add an optional canonical Parquet count/digest/resource gate for contributors,
  and extend Parquet verification rejection coverage.

## 0.59.0rc3 — 2026-10-28

- Establish the curated source repository, focused documentation and a small,
  redistributable test fixture set.
- Preserve the existing Python physical readers, exporters and optional Rust
  DataFusion adapter; no new proprietary format or indexed-search claim.
- Include installed decoder setup and missing-dependency diagnostics.
