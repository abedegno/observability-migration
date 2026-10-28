# Changelog

## Unreleased

## 1.0.0 — qualification build, publication pending

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
