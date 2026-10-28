# Contributing

Contributions should improve reliable read-only extraction, supported-profile
coverage, verification, documentation or performance. Keep original event bytes,
exact stored times, metadata and provenance intact. Unknown input must fail
explicitly rather than being skipped or silently repaired.

## Development

Create a Python 3.13/3.14 virtual environment, install `.[test]`, and register the
external pinned decoder as described in the README. Then run:

```sh
python -m pip install -e '.[test]'
python -m pytest -q
```

Tests include genuine CC0 gzip/Zstd events and native metric samples, alongside
ordinary compression/container and canonical-record tests. Never fabricate a
proprietary binary format as positive compatibility evidence. New format support
needs authorised original bytes, an independent native reference and explicit
refusal tests. Keep data-sharing permission and source hashes with each fixture.

Use a separate branch and describe the problem, observable change and verification
in the pull request. Include tests for parser or behavioral changes. Keep changes
focused and avoid updating unrelated formatting. Optional engine/native changes
need their own lifecycle and residual-correctness checks.

For bug reports, give package/Python/platform versions, a minimal command, redacted
error and recognised source profile. Do not upload customer archives, credentials
or private object names. Share reproduction data only with permission. Use
[security reporting](SECURITY.md) for potential vulnerabilities.

Original code is Apache-2.0. Preserve dependency notices and each fixture's
licence/provenance. Do not vendor the external decoder or vendor software.

## Optional Parquet resource check

With the candidate installed, run the developer tool in a fresh process on a
completed canonical Parquet export:

```sh
python scripts/benchmark_parquet_read.py /derived/events.parquet
```

Optional `--expected-events`, `--expected-raw-wire-sha256` and `--max-rss-mib`
arguments turn observations into explicit gates. The digest hashes each row's
raw bytes followed by record-wire bytes, each prefixed with an unsigned eight-byte
little-endian length. Use an independently retained expectation when available.
The tool reads one file; it does not validate a dataset manifest or compare the
original journal. Use `obsmigrate buckets verify` for original-source comparison.

`read_seconds` covers traversal and hashing after imports; peak RSS covers the
whole process, including imports. RSS measurement supports Linux and macOS.
Compare unchanged files under the same interpreter, Arrow version, allocator and
CPU/memory configuration. Choose a workload-specific budget: batch/count bounds
are not a universal process RSS guarantee. Keep larger authorised inputs and raw
benchmark records outside the public repository.

## Optional native event SQL check

With the optional DataFusion bridge installed, run the source/engine parity gate
on an authorised immutable linked event bucket:

```sh
python scripts/benchmark_event_federation.py /offline/bucket --metadata-cache-bytes 33554432
```

Run separate processes with budgets 0, 8388608 and 33554432 for fallback/partial/
full-cache observations. Coverage depends on the actual metadata cardinality.
The tool compares actual SQL with a materialized full-canonical-row reference,
reports initial source validation, reference construction and provider registration
separately, and records SQL planning/execution time and scan work. The reference
uses the same reader; this is engine equivalence, not independent format proof.
Peak RSS includes reference data and queries. `--retain-opaque-trailer` is an
explicit experimental opt-in when required. Keep original sources and raw results
outside the public repository; one warm workload is not a capacity guarantee.
