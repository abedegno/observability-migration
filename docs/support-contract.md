# Offline access support contract

This contract applies to the approved, published
[1.0.0 stable release](https://github.com/abedegno/observability-migration/releases/tag/v1.0.0).
A prerelease, source checkout or passing test count does not independently establish
stable support beyond this finite contract.

## Supported surface

The stable surface is the documented portable CLI for decoder setup/diagnostics,
inspection, event/metric export and verification, event jobs/resume and read-only
S3 acquisition. Python API guarantees cover `obs_migrate.journal.events`,
`obs_migrate.events.batches`, `obs_migrate.export.export`,
`obs_migrate.export.verify` and `obs_migrate.dataset.read_dataset`.
Other internal classes and optional SQL/native provider APIs are experimental.

Keep documented option meanings, nonzero refusals, no-overwrite output publication,
explicit offline acquisition assertions and schema identity. Within 1.x, changes
to those interfaces or exported meanings require a documented migration path;
breaking interface changes require a major version. A fidelity defect can require
refusing or withdrawing an unsafe profile, with affected versions and recovery
instructions documented. Tests and release qualifications identify their scope.

## Physical profile boundary

The existing full preflight and resource checks define the admitted subset of
each family in [formats](formats.md), not the producing Splunk version alone:

| Family | Boundary |
|---|---|
| Event journals | Observed v1/alignment 5, metadata representations 0/8; gzip/Zstd; plain is transport coverage, not producer certification. |
| Linked event leaves | Recognised legacy/TSIDX7/8/9 profiles, including bounded TSIDX9 flags 0/19/27; full journal/index linkage and unknown-tail refusal. |
| Retained metric journals | Observed opcode 39 and metadata 0/10; exact stored time/value/wire fields, distinct from indexed time. |
| Metric leaves | Recognised homogeneous MSIDX1/2 and bounded MSIDX4 flags 0/5/96/101; unknown mask/compression contexts refuse. |
| Event jobs | Explicit source/copy selection, per-bucket identity and ordinal reset, duplicates retained, complete manifest publication and verified replay resume. Metric inputs refuse event jobs. |
| Acquired SmartStore/S3 | Recognised acquired inventories and complete masks; conditional/version-bound read-only transport with explicit consistency window and caller-asserted quiescence. |

Proprietary-format readers retain their explicit experimental labels and flags.
This interface contract does not certify unobserved physical profiles or remove
those guards. Opaque-tail retention and optional Arrow/DataFusion/Rust adapters
remain experimental. Complete source validation and exhaustion are required;
persisted validation markers cannot replace them. New accepted layouts need
genuine authorised fixtures and independent native references.

## Fidelity and provenance

Preserve exact stored raw and wire bytes, exact stored timestamp components,
ordered typed metadata, physical ordinals and source identities under the
[data format](data-format.md). Missing information remains unknown. Metric schemas
and linked/job schemas remain distinct from journal schema 3. Consumers must
exhaust iterators successfully before committing output.

Physical extraction retains duplicates and deleted physical records by default.
`undeleted` applies reconciled masks only. Neither selection proves native search
visibility. Pre-ingestion transformations cannot be undone; row exports do not
reconstruct complete bucket files. Keep original sources and independently verify
exports. A content checksum is not authentication without a trusted expectation.

## Installation and platforms

Qualification targets are CPython 3.13/3.14 on macOS arm64 and Linux amd64.
Linux Docker emulation and hosted runners are identified separately in release
qualification. Other versions allowed by package metadata are not implicitly
qualified; Windows, other architectures and free-threaded builds are excluded.

The original-code portable wheel requires pinned PyArrow/Zstandard and the
separately supplied, hash-checked MIT decoder documented in README. Neither that
decoder nor native binaries/dependency wheelhouses are bundled. Optional S3 SDK
and federation dependencies have separate qualification. MIT confirmation is
accepted; future decoder bundling is a separate distribution decision.

## Exclusions and maintenance

No SPL/general indexed-search equivalence, search-time knowledge objects,
universal DDSS packaging, atomic live snapshots, distributed ownership, metric
deletion semantics, compressed random seeking, destination loaders or production
native binary wheels are promised. Logical resource limits are not hard RSS caps.
See [support and maintenance](../SUPPORT.md) for intake and release policy.
