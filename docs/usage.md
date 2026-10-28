# Usage and recovery

Work from closed, immutable, authorised copies. Source files are read-only;
outputs must be outside source buckets and must not already exist. Compression
is detected from bytes rather than filename extensions. Unknown or inconsistent
profiles fail without publishing a complete output.

## Journals and exports

```sh
obsmigrate buckets inspect /offline/bucket
obsmigrate buckets export /offline/bucket --experimental --out /derived/events.parquet
obsmigrate buckets verify /offline/bucket --experimental --against /derived/events.parquet
```

Use `--format ndjson` for Base64-encoded binary fields. Parquet uses binary fields
directly. `display_text` is a derived convenience value; `raw` is authoritative.
Supply `--index`, `--bucket-id` or `--object-uri` only as trusted explicit context,
and repeat that context during verification. Local paths are omitted from export
provenance by default. A trusted `--expected-source-sha256` authenticates the input
against a previously obtained identity; computing a new hash alone does not.

For manifest-managed partitioned Parquet:

```sh
obsmigrate buckets export /offline/bucket --experimental \
  --format parquet-dataset --out /derived/events.dataset
# After interruption, repeat the same command with --resume.
obsmigrate buckets export /offline/bucket --experimental \
  --format parquet-dataset --out /derived/events.dataset --resume
obsmigrate buckets verify /offline/bucket --experimental \
  --format parquet-dataset --against /derived/events.dataset
```

Resume replays and verifies the source prefix; it does not seek randomly through
compressed data. Read only completed datasets through
`obs_migrate.dataset.read_dataset`, which validates the authoritative manifest.
An unrestricted file glob can include provisional orphan files.

## Linked event and metric leaves

```sh
obsmigrate events snapshot-export /offline/event-bucket \
  --experimental --offline-snapshot --out /derived/linked-events.parquet
obsmigrate metrics journal-export /offline/retained-metric-bucket \
  --experimental --out /derived/journal-metrics.parquet
obsmigrate metrics bucket-export /offline/metric-bucket \
  --experimental --offline-snapshot --out /derived/indexed-metrics.parquet
```

These formats have distinct schemas and validation contracts. Linked events
require complete journal/index reconciliation. Retained metric journal times do
not necessarily equal indexed metric times. General indexed search and metric
deletion visibility are not implemented.

## Diagnostics

`obsmigrate doctor` reports environment readiness, not source compatibility.
If a dependency is missing, install the package with the same environment's
Python and run `python -m pip check`. Register the exact audited decoder with
`obsmigrate setup-decoder --source DIRECTORY`. Changed decoder bytes are refused.
Use a fresh environment when migrating from the older `obs-migrate` distribution,
which owns the same Python namespace.

Results are JSON on stdout; failures use stderr and a nonzero status. A failed or
cancelled iterator must not be treated as a successful partial migration. Retain
original sources and use the verifier before consuming published exports.
