# Usage and recovery

Work from closed, immutable, authorised copies. Source files are read-only;
outputs must be outside source buckets and must not already exist. Compression
is detected from bytes rather than filename extensions. Unknown or inconsistent
profiles fail without publishing a complete output.

## Journals and exports

Recognised local gzip, Zstd, standard-frame LZ4 and plain journals use the same
commands. A bucket may contain exactly one `rawdata/journal`, `journal.gz`,
`journal.zst` or `journal.lz4`. The package installs the pinned Python LZ4 codec;
no external LZ4 executable is required.

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

For recognised event buckets that exceed the default linked-reader count limits,
select the existing bounded large profile explicitly:

```sh
obsmigrate events snapshot-inspect /offline/event-bucket \
  --experimental --offline-snapshot --backend journal --resource-profile large
```

The large profile permits up to one million linked events and 512 MiB of
decompressed journal data. Other count/byte limits still apply. These are logical
budgets, not a process memory guarantee; pinned-source and eager Arrow caps remain
unchanged. It does not accept unknown physical layouts. Ordinary journal export
uses a separate streaming path and does not require this linked-index option.

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


Larger MSIDX4 bulk scans use an explicit resource profile:

```sh
obsmigrate metrics bucket-export /offline/metric-bucket --experimental \
  --offline-snapshot --resource-profile large --out /derived/large-metrics.parquet
obsmigrate metrics bucket-verify /offline/metric-bucket --experimental \
  --offline-snapshot --resource-profile large --against /derived/large-metrics.parquet
```

The profile admits at most 1,000,000 value entries and column values per slice,
uses incremental XOR cursors with an 8 MiB logical byte budget, and leaves other
limits and full validation unchanged. Single-file `metrics tsidx-export` and
`tsidx-verify` accept the same option. Default calls keep their previous budgets.
Explicit profiles are limited to MSIDX4 bulk access; larger capture/selection/SQL
workloads and legacy MSIDX1/2 profiles are not promoted. The byte budget is block
wire bytes plus a 1,024-byte allowance per cursor, not a process RSS limit.
