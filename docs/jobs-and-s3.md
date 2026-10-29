# Event jobs and S3 acquisition

Jobs compose supported offline event leaves. They retain per-source identities
and ordinal resets, preserve duplicate events and publish a complete manifest only
after every selected source passes. Metrics use separate leaf commands and cannot
be supplied as event jobs.

## Local event job

Save a job file, replacing the example path, bucket name and index label with your
actual authorised source. Select exact immediate bucket names, not wildcard paths.

```json
{
  "spec_version": 1,
  "visibility": "physical",
  "inputs": [{
    "input_id": "archive",
    "kind": "local",
    "root": "/offline/index/db",
    "index": "application",
    "selectors": ["db_1534778228_1534766520_308"]
  }]
}
```

```sh
obsmigrate jobs plan --spec job.json --offline-snapshot
obsmigrate jobs export --spec job.json --offline-snapshot --out /derived/job
obsmigrate jobs verify --spec job.json --offline-snapshot --against /derived/job
# After interruption, repeat export with the same options and --resume.
```

Relative roots are resolved against the job-file directory. A trusted
`expected_identity_sha256` authenticates a previously recorded input identity;
without it, the run binds observed bytes but does not authenticate them. Use
`visibility: "undeleted"` only for the supported reconciled mask contract.
Whole-journal duplicates require the explicit duplicate override. Equal events
are retained. `--format arrow` selects Arrow IPC; preserve it on resume.

## Optional job progress (1.1.0rc1)

Add `--progress` to `jobs plan`, `jobs export` or `jobs verify` to stream stage
records to stderr while keeping the existing final stdout JSON unchanged:

```sh
obsmigrate jobs export --spec job.json --offline-snapshot --out /derived/job \
  --progress 2>/derived/job-progress.jsonl
obsmigrate jobs export --spec job.json --offline-snapshot --out /derived/job \
  --resume --progress 2>/derived/resume-progress.jsonl
obsmigrate jobs verify --spec job.json --offline-snapshot --against /derived/job \
  --progress 2>/derived/verify-progress.jsonl
```

Keep log destinations outside original sources. This opt-in is available in
1.1.0rc1; the frozen 1.0.0 wheel does not have the flag.
Without it, stderr and stdout behavior remains unchanged.

Progress records have `type: "job_progress"`, `progress_schema: 1`, `operation`,
`stage` and `status`. Member records add the one-based `member_number`,
`members_total`, opaque `source_key`, and validated `input_id`/`selector` when safe
to display. Selectors outside the bounded ASCII identifier set are omitted; the
source key still identifies the selection. Completed stages include wall-clock
`duration_seconds`. Records are JSON-escaped and flushed individually. Existing
final CLI failure/cancellation JSON remains a separate record without that type.

Stages distinguish planning, source validation, export, checkpoint reuse, source
verification, checkpoint checks, artifact integrity and final local manifest
publication. `validation` finishes only after the source has been opened and fully
preflighted. `reuse` revalidates existing output against the source, including for
a completed job; it does not mean skipped work. `job/completed` appears only after
all normal source/artifact checks and any complete-manifest publication succeed.

This reports stage boundaries, not a heartbeat, percentage, ETA or progress within
a native parser. A long stage can remain active without another record. No root,
file path, object URI, event body or exception text is included in a progress
record. Identifiers deliberately name the selected member and can themselves be
sensitive; retain logs accordingly. These records establish no new source
consistency, native search visibility or compressed-seeking guarantee.

Library callers of `job_dataset.export_job` and `verify_job` can pass
`progress=callback` to receive the same bounded stage dictionaries synchronously.
Callbacks must remain observational and must not mutate sources/outputs or reenter
the job. Callback errors can abort work, leaving normal validated checkpoints;
if terminal reporting fails during another error/cancellation, the original error
wins. Reporting failure after a manifest commit cannot roll that commit back.
Progress is transient and is never stored in the selection, data or manifest.

## Read-only S3 acquisition

Install the optional maintained SDK dependency with `pip install '.[s3]'`.
Acquisition uses its credential chain and writes an explicit local acquired
inventory. Use a closed/quiescent prefix and a new local output directory:

```sh
obsmigrate smartstore acquire --s3-bucket my-archive \
  --prefix indexes/application/ --offline-snapshot --out /derived/acquired
```

The command performs bounded remote List/Get operations; it does not write to
S3. Conditional reads and complete inventory checks detect observed changes,
but do not create an atomic snapshot across concurrent writers. `--version-mode
versioned` requires actual object version identities; this transport mode does
not establish native versioned-producer compatibility or distributed ownership.
Do not put credentials or signed URLs into job files.

Acquired SmartStore job inputs use `kind: "smartstore"`, a local acquired root
and exact native receipt selectors. Bind the complete acquired inventory and
masks; a receipt hash alone does not identify later deletion state. Keep source
objects and acquisition provenance. Unknown/incomplete layouts refuse.

## Select a historical SmartStore capture

Keep each completed acquisition in a separate new directory, including its
`objects.json`, original native receipt and every listed payload. Record the
inventory SHA256 in an independently trusted acquisition record. A later uploader
can replace the receipt and payload namespace while retaining the same bucket ID.
A bucket ID or journal hash alone therefore does not select a historical capture.

First choose the acquired directory, then inspect it and copy the exact bucket ID
from its report. Replace the paths, SHA256 and bucket ID below with that capture's
values; keep derived output outside the acquired directory:

```sh
obsmigrate smartstore inspect /offline/history/before/acquired \
  --offline-snapshot --expected-inventory-sha256 TRUSTED_INVENTORY_SHA256
obsmigrate smartstore export /offline/history/before/acquired \
  --offline-snapshot --expected-inventory-sha256 TRUSTED_INVENTORY_SHA256 \
  --bucket-id EXACT_BUCKET_ID_FROM_REPORT --experimental --backend journal \
  --format parquet --out /derived/before.parquet
obsmigrate smartstore verify /offline/history/before/acquired \
  --offline-snapshot --expected-inventory-sha256 TRUSTED_INVENTORY_SHA256 \
  --bucket-id EXACT_BUCKET_ID_FROM_REPORT --experimental --backend journal \
  --format parquet --against /derived/before.parquet
```

The library uses the same selection through
`AcquiredObjects(path, offline_snapshot=True, expected_inventory_sha256=...)`
and `event_bucket(exact_bucket_id, experimental=True, ...)`. To read the later
capture, use its own directory and inventory hash. Verification remains bound to
the chosen source; an export from another capture is not interchangeable merely
because its journal bytes match. A hash calculated only from the current input
detects changes relative to that input but does not independently authenticate it.

Saving an old receipt alone cannot recover an earlier payload prefix removed from
the store. Reusing that receipt with a later capture's payloads refuses. Preserve
complete acquisitions before replacement and retain their provenance. Do not merge
the two histories to simulate a native simultaneous-copy snapshot or silently
ingest both as independent event sets; they can contain the same stored events.

`tests/fixtures/smartstore-history-10.6` contains genuine before/after replacement
captures and native references: fourteen events, identical journal bytes, different
receipts and uploader/index metadata. Each original acquisition has one complete
copy. This is offline physical access with caller-asserted quiescence; it does not
elect an owner, establish native search visibility, qualify deletion masks across
replacement, or establish simultaneous copies, S3 version history or DDSS packaging.
