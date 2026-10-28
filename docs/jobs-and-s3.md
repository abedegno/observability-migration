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
