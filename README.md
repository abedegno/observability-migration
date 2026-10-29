# Observability Migration

Read-only access to historical observability data, starting with native Splunk
buckets and journals. Extract stored events and metrics into Arrow, Parquet or
NDJSON while retaining original bytes, exact timestamp components, metadata and
source provenance.

The toolkit provides experimental readers for explicitly recognised physical
profiles. It does not implement SPL, general indexed search, live snapshots or
complete DDSS archive compatibility. See [supported formats](docs/formats.md).

## Quick start

Use Python 3.13 or 3.14 and an immutable, authorised offline copy of your data.
The Python distribution is `observability-migration-toolkit`, the CLI is
`obsmigrate`, and the import namespace is `obs_migrate`.

```sh
git clone https://github.com/abedegno/observability-migration.git
cd observability-migration
python3.14 -m venv .venv
.venv/bin/python -m pip install .
.venv/bin/obsmigrate --version
```

The original journal decoder is supplied separately. Obtain its audited source
and register the directory containing its two Python modules:

```sh
git clone https://github.com/ponquersohn/splunk_ddss_extractor.git upstream/splunk_ddss_extractor
git -C upstream/splunk_ddss_extractor checkout --detach d2885c63b36f115189f810eb0f43a888cc73c967
.venv/bin/obsmigrate setup-decoder \
  --source "$PWD/upstream/splunk_ddss_extractor/src/splunk_ddss_extractor"
.venv/bin/obsmigrate doctor
```

Both module hashes are checked before execution. Journal extraction requires no
Splunk installation, Rust compiler or external Zstd executable. The optional
[source bootstrap](scripts/bootstrap-python.sh) also supports setup with Git/uv.

```sh
.venv/bin/obsmigrate buckets inspect /offline/bucket
.venv/bin/obsmigrate buckets export /offline/bucket \
  --experimental --format parquet --out /tmp/events.parquet
.venv/bin/obsmigrate buckets verify /offline/bucket \
  --experimental --format parquet --against /tmp/events.parquet
```

Choose a new output path. Keep the original bucket: exported rows do not
reconstruct complete bucket files. Default physical extraction retains physical
events, including deleted records where present. `undeleted` applies only
reconciled masks, not all Splunk search visibility.

## Library

```python
from obs_migrate.journal import events
from obs_migrate.events import batches

for batch in batches(events('/offline/bucket', experimental=True)):
    consume(batch)
```

Consume the iterator successfully to completion before committing a result.
Arrow batches form the boundary for downstream analytics and the optional
DataFusion adapter.

## Documentation

- [Usage and recovery](docs/usage.md)
- [Event jobs and S3 acquisition](docs/jobs-and-s3.md)
- [Supported formats and limitations](docs/formats.md)
- [Event data contract](docs/data-format.md)
- [Optional federation](docs/federation.md)
- [Contributing and tests](CONTRIBUTING.md)
- [Release procedure](RELEASE.md) and [changelog](CHANGELOG.md)
- [Security reporting](SECURITY.md)

Original code is Apache-2.0. Bundled test data retains its own CC0 licence and
provenance. The separately supplied decoder is MIT; see
[third-party acknowledgements](THIRD_PARTY_NOTICES.md).

The [offline access contract](docs/support-contract.md) defines the finite scope
for a stable build. [Support and maintenance](SUPPORT.md) describes best-effort
reporting, triage and release policy; optional federation remains experimental.

### Published prerelease: 1.1.0rc3

The [1.1.0rc3 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v1.1.0rc3)
was published on 29 October 2026 from frozen source
`ebca8ac26d950acdacb11ccf6cd5e5bb374203e9`. It includes standard-frame LZ4
journals, explicit larger metric capture/selection/SQL profiles and caller-selected
columns for capture and snapshot SQL. Full physical validation remains mandatory;
default limits and independent input/result budgets remain unchanged. The release's
qualification and notes identify its exact assets, platforms and optional providers.
Stable 1.0.0 remains the latest stable release.

### Unreleased source changes

Current `main` also covers observed SmartStore TSIDX9 deletion bases, distinct
origin/uploader identities and reconciled replica-uploaded deletion masks. Genuine
historical uploader-replacement fixtures and [capture-selection guidance](docs/jobs-and-s3.md#select-a-historical-smartstore-capture)
cover separate immutable before/after acquisitions. These changes postdate the
published rc3 source and are not part of its frozen assets, even though package
metadata still reports `1.1.0rc3`. See [Unreleased](CHANGELOG.md#unreleased) and
[supported formats](docs/formats.md) for their finite experimental scope. Source
changes do not expand the published stable support contract or establish SPL,
general indexed search, concurrent-copy election or complete DDSS compatibility.

### Published prerelease: 1.1.0rc2

The [1.1.0rc2 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v1.1.0rc2) includes support for empty bucket-info index-time annotations
and the observed extended Float64 event metadata profile. The explicit
`--resource-profile large` on MSIDX4 metric bucket/file export and verification
admits up to 1,000,000 value/time entries and per-column values; defaults remain
100,000. Other limits and full source validation remain in force. Larger metric
capture, SQL and federation are outside this qualification. Metric Parquet reads
use local non-threaded batches without changing global Arrow settings; memory
use still depends on the workload and platform. See the [data contract](docs/data-format.md)
for exact retention and refusal boundaries. Stable 1.0.0 remains the latest stable release.
