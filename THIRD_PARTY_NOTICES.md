# Third-party acknowledgements

Original code is Apache-2.0; see LICENSE and NOTICE. Dependency implementations
and native binaries are not bundled in the toolkit wheel.

## External journal decoder

- Project: [splunk_ddss_extractor](https://github.com/ponquersohn/splunk_ddss_extractor)
- Audited commit: `d2885c63b36f115189f810eb0f43a888cc73c967`
- Metadata author: Lech Lachowicz
- Licence: MIT, confirmed by its maintainer

Only the separately supplied, hash-verified `decoder.py` and `stream.py` modules
are used. The upstream package initializer and Rust extension are bypassed. These
decoder implementations are not included in this distribution. Preserve their
applicable upstream notices when supplying them separately.

The [MIT reference text](docs/licenses/MIT.txt) accompanies the toolkit's notices;
it does not invent an upstream copyright line or replace notices accompanying
the original decoder source.

## Dependencies and test fixtures

PyArrow, Zstandard, Python LZ4 (BSD-3-Clause), optional Python DataFusion and Boto3/botocore are separately
installed and retain their distribution notices. The Rust adapter uses its pinned
Cargo dependency graph; no compiled dependency or vendor software is bundled.

BOTS samples and controlled native test data are CC0-1.0. Their licences and
original source/native-reference hashes are retained with the fixtures. Canonical
record and compression tests are distinct from proprietary-format fixture evidence.
