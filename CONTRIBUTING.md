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
