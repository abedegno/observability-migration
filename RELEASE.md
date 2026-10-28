# Releases

Releases are built only from this curated repository. Source archives and release
assets must contain source, user/developer documentation, tests and applicable
licences. Plans, work logs, private qualification records, lab automation,
credentials and dependency caches are not release content.

For a release candidate:

1. Commit the intended version and changelog; use a fresh environment to build
   the wheel and verify its metadata, installed command and package version.
2. Run the repository test suite against the source and installed wheel with the
   separately supplied audited decoder. Record actual platforms and optional skips.
3. Inspect source/wheel contents and notices, verify original fixture hashes,
   and compute SHA256 checksums for the exact assets to be uploaded.
4. Create an immutable version tag at the actual build-input commit. Identify
   release qualifications accurately; do not reuse another source tree's archive,
   tag target or validation receipt as if it belonged to this repository.
5. Publish only after the repository owner approves the exact release assets.
   Do not replace published files or force-retag. Publish a new version for fixes.

The [0.59.0rc3 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v0.59.0rc3)
is published from commit `5f196a0d5afc34377efe0be760d533266bc79502`.
Its attached qualification summary describes the exact tested assets. PyPI
publication and production native wheels remain separate work.

The [0.59.0rc4 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v0.59.0rc4)
is published from commit `9e4fcca2a21e69a5e3ebdf1fe4fe0e2a746e403f`. Later
changes under the changelog's Unreleased heading are not part of those frozen assets.

The [0.59.0rc5 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v0.59.0rc5)
is published from commit `3ee902498d8262230d6ca5e111fc266a74573459` with bounded
metadata projections and native query stream cleanup.
The optional native adapter requires separately built binaries; qualifying an
existing development bridge does not qualify production native wheels.

The [1.0.0 stable release](https://github.com/abedegno/observability-migration/releases/tag/v1.0.0)
is published from commit `2dd48792667a9e090a830b6b735dcab238aba359` after exact-asset
qualification and owner approval. Its finite [support contract](docs/support-contract.md)
and [maintenance policy](SUPPORT.md) apply; proprietary-profile experimental flags
and optional native provider exclusions remain unchanged. Later documentation
updates on main do not alter the frozen release assets.

The [1.1.0rc1 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v1.1.0rc1)
is published from commit `fd26db379f1cb4f01b808a4480328126a79a6e72` after exact-asset
qualification and owner approval. It packages opt-in job progress, bounded file
reads, posting preflight overhead reduction and cancellation-recovery coverage.
All five public asset downloads match the approved files. The 1.0.0 stable
contract and assets remain unchanged, and 1.0.0 remains the latest stable release.
No new physical profile or indexed-search support is introduced. Qualification
of existing development native binaries remains separate from production native
wheel distribution.

The [1.1.0rc2 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v1.1.0rc2)
is published from commit `722b5d1d533a146893e39471eead569ac95a7e0b` after exact-asset
qualification and owner approval. All five public asset downloads match the
approved files and the immutable tag resolves to that build input. It packages
nullable bucket-info annotations, the observed extended event metadata profile,
opt-in bounded larger MSIDX4 bulk extraction and lower metric Parquet reader
allocation peaks. Published 1.1.0rc1 and stable 1.0.0 assets stay unchanged;
1.0.0 remains latest stable. Profile limits, original-input retention and physical
extraction versus search-visibility boundaries continue to apply. Larger metric
SQL/capture is not qualified by bulk export. Later documentation changes on main
do not alter the frozen assets or their preparation-time qualification summary.
