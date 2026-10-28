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
publication, production native wheels and stable 1.0 promotion remain separate work.

The [0.59.0rc4 prerelease](https://github.com/abedegno/observability-migration/releases/tag/v0.59.0rc4)
is published from commit `9e4fcca2a21e69a5e3ebdf1fe4fe0e2a746e403f`. Later
changes under the changelog's Unreleased heading are not part of those frozen assets.

The 0.59.0rc5 candidate adds bounded metadata projections and native query stream
cleanup. Publication remains pending exact-asset qualification and owner approval.
The optional native adapter requires separately built binaries; qualifying an
existing development bridge does not qualify production native wheels.
