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

The current 0.59.0rc3 is an unreleased source candidate. No public release, PyPI
upload, production native wheel or stable 1.0 promotion is implied by this file.
