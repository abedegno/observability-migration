# Test fixtures

The fixtures are small, redistributable examples with original binary bytes:

- `botsv3`: selected gzip buckets from Splunk's public CC0 BOTS v3 archive.
  `provenance.json` retains the original archive identity, member paths and
  SHA256 entries for the included subset.
- `current-producer-10.6.0.5`: controlled nonprivate CC0 Zstd event buckets and
  retained native CSV/search references. The manifest retains original producer,
  input and file hashes. Acquisition automation is not part of the fixture set.
- `metrics`: five-point native MSIDX1/MSIDX2/MSIDX4 controls, retained journals
  and an independent native CSV control. The manifest records source member paths
  and exact hashes; the samples are CC0.

These samples demonstrate limited observed profiles. They do not certify every
producer setting, DDSS layout, query semantic or proprietary format variation.
Never substitute fabricated proprietary binary data for compatibility evidence.
