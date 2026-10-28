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

- `extended-events-10.6`: three controlled nonprivate CC0 HEC events with string
  and Float64 indexed fields, produced by a network-disabled local Splunk 10.6.0.5
  lab. The manifest binds original bucket bytes, original JSON inputs and an
  independent offline native CSV reference. Source stability and stopped-service
  observations are explicit in provenance; no NAS data or credentials are included.

- `metric-xor-9`: an unchanged native CC0 MSIDX4 flags-5 control from the existing
  9.0.0 metric-compression lab, with 1,201 points, four original numeric blocks,
  original JSON inputs and native export reference. Manifest hashes bind the subset;
  no NAS data or fabricated binary formats are included.

These samples demonstrate limited observed profiles. They do not certify every
producer setting, DDSS layout, query semantic or proprietary format variation.
Never substitute fabricated proprietary binary data for compatibility evidence.
