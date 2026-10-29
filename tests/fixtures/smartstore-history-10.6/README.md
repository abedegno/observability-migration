# Native SmartStore historical uploader replacement

Controlled CC0-1.0 fixture produced by Splunk 10.6.0.5 in an isolated lab with replication and search factors of two.
No Splunk installation or licence is required to run the offline reader tests.

The source loses only storage connectivity. The replica uploads the first copy.
For `00-source-blocked`, the source is disconnected/frozen and the replica daemon
is stopped during original List/Get/List acquisition. Storage is then restored and
the source resumed. After a bounded observation window and native query returning fourteen events,
both peer daemons are stopped for `01-source-recovered`. The current remote store
now contains the source namespace/receipt and no earlier replica namespace.

Both captures contain one complete native receipt/payload set for the same bucket,
with different uploader GUIDs and receipt hashes. Compressed journal bytes match;
TSIDX filenames/index metadata differ. Original before/after search references,
both native CSV exports and controlled input retain fourteen events including duplicates.
Inventories, native receipts and payloads are literal byte copies. The generated
provenance maps each copied file to its original relative acquisition path; the
closed manifest hashes all fixture files except itself.

Open each `acquired` directory separately with AcquiredObjects. A preserved old
receipt alone cannot recover a payload prefix missing from a later store snapshot.
Do not combine these histories into a claimed native concurrent-copy acquisition.
Negative tests alter ordinary inventory wrappers or reuse original receipt bytes;
they do not construct a proprietary binary format.

This finite control does not qualify transient overlap, automatic ownership,
replacement masks, native indexed search, S3 version history or DDSS packaging.
Native receipt content_hash remains opaque. Local caches, deployment/configuration,
licence material and engineering plans are outside this curated fixture.
