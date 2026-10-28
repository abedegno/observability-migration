#!/usr/bin/env python3
"""Measure a canonical Parquet traversal in a fresh process; no journal decoding."""
import argparse
import hashlib
import json
import math
import platform
import re
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('parquet')
    parser.add_argument('--expected-events', type=int)
    parser.add_argument('--expected-raw-wire-sha256')
    parser.add_argument('--max-rss-mib', type=float)
    args = parser.parse_args()
    if sys.platform not in ('linux', 'darwin'):
        parser.error('peak RSS measurement currently supports Linux and macOS')
    if args.expected_events is not None and args.expected_events < 0:
        parser.error('expected events must be nonnegative')
    if args.max_rss_mib is not None and (not math.isfinite(args.max_rss_mib) or args.max_rss_mib <= 0):
        parser.error('RSS budget must be finite and positive')
    if args.expected_raw_wire_sha256 is not None and not re.fullmatch('[0-9a-f]{64}', args.expected_raw_wire_sha256):
        parser.error('expected digest must be 64 lowercase hexadecimal characters')

    import resource
    import pyarrow as pa
    from obs_migrate.export import read_export

    start = time.monotonic()
    digest = hashlib.sha256()
    count = 0
    for row in read_export(args.parquet, 'parquet'):
        for key in ('raw', 'record_wire'):
            value = row[key]
            digest.update(len(value).to_bytes(8, 'little'))
            digest.update(value)
        count += 1
    elapsed = time.monotonic() - start
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mib = peak / (1024**2 if sys.platform == 'darwin' else 1024)
    failed = []
    if args.expected_events is not None and count != args.expected_events:
        failed.append('count')
    if args.expected_raw_wire_sha256 is not None and digest.hexdigest() != args.expected_raw_wire_sha256:
        failed.append('digest')
    if args.max_rss_mib is not None and peak_mib > args.max_rss_mib:
        failed.append('memory')
    print(json.dumps(dict(status='FAIL' if failed else 'PASS', failed_gates=failed,
        events=count, raw_wire_sha256=digest.hexdigest(), read_seconds=elapsed,
        peak_rss_mib=peak_mib, python=platform.python_version(), platform=sys.platform,
        pyarrow=pa.__version__, arrow_cpu_count=pa.cpu_count(),
        allocator=pa.default_memory_pool().backend_name,
        scope='Parquet traversal; not original-source verification or native format validation')))
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
