#!/usr/bin/env python3
"""Optional native SQL parity/work gate on an authorised immutable event bucket.

Uses full canonical rows as the materialized SQL reference. This tests provider
and engine equivalence, not an independent proprietary-format decoder oracle.
"""
import argparse
import json
from pathlib import Path
import platform
import resource
from time import perf_counter

import pyarrow as pa
import datafusion
from obs_migrate.event_bucket import EventBucket, SCHEMA
from obs_migrate.federation import BucketSession


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('bucket', type=Path)
    parser.add_argument('--metadata-cache-bytes', type=int, default=32*1024**2)
    parser.add_argument('--repeat', type=int, default=2)
    parser.add_argument('--retain-opaque-trailer', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.repeat <= 20:
        parser.error('--repeat must be 1..20')
    started = perf_counter()
    with EventBucket(args.bucket, experimental=True, offline_snapshot=True,
                     backend='journal', cache_projections=True,
                     metadata_cache_bytes=args.metadata_cache_bytes,
                     retain_opaque_trailer=args.retain_opaque_trailer) as source:
        open_seconds = perf_counter()-started
        started = perf_counter()
        names = ('record_ordinal','metadata_json','event_time','host')
        schema = pa.schema([SCHEMA.field(name) for name in names])
        rows = [{name:row[name] for name in names} for row in source.records()]
        if not rows:
            raise ValueError('benchmark requires a nonempty source')
        reference = datafusion.SessionContext()
        reference.register_record_batches('events', [pa.Table.from_pylist(rows, schema=schema).to_batches()])
        reference_seconds = perf_counter()-started
        session = BucketSession()
        started = perf_counter()
        session.register_native_event_snapshot('events', source, routing_pushdown=True, time_pushdown=True)
        registration_seconds = perf_counter()-started
        bound = rows[len(rows)//2]['event_time']['seconds']
        sqls = [
            ('all', 'select record_ordinal,metadata_json from events order by record_ordinal'),
            ('residual', f"select record_ordinal,metadata_json from events where event_time['seconds']>={bound} and (record_ordinal%2=0 or metadata_json='') order by record_ordinal"),
            ('limit', 'select record_ordinal,metadata_json from events limit 3'),
            ('aggregate', 'select count(*) as n,sum(length(metadata_json)) as size from events'),
            ('reuse', 'select record_ordinal,metadata_json from events order by record_ordinal'),
        ]
        trials = []
        for name,sql in sqls:
            expected = reference.sql(sql).to_arrow_table()
            for repeat in range(args.repeat):
                result = session.sql(sql)
                # Validation time is outside the SQL clock stored by BucketSession.
                if not result.table.equals(expected, check_metadata=False):
                    raise AssertionError('native SQL differs from canonical materialized reference: '+name)
                trials.append({'case':name,'repeat':repeat,'rows':result.table.num_rows,
                    'sql_plan_and_execute_seconds':result.manifest['timings']['sql_plan_and_execute_seconds'],
                    'arrow_bytes':result.manifest['arrow_bytes'],
                    'scans':result.manifest['native_scans']['events']})
        report = source.report()
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print(json.dumps({'status':'PASS','rows':len(rows),'snapshot_sha256':source.snapshot_sha256,
            'metadata_cache':report['metadata_projection_cache'], 'source_open_seconds':open_seconds,
            'full_canonical_reference_seconds':reference_seconds,'provider_registration_seconds':registration_seconds,
            'process_peak_rss_mib':rss/(1024**2 if platform.system()=='Darwin' else 1024),
            'python':platform.python_version(),'system':platform.system(),'machine':platform.machine(),
            'datafusion':datafusion.__version__,'pyarrow':pa.__version__,'trials':trials,
            'scope':'same-reader canonical materialized reference; reused optional bridge; warm queries; initial validation retained; peak includes reference; no production capacity or indexed-search claim'}, indent=2))


if __name__ == '__main__':
    main()
