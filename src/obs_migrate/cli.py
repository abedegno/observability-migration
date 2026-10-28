import argparse
import json
from pathlib import Path
import sys

from . import __version__


def main(argv=None):
    parser = argparse.ArgumentParser(prog="obsmigrate")
    parser.add_argument("--version",action="version",version=f"%(prog)s {__version__}")
    root = parser.add_subparsers(dest="group", required=True)
    root.add_parser('doctor',help='read-only environment readiness; does not validate a source')
    setup=root.add_parser('setup-decoder',help='register an external, hash-verified pinned Python source in this virtual environment')
    setup.add_argument('--source',required=True)
    jobs=root.add_parser('jobs',help='explicit offline multi-bucket event jobs; no native search semantics').add_subparsers(dest='command',required=True)
    for name in ('plan','export','verify'):
        p=jobs.add_parser(name);p.add_argument('--spec',required=True);p.add_argument('--offline-snapshot',action='store_true')
        if name=='export':
            p.add_argument('--out',required=True);p.add_argument('--format',choices=['parquet','arrow'],default='parquet');p.add_argument('--resume',action='store_true')
        elif name=='verify':p.add_argument('--against',required=True)
    tsidx_commands = root.add_parser("tsidx", help="header diagnostics and experimental native event catalogs/values; no indexed search").add_subparsers(dest="command", required=True)
    tsidx_commands.add_parser("inspect").add_argument("path")
    p=tsidx_commands.add_parser('extension-inspect',help='read-only trailing-byte observations after full prefix validation; extraction policy unchanged')
    p.add_argument('path');p.add_argument('--experimental',action='store_true')
    p.add_argument('--resource-profile',choices=['default','large'],default='default')
    event_commands=root.add_parser("events",help="experimental immutable physical event snapshots; initial full journal scan").add_subparsers(dest="command",required=True)
    for name in ('snapshot-inspect','snapshot-export','snapshot-verify','snapshot-sql'):
        p=event_commands.add_parser(name);p.add_argument('path')
        p.add_argument('--experimental',action='store_true');p.add_argument('--offline-snapshot',action='store_true')
        p.add_argument('--include-inactive',action='store_true');p.add_argument('--index');p.add_argument('--bucket-id');p.add_argument('--object-uri')
        p.add_argument('--visibility',choices=['physical','undeleted'],default='physical',help='exclude only reconciled journal delete masks; full physical validation still required; not native search visibility')
        p.add_argument('--backend',choices=['arrow','journal'],help='default arrow; native SQL provider defaults to journal')
        p.add_argument('--retain-opaque-trailer',action='store_true',help='preserve an uninterpreted index tail; strict rejection remains default')
        p.add_argument('--expected-snapshot-sha256',help='trusted previously recorded full snapshot identity; checked before journal decoding')
        p.add_argument('--resource-profile',choices=['default','large'],default='default',help='explicit native event count/decompression budgets; source and Arrow caps unchanged')
        p.add_argument('--cache-projections',action='store_true',help='opt-in native scalar/raw/metadata cache; requires journal backend')
        p.add_argument('--metadata-cache-bytes',type=int,help='metadata cache accounting budget: 0..67108864 bytes; default 33554432; requires projection caching to retain entries')
        p.add_argument('--term-base64',help='exact physical lexicon bytes; no SPL or tokenization')
        if name=='snapshot-inspect':p.add_argument('--observe-delete-bitmap',action='store_true',help='compare observed one-bit native arrays with complete reconciled journal masks; no search visibility claim')
        if name!='snapshot-inspect':
            p.add_argument('--format',choices=['parquet','arrow'],default='parquet')
            p.add_argument('--out' if name in ('snapshot-export','snapshot-sql') else '--against',required=True)
        if name=='snapshot-sql':
            p.add_argument('--sql',required=True,help='read-only SQL over events; filters remain engine residuals')
            p.add_argument('--provider',choices=['arrow','native'],default='arrow',help='eager Arrow MemTable or optional direct native stream')
            p.add_argument('--time-pushdown',action='store_true',help='opt-in exact stored event/index time candidates; residuals retained')
            p.add_argument('--routing-pushdown',action='store_true',help='opt-in exact derived binary routing candidates; residuals retained')
            for field in ('input','result','engine'):p.add_argument('--max-'+field+'-bytes',type=int,default=256*1024**2)
    for name in ("lexicon-inspect","lexicon-export","lexicon-verify"):
        p=tsidx_commands.add_parser(name);p.add_argument("path");p.add_argument("--experimental",action="store_true")
        p.add_argument('--retain-opaque-trailer',action='store_true')
        p.add_argument('--resource-profile',choices=['default','large'],default='default')
        if name=='lexicon-inspect':p.add_argument('--summary',action='store_true',help='source/profile summary without materializing catalog entries')
        if name!="lexicon-inspect":
            p.add_argument("--format",choices=["parquet","arrow"],default="parquet")
            p.add_argument("--out" if name=="lexicon-export" else "--against",required=True)
    for name in ("index-inspect","postings","values-export","values-verify"):
        p=tsidx_commands.add_parser(name);p.add_argument("path");p.add_argument("--experimental",action="store_true")
        p.add_argument('--retain-opaque-trailer',action='store_true')
        p.add_argument('--resource-profile',choices=['default','large'],default='default')
        p.add_argument("--term-base64",required=name=="postings",help="exact native lexicon term bytes; no tokenization/SPL translation")
        if name in ("values-export","values-verify"):
            p.add_argument("--format",choices=["parquet","arrow"],default="parquet")
            p.add_argument("--out" if name=="values-export" else "--against",required=True)
    metric_commands = root.add_parser("metrics", help="native JSON and experimental journal/MSIDX scans; no indexed search").add_subparsers(dest="command", required=True)
    for name in ("export", "verify", "journal-export", "journal-verify", "tsidx-export", "tsidx-verify", "bucket-export", "bucket-verify"):
        p = metric_commands.add_parser(name)
        if name.startswith('journal-'):p.add_argument('--expected-source-sha256',help='trusted hash of original compressed journal bytes')
        journal_mode = name.startswith("journal-")
        tsidx_mode = name.startswith("tsidx-")
        bucket_mode = name.startswith("bucket-")
        p.add_argument("path", help="explicit offline metric bucket snapshot; physical points only" if bucket_mode else "one metric MSIDX1/MSIDX2/MSIDX4 file; physical stored points only" if tsidx_mode else "retained metric journal/bucket; physical records only" if journal_mode else "NDJSON acquired with Splunk native exporttool default mode")
        p.add_argument("--format", choices=["ndjson", "parquet"], default="parquet")
        p.add_argument("--out" if name.endswith("export") else "--against", required=True)
        p.add_argument("--index", help="trusted caller provenance label")
        if journal_mode or tsidx_mode or bucket_mode:
            p.add_argument("--experimental", action="store_true")
            p.add_argument("--bucket-id", help="trusted caller provenance label")
        else:
            p.add_argument("--native-reader", help="trusted caller native binary/version provenance label")
        if bucket_mode:
            p.add_argument("--offline-snapshot", action="store_true", help="assert a consistent closed offline source snapshot")
            p.add_argument("--include-inactive", action="store_true")
    for name in ("select-explain","select-export","select-verify","bucket-select-explain","bucket-select-export","bucket-select-verify","bucket-sql","capture-sql"):
        p=metric_commands.add_parser(name,help="exact physical metric predicates; full preflight scan; no search visibility")
        bucket_selection=name.startswith("bucket-select-") or name in ("bucket-sql","capture-sql")
        p.add_argument("path",help="toolkit capture ZIP bound by --capture-sha256" if name=="capture-sql" else "explicit offline metric bucket snapshot" if bucket_selection else "one verified MSIDX1/MSIDX2/MSIDX4 file")
        if bucket_selection and name!="capture-sql":
            p.add_argument("--offline-snapshot",action="store_true");p.add_argument("--include-inactive",action="store_true")
        p.add_argument("--experimental",action="store_true")
        if name=="capture-sql":
            p.add_argument("--capture-sha256",required=True)
            p.add_argument("--provider",choices=["dataset","native"],default="dataset",help="lazy Parquet bridge or optional direct Rust/Arrow provider")
        for field in ("metric-name","host","source","sourcetype"):
            p.add_argument("--"+field,help="case-sensitive exact UTF-8 bytes")
        p.add_argument("--dimension",action="append",default=[],metavar="KEY=VALUE",help="AND exact dimension pair; split at first =; empty values allowed")
        if not name.endswith("explain"):
            p.add_argument("--format",choices=["parquet","arrow"] if name in ("bucket-sql","capture-sql") else ["ndjson","parquet"],default=None if name=="bucket-select-verify" else "parquet",help="SQL result file format" if name in ("bucket-sql","capture-sql") else "payload format inside the ZIP bundle" if bucket_selection else "point file format")
            p.add_argument("--out" if name.endswith("export") or name in ("bucket-sql","capture-sql") else "--against",required=True,help="SQL result with embedded provenance" if name in ("bucket-sql","capture-sql") else "snapshot-bound result ZIP bundle" if bucket_selection else "point file")
        if name in ("bucket-sql","capture-sql"):
            p.add_argument("--sql",required=True,help="read-only SQL over table points; all SQL filters remain engine residuals")
            p.add_argument("--max-input-bytes",type=int,default=256*1024**2)
            p.add_argument("--max-result-bytes",type=int,default=256*1024**2)
            p.add_argument("--max-engine-bytes",type=int,default=256*1024**2)
        if bucket_selection or not name.endswith("explain"):
            p.add_argument("--index");p.add_argument("--bucket-id")
    p=metric_commands.add_parser('snapshot-sql',help='bounded full physical metric scan into Arrow; all SQL filters residual; no native pruning')
    p.add_argument('path');p.add_argument('--experimental',action='store_true');p.add_argument('--offline-snapshot',action='store_true')
    p.add_argument('--include-inactive',action='store_true');p.add_argument('--index');p.add_argument('--bucket-id')
    p.add_argument('--expected-snapshot-sha256',help='trusted complete metric inventory identity, checked before Arrow materialization')
    p.add_argument('--sql',required=True,help='read-only SQL over points; complete physical source scan')
    p.add_argument('--out',required=True);p.add_argument('--format',choices=['parquet','arrow'],default='parquet')
    for field in ('input','result','engine'):p.add_argument('--max-'+field+'-bytes',type=int,default=256*1024**2)
    p=metric_commands.add_parser("capture-create",help="lossless native capture for immutable in-process validation reuse")
    p.add_argument("path");p.add_argument("--out",required=True)
    p.add_argument("--experimental",action="store_true");p.add_argument("--offline-snapshot",action="store_true")
    p.add_argument("--include-inactive",action="store_true")
    p=metric_commands.add_parser("bucket-inventory")
    p.add_argument("path");p.add_argument("--offline-snapshot",action="store_true");p.add_argument("--include-inactive",action="store_true")
    buckets = root.add_parser("buckets").add_subparsers(dest="command", required=True)
    smart = root.add_parser('smartstore', help='read-only S3 acquisition and offline receipt access; no automatic ownership selection').add_subparsers(dest='command',required=True)
    p=smart.add_parser('acquire',help='bounded SDK List/Get acquisition of an explicit quiescent prefix')
    p.add_argument('--s3-bucket',required=True);p.add_argument('--prefix',required=True);p.add_argument('--out',required=True)
    p.add_argument('--offline-snapshot',action='store_true');p.add_argument('--resume',action='store_true')
    p.add_argument('--version-mode',choices=['conditional','versioned'],default='conditional')
    p.add_argument('--profile');p.add_argument('--region');p.add_argument('--endpoint-url');p.add_argument('--ca-bundle')
    p.add_argument('--allow-http',action='store_true',help='explicit plaintext transport for an authorised local provider')
    for name in ('inspect','export','verify'):
        p=smart.add_parser(name);p.add_argument('path');p.add_argument('--offline-snapshot',action='store_true')
        p.add_argument('--expected-inventory-sha256')
        if name!='inspect':
            p.add_argument('--bucket-id',required=True);p.add_argument('--experimental',action='store_true')
            p.add_argument('--backend',choices=['arrow','journal'],default='arrow')
            p.add_argument('--visibility',choices=['physical','undeleted'],default='physical',help='reuse complete native delete-bit/journal-mask reconciliation; not native search visibility')
            p.add_argument('--format',choices=['arrow','parquet'],default='parquet')
            p.add_argument('--out' if name=='export' else '--against',required=True)
    p = buckets.add_parser('catalogue', help='hash a closed index root; retain all candidates and markers; no automatic activity selection')
    p.add_argument('path');p.add_argument('--offline-snapshot', action='store_true')
    p.add_argument('--expected-catalogue-sha256')
    p.add_argument('--select', action='append', help='explicit physical bucket name; repeat for each copy')
    p.add_argument('--allow-duplicate-journals', action='store_true')
    for name in ("inspect", "inventory", "export", "verify"):
        p = buckets.add_parser(name)
        p.add_argument("path")
        if name in ('inspect','export','verify'):
            p.add_argument('--index-type',choices=['unknown','event','metric'],default='unknown',
                           help='trusted caller hint for event adapter; metric access is under metrics')
        if name == "inventory":
            p.add_argument("--include-disabled", action="store_true")
        if name in ("export", "verify"):
            p.add_argument("--expected-source-sha256",help="trusted hash of original compressed journal bytes")
            p.add_argument("--visibility", choices=["physical", "undeleted"], default="physical")
            p.add_argument("--include-disabled", action="store_true")
            p.add_argument("--experimental", action="store_true")
            p.add_argument("--index")
            p.add_argument("--bucket-id")
            p.add_argument("--object-uri", help="explicit opt-in provenance label; local path omitted by default")
            p.add_argument("--format", choices=["ndjson", "parquet", "parquet-dataset"], default="parquet")
            p.add_argument("--out" if name == "export" else "--against", required=True)
            if name == "export":
                p.add_argument("--resume", action="store_true", help="validate and replay a partitioned export checkpoint")
                p.add_argument("--partition-rows", type=int)
                p.add_argument("--partition-bytes", type=int, help="target canonical bytes per partition, plus one bounded event")
    args = parser.parse_args(argv)
    journal = None
    stats = {}
    try:
        if args.group not in ('doctor', 'setup-decoder'):
            from . import journal
            from .export import export, verify
        if args.group=='doctor':
            from .runtime import doctor
            result=doctor()
        elif args.group=='setup-decoder':
            from .decoder_source import configure
            result=configure(args.source)
        elif args.group == 'events':
            import base64
            from . import event_bucket
            term=None if args.term_base64 is None else base64.b64decode(args.term_base64,validate=True)
            from .event_limits import LARGE_EVENTS
            limits=LARGE_EVENTS if args.resource_profile=='large' else None
            backend=args.backend or ('journal' if limits is not None or args.cache_projections or getattr(args,'provider','arrow')=='native' else 'arrow')
            if getattr(args,'time_pushdown',False) and getattr(args,'provider','arrow')!='native':raise ValueError('time pushdown requires native event provider')
            if getattr(args,'routing_pushdown',False) and getattr(args,'provider','arrow')!='native':raise ValueError('routing pushdown requires native event provider')
            if getattr(args,'provider','arrow')=='native' and backend!='journal':raise ValueError('native SQL provider requires --backend journal')
            with event_bucket.EventBucket(args.path,experimental=args.experimental,offline_snapshot=args.offline_snapshot,
                include_inactive=args.include_inactive,index=args.index,bucket_id=args.bucket_id,object_uri=args.object_uri,backend=backend,
                retain_opaque_trailer=args.retain_opaque_trailer,cache_projections=args.cache_projections,metadata_cache_bytes=args.metadata_cache_bytes,limits=limits,expected_snapshot_sha256=args.expected_snapshot_sha256,
                visibility=args.visibility) as snapshot:
                if args.command=='snapshot-inspect':
                    result=snapshot.describe_selection(term)
                    if args.observe_delete_bitmap:result['delete_bitmap_observation']=snapshot.deletion_report()
                elif args.command=='snapshot-sql':
                    from .federation import BucketSession
                    session=BucketSession(max_input_bytes=args.max_input_bytes,max_result_bytes=args.max_result_bytes,
                        max_engine_bytes=args.max_engine_bytes)
                    register=session.register_native_event_snapshot if args.provider=='native' else session.register_event_snapshot
                    if args.provider=='native':register('events',snapshot,term=term,routing_pushdown=args.routing_pushdown,time_pushdown=args.time_pushdown)
                    else:register('events',snapshot,term=term)
                    result=session.sql(args.sql).write(args.out,args.format)
                elif args.command=='snapshot-export':result=event_bucket.export(snapshot,args.out,args.format,term=term)
                else:result=event_bucket.verify(snapshot,args.against,args.format,term=term)
        elif args.group == "tsidx":
            if args.command=="inspect":
                from .tsidx import inspect_header
                result=inspect_header(args.path)
            elif args.command=='extension-inspect':
                from .event_extensions import inspect
                from .event_limits import LARGE_EVENTS
                result=inspect(args.path,experimental=args.experimental,
                    limits=LARGE_EVENTS if args.resource_profile=='large' else None)
            elif args.command in ("index-inspect","postings","values-export","values-verify"):
                from .event_limits import LARGE_EVENTS
                limits=LARGE_EVENTS if args.resource_profile=='large' else None
                import base64
                from . import event_index
                term=None if args.term_base64 is None else base64.b64decode(args.term_base64,validate=True)
                if args.command in ("index-inspect","postings"):
                    with event_index.open_index(args.path,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits) as index:
                        result=index.report()
                        if args.command=="postings":result.update(term_base64=args.term_base64,value_ids=index.posting_ids(term))
                elif args.command=="values-export":result=event_index.export(args.path,args.out,args.format,term=term,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits)
                else:result=event_index.verify(args.path,args.against,args.format,term=term,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits)
            else:
                from . import event_lexicon
                from .event_limits import LARGE_EVENTS
                limits=LARGE_EVENTS if args.resource_profile=='large' else None
                if args.command=='lexicon-inspect':
                    if limits is not None and not args.summary:raise ValueError('large catalog inspection requires --summary; use lexicon-export for all entries')
                    if args.summary:
                        with event_lexicon.open_lexicon(args.path,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits) as catalog:result=catalog.report()
                    else:result=event_lexicon.inspect(args.path,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits)
                elif args.command=="lexicon-export":result=event_lexicon.export(args.path,args.out,args.format,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits)
                else:result=event_lexicon.verify(args.path,args.against,args.format,experimental=args.experimental,retain_opaque_trailer=args.retain_opaque_trailer,limits=limits)
        elif args.group == "metrics":
            if args.command=='snapshot-sql':
                from .federation import BucketSession
                session=BucketSession(max_input_bytes=args.max_input_bytes,max_result_bytes=args.max_result_bytes,
                                      max_engine_bytes=args.max_engine_bytes)
                session.register_metric_snapshot('points',args.path,experimental=args.experimental,
                    offline_snapshot=args.offline_snapshot,include_inactive=args.include_inactive,index=args.index,
                    bucket_id=args.bucket_id,expected_snapshot_sha256=args.expected_snapshot_sha256)
                result=session.sql(args.sql).write(args.out,args.format)
            elif args.command=="capture-create":
                from .capture import create
                result=create(args.path,args.out,experimental=args.experimental,
                    offline_snapshot=args.offline_snapshot,include_inactive=args.include_inactive)
            elif args.command.startswith(("select-","bucket-select-")) or args.command in ("bucket-sql","capture-sql"):
                from . import metric_select
                pairs=[]
                for value in args.dimension:
                    key,separator,operand=value.partition("=")
                    if not separator:raise ValueError("dimension predicates require KEY=VALUE")
                    pairs.append((key.encode("utf8"),operand.encode("utf8")))
                fields={key:getattr(args,key).encode("utf8") if getattr(args,key) is not None else None
                        for key in ("metric_name","host","source","sourcetype")}
                predicate=metric_select.Predicate(**fields,dimensions=tuple(pairs))
                if args.command in ("bucket-sql","capture-sql"):
                    from .federation import BucketSession
                    session=BucketSession(max_input_bytes=args.max_input_bytes,max_result_bytes=args.max_result_bytes,
                                          max_engine_bytes=args.max_engine_bytes)
                    if args.command=="capture-sql":
                        from .capture import CapturedBucket
                        with CapturedBucket(args.path,expected_sha256=args.capture_sha256,experimental=args.experimental) as captured:
                            register=session.register_native_capture if args.provider=="native" else session.register_capture
                            register("points",captured,predicate,index=args.index,bucket_id=args.bucket_id)
                            result=session.sql(args.sql).write(args.out,args.format)
                    else:
                        session.register("points",args.path,predicate,experimental=args.experimental,
                            offline_snapshot=args.offline_snapshot,include_inactive=args.include_inactive,index=args.index,bucket_id=args.bucket_id)
                        result=session.sql(args.sql).write(args.out,args.format)
                elif args.command.startswith("bucket-select-"):
                    from . import bucket_select
                    options=dict(experimental=args.experimental,offline_snapshot=args.offline_snapshot,
                        include_inactive=args.include_inactive,index=args.index,bucket_id=args.bucket_id)
                    if args.command.endswith("explain"):result=bucket_select.explain(args.path,predicate,**options)
                    elif args.command.endswith("export"):result=bucket_select.export(args.path,predicate,args.out,args.format,**options)
                    else:result=bucket_select.verify(args.path,predicate,args.against,args.format,**options)
                elif args.command=="select-explain":
                    result=metric_select.explain(args.path,predicate,experimental=args.experimental)
                else:
                    options=dict(experimental=args.experimental,index=args.index,bucket_id=args.bucket_id)
                    if args.command=="select-export":result=metric_select.export(args.path,predicate,args.out,args.format,**options)
                    else:result=metric_select.verify(args.path,predicate,args.against,args.format,**options)
            elif args.command.startswith("bucket-"):
                from . import metric_bucket
                options=dict(offline_snapshot=args.offline_snapshot,include_inactive=args.include_inactive)
                if args.command=="bucket-inventory":
                    result=metric_bucket.Snapshot(args.path,**options).inventory()
                elif args.command=="bucket-export":
                    result=metric_bucket.export(args.path,args.out,args.format,experimental=args.experimental,
                                                index=args.index,bucket_id=args.bucket_id,**options)
                else:
                    result=metric_bucket.verify(args.path,args.against,args.format,experimental=args.experimental,
                                                index=args.index,bucket_id=args.bucket_id,**options)
            elif args.command.startswith("tsidx-"):
                from .metric_storage import reader_for
                reader=reader_for(args.path)
                if args.command == "tsidx-export":
                    source,output=Path(args.path).resolve(),Path(args.out).resolve()
                    if output.is_relative_to(source.parent):
                        raise ValueError("output must be outside the source archive/bucket")
                points=reader.records(args.path,experimental=args.experimental,index=args.index,bucket_id=args.bucket_id)
                if args.command == "tsidx-export":
                    result=reader.export(points,args.out,args.format)
                else:
                    result=reader.verify(points,args.against,args.format)
            elif args.command.startswith("journal-"):
                from . import metric_journal
                if args.command == "journal-export":
                    source = journal.resolve(args.path)
                    output, original = Path(args.out).resolve(), Path(args.path).resolve()
                    if source == output or (original.is_dir() and output.is_relative_to(original)) or (source.parent.name == "rawdata" and output.is_relative_to(source.parent.parent)):
                        raise ValueError("output must be outside the source archive/bucket")
                points = metric_journal.records(args.path, experimental=args.experimental, index=args.index, bucket_id=args.bucket_id,expected_source_sha256=args.expected_source_sha256)
                if args.command == "journal-export":
                    result = metric_journal.export(points, args.out, args.format)
                else:
                    result = metric_journal.verify(points, args.against, args.format)
            else:
                from . import metrics
                points = metrics.native_points(args.path, index=args.index, native_reader=args.native_reader)
                if args.command == "export":
                    result = metrics.export(points, args.out, args.format)
                else:
                    result = metrics.verify(points, args.against, args.format)
        elif args.group == 'jobs':
            from .index_job import load_spec,plan_job,manifest
            from .job_dataset import export_job,verify_job
            plan=plan_job(**load_spec(args.spec),offline_snapshot=args.offline_snapshot)
            if args.command=='plan':result={'selection_sha256':plan.selection_sha256,**manifest(plan)}
            elif args.command=='export':result=export_job(plan,args.out,format=args.format,resume=args.resume)
            else:result=verify_job(plan,args.against)
        elif args.group=='smartstore' and args.command=='acquire':
            if not args.offline_snapshot:raise ValueError('S3 acquisition requires --offline-snapshot before SDK credential lookup')
            from contextlib import closing
            from .s3_acquisition import make_client,acquire
            with closing(make_client(profile=args.profile,region=args.region,endpoint_url=args.endpoint_url,
                                     allow_http=args.allow_http,ca_bundle=args.ca_bundle)) as client:
                result=acquire(client,args.s3_bucket,args.prefix,Path(args.out),offline_snapshot=True,
                               resume=args.resume,version_mode=args.version_mode)
        elif args.group == 'smartstore':
            from .smartstore import AcquiredObjects
            from . import event_bucket
            source=AcquiredObjects(args.path,offline_snapshot=args.offline_snapshot,
                                   expected_inventory_sha256=args.expected_inventory_sha256)
            if args.command=='inspect':result=source.report()
            else:
                if args.command=='export' and Path(args.out).resolve().is_relative_to(Path(args.path).resolve()):
                    raise ValueError('SmartStore output must be outside acquisition source')
                with source.event_bucket(args.bucket_id,experimental=args.experimental,backend=args.backend,visibility=args.visibility) as snapshot:
                    result=event_bucket.export(snapshot,args.out,args.format) if args.command=='export' else event_bucket.verify(snapshot,args.against,args.format)
        elif args.command == 'catalogue':
            from .bucket_catalogue import catalogue, select
            result = catalogue(args.path, offline_snapshot=args.offline_snapshot,
                               expected_sha256=args.expected_catalogue_sha256)
            if args.select:
                result['explicit_selection'] = select(result, args.select, allow_duplicate_journals=args.allow_duplicate_journals)
            elif args.allow_duplicate_journals:
                raise ValueError('--allow-duplicate-journals requires --select')
        elif args.command == "inventory":
            from .buckets import inventory
            result = {"buckets": inventory(args.path, args.include_disabled), "search_visibility": "unverified"}
        elif args.command == "inspect":
            result = journal.inspect(args.path,index_type=args.index_type)
        else:
            journal.validate_index_type(args.index_type)
            source = journal.resolve(args.path)
            if args.command == "export":
                if args.format != "parquet-dataset" and (args.resume or args.partition_rows is not None or args.partition_bytes is not None):
                    raise ValueError("resume/partition options require --format parquet-dataset")
                output = Path(args.out).resolve()
                # Never create files inside an input bucket directory.
                original = Path(args.path).resolve()
                if (source == output or (original.is_dir() and output.is_relative_to(original))
                        or (source.parent.name == "rawdata" and output.is_relative_to(source.parent.parent))):
                    raise ValueError("output must be outside the source archive/bucket")
            rows = journal.events(source, experimental=args.experimental, index=args.index,
                                  bucket_id=args.bucket_id, object_uri=args.object_uri, stats=stats,
                                  visibility=args.visibility, include_disabled=args.include_disabled,index_type=args.index_type,expected_source_sha256=args.expected_source_sha256)
            if args.command == "export":
                if args.format == "parquet-dataset":
                    from .dataset import export_dataset
                    export_dataset(rows, args.out, resume=args.resume,
                                   partition_rows=100_000 if args.partition_rows is None else args.partition_rows,
                                   partition_bytes=64*1024*1024 if args.partition_bytes is None else args.partition_bytes,
                                   stats=stats)
                else:
                    export(rows, args.out, args.format, stats=stats)
                result = stats
            else:
                result = verify(rows, args.against, args.format)
        print(json.dumps(result, sort_keys=True))
        return 0
    except KeyboardInterrupt:
        print(json.dumps({"status": "cancelled"}), file=sys.stderr)
        return 130
    except (OSError, ValueError, RuntimeError, ImportError) as e:
        message=str(e)
        if isinstance(e,ModuleNotFoundError) and e.name in ('pyarrow','zstandard'):
            message=('required dependency '+e.name+' is unavailable; use this environment\'s Python to run '
                     f'python -m pip install /path/to/observability_migration_toolkit-{__version__}-py3-none-any.whl '
                     '(offline: add --no-index --find-links /path/to/wheelhouse), then python -m pip check')
        elif args.group=='jobs' and not isinstance(e,ValueError):message='job filesystem or dependency operation failed ('+type(e).__name__+')'
        elif args.group=='smartstore' and args.command=='acquire':
            from .s3_acquisition import AcquisitionError
            if isinstance(e,ImportError):message='S3 acquisition requires the optional observability-migration-toolkit[s3] extra'
            elif not isinstance(e,AcquisitionError):message='S3 acquisition configuration, filesystem or SDK operation failed ('+type(e).__name__+')'
        print(json.dumps({"status": "failed", "error": message,
                          "errors": 1, "decode_errors": int(journal is not None and isinstance(e, journal.JournalError) and not isinstance(e,journal.UnsupportedIndexType)),
                          "unsupported_index_type":journal is not None and isinstance(e,journal.UnsupportedIndexType)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
