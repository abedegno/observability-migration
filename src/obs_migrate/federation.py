"""Bounded, eager DataFusion MemTables over verified offline metric snapshots.

Exact source predicates are explicit. Captured buckets support a lazy dataset
bridge and an optional direct FFI stream with exact routing, metric, dimension and stored-time SQL pushdown.
SQL residuals remain in DataFusion; no live membership/native search claim.
Importing this module does not require the optional DataFusion dependency.
"""
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import re
import threading
import weakref
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq

from . import bucket_select, metric_bucket, metric_select
from .export import atomic_output

PROVENANCE_KEY=b'obs_migrate.sql_provenance'
DEFAULT_MAX_BYTES=256*1024**2


def _copy(value):
    return json.loads(metric_bucket._json(value))


def _digest(table):
    # No self-referential provenance metadata; one canonical chunk/batch layout.
    table=table.replace_schema_metadata(None).combine_chunks()
    sink=pa.BufferOutputStream()
    with pa.ipc.new_stream(sink,table.schema) as writer:writer.write_table(table,max_chunksize=1024)
    return hashlib.sha256(sink.getvalue()).hexdigest()


@dataclass(frozen=True)
class SQLResult:
    """Immutable Arrow result; complete provenance is in schema metadata, even empty."""
    table:pa.Table
    _source_roots:tuple=field(default=(),repr=False)

    @property
    def manifest(self):
        return json.loads(self.table.schema.metadata[PROVENANCE_KEY])

    def write(self,output,format='parquet'):
        if format not in ('parquet','arrow'):raise ValueError('SQL result format must be parquet or arrow')
        # Avoid placing derived output in any bound source snapshot.
        target=Path(output).resolve()
        for root in self._source_roots:
            if target.is_relative_to(Path(root)):raise ValueError('output must be outside source buckets')
        with atomic_output(output) as stream:
            if format=='parquet':pq.write_table(self.table,stream,compression='zstd')
            else:
                with pa.ipc.new_file(stream,self.table.schema) as writer:writer.write_table(self.table)
        return {'output_bytes':Path(output).stat().st_size,'rows':self.table.num_rows,
                'query_sha256':self.manifest['query_sha256'],'result_sha256':self.manifest['result_sha256']}


class BucketSession:
    """Read-only SQL session, one or more eager snapshot-bound MemTable providers.

    Materialization and query results each have an Arrow buffer byte bound, not
    a process RSS bound. The engine has a separate reservation pool and disk spill is disabled; these
    are not total process memory guarantees. Sources
    are rehashed before/after SQL; any change aborts result publication. Tables
    may have different verified legacy profiles. Joins/UNION ALL are SQL engine
    operations, never evidence of native Splunk search equivalence.
    """
    def __init__(self,*,max_input_bytes=DEFAULT_MAX_BYTES,max_result_bytes=DEFAULT_MAX_BYTES,max_engine_bytes=DEFAULT_MAX_BYTES):
        if (type(max_input_bytes) is not int or type(max_result_bytes) is not int or type(max_engine_bytes) is not int or
            min(max_input_bytes,max_result_bytes,max_engine_bytes)<1):raise ValueError('Arrow byte limits must be positive integers')
        try:
            import datafusion
        except ImportError as error:
            raise ImportError('DataFusion is optional; install obs-migrate[federation]') from error
        self._df=datafusion
        runtime=datafusion.RuntimeEnvBuilder().with_greedy_memory_pool(max_engine_bytes).with_disk_manager_disabled()
        self._context=datafusion.SessionContext(datafusion.SessionConfig().with_target_partitions(1),runtime)
        self._sources={};self._bytes=0;self._lock=threading.Lock()
        self._capture_bytes=0;self._capture_budget_lock=threading.Lock();self._native_query_bytes=0
        self.max_input_bytes=max_input_bytes;self.max_result_bytes=max_result_bytes

    def register(self,name,path,predicate=None,**options):
        """Validate all files, apply explicit exact predicate, register a MemTable.

        Requires experimental=True and offline_snapshot=True. Nothing is
        registered until complete source exhaustion and validation succeed.
        """
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',name):
            raise ValueError('table names require lowercase ASCII identifiers, at most 63 characters')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if name in self._sources:raise ValueError('table already registered')
            if len(self._sources)>=32:raise ValueError('session table limit exceeded')
            started=perf_counter()
            selection=bucket_select.Selection(path,metric_select.Predicate() if predicate is None else predicate,**options)
            batches=[];size=0;stream=selection.batches()
            try:
                for batch in stream:
                    size+=batch.nbytes
                    if self._bytes+self._capture_bytes+size>self.max_input_bytes:raise ValueError('input Arrow byte limit exceeded')
                    batches.append(batch)
            finally:stream.close()
            report=selection.report()
            selection.snapshot.finish()
            if not batches:
                batches=[pa.RecordBatch.from_pylist([],schema=selection.reader.SCHEMA)]
            self._context.register_record_batches(name,[batches])
            entry={'selection':selection,'report':_copy(report),'arrow_bytes':size,
                   'check':selection.snapshot.finish,'roots':(selection.snapshot.root,),'provider':'eager_MemTable',
                   'prepare_seconds':perf_counter()-started,'rows':report['candidate_points']}
            self._sources[name]=entry;self._bytes+=size
            return self.inventory()[name]
        finally:self._lock.release()

    def register_metric_snapshot(self,name,path,*,experimental=False,offline_snapshot=False,
                                 include_inactive=False,index=None,bucket_id=None,expected_snapshot_sha256=None):
        """Eager complete physical metric scan, including supported MSIDX4 profiles.

        Reuse existing inventory/record readers; no native source predicate or
        posting pruning is performed. All SQL filtering stays in the engine.
        Sources are checked throughout preparation and before/after each query.
        """
        from .source_identity import normalize_sha256,require_sha256
        expected_snapshot_sha256=normalize_sha256(expected_snapshot_sha256)
        if not experimental:raise ValueError('metric snapshot registration is experimental')
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',name):
            raise ValueError('table names require lowercase ASCII identifiers, at most 63 characters')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if name in self._sources:raise ValueError('table already registered')
            if len(self._sources)>=32:raise ValueError('session table limit exceeded')
            started=perf_counter()
            snapshot=metric_bucket.Snapshot(path,offline_snapshot=offline_snapshot,include_inactive=include_inactive)
            require_sha256(snapshot.sha256,expected_snapshot_sha256,'metric snapshot')
            batches=[];size=rows=0
            records=snapshot.records(experimental=True,index=index,bucket_id=bucket_id)
            stream=snapshot.reader.batches(records)
            try:
                for batch in stream:
                    size+=batch.nbytes;rows+=batch.num_rows
                    if self._bytes+self._capture_bytes+size>self.max_input_bytes:
                        raise ValueError('input Arrow byte limit exceeded')
                    batches.append(batch)
            finally:
                stream.close();records.close()
            if not rows:raise ValueError('empty metric snapshot is unverified')
            snapshot.finish()
            query={'metric_snapshot_query_schema':1,'snapshot_sha256':snapshot.sha256,
                   'source_format':snapshot.reader.FORMAT,'selection':'all_physical_points;complete_scan;no_source_predicates',
                   'labels':{'index':index,'bucket_id':bucket_id}}
            report={'query':query,'query_sha256':hashlib.sha256(metric_bucket._json(query)).hexdigest(),
                    'snapshot_manifest':snapshot.inventory(),'source_format':snapshot.reader.FORMAT,
                    'physical_points':rows,'full_preflight_scan':True,'sql_filter_pushdown':False,
                    'scan_strategy':'eager_full_physical_scan;SQL_residual_filters',
                    'search_visibility':'UNVERIFIED','indexed_search_supported':False}
            self._context.register_record_batches(name,[batches])
            self._sources[name]={'report':report,'arrow_bytes':size,'rows':rows,
                'prepare_seconds':perf_counter()-started,'check':snapshot.finish,
                'roots':(snapshot.root,),'provider':'full_scan_metric_MemTable'}
            self._bytes+=size
            return self.inventory()[name]
        finally:self._lock.release()

    def register_capture(self,name,capture,predicate=None,*,index=None,bucket_id=None):
        """Register a lazy DatasetTable over an already validated immutable capture.

        Reuses native metadata and posting indexes. On first provider access,
        only explicit predicate candidates are encoded to bounded Parquet bytes;
        subsequent SQL scans reuse those bytes. SQL filters remain residuals.
        """
        from .capture import CapturedBucket
        from .capture_dataset import dataset
        if not isinstance(capture,CapturedBucket):raise ValueError('register_capture requires a CapturedBucket')
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',name):
            raise ValueError('table names require lowercase ASCII identifiers, at most 63 characters')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if name in self._sources:raise ValueError('table already registered')
            if len(self._sources)>=32:raise ValueError('session table limit exceeded')
            started=perf_counter();selection=capture.select(predicate,index=index,bucket_id=bucket_id)
            owner=weakref.ref(self)
            def reserve(size):
                session=owner()
                if session is None:raise ValueError('capture dataset session is no longer available')
                with session._capture_budget_lock:
                    if session._bytes+session._capture_bytes+size>session.max_input_bytes:
                        raise ValueError('total capture dataset byte limit exceeded')
                    session._capture_bytes+=size
            value,handler=dataset(selection,self.max_input_bytes,reserve)
            self._context.register_dataset(name,value)
            report=selection.report()
            self._sources[name]={'selection':selection,'report':report,'arrow_bytes':0,
                'prepare_seconds':perf_counter()-started,'rows':report['candidate_points'],
                'check':capture.check,'roots':(),'provider':'lazy_capture_DatasetTable','handler':handler,
                'capture':{'capture_sha256':capture.sha256,'capture_bytes':capture.size,
                           'validation':'full_preflight_once_per_open;pinned_immutable_native_bytes',
                           'origin_recheck':False,'validation_reused':True}}
            return self.inventory()[name]
        finally:self._lock.release()

    def register_event_snapshot(self,name,snapshot,*,term=None):
        """Register immutable linked raw events in the existing bounded session.

        An optional exact-byte term is an explicit physical source selection.
        SQL filters remain engine residuals; no tokenization/SPL translation or
        automatic SQL-to-event-posting pushdown is performed. The snapshot must
        stay open; source paths are never reopened by this registration/query.
        """
        from .event_bucket import EventBucket,PROVENANCE_KEY as EVENT_PROVENANCE
        if not isinstance(snapshot,EventBucket):raise ValueError('event registration requires an EventBucket')
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',name):
            raise ValueError('table names require lowercase ASCII identifiers, at most 63 characters')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if name in self._sources:raise ValueError('table already registered')
            if len(self._sources)>=32:raise ValueError('session table limit exceeded')
            started=perf_counter();table=snapshot.table(term);size=table.nbytes
            if self._bytes+self._capture_bytes+size>self.max_input_bytes:raise ValueError('input Arrow byte limit exceeded')
            report=json.loads(table.schema.metadata[EVENT_PROVENANCE])
            query={'event_query_schema':1,'snapshot_sha256':snapshot.snapshot_sha256,
                'physical_selection':{key:report['query'][key] for key in
                    ('term_base64','semantics','selected_events','order')},'labels':report['labels']}
            report['query_sha256']=hashlib.sha256(metric_bucket._json(query)).hexdigest()
            report['source_query']=query
            # Per-source envelopes belong in session/result provenance, not the
            # relational schema: differing metadata breaks DataFusion UNION ALL.
            from .event_bucket import SCHEMA as EVENT_SCHEMA
            table=table.replace_schema_metadata(EVENT_SCHEMA.metadata)
            batches=table.to_batches(max_chunksize=1024)
            if not batches:batches=[pa.RecordBatch.from_pylist([],schema=table.schema)]
            snapshot._check()
            self._context.register_record_batches(name,[batches])
            self._sources[name]={'report':report,'arrow_bytes':size,'rows':table.num_rows,
                'prepare_seconds':perf_counter()-started,'check':snapshot._check,
                'roots':(snapshot._origin_root,),'provider':'immutable_event_MemTable'}
            self._bytes+=size
            return self.inventory()[name]
        finally:self._lock.release()

    def register_native_capture(self,name,capture,predicate=None,*,index=None,bucket_id=None):
        """Direct streaming FFI provider; exact routing, metric, dimension and stored-time SQL pushdown.

        The optional original Rust extension handles DataFusion interfaces only;
        existing immutable native readers supply bounded Arrow batches. Every
        pushed predicate remains an engine residual (Inexact contract).
        """
        from .capture import CapturedBucket
        from .native_scan import NativeScan
        if not isinstance(capture,CapturedBucket):raise ValueError('native registration requires a CapturedBucket')
        if self._df.__version__!='55.0.0':raise ValueError('native provider requires tested DataFusion Python 55.0.0')
        try:import _native_provider as bridge
        except ImportError as error:raise ImportError('build the optional Rust bridge: python scripts/build_native_provider.py') from error
        if (bridge.BRIDGE_REVISION,bridge.DATAFUSION_RUST_VERSION,bridge.ARROW_RUST_VERSION)!=('obs-native-ffi-3','55.2.0','59.3.0'):
            raise ValueError('incompatible native provider build')
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',name):
            raise ValueError('table names require lowercase ASCII identifiers, at most 63 characters')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if name in self._sources:raise ValueError('table already registered')
            if len(self._sources)>=32:raise ValueError('session table limit exceeded')
            started=perf_counter()
            owner=weakref.ref(self)
            def reserve(size):
                session=owner()
                if session is None:raise ValueError('native scan session is no longer available')
                with session._capture_budget_lock:
                    if session._bytes+session._capture_bytes+session._native_query_bytes+size>session.max_input_bytes:
                        raise ValueError('native query Arrow byte limit exceeded')
                    session._native_query_bytes+=size
            backend=NativeScan(capture,predicate,index,bucket_id,self.max_input_bytes,reserve)
            provider=bridge.NativeProvider(backend.schema,backend)
            self._context.register_table(name,provider)
            report=backend.base.report()
            self._sources[name]={'selection':backend.base,'report':report,'arrow_bytes':0,
                'prepare_seconds':perf_counter()-started,'rows':report['candidate_points'],'check':capture.check,
                'roots':(),'provider':'direct_native_FFI','native':backend,
                'capture':{'capture_sha256':capture.sha256,'capture_bytes':capture.size,
                    'validation':'full_preflight_once_per_open;pinned_immutable_native_bytes',
                    'origin_recheck':False,'validation_reused':True,'native_bridge_revision':bridge.BRIDGE_REVISION,
                    'datafusion_rust':bridge.DATAFUSION_RUST_VERSION,'arrow_rust':bridge.ARROW_RUST_VERSION}}
            return self.inventory()[name]
        finally:self._lock.release()

    def register_native_event_snapshot(self,name,snapshot,*,term=None,routing_pushdown=False,time_pushdown=False):
        """Stream addressed native records without an eager source MemTable.

        Uses the existing optional generic bridge. SQL residuals stay active;
        optional routing candidates use exact binary necessary equalities only.
        """
        from .event_bucket import EventBucket
        from .native_event_scan import NativeEventScan
        if type(routing_pushdown) is not bool or type(time_pushdown) is not bool:raise ValueError('event pushdown options must be bool')
        if not isinstance(snapshot,EventBucket) or snapshot.backend!='journal':
            raise ValueError('native event registration requires an EventBucket with journal backend')
        if self._df.__version__!='55.0.0':raise ValueError('native provider requires tested DataFusion Python 55.0.0')
        try:import _native_provider as bridge
        except ImportError as error:raise ImportError('build the optional Rust bridge: python scripts/build_native_provider.py') from error
        if (bridge.BRIDGE_REVISION,bridge.DATAFUSION_RUST_VERSION,bridge.ARROW_RUST_VERSION)!=('obs-native-ffi-3','55.2.0','59.3.0'):
            raise ValueError('incompatible native provider build')
        if time_pushdown and getattr(bridge,'EVENT_TIME_CLAUSES_REVISION',None)!=1:
            raise ValueError('event time pushdown requires rebuilt bridge with EVENT_TIME_CLAUSES_REVISION=1')
        if not isinstance(name,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',name):
            raise ValueError('table names require lowercase ASCII identifiers, at most 63 characters')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if name in self._sources:raise ValueError('table already registered')
            if len(self._sources)>=32:raise ValueError('session table limit exceeded')
            started=perf_counter();owner=weakref.ref(self)
            def reserve(size):
                session=owner()
                if session is None:raise ValueError('native event session is no longer available')
                with session._capture_budget_lock:
                    if session._bytes+session._capture_bytes+session._native_query_bytes+size>session.max_input_bytes:
                        raise ValueError('native query Arrow byte limit exceeded')
                    session._native_query_bytes+=size
            backend=NativeEventScan(snapshot,term,self.max_input_bytes,reserve,routing_pushdown=routing_pushdown,time_pushdown=time_pushdown)
            self._context.register_table(name,bridge.NativeProvider(backend.schema,backend))
            report=backend.report()
            self._sources[name]={'report':report,'arrow_bytes':0,'rows':report['query']['selected_events'],
                'prepare_seconds':perf_counter()-started,'check':snapshot._check,'roots':(snapshot._origin_root,),
                'provider':'direct_native_event_FFI'+('_time' if time_pushdown else '')+('_routing' if routing_pushdown else ''),
                'native':backend,'sql_pushdown':routing_pushdown or time_pushdown}
            return self.inventory()[name]
        finally:self._lock.release()

    def inventory(self):
        return {name:{'selection':_copy(entry['report']),'arrow_bytes':entry['handler'].arrow_bytes if 'handler' in entry else entry['arrow_bytes'],
                      'prepare_seconds':entry['prepare_seconds'],'rows':entry['rows']}
                for name,entry in self._sources.items()}

    def sql(self,sql):
        """Execute read-only SQL; retain SQL filters as engine residuals.

        Optional native tables also push verified necessary byte equalities into
        native candidate selection using the Inexact pushdown contract.

        Every registered table is conservatively included in result provenance,
        even if the optimizer never reads it. No parameters or text rewriting:
        callers can use SQL literals (binary values via decode(hex, 'hex')).
        """
        if not isinstance(sql,str) or not sql.strip() or len(sql.encode())>1024*1024:
            raise ValueError('SQL must be nonempty text of at most 1 MiB')
        if not self._lock.acquire(blocking=False):raise ValueError('session operation is already active')
        try:
            if not self._sources:raise ValueError('register at least one offline bucket first')
            for entry in self._sources.values():entry['check']()
            self._native_query_bytes=0
            for entry in self._sources.values():
                if 'native' in entry:entry['native'].reset()
            options=self._df.SQLOptions().with_allow_ddl(False).with_allow_dml(False).with_allow_statements(False)
            started=perf_counter()
            try:
                frame=self._context.sql(sql,options=options)
                schema=frame.schema();batches=[];size=0
                stream=frame.execute_stream();failure=None
                try:
                    for wrapped in stream:
                        batch=wrapped.to_pyarrow();size+=batch.nbytes
                        if size>self.max_result_bytes:
                            failure=ValueError('result Arrow byte limit exceeded');break
                        batches.append(batch)
                except BaseException as error:
                    failure=error
                # DataFusion 55's Python wrapper owns the native stream in rbs.
                # Iterator tracebacks can retain that wrapper after cancellation;
                # detach its native owner before propagating, preserving tracebacks.
                # Destruction may close Python generators, so do it outside unwind.
                stream.rbs=None
                del stream
                if failure is not None:raise failure
                table=pa.Table.from_batches(batches,schema=schema)
            except Exception as error:raise ValueError('DataFusion SQL failed: '+str(error)) from error
            elapsed=perf_counter()-started
            for entry in self._sources.values():entry['check']()
            query={'sql':sql,'engine':'Apache DataFusion','engine_version':self._df.__version__,
                   'tables':{name:entry['report']['query_sha256'] for name,entry in sorted(self._sources.items())},
                   'provider':','.join(sorted({entry['provider'] for entry in self._sources.values()})),'sql_filter_pushdown':any('native' in entry and entry.get('sql_pushdown',True) for entry in self._sources.values())}
            captures={name:entry['capture'] for name,entry in sorted(self._sources.items()) if 'capture' in entry}
            if captures:query['captures']=captures
            manifest={'sql_provenance_schema':1,'query':query,
                'query_sha256':hashlib.sha256(metric_bucket._json(query)).hexdigest(),
                'sources':{name:_copy(entry['report']) for name,entry in sorted(self._sources.items())},
                'rows':table.num_rows,'arrow_bytes':size,'result_sha256':_digest(table),
                'timings':{'sql_plan_and_execute_seconds':elapsed,
                           'source_prepare_seconds':{name:entry['prepare_seconds'] for name,entry in self._sources.items()}},
                'full_preflight_scan':True,'performance_claim':False,
                'search_visibility':'UNVERIFIED','indexed_search_supported':False,
                'provenance_scope':'all_registered_tables;SQL may project or aggregate away row provenance'}
            if captures:
                manifest['capture_validation']=captures
                manifest['capture_datasets']={name:{'materializations':entry['handler'].materializations,
                    'arrow_bytes':entry['handler'].arrow_bytes} for name,entry in self._sources.items() if 'handler' in entry}
            native={name:entry['native'].events() for name,entry in self._sources.items() if 'native' in entry}
            if native:manifest['native_scans']=native
            encoded=metric_bucket._json(manifest)
            if len(encoded)>8*1024**2:raise ValueError('SQL provenance byte limit exceeded')
            metadata=dict(table.schema.metadata or {});metadata[PROVENANCE_KEY]=encoded
            return SQLResult(table.replace_schema_metadata(metadata),tuple(root for entry in self._sources.values() for root in entry['roots']))
        finally:self._lock.release()
