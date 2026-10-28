"""Lazy native event Arrow streams for the existing generic Rust FFI bridge."""
import base64
import json
import threading

import pyarrow as pa
from . import event_bucket,metric_bucket
from .native_scan import _batches


class NativeEventScan:
    def __init__(self,snapshot,term,max_bytes,reserve,*,routing_pushdown=False,time_pushdown=False):
        self.snapshot=snapshot;self.term=term;self.max_bytes=max_bytes;self.reserve=reserve
        if snapshot.backend!='journal':raise ValueError('native event provider requires journal backend')
        self._report=snapshot.describe_selection(term)
        self.routing_index=None
        if routing_pushdown:
            from .event_routing import RoutingIndex
            self.routing_index=RoutingIndex(snapshot)
            self._report['derived_routing_index']=self.routing_index.report()
        self.time_index=None
        if time_pushdown:
            from .event_time import EventTimeIndex
            self.time_index=EventTimeIndex(snapshot)
            self._report['derived_time_index']=self.time_index.report()
            self._report['event_time_clauses_revision']=1
        logical={'event_query_schema':1,'snapshot_sha256':snapshot.snapshot_sha256,
            'physical_selection':{k:self._report['query'][k] for k in ('term_base64','semantics','selected_events','order')},
            'labels':self._report['labels']}
        import hashlib
        self._report['source_query']=logical;self._report['query_sha256']=hashlib.sha256(metric_bucket._json(logical)).hexdigest()
        # Keep per-source reports in the session/result envelope. Relational
        # schemas must agree across buckets for DataFusion UNION ALL.
        self.schema=event_bucket.SCHEMA
        self._events=[];self._event_lock=threading.Lock()

    def report(self):self.snapshot._check();return json.loads(metric_bucket._json(self._report))
    def reset(self):
        with self._event_lock:self._events=[]
    def events(self):
        with self._event_lock:return json.loads(metric_bucket._json(self._events))

    def scan(self,clauses,projection):
        source=self.snapshot;source._check();projection=tuple(projection)
        if len(set(projection))!=len(projection) or any(type(i) is not int or not 0<=i<len(self.schema) for i in projection):
            raise ValueError('invalid native event Arrow projection')
        if not source._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
        try:ids=source._selection(self.term)
        finally:source._lock.release()
        base_count=self._report['query']['selected_events'];applied=[]
        if self.routing_index is not None:
            candidate=None if ids is None else set(ids)
            for namespace,key,value in clauses:
                key=bytes(key);value=bytes(value)
                if namespace=='field' and key in (b'host',b'source',b'sourcetype'):
                    match=set(self.routing_index.candidates(key.decode('ascii'),value))
                    candidate=match if candidate is None else candidate&match
                    applied.append({'field':key.decode('ascii'),'value_base64':base64.b64encode(value).decode()})
            if applied:ids=sorted(candidate)
        time_ranges={};time_clauses=[];time_fallback=False;precision_fallback=[]
        if self.time_index is not None:
            from .stored_time import TimeBound,TimeRange
            for namespace,key,value in clauses:
                key=bytes(key);value=bytes(value)
                field=namespace.removesuffix(':decimal9')
                if field not in ('event_time','index_time') or key not in (b'eq',b'gt',b'ge',b'lt',b'le'):continue
                if namespace.endswith(':decimal9') and self.time_index.report()['maximum_fraction_digits'][field]>9:
                    precision_fallback.append(field);continue
                try:
                    if len(value)>32:continue
                    number=int(value);bound=TimeBound.from_ticks(number,inclusive=key in (b'eq',b'ge',b'le'))
                except (ValueError,UnicodeError):continue
                part=TimeRange(bound if key in (b'eq',b'gt',b'ge') else None,
                    bound if key in (b'eq',b'lt',b'le') else None)
                time_ranges[field]=time_ranges.get(field,TimeRange()).intersect(part)
                time_clauses.append({'field':field,'expression':'decimal9' if namespace.endswith(':decimal9') else 'whole_seconds','operator':key.decode(),'ticks_nanoseconds':str(number)})
            if len(time_clauses)>32:time_ranges={};time_fallback=True
            candidate=None if ids is None else set(ids)
            for field,bound in time_ranges.items():
                match=set(self.time_index.candidates(field,bound))
                candidate=match if candidate is None else candidate&match
            if time_ranges:ids=sorted(candidate)
        count=base_count if ids is None else len(ids)
        event={'candidate_events':count,'received_sql_clauses':[{'namespace':namespace,
            'key_base64':base64.b64encode(bytes(key)).decode(),'value_base64':base64.b64encode(bytes(value)).decode()}
            for namespace,key,value in clauses],
            'sql_pruning_applied':False,'residual_filters':'retained_by_DataFusion;event_clauses_not_used_for_pruning',
            'projection':[self.schema.names[i] for i in projection],'emitted_rows':0,'metadata_only_rows':0,
            'native_records_decoded':0,'native_record_bytes_read':0,'constructed_arrow_bytes':0,'completed':False}
        if self.routing_index is not None:event.update(base_candidate_events=base_count,routing_equalities=applied,
            sql_pruning_applied=bool(applied),residual_filters='retained_by_DataFusion;derived_exact_routing_is_necessary_condition')
        if self.time_index is not None:event.update(base_candidate_events=base_count,
            pushed_time_ranges=time_clauses,time_clause_limit_fallback=time_fallback,
            time_precision_residual_fields=sorted(set(precision_fallback)),
            sql_pruning_applied=bool(applied or time_ranges),
            residual_filters='retained_by_DataFusion;derived_exact_candidates_are_necessary_conditions')
        with self._event_lock:self._events.append(event)
        schema=pa.schema([self.schema[i] for i in projection],metadata=self.schema.metadata)
        if not projection:
            for start in range(0,count,1024):
                source._check();amount=min(1024,count-start)
                batch=pa.RecordBatch.from_struct_array(pa.array([{}]*amount,type=pa.struct([]))).replace_schema_metadata(schema.metadata)
                event['metadata_only_rows']+=amount;event['emitted_rows']+=amount;yield batch
            event['completed']=True;return
        work={};cached=source._journal.cache_projections
        native=source._projected_native_records(ids,tuple(schema.names),work) if cached else source._native_records(ids,work)
        sentinel=object();size=0
        def rows():
            try:
                while True:
                    if not source._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
                    try:source._check();value=next(native,sentinel)
                    finally:source._lock.release()
                    event.update(work)
                    if value is sentinel:break
                    row=value if cached else value[0]
                    yield {name:row[name] for name in schema.names}
            finally:native.close()
        selected=rows()
        try:
            for batch in _batches(selected,schema,min(self.max_bytes,8*1024**2)):
                size+=batch.nbytes
                if size>self.max_bytes:raise ValueError('native event scan Arrow byte limit exceeded')
                self.reserve(batch.nbytes);event['emitted_rows']+=batch.num_rows
                event['constructed_arrow_bytes']+=batch.nbytes;yield batch
        finally:selected.close()
        event['completed']=True
