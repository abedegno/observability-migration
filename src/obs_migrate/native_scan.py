"""Direct native Arrow streams for DataFusion's optional Rust FFI provider.

SQL pushdown is a necessary-condition optimization; engine residual filters stay
in place. No SQL text rewriting, Parquet buffer, new native parser or live source
cache is used. Streams release the shared capture lock between bounded batches.
"""
import base64
import json
import threading

import pyarrow as pa

from . import metric_bucket,metric_select
from .stored_time import TimeBound,TimeRange


def _value_bound(value):
    """Conservative Arrow buffer budget, without canonical JSON/Base64 encoding.

    All supported scalar widths are <= 8. Offset/validity overhead is covered
    per value, recursively through list and struct children. Actual batch bytes
    are still charged against the scan and session caps before yielding.
    """
    if isinstance(value,bytes):return 16+len(value)
    if isinstance(value,str):return 16+len(value.encode('utf-8'))
    if isinstance(value,dict):return 16+sum(_value_bound(v) for v in value.values())
    if isinstance(value,(list,tuple)):return 16+sum(_value_bound(v) for v in value)
    return 16


def _batches(rows,schema,max_bytes):
    pending=[];size=0
    for row in rows:
        amount=sum(_value_bound(value) for value in row.values())
        if pending and (len(pending)>=256 or size+amount>max_bytes):
            yield pa.RecordBatch.from_pylist(pending,schema=schema)
            pending=[];size=0
        pending.append(row);size+=amount
    if pending:yield pa.RecordBatch.from_pylist(pending,schema=schema)


class NativeScan:
    def __init__(self,capture,predicate,index,bucket_id,max_bytes,reserve):
        self.capture=capture;self.base=capture.select(predicate,index=index,bucket_id=bucket_id)
        self.schema=capture.reader.SCHEMA;self.max_bytes=max_bytes;self.reserve=reserve
        self._events=[];self._event_lock=threading.Lock()
        self._first=None
        for name,plan in self.base._plans.items():
            if plan.point_ids:
                self._first=(name,plan.point_ids[0]);break

    def reset(self):
        with self._event_lock:self._events=[]

    def events(self):
        with self._event_lock:return json.loads(metric_bucket._json(self._events))

    def scan(self,clauses,projection):
        """Generator first evaluated at the execution stream's first poll."""
        self.capture.check();projection=tuple(projection)
        if len(set(projection))!=len(projection) or any(type(i) is not int or not 0<=i<len(self.schema) for i in projection):
            raise ValueError('invalid native Arrow projection')
        fields={k:getattr(next(iter(self.base._plans.values())).predicate,k) for k in metric_select.FIELDS}
        base_predicate=next(iter(self.base._plans.values())).predicate
        contradiction=False;normalized=[];dimensions=set(base_predicate.dimensions)
        time_range=base_predicate.time_range or TimeRange();time_clauses=[]
        for namespace,key,value in clauses:
            key=bytes(key);value=bytes(value)
            if namespace=='field' and key in (b'metric_name',b'host',b'source',b'sourcetype'):
                field=key.decode('ascii')
                if 1+len(key)+2+len(value)>2047:raise ValueError('unsupported native SQL pushdown clause')
                if fields[field] is not None and fields[field]!=value:contradiction=True
                fields[field]=value
                normalized.append({'field':field,'value_base64':base64.b64encode(value).decode()})
            elif namespace=='dimension':
                # Reuse the existing byte-key, length and namespace validation.
                pair=metric_select.Predicate(dimensions=((key,value),)).dimensions[0]
                dimensions.add(pair)
                normalized.append({'namespace':'dimension','key_base64':base64.b64encode(key).decode(),
                                   'value_base64':base64.b64encode(value).decode()})
            elif namespace=='time':
                if key not in (b'eq',b'gt',b'ge',b'lt',b'le') or len(value)>32:
                    raise ValueError('unsupported native time clause')
                try:
                    number=int(value)
                    if str(number).encode()!=value:raise ValueError('noncanonical time integer')
                    bound=TimeBound.from_ticks(number,inclusive=key in (b'eq',b'ge',b'le'))
                except (ValueError,OverflowError) as error:raise ValueError('invalid native time clause') from error
                limit=TimeRange(bound if key in (b'eq',b'ge',b'gt') else None,bound if key in (b'eq',b'le',b'lt') else None)
                time_range=time_range.intersect(limit)
                time_clauses.append({'operator':key.decode(),'ticks_nanoseconds':value.decode(),'field':'indexed_time'})
            else:raise ValueError('unsupported native SQL pushdown clause')
        fallback=False
        if sum(v is not None for v in fields.values())+len(dimensions)+time_range.bound_count>metric_select.MAX_CLAUSES:
            predicate=base_predicate;fallback=True
        else:predicate=metric_select.Predicate(**fields,dimensions=tuple(dimensions),time_range=time_range if time_range.bound_count else base_predicate.time_range)
        selected=self.capture.select(predicate,index=self.base.index,bucket_id=self.base.bucket_id)
        count=0 if contradiction else selected.report()['candidate_points']
        event={'pushed_equalities':normalized,'pushed_time_ranges':time_clauses,
               'candidate_points':count,'contradiction':contradiction,'clause_limit_fallback':fallback,'projection':[self.schema.names[i] for i in projection],
               'emitted_rows':0,'metadata_only_rows':0,'constructed_arrow_bytes':0,'completed':False,
               'row_decoding':'requested_columns_and_predicate_dependencies',
               'residual_filters':'retained_by_DataFusion'}
        with self._event_lock:self._events.append(event)
        projected=pa.schema([self.schema[i] for i in projection],metadata=self.schema.metadata)
        if contradiction:
            event['completed']=True
            return
        # For COUNT(*) the engine requests no columns. Preflight and exact native
        # plans already establish cardinality; no canonical rows need decoding.
        if not projection:
            for start in range(0,count,1024):
                self.capture.check();amount=min(1024,count-start)
                array=pa.array([{}]*amount,type=pa.struct([]))
                batch=pa.RecordBatch.from_struct_array(array).replace_schema_metadata(projected.metadata)
                event['metadata_only_rows']+=amount
                yield batch
            event['completed']=True
            return
        manifest=self.capture.manifest;size=0
        names=tuple(projected.names)
        # The original logical first-row identity is needed only when its
        # per-row provenance is requested. Result-level source proof is always
        # present, including empty and aggregate queries.
        provenance='storage_json' in names
        decode_names=tuple(dict.fromkeys((*names,'point_ordinal'))) if provenance else names
        def annotated():
            sentinel=object()
            for name,plan in selected._plans.items():
                rows=self.capture._sources[name]._emit(plan,self.base.index,self.base.bucket_id,columns=decode_names)
                try:
                    while True:
                        if not self.capture._lock.acquire(blocking=False):raise ValueError('captured bucket scan is active')
                        try:
                            self.capture.check();row=next(rows,sentinel)
                        finally:self.capture._lock.release()
                        if row is sentinel:break
                        # Keep logical table row provenance independent of the
                        # optimizer's scan predicate and batching decisions.
                        if provenance:
                            state=json.loads(row['storage_json'])
                            state.update(bucket_snapshot_sha256=manifest['snapshot_sha256'],slice_file=name,
                                         bucket_query_sha256=self.base.sha256)
                            if (name,row['point_ordinal'])==self._first:state['bucket_snapshot_manifest']=manifest['snapshot_manifest']
                            row['storage_json']=metric_bucket._json(state).decode()
                        if decode_names!=names:row={key:row[key] for key in names}
                        yield row
                finally:rows.close()
        rows=annotated()
        try:
            for batch in _batches(rows,projected,min(self.max_bytes,8*1024**2)):
                size+=batch.nbytes
                if size>self.max_bytes:raise ValueError('native scan Arrow byte limit exceeded')
                self.reserve(batch.nbytes)
                event['emitted_rows']+=batch.num_rows;event['constructed_arrow_bytes']+=batch.nbytes
                yield batch
        finally:rows.close()
        event['completed']=True
