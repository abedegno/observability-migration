"""Experimental exact byte predicates over validated physical metric profiles.

MSIDX1 intersects real metadata postings; MSIDX2/4 intersect dimension-set
memberships. A full preflight scan remains mandatory: no speedup, search
visibility or native SPL equivalence is implied.
"""
from array import array
from bisect import bisect_left,bisect_right
import base64
from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path

from . import msidx,msidx1,msidx2,metric_storage
from .stored_time import TimeRange,ticks

FIELDS=('metric_name','host','source','sourcetype')
MAX_CLAUSES=32


@dataclass(frozen=True)
class Predicate:
    metric_name:bytes|None=None
    host:bytes|None=None
    source:bytes|None=None
    sourcetype:bytes|None=None
    dimensions:tuple=()
    time_range:TimeRange|None=None

    def __post_init__(self):
        pairs=self.dimensions
        if not isinstance(pairs,(tuple,list)) or any(not isinstance(p,(tuple,list)) or len(p)!=2 for p in pairs):
            raise ValueError('dimensions must be byte key/value pairs')
        for key,value in pairs:
            if not isinstance(key,bytes) or not key or b'::' in key or not isinstance(value,bytes):
                raise ValueError('dimension predicates require nonempty byte keys without :: and byte values')
            if 1+len(key)+2+len(value)>2047:raise ValueError('predicate term exceeds supported byte bound')
        object.__setattr__(self,'dimensions',tuple(sorted(set(tuple(p) for p in pairs))))
        for key in FIELDS:
            value=getattr(self,key)
            if value is not None and (not isinstance(value,bytes) or 1+len(key)+2+len(value)>2047):
                raise ValueError('field predicates require bounded byte values')
        if self.time_range is not None and not isinstance(self.time_range,TimeRange):raise ValueError('time_range requires TimeRange')
        if len(self.clauses())+(0 if self.time_range is None else self.time_range.bound_count)>MAX_CLAUSES:raise ValueError('too many exact predicate clauses')

    def clauses(self):
        return tuple(('field',key.encode(),getattr(self,key)) for key in FIELDS if getattr(self,key) is not None)+tuple(
            ('dimension',key,value) for key,value in self.dimensions)

    def matches(self,row):
        if self.time_range is not None and not self.time_range.contains(ticks(**row['indexed_time'])):return False
        if self.metric_name is not None and not any(m['name']==self.metric_name for m in row['measurements']):return False
        if any(getattr(self,key) is not None and row[key]!=getattr(self,key) for key in FIELDS[1:]):return False
        if not self.dimensions:return True
        present={(d['key'],d['value']) for d in row['dimensions']}
        return all(pair in present for pair in self.dimensions)

    def describe(self):
        encode=lambda value:base64.b64encode(value).decode('ascii')
        result={'operator':'AND','comparison':'exact_bytes;case_sensitive;no_normalization',
            'clauses':[{'namespace':namespace,'key_base64':encode(key),'value_base64':encode(value)}
                       for namespace,key,value in self.clauses()]}
        if self.time_range is not None:result['indexed_time_range']=self.time_range.describe()
        return result


@dataclass(frozen=True)
class Plan:
    predicate:Predicate
    point_ids:tuple
    dimension_ids:frozenset|None
    source_sha256:str


class _Source:
    def __init__(self,data,sha,module):
        self.data=data;self.sha256=sha;self.module=module;self.closed=False
        self.stamp=None if type(data) is bytes else msidx._stable(os.fstat(data.fd))
        self.reader=module._Reader(data)
        self._posting_cache={};self._cached_members=0;self._posting_cache_limit=1_000_000
        # Validate *all* physical rows before any plan or empty result. Unsupported
        # data cannot disappear merely because a predicate excludes its points.
        self.total=0;self.minimum_seconds=None;self.maximum_seconds=None
        # Only immutable captures retain a packed time index: 8-byte ticks plus
        # 4-byte ordinals per point, bounded by existing capture point caps.
        point_times=array('q') if self.stamp is None else None
        for row in self.reader.rows(sha,None,None):
            self.total+=1;seconds=row['indexed_time']['seconds']
            if point_times is not None:point_times.append(ticks(**row['indexed_time']))
            self.minimum_seconds=seconds if self.minimum_seconds is None else min(self.minimum_seconds,seconds)
            self.maximum_seconds=seconds if self.maximum_seconds is None else max(self.maximum_seconds,seconds)
        self._time_ticks=self._time_ordinals=None
        if point_times is not None:
            order=sorted(range(self.total),key=point_times.__getitem__)
            self._time_ticks=array('q',(point_times[i] for i in order));self._time_ordinals=array('I',order)
        self.check();self.lookup={}
        if module is msidx1:
            for tid,term in enumerate(self.reader.terms):
                # Search (unprefixed) terms may normalize host case; exact
                # byte predicates must use the validated metadata namespace.
                if not term.startswith(b' ') or term.startswith(b' _') or b'::' not in term:continue
                key,value=term[1:].split(b'::',1)
                namespace='field' if key in tuple(k.encode() for k in FIELDS) else 'dimension'
                token=(namespace,key,value)
                if token in self.lookup:raise ValueError('duplicate legacy field lexicon terms are unverified')
                self.lookup[token]=tid
            self.strategy='msidx1_metadata_posting_intersection'
        else:
            for did in range(len(self.reader.dimsets)):
                _,_,name,routing,dims=self.reader.metadata(did)
                names=name if module is msidx else (name,)
                tokens=[('field',b'metric_name',value) for value in names]+[('field',k.encode(),v) for k,v in routing.items() if v is not None]
                tokens += [('dimension',d['key'],d['value']) for d in dims]
                for token in tokens:self.lookup.setdefault(token,set()).add(did)
            self.strategy='msidx4_dimension_set_intersection' if module is msidx else 'msidx2_dimension_set_intersection'
        self.check()

    def check(self):
        if self.closed:raise ValueError('metric selection source is closed')
        if self.stamp is not None and msidx._stable(os.fstat(self.data.fd))!=self.stamp:raise ValueError('metric selection source changed')

    def plan(self,predicate):
        if not isinstance(predicate,Predicate):raise ValueError('selection requires an exact Predicate')
        self.check();clauses=predicate.clauses();chosen=None
        for clause in clauses:
            member=self.lookup.get(clause)
            if member is None:ids=set()
            elif self.module is msidx1:
                ids=self._posting_cache.get(member)
                if ids is None:
                    offset,length=self.reader.postings[member]
                    expected=self.reader.u32be(self.reader.meta+member*12)
                    ids=frozenset(self.reader.posting_ids(self.reader.span(offset,length),expected))
                    # Only immutable byte sources may reuse postings. Bound total
                    # cached memberships, rather than caching arbitrary queries.
                    if self.stamp is None and self._cached_members+len(ids)<=self._posting_cache_limit:
                        self._posting_cache[member]=ids;self._cached_members+=len(ids)
            else:ids=member
            chosen=set(ids) if chosen is None else chosen.intersection(ids)
        if self.module is msidx1:
            points=tuple(range(self.total)) if chosen is None else tuple(sorted(chosen));groups=None
        else:
            groups=frozenset(range(len(self.reader.dimsets)) if chosen is None else chosen)
            points=[];ordinal=0
            for _,count,did in self.reader.groups:
                if did in groups:points.extend(range(ordinal,ordinal+count))
                ordinal+=count
            points=tuple(points)
        if predicate.time_range is not None:
            bound=predicate.time_range
            if self._time_ticks is not None:
                lower=bound.lower;upper=bound.upper
                start=0 if lower is None else (bisect_left if lower.inclusive else bisect_right)(self._time_ticks,lower.ticks)
                stop=len(self._time_ticks) if upper is None else (bisect_right if upper.inclusive else bisect_left)(self._time_ticks,upper.ticks)
                allowed=set(self._time_ordinals[start:max(start,stop)])
                points=tuple(i for i in points if i in allowed)
            else:
                # File-backed sources retain no time cache; stable-source checks
                # bracket a fresh projected read of candidate timestamps.
                options={'point_ids':points} if self.module is msidx1 else {'dimension_ids':groups,'point_ids':points}
                points=tuple(row['point_ordinal'] for row in self.reader.rows(self.sha256,None,None,columns=('indexed_time','point_ordinal'),**options)
                             if bound.contains(ticks(**row['indexed_time'])))
            if self.module in (msidx2,msidx):
                present=set();ordinal=0
                for _,count,did in self.reader.groups:
                    at=bisect_left(points,ordinal)
                    if at<len(points) and points[at]<ordinal+count:present.add(did)
                    ordinal+=count
                groups=frozenset(present)
        self.check()
        return Plan(predicate,points,groups,self.sha256)

    def explain(self,predicate):
        return self._report(self.plan(predicate))

    def _report(self,plan):
        report={'source_sha256':self.sha256,'source_bytes':len(self.data),'source_format':self.module.FORMAT,
            'predicate':plan.predicate.describe(),'candidate_strategy':self.strategy,
            'fully_validated_points':self.total,'candidate_points':len(plan.point_ids),
            'candidate_dimension_sets':len(plan.dimension_ids) if plan.dimension_ids is not None else None,
            'full_preflight_scan':True,'performance_claim':False,'search_visibility':'UNVERIFIED',
            'indexed_search_supported':False,'physical_predicate_selection':True}
        if plan.predicate.time_range is not None:
            report['time_candidate_strategy']='immutable_preflight_nanosecond_index' if self._time_ticks is not None else 'fresh_candidate_timestamp_scan'
            report['time_index_bytes']=0 if self._time_ticks is None else len(self._time_ticks)*self._time_ticks.itemsize+len(self._time_ordinals)*self._time_ordinals.itemsize
        return report

    def _emit(self,plan,index=None,bucket_id=None,*,columns=None):
        self.check();count=0
        if plan.source_sha256!=self.sha256:raise ValueError('candidate plan belongs to another source')
        options={'point_ids':plan.point_ids} if self.module is msidx1 else {'dimension_ids':plan.dimension_ids,**({'point_ids':plan.point_ids} if plan.predicate.time_range is not None else {})}
        if columns is not None:
            required=set(columns)
            required.update(key for key in FIELDS[1:] if getattr(plan.predicate,key) is not None)
            if plan.predicate.metric_name is not None:required.add('measurements')
            if plan.predicate.dimensions:required.add('dimensions')
            if plan.predicate.time_range is not None:required.add('indexed_time')
            options['columns']=tuple(required)
        for row in self.reader.rows(self.sha256,index,bucket_id,**options):
            if not plan.predicate.matches(row):raise ValueError('candidate predicate residual mismatch')
            count+=1;yield row if columns is None else {key:row[key] for key in columns}
        if count!=len(plan.point_ids):raise ValueError('candidate cardinality mismatch')
        self.check()

    def records(self,predicate,*,index=None,bucket_id=None):
        yield from self._emit(self.plan(predicate),index,bucket_id)


    def batches(self,predicate,*,index=None,bucket_id=None,max_rows=1024,max_bytes=8*1024*1024):
        yield from self.module.batches(self.records(predicate,index=index,bucket_id=bucket_id),max_rows,max_bytes)


@contextmanager
def open_source(path,*,experimental=False):
    if not experimental:raise ValueError('metric selection is experimental; pass --experimental')
    module=metric_storage.reader_for(path)
    if module not in (msidx1,msidx2,msidx):raise ValueError('exact selection supports verified MSIDX1/MSIDX2/MSIDX4 profiles only')
    with msidx._source(path) as (data,sha):
        source=_Source(data,sha,module)
        try:yield source
        finally:
            try:source.check()
            finally:source.closed=True


def records(path,predicate,*,experimental=False,index=None,bucket_id=None):
    with open_source(path,experimental=experimental) as source:
        yield from source.records(predicate,index=index,bucket_id=bucket_id)


def batches(path,predicate,*,experimental=False,index=None,bucket_id=None,max_rows=1024,max_bytes=8*1024*1024):
    with open_source(path,experimental=experimental) as source:
        yield from source.batches(predicate,index=index,bucket_id=bucket_id,max_rows=max_rows,max_bytes=max_bytes)


def explain(path,predicate,*,experimental=False):
    with open_source(path,experimental=experimental) as source:return source.explain(predicate)


def _transport(path,predicate,target,format,experimental,index,bucket_id,verify):
    module=metric_storage.reader_for(path);report={}
    if module not in (msidx1,msidx2,msidx):raise ValueError('exact selection supports verified MSIDX1/MSIDX2/MSIDX4 profiles only')
    if not verify and Path(target).resolve().is_relative_to(Path(path).resolve().parent):
        raise ValueError('output must be outside the source archive/bucket')
    def selected():
        with open_source(path,experimental=experimental) as source:
            plan=source.plan(predicate);report.update(source._report(plan))
            yield from source._emit(plan,index,bucket_id)
    rows=selected()
    result=module.verify(rows,target,format) if verify else module.export(rows,target,format)
    return {**result,**report}


def export(path,predicate,output,format='parquet',*,experimental=False,index=None,bucket_id=None):
    return _transport(path,predicate,output,format,experimental,index,bucket_id,False)


def verify(path,predicate,against,format='parquet',*,experimental=False,index=None,bucket_id=None):
    return _transport(path,predicate,against,format,experimental,index,bucket_id,True)
