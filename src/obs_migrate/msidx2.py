"""Observed Splunk 7.3 MSIDX2 physical metric scan; no vendor runtime.

Legacy grouped timestamp/value arrays, Float64 width 64 and DSMAGIC0 only.
Generic probe bounds are not metric timestamp bounds. Indexed membership is
unverified; original bucket files remain necessary for source preservation.
"""
from bisect import bisect_left
import base64
import json
import math
import struct

from . import msidx
from .metric_io import MetricRecordIO
from .legacy_msidx import numeric_bytes,padding_state

FORMAT='splunk-msidx2-physical-points'
DECODER='obs-msidx2-1'
EXACT_HOST_DECODER='obs-msidx2-exact-host-1'
SCHEMA=msidx.SCHEMA.with_metadata({**msidx.SCHEMA.metadata,
    b'access':b'physical_msidx2_scan;no_vendor_binary',b'obs_migrate.msidx_profile':b'msidx2'})
_io=MetricRecordIO(SCHEMA,FORMAT,msidx._io.binary_fields,count_field='stored_points',schema_version=2,
                  status={'indexed_time':'decoded_storage','search_visibility':'UNVERIFIED','indexed_search_supported':False})
json_bytes=_io.json_bytes
from_json=_io.from_json
batches=_io.batches
export=_io.export
verify=_io.verify
read_export=_io.read_export


class _Reader(msidx._Reader):
    """Reuse bounded source/lexicon primitives, not the MSIDX4 disk layout."""
    def __init__(self,data):
        self.data=data;self.size=len(data)
        if self.span(0,8)!=b'Msidx_2\0':raise msidx.MsidxError('unsupported MSIDX2 signature')
        self.nvalues=msidx._count(self.u32be(8),'legacy value entries')
        self.nterms=msidx._count(self.u32be(16),'legacy terms')
        self.numeric_bits=self.u32be(40)
        self.numeric_bytes=numeric_bytes(self)
        for at in (44,48,52,56):
            if self.u32be(at)>32:raise msidx.MsidxError('unsupported legacy routing/delete bit width')
        self.numeric_offset=msidx._strict8(64+self.nvalues*8)
        pos=msidx._strict8(self.numeric_offset+self.u32be(20))
        for at in (24,28,32,36):pos=msidx._strict8(pos+self.u32be(at))
        plo=pos;meta=msidx._strict8(plo+self.nterms*8)
        self.lexicon_offset=meta+msidx._strict8(self.nterms*12)
        self.terms,lexend=self.lexicon(self.lexicon_offset)
        posting_bytes=self.u64be(plo+(self.nterms-1)*8)
        self.dimsets_offset=msidx._strict8(lexend+posting_bytes)
        self.dimsets=self.legacy_dimensions(self.dimsets_offset)
        self.groups=[];seen=set();position=0
        while position<self.nvalues:
            count,did=struct.unpack('>II',self.span(64+position*8,8))
            if did>=len(self.dimsets) or did in seen or not 1<=count<=self.nvalues-position-1:
                raise msidx.MsidxError('invalid or repeated legacy dimension group')
            seen.add(did);self.groups.append((position,count,did));position+=count+1
        if len(seen)!=len(self.dimsets):raise msidx.MsidxError('legacy dimension groups do not cover all dimension sets')
        self._exact_routing={did:{} for _,_,did in self.groups}
        groups={position:(count,did) for position,count,did in self.groups}
        for tid,term in enumerate(self.terms):
            role=next((k for k in ('host','source','sourcetype') if term.startswith(b' '+k.encode()+b'::')),None)
            if role is None:continue
            start=0 if tid==0 else self.u64be(plo+(tid-1)*8);end=self.u64be(plo+tid*8)
            if not 0<=start<end<=posting_bytes:raise msidx.MsidxError('invalid legacy exact routing posting span')
            position=lexend+start;bound=lexend+end;previous=0;first=True;weight=0
            expected=self.u32be(meta+tid*12)
            value=term[len(role)+3:]
            provenance={'term_id':tid,'term_wire_base64':base64.b64encode(term).decode(),
                'posting_offset':lexend+start,'posting_bytes':end-start,
                'metadata_offset':meta+tid*12,'weighted_points':expected}
            while position<bound:
                before=position;delta,position=self.varint(position,bound)
                if (position-before>1 and self.data[position-1]==0) or (not first and delta==0):
                    raise msidx.MsidxError('unverified legacy routing posting integer/run')
                point=previous+delta
                if point not in groups:raise msidx.MsidxError('legacy routing posting does not address a group preamble')
                count,did=groups[point]
                if role in self._exact_routing[did]:raise msidx.MsidxError('ambiguous legacy exact routing group')
                self._exact_routing[did][role]=(value,provenance)
                weight+=count;previous=point;first=False
            if weight!=expected:raise msidx.MsidxError('legacy routing weighted posting cardinality mismatch')
        self.exact_host_namespace=False
        for did in self._exact_routing:self.metadata(did)


    def legacy_dimensions(self,offset):
        if self.span(offset,8)!=b'DSMAGIC0':raise msidx.MsidxError('unsupported legacy dimension signature')
        count=msidx._count(self.u32le(offset+8),'legacy dimension sets')
        words=msidx._count(self.u32le(offset+12),'legacy dimension words')
        base=offset+16+count*4
        if base+words*4!=self.size:raise msidx.MsidxError('legacy dimension area does not end at source EOF')
        result=[];last=0
        for i in range(count):
            end=self.u32le(offset+16+i*4)
            if not last+5<=end<=words or end-last>4096:raise msidx.MsidxError('invalid legacy dimension word range')
            pos=base+last*4
            row=struct.unpack('<'+str(end-last)+'I',self.span(pos,(end-last)*4))
            if row[0]!=len(row):raise msidx.MsidxError('legacy dimension word count mismatch')
            if any(tid>=self.nterms for tid in row[1:] if tid!=0xffffffff):
                raise msidx.MsidxError('legacy dimension term ID out of bounds')
            result.append((pos,row));last=end
        if last!=words:raise msidx.MsidxError('unreferenced legacy dimension words')
        return result

    def term(self,tid,prefix=None):
        if tid>=len(self.terms):raise msidx.MsidxError('legacy term ID out of bounds')
        term=self.terms[tid]
        # The legacy dimension dictionary uses unprefixed terms. Space-prefixed
        # search/metadata terms are a separate namespace, never interchangeable.
        if term.startswith(b' '):raise msidx.MsidxError('unsupported legacy dimension term namespace')
        if prefix is None:return term
        if not term.startswith(prefix):raise msidx.MsidxError('legacy dimension/routing role mismatch')
        return term[len(prefix):]

    def metadata(self,did):
        dimpos,ds=self.dimsets[did]
        _,metric,h,source,sourcetype,*ids=ds
        name=self.term(metric,b'metric_name::')
        if not name:raise msidx.MsidxError('empty legacy metric name')
        routing={key:None if tid==0xffffffff else self.term(tid,key.encode()+b'::')
                 for key,tid in (('host',h),('source',source),('sourcetype',sourcetype))}
        for key,value in tuple(routing.items()):
            exact=self._exact_routing[did].get(key)
            if value is None:
                if exact is not None:raise msidx.MsidxError('unexpected exact routing for NULL dimension field')
                continue
            if exact is None:raise msidx.MsidxError('incomplete legacy exact routing coverage')
            if exact[0]!=value:
                if key!='host' or exact[0].lower()!=value:
                    raise msidx.MsidxError('unverified legacy routing normalization')
                self.exact_host_namespace=True
            routing[key]=exact[0]
        dims=[]
        for tid in ids:
            term=self.term(tid)
            if b'::' not in term:raise msidx.MsidxError('legacy dimension lacks key separator')
            key,value=term.split(b'::',1)
            if not key:raise msidx.MsidxError('empty legacy dimension key')
            dims.append({'key':key,'value':value})
        return dimpos,ds,name,routing,dims

    def rows(self,sha,index,bucket_id,*,dimension_ids=None,columns=None,point_ids=None):
        wanted=set(SCHEMA.names) if columns is None else set(columns)
        if wanted-set(SCHEMA.names):raise ValueError("unknown metric projection column")
        provenance="storage_json" in wanted
        need_time=bool(wanted & {"indexed_time","timestamp_wire","storage_json"})
        need_value="measurements" in wanted
        ordinal=0
        for preamble,count,did in self.groups:
            if dimension_ids is not None and did not in dimension_ids:
                ordinal+=count
                continue
            dimpos,ds,name,routing,dims=self.metadata(did)
            group_start=ordinal;group_end=ordinal+count
            local_ids=range(count) if point_ids is None else (p-group_start for p in point_ids[bisect_left(point_ids,group_start):bisect_left(point_ids,group_end)])
            for local in local_ids:
                ordinal=group_start+local
                value_index=preamble+1+local;offset=64+value_index*8
                timestamp=None;seconds=None
                if need_time:
                    timestamp=self.span(offset,8);seconds,operand=struct.unpack('>II',timestamp)
                    if operand!=did or seconds>2147483647:
                        raise msidx.MsidxError('unverified legacy timestamp/dimension operand')
                value_offset=self.numeric_offset+value_index*8;bits=value=None
                if need_value:
                    bits=self.span(value_offset,8);value=struct.unpack('<d',bits)[0]
                    if not math.isfinite(value):raise msidx.MsidxError('legacy nonfinite values not yet verified')
                state=None
                if provenance:
                    state={'header_wire_base64':base64.b64encode(self.span(0,60)).decode(),
                           'lexicon_offset':self.lexicon_offset,'dimension_sets_offset':self.dimsets_offset,
                           'dimension_set_offset':dimpos,'preamble_offset':64+preamble*8,
                           'numeric_array_offset':self.numeric_offset,'numeric_width':self.numeric_bits,
                           'measurements':[{'term_id':ds[1],'offset':value_offset,'encoding':'float64-le'}],
                           'timestamp_encoding':'seconds-dimension-be','timestamp_bit_offset':0,'timestamp_bit_width':64,
                           'visibility':'physical;search_membership_unverified','source_archive_required':True,
                           'generic_header_time_bounds':'NOT_METRIC_BOUNDS;not_used'}
                    state.update(padding_state(self))
                    if self.exact_host_namespace:state['exact_routing_postings']={key:item[1] for key,item in self._exact_routing[did].items()}
                row={'schema_version':2,'source_format':FORMAT,'indexed_time':{'seconds':seconds,'fraction':'0'},
                       'journal_time':None,'index_time':None,**routing,
                       'measurements':[{'name':name,'value_bits_le':bits,'value_float64':value}],
                       'dimensions':dims,'raw':None,'timestamp_wire':timestamp,
                       'dimension_set_wire':self.span(dimpos,len(ds)*4) if 'dimension_set_wire' in wanted else None,
                       'preamble_wire':self.span(64+preamble*8,8) if 'preamble_wire' in wanted else None,
                       'storage_json':json.dumps(state,sort_keys=True,separators=(',',':')) if provenance else None,
                       'index':index,'bucket_id':bucket_id,'source_sha256':sha,'source_bytes':self.size,
                       'point_ordinal':ordinal,'dimension_set_id':did,'tsidx_value_index':value_index,
                       'timestamp_offset':offset,'decoder':EXACT_HOST_DECODER if self.exact_host_namespace else DECODER}
                yield row if columns is None else {key:row[key] for key in columns}
            ordinal=group_end


def records(path,*,experimental=False,index=None,bucket_id=None):
    if not experimental:raise msidx.MsidxError('MSIDX2 decoder is experimental; pass --experimental')
    with msidx._source(path) as (data,sha):
        yield from _Reader(data).rows(sha,index,bucket_id)
