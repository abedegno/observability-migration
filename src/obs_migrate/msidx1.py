"""Observed Splunk 7.0 Float64 metric arrays and complete metadata postings.

The shared Splunk6 magic alone does not identify metrics. This reader requires
width 64 and verifies metric/catalog/dimension coverage. metric_select adds
exact physical candidate selection; native search membership stays unverified.
"""
import base64
import json
import math
import re
import struct

import pyarrow as pa

from . import msidx
from .metric_io import MetricRecordIO
from .legacy_msidx import numeric_bytes,padding_state

FORMAT='splunk-msidx1-physical-points'
DECODER='obs-msidx1-1'
RLE_DECODER='obs-msidx1-rle-1'
EXACT_HOST_DECODER='obs-msidx1-exact-host-1'
# Legacy files have neither dimension-set records nor group preambles. NULL is
# deliberate; never synthesize bytes/IDs to imitate a later storage layout.
SCHEMA=pa.schema([pa.field(f.name,f.type,True) if f.name in
    ('dimension_set_wire','dimension_set_id','preamble_wire') else f for f in msidx.SCHEMA],
    metadata={**msidx.SCHEMA.metadata,b'access':b'physical_msidx1_scan;no_vendor_binary',
              b'obs_migrate.msidx_profile':b'msidx1'})
_io=MetricRecordIO(SCHEMA,FORMAT,msidx._io.binary_fields,count_field='stored_points',schema_version=2,
    status={'indexed_time':'decoded_storage','search_visibility':'UNVERIFIED','indexed_search_supported':False})
json_bytes=_io.json_bytes
from_json=_io.from_json
batches=_io.batches
export=_io.export
verify=_io.verify
read_export=_io.read_export


class _Reader(msidx._Reader):
    """Share positional/endian/lexicon primitives, not MSIDX4 layout parsing."""
    def __init__(self,data):
        self.data=data;self.size=len(data)
        if self.span(0,8)!=b'Splunk6\0':raise msidx.MsidxError('unsupported MSIDX1 signature')
        self.nvalues=msidx._count(self.u32be(8),'MSIDX1 values')
        self.nterms=msidx._count(self.u32be(16),'MSIDX1 terms')
        self.npostings=msidx._count(self.u32be(12),'MSIDX1 postings')
        self.numeric_bytes=numeric_bytes(self)
        self.numeric_offset=msidx._strict8(64+self.nvalues*8)
        pos=msidx._strict8(self.numeric_offset+self.u32be(20));self.arrays={}
        for size_at,width_at,key in ((24,44,'source'),(28,48,'sourcetype'),(32,52,'host'),(36,56,'delete')):
            size=self.u32be(size_at);width=self.u32be(width_at)
            if not 0<=width<=31 or size!=(self.nvalues*width+7)//8:
                raise msidx.MsidxError('unverified MSIDX1 routing/delete array profile')
            self.arrays[key]=(pos,width,self.span(pos,size));pos=msidx._strict8(pos+size)
        self.plo=pos;self.meta=msidx._strict8(pos+self.nterms*8)
        self.lexicon_offset=self.meta+msidx._strict8(self.nterms*12)
        self.terms,self.posting_offset=self.lexicon(self.lexicon_offset)
        self.point_terms=[[] for _ in range(self.nvalues)];self.postings=[]
        last=0;total=0;self.has_runs=False
        for tid in range(self.nterms):
            end=self.u64be(self.plo+tid*8)
            if end<=last or end>self.size-self.posting_offset:
                raise msidx.MsidxError('invalid MSIDX1 posting offsets')
            expected=msidx._count(self.u32be(self.meta+tid*12),'MSIDX1 term posting count')
            total+=expected
            if total>self.npostings:raise msidx.MsidxError('MSIDX1 total posting count mismatch')
            wire=self.span(self.posting_offset+last,end-last)
            ids=self.posting_ids(wire,expected)
            # Preserve all namespaces. Only the explicit field namespace is
            # projected below; internal/subsecond terms retain their own role.
            for point in ids:self.point_terms[point].append(tid)
            self.postings.append((self.posting_offset+last,end-last));last=end
        self.exact_host_namespace=False
        for tids in self.point_terms:
            normal=[self.terms[t][6:] for t in tids if self.terms[t].startswith(b'host::')]
            exact=[self.terms[t][7:] for t in tids if self.terms[t].startswith(b' host::')]
            if normal!=exact:self.exact_host_namespace=True
        if total!=self.npostings or self.posting_offset+last!=self.size:
            raise msidx.MsidxError('MSIDX1 posting count/EOF mismatch')

    def posting_ids(self,wire,expected):
        from .legacy_postings import decode
        ids,runs=decode(wire,expected,self.nvalues,label='MSIDX1',error_type=msidx.MsidxError)
        self.has_runs=self.has_runs or runs
        return ids

    def array_value(self,key,point):
        _,width,data=self.arrays[key]
        return sum(((data[(point*width+bit)//8]>>((point*width+bit)%8))&1)<<bit for bit in range(width))

    def rows(self,sha,index,bucket_id,*,point_ids=None,columns=None):
        wanted=set(SCHEMA.names) if columns is None else set(columns)
        if wanted-set(SCHEMA.names):raise ValueError("unknown metric projection column")
        provenance="storage_json" in wanted
        need_time=bool(wanted & {"indexed_time","timestamp_wire","storage_json"})
        need_value="measurements" in wanted
        points=range(self.nvalues) if point_ids is None else point_ids
        for point in points:
            tids=self.point_terms[point]
            plain={};spaced={};internal={};refs=[];declared_dims=[]
            for tid in tids:
                term=self.terms[tid]
                namespace=spaced if term.startswith(b' ') else internal if term.startswith(b'_') else plain
                field=term[1:] if namespace is spaced else term
                if b'::' not in field:raise msidx.MsidxError('unverified MSIDX1 non-field term')
                key,value=field.split(b'::',1)
                if key==b'_dims' and namespace is internal:
                    declared_dims.append(value)
                else:
                    if not key or key in namespace:raise msidx.MsidxError('ambiguous MSIDX1 field membership')
                    namespace[key]=value
                if provenance:
                    offset,length=self.postings[tid]
                    refs.append({'term_id':tid,'term_wire_base64':base64.b64encode(term).decode(),
                        'posting_offset':offset,'posting_bytes':length,'metadata_offset':self.meta+tid*12})
            if plain!=spaced:
                # Observed native 7.0 host search terms merge ASCII case. Exact
                # space-prefixed metadata and their postings retain original
                # bytes. Other namespace differences remain unsupported.
                if (set(plain)!=set(spaced) or any(plain[k]!=spaced[k] for k in plain if k!=b'host') or
                    b'host' not in plain or plain[b'host']!=spaced[b'host'].lower()):
                    raise msidx.MsidxError('unverified MSIDX1 field namespace coverage')
                plain=dict(spaced)
            required=(b'metric_name',b'host',b'source',b'sourcetype')
            if any(key not in plain for key in required) or not plain[b'metric_name']:
                raise msidx.MsidxError('incomplete MSIDX1 metric/routing metadata')
            dims=[{'key':key,'value':value} for key,value in plain.items() if key not in required]
            # Catalog is a per-point schema declaration, not a heuristic term hit.
            catalog=internal.get(b'_catalog',b'').split(b'|')
            if catalog[0]!=plain[b'metric_name'] or catalog[1:]!=sorted(d['key'] for d in dims):
                raise msidx.MsidxError('unverified MSIDX1 metric catalog/dimension coverage')
            if sorted(declared_dims)!=sorted(d['key'] for d in dims):
                raise msidx.MsidxError('MSIDX1 dimension declaration mismatch')
            if set(internal)-{b'_catalog',b'_subsecond'}:
                raise msidx.MsidxError('unverified MSIDX1 internal term role')
            fraction=internal.get(b'_subsecond',b'0')
            if fraction!=b'0':
                if not re.fullmatch(rb'\.[0-9]{1,9}',fraction):raise msidx.MsidxError('unverified MSIDX1 subsecond term')
                fraction=b'0'+fraction
            offset=64+point*8;timestamp=None;seconds=address=None
            if need_time:
                timestamp=self.span(offset,8);seconds,address=struct.unpack('>II',timestamp)
                if seconds>2147483647:raise msidx.MsidxError('unverified MSIDX1 timestamp seconds')
            value_offset=self.numeric_offset+point*8;bits=value=None
            if need_value:
                bits=self.span(value_offset,8);value=struct.unpack('<d',bits)[0]
                if not math.isfinite(value):raise msidx.MsidxError('MSIDX1 nonfinite values not yet verified')
            state=None
            if provenance:
                state={'header_wire_base64':base64.b64encode(self.span(0,60)).decode(),
                    'numeric_array_offset':self.numeric_offset,'measurements':[{'offset':value_offset,'encoding':'float64-le'}],
                    'lexicon_offset':self.lexicon_offset,'field_postings':refs,
                    'timestamp_encoding':'seconds-journal-address-be;subsecond-term',
                    'journal_address_operand':address,'dimension_order':'storage_lexicon',
                    'routing_delete_arrays':{key:{'offset':a[0],'width':a[1],'value':self.array_value(key,point)} for key,a in self.arrays.items()},
                    'visibility':'physical;search_membership_unverified','source_archive_required':True}
                state.update(padding_state(self))
                if self.exact_host_namespace:state['host_namespace']='exact_space_prefixed_metadata;plain_search_host_ASCII_casefolded'
            row={'schema_version':2,'source_format':FORMAT,'indexed_time':{'seconds':seconds,'fraction':fraction.decode('ascii')},
                'journal_time':None,'index_time':None,**{key:plain[key.encode()] for key in ('host','source','sourcetype')},
                'measurements':[{'name':plain[b'metric_name'],'value_bits_le':bits,'value_float64':value}],
                'dimensions':dims,'raw':None,'timestamp_wire':timestamp,'dimension_set_wire':None,'preamble_wire':None,
                'storage_json':json.dumps(state,sort_keys=True,separators=(',',':')) if provenance else None,
                'index':index,'bucket_id':bucket_id,'source_sha256':sha,'source_bytes':self.size,'point_ordinal':point,
                'dimension_set_id':None,'tsidx_value_index':point,'timestamp_offset':offset,'decoder':EXACT_HOST_DECODER if self.exact_host_namespace else RLE_DECODER if self.has_runs else DECODER}
            yield row if columns is None else {key:row[key] for key in columns}


def records(path,*,experimental=False,index=None,bucket_id=None):
    if not experimental:raise msidx.MsidxError('MSIDX1 decoder is experimental; pass --experimental')
    with msidx._source(path) as (data,sha):yield from _Reader(data).rows(sha,index,bucket_id)
