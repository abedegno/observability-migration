"""Experimental physical native event values/postings, distinct from native SPL.

Every value and posting is preflight-validated before any selection. Opaque
routing/deletion arrays remain outside this contract. Address units require a
separate journal/native linkage proof; values retain exact encoded source spans.
"""
from array import array
from bisect import bisect_left
from contextlib import contextmanager
import base64
import hashlib
import json
from pathlib import Path
import os
import struct

import pyarrow as pa
import pyarrow.parquet as pq

from . import event_lexicon,legacy_postings,msidx
from .export import atomic_output

DECODER='obs-event-index-tsidx8-1'
MAX_POSTINGS=5_000_000
SCHEMA=pa.schema([
    pa.field('value_id',pa.uint32(),False),pa.field('indexed_seconds',pa.int64(),False),
    pa.field('address_operand',pa.uint64(),False),pa.field('value_offset',pa.uint64(),False),
    pa.field('value_wire',pa.binary(),False),pa.field('value_bit_offset',pa.uint64(),False),
    pa.field('value_bit_width',pa.uint16(),False),pa.field('value_encoding',pa.string(),False),
    pa.field('source_sha256',pa.string(),False),pa.field('source_bytes',pa.uint64(),False),pa.field('decoder',pa.string(),False),
],metadata={b'obs_migrate.event_index_schema':b'1',b'indexed_search_supported':b'false',b'pruning_supported':b'false'})
PROVENANCE_KEY=b'obs_migrate.event_index_provenance'


class Index:
    def __init__(self,catalog):
        self.catalog=catalog;self.reader=r=catalog._reader
        self.decoder='obs-event-index-tsidx9-1' if getattr(r,'tsidx9',False) else 'obs-event-index-splunk6-1' if getattr(r,'legacy',False) else 'obs-event-index-tsidx7-1' if getattr(r,'tsidx7',False) else DECODER
        if getattr(r,'tsidx9',False) and r.flags==0:self.decoder='obs-event-index-tsidx9-plain-1'
        if getattr(r,'tsidx9',False) and r.flags==19:self.decoder='obs-event-index-tsidx9-inline-1'
        if hasattr(r,'opaque_trailer'):self.decoder=self.decoder.rsplit('-',1)[0]+'-opaque-tail-1'
        self._stamp=msidx._stable(os.fstat(r.data.fd)) if hasattr(r.data,'fd') else None;self.closed=False
        if r.npostings>(r.limits.postings if getattr(r,'limits',None) is not None else MAX_POSTINGS):raise ValueError('event posting preflight cardinality limit exceeded')
        self.seconds=array('I');self.addresses=array('I')
        self.rle_terms=0;self._preflight=False
        for i in range(r.nvalues):
            seconds,address,_,_,_,_=self._value(i)
            if not r.earliest<=seconds<=r.latest or not r.lowest_address<=address<=r.highest_address:
                raise ValueError('event value outside header bounds')
            self.seconds.append(seconds);self.addresses.append(address)
        if (min(self.seconds),max(self.seconds))!=(r.earliest,r.latest) or (min(self.addresses),max(self.addresses))!=(r.lowest_address,r.highest_address):
            raise ValueError('event value/header extrema mismatch')
        if len(set(self.addresses))!=r.nvalues:raise ValueError('duplicate event address operands are unverified')
        for tid in range(r.nterms):
            ids,runs=self._postings(tid)
            if (min(self.seconds[i] for i in ids),max(self.seconds[i] for i in ids))!=r.metadata[tid][1:]:
                raise ValueError('event posting/value time metadata mismatch')
            self.rle_terms+=runs
        self._preflight=True;self._check()

    def _check(self):
        self.catalog._check()
        if self.closed:raise ValueError('event index is closed')
        if self._stamp is not None and msidx._stable(os.fstat(self.reader.data.fd))!=self._stamp:raise ValueError('event index source changed')

    def _value(self,i):
        r=self.reader
        if not 0<=i<r.nvalues:raise ValueError('event value ID outside source')
        if r.flags in (0,1):
            offset=getattr(r,'value_offset',128)+i*8;wire=r.span(offset,8);seconds,address=struct.unpack('>II',wire)
            return seconds,address,offset,wire,offset*8,64
        width=r.time_width+r.address_width;bit=i*width;offset=128+bit//8
        wire=r.span(offset,(bit%8+width+7)//8)
        value=int.from_bytes(wire,'little')>>(bit%8)
        seconds=r.earliest+(value&((1<<r.time_width)-1))
        address=r.lowest_address+((value>>r.time_width)&((1<<r.address_width)-1))
        return seconds,address,offset,wire,128*8+bit,width

    def _postings(self,tid):
        r=self.reader
        if not 0<=tid<r.nterms:raise ValueError('event term ID outside source')
        target=r.reference_ids[tid]
        if target is not None:tid=target
        if r.inline_ids[tid] is not None:return [r.inline_ids[tid]],False
        offset,length=r.postings[tid]
        if r.posting_kinds[tid]=='bitmap':return legacy_postings.bitmap(r.span(offset,length),r.metadata[tid][0],r.nvalues)
        return legacy_postings.decode(r.span(offset,length),r.metadata[tid][0],r.nvalues,label='Splunk6 event' if getattr(r,'legacy',False) else 'TSIDX9' if getattr(r,'tsidx9',False) else 'TSIDX8')

    def _term_id(self,term):
        # Full lexicon preflight proves strict byte ordering and uniqueness.
        # Reuse that ordered storage rather than retaining a second hash map.
        tid=bisect_left(self.reader.terms,term)
        return tid if tid<len(self.reader.terms) and self.reader.terms[tid]==term else None

    def posting_ids(self,term):
        self._check()
        if not isinstance(term,bytes) or not term or len(term)>2047:raise ValueError('event term requires 1..2047 exact bytes')
        tid=self._term_id(term)
        ids=() if tid is None else tuple(self._postings(tid)[0])
        self._check();return ids

    def report(self):
        self._check();r=self.reader
        return {'event_index_schema':1,'decoder':self.decoder,'source_sha256':self.catalog.sha256,'source_bytes':r.size,
            'catalog':self.catalog.report(),'fully_validated_values':r.nvalues,'fully_validated_posting_memberships':r.npostings,
            'rle_terms':self.rle_terms,'packed_value_cache_bytes':self.seconds.itemsize*len(self.seconds)+self.addresses.itemsize*len(self.addresses),
            'postings_decoded':True,'values_decoded':True,'journal_addresses_resolved':False,
            'indexed_search_supported':False,'pruning_supported':False,'search_visibility':'UNVERIFIED',
            'scope':'physical exact lexicon membership and stored second/address operands; no routing/deletion/native search certification'}

    def records(self,term=None):
        self._check();r=self.reader
        ids=range(r.nvalues) if term is None else self.posting_ids(term)
        for i in ids:
            yield self.value(i)
        self._check()

    def value(self,value_id):
        """One preflight-validated physical value, without scanning earlier IDs."""
        self._check();r=self.reader;i=value_id
        if type(i) is not int or not 0<=i<r.nvalues:raise ValueError('event value ID outside source')
        seconds,address,offset,wire,bit,width=self._value(i)
        if (seconds,address)!=(self.seconds[i],self.addresses[i]):raise ValueError('event value changed after preflight')
        return {'value_id':i,'indexed_seconds':seconds,'address_operand':address,'value_offset':offset,'value_wire':wire,
            'value_bit_offset':bit,'value_bit_width':width,'value_encoding':'seconds-address-be32' if r.flags in (0,1) else 'time-address-delta-lsb',
            'source_sha256':self.catalog.sha256,'source_bytes':r.size,'decoder':self.decoder}

    def schema(self,term=None):
        report=self.report()
        if term is not None:
            ids=self.posting_ids(term)
            report.update(term_base64=base64.b64encode(term).decode(),term_id=self._term_id(term),selected_values=len(ids))
        return SCHEMA.with_metadata({**SCHEMA.metadata,PROVENANCE_KEY:event_lexicon._json(report)})

    def batches(self,term=None):
        schema=self.schema(term);pending=[]
        for row in self.records(term):
            pending.append(row)
            if len(pending)==1024:yield pa.RecordBatch.from_pylist(pending,schema=schema);pending=[]
        if pending:yield pa.RecordBatch.from_pylist(pending,schema=schema)


@contextmanager
def open_bytes(data,*,experimental=False,retain_opaque_trailer=False,limits=None):
    """Validate owned immutable source bytes; no mutable-path validation reuse."""
    if not experimental:raise ValueError('event index access is experimental')
    if type(data) is not bytes or not 128<=len(data)<=event_lexicon.MAX_FILE_BYTES:
        raise ValueError('event index requires bounded immutable bytes')
    catalog=event_lexicon.Lexicon(event_lexicon.reader_for(data,retain_opaque_trailer=retain_opaque_trailer,limits=limits),hashlib.sha256(data).hexdigest())
    value=None
    try:
        value=Index(catalog);yield value;value._check()
    finally:
        if value is not None:value.closed=True
        catalog.closed=True


@contextmanager
def open_index(path,*,experimental=False,retain_opaque_trailer=False,limits=None):
    with event_lexicon.open_lexicon(path,experimental=experimental,retain_opaque_trailer=retain_opaque_trailer,limits=limits) as catalog:
        value=None
        try:
            value=Index(catalog);yield value;value._check()
        finally:
            if value is not None:value.closed=True


def export(path,output,format='parquet',*,term=None,experimental=False,retain_opaque_trailer=False,limits=None):
    if format not in ('parquet','arrow'):raise ValueError('event index export requires parquet or arrow')
    if Path(output).resolve().is_relative_to(Path(path).resolve().parent):
        raise ValueError('output must be outside source bucket')
    with atomic_output(output) as file:
        with open_index(path,experimental=experimental,retain_opaque_trailer=retain_opaque_trailer,limits=limits) as index:
            schema=index.schema(term);report=json.loads(schema.metadata[PROVENANCE_KEY])
            if format=='parquet':
                with pq.ParquetWriter(file,schema,compression='zstd',write_statistics=False) as writer:
                    for batch in index.batches(term):writer.write_batch(batch)
            else:
                with pa.ipc.new_file(file,schema) as writer:
                    for batch in index.batches(term):writer.write_batch(batch)
    return report


def verify(path,against,format='parquet',*,term=None,experimental=False,retain_opaque_trailer=False,limits=None):
    from itertools import zip_longest
    if format not in ('parquet','arrow'):raise ValueError('event index verify requires parquet or arrow')
    with open_index(path,experimental=experimental,retain_opaque_trailer=retain_opaque_trailer,limits=limits) as index:
        schema=index.schema(term);count=0
        with Path(against).open('rb') as file:
            before=os.fstat(file.fileno())
            if format=='parquet':
                reader=pq.ParquetFile(file);actual_schema=reader.schema_arrow;batches=reader.iter_batches(batch_size=1024)
            else:
                reader=pa.ipc.open_file(file);actual_schema=reader.schema;batches=(reader.get_batch(i) for i in range(reader.num_record_batches))
            if not actual_schema.equals(schema,check_metadata=True):raise ValueError('event index export schema/provenance mismatch')
            actual=(r for batch in batches for r in batch.to_pylist());sentinel=object()
            for wanted,got in zip_longest(index.records(term),actual,fillvalue=sentinel):
                if wanted!=got:raise ValueError('event index export value mismatch')
                count+=1
            if msidx._stable(before)!=msidx._stable(os.fstat(file.fileno())):raise ValueError('event index export changed')
        report=json.loads(schema.metadata[PROVENANCE_KEY])
    return {**report,'verified_values':count,'comparison':'all value fields and source/query envelope, ordered'}
