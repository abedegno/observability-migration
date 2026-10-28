"""Strict experimental native event catalogs, never search/pruning.

Reuse verified endian/varint/prefix-block primitives. Values, routing arrays and
stored posting bodies are opaque; inline singleton and shared-term IDs are
explicit. Retain the original file for source fidelity.
"""
import base64
from array import array
import hashlib
from contextlib import contextmanager
import json
from itertools import pairwise
import os
from pathlib import Path
import stat
import struct
from .term_metadata import TermMetadata
from .posting_spans import PostingSpans,MISSING

import pyarrow as pa
import pyarrow.parquet as pq

from . import msidx,event_limits
from .export import atomic_output

FORMAT='splunk-tsidx8-event-lexicon'
DECODER='obs-event-lexicon-tsidx8-1'
FLAGS3_DECODER='obs-event-lexicon-tsidx8-3-1'
MAX_FILE_BYTES=256*1024**2
MAX_OPAQUE_TRAILER_BYTES=16*1024**2
SCHEMA=pa.schema([
    pa.field('term_id',pa.uint32(),False),pa.field('term',pa.binary(),False),
    pa.field('posting_count',pa.uint32(),False),
    pa.field('metadata_earliest_seconds',pa.int64(),False),pa.field('metadata_latest_seconds',pa.int64(),False),
    pa.field('posting_offset',pa.uint64(),False),pa.field('posting_bytes',pa.uint64(),False),
    pa.field('metadata_bit_offset',pa.uint64(),False),pa.field('metadata_bit_width',pa.uint16(),False),
    pa.field('lexicon_block_offset',pa.uint64(),False),pa.field('lexicon_block_bytes',pa.uint32(),False),
    pa.field('term_in_block',pa.uint16(),False),
    pa.field('source_sha256',pa.string(),False),pa.field('source_bytes',pa.uint64(),False),
    pa.field('decoder',pa.string(),False),
],metadata={b'obs_migrate.event_lexicon_schema':b'1',b'indexed_search_supported':b'false',b'pruning_supported':b'false'})
SCHEMA_V2=pa.schema([pa.field(f.name,f.type,True) if f.name in ('posting_offset','posting_bytes') else f for f in SCHEMA]+[
    pa.field('posting_kind',pa.string(),False),pa.field('inline_value_id',pa.uint32()),
    pa.field('directory_start_operand_offset',pa.uint64()),pa.field('directory_end_operand_offset',pa.uint64()),
    pa.field('directory_start_operand_wire',pa.binary(8)),pa.field('directory_end_operand_wire',pa.binary(8)),
],metadata={**SCHEMA.metadata,b'obs_migrate.event_lexicon_schema':b'2'})
SCHEMA_V3=pa.schema([pa.field(f.name,pa.binary(4),f.nullable) if f.name in
    ('directory_start_operand_wire','directory_end_operand_wire') else f for f in SCHEMA_V2]+[
    pa.field('referenced_term_id',pa.uint32()),
],metadata={**SCHEMA.metadata,b'obs_migrate.event_lexicon_schema':b'3'})
PROVENANCE_KEY=b'obs_migrate.event_lexicon_provenance'


def _json(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def _align8(value):return (value+7)//8*8


class _Reader(msidx._Reader):
    def _configure_limits(self,limits):
        self.limits=event_limits.validate(limits)
        if limits is not None:self.max_lexicon_bytes=limits.lexicon_bytes

    def _count(self,value,label,kind):
        if self.limits is None:return msidx._count(value,label)
        if not 1<=value<=getattr(self.limits,kind):raise ValueError(label+' outside explicit event resource bounds')
        return value

    def __init__(self,data,*,retain_opaque_trailer=False,limits=None):
        if type(retain_opaque_trailer) is not bool:raise ValueError('opaque trailer policy requires a boolean')
        self._configure_limits(limits)
        self.retain_opaque_trailer=retain_opaque_trailer
        self.data=data;self.size=len(data)
        magic=self.span(0,8);self.tsidx9=magic==b'_TSIDX9\0';self.tsidx7=magic==b'_TSIDX7\0'
        if magic not in (b'_TSIDX7\0',b'_TSIDX8\0',b'_TSIDX9\0'):raise ValueError('unsupported event lexicon signature')
        self.value_offset=88 if self.tsidx7 else 128
        self.header=self.span(0,self.value_offset)
        self.flags=self.u64be(72)
        if self.flags not in ((0,19,27) if self.tsidx9 else (0,1) if self.tsidx7 else (0,3)) or self.span(68,4)!=bytes(4) or (self.span(80,8)!=bytes(8) if self.tsidx7 else self.span(88,40)!=bytes(40)):
            raise ValueError('unsupported event TSIDX flags or header extension')
        self.nvalues=self._count(self.u32be(8),'event values','values')
        self.npostings=self.u32be(12);self.nterms=self._count(self.u32be(16),'event terms','terms')
        if not self.nterms<=self.npostings<=self.nvalues*self.nterms:raise ValueError('invalid event posting count')
        self.earliest,self.latest=self.u32be(60),self.u32be(64)
        if not 0<=self.earliest<=self.latest<=2147483647:raise ValueError('unsupported event timestamp bounds')
        if self.tsidx7:
            addresses=[self.u32be(self.value_offset+i*8+4) for i in range(self.nvalues)]
            self.lowest_address,self.highest_address=min(addresses),max(addresses)
        else:self.lowest_address,self.highest_address=self.u32be(80),self.u32be(84)
        if self.lowest_address>self.highest_address:raise ValueError('invalid event address bounds')
        self.address_width=(self.highest_address-self.lowest_address).bit_length()
        self.time_width=(self.latest-self.earliest).bit_length()
        value_bytes=self.nvalues*8 if self.flags in (0,1) else ((self.nvalues*(self.address_width+self.time_width)+63)//64)*8
        self.sections={'header':{'offset':0,'bytes':self.value_offset},'values':{'offset':self.value_offset,'bytes':value_bytes,'decoded':False}}
        if self.flags not in (0,1):self.sections['values'].update(time_delta_bits=self.time_width,address_delta_bits=self.address_width)
        pos=msidx._strict8(self.value_offset+value_bytes)
        self.sections['values']['allocated_bytes']=pos-self.value_offset
        self.span(self.value_offset,pos-self.value_offset)
        for size_at,width_at,name in ((20,40,'event_time'),(24,44,'source'),(28,48,'sourcetype'),(32,52,'host'),(36,56,'delete')):
            size=self.u32be(size_at);width=self.u32be(width_at)
            if not 1<=width<=32 or size!=(self.nvalues*width+7)//8:raise ValueError('unsupported event array width/size')
            self.span(pos,size);self.sections[name]={'offset':pos,'bytes':size,'allocated_bytes':msidx._strict8(pos+size)-pos,'bits_per_value':width,'decoded':False}
            pos=msidx._strict8(pos+size)
        self.directory_width=4 if self.tsidx9 and self.flags in (19,27) else 8
        self.plo=pos;self.sections['posting_offsets']={'offset':pos,'bytes':self.nterms*self.directory_width}
        self.meta=msidx._strict8(pos+self.nterms*self.directory_width)
        self.sections['posting_offsets']['allocated_bytes']=self.meta-pos
        self.count_width=self.nvalues.bit_length();self.time_width=(self.latest-self.earliest).bit_length()
        self.meta_width=self.count_width+2*self.time_width
        self.meta_size=((self.nterms*self.meta_width+63)//64)*8
        self.lexicon_offset=self.meta+self.meta_size
        self.span(self.meta,self.meta_size)
        self.sections['term_metadata']={'offset':self.meta,'bytes':self.meta_size,'bits_per_term':self.meta_width}
        self._finish_lexicon()

    def _finish_lexicon(self):
        # This is the existing complete prefix-block decoder, not an ASCII scan.
        self.terms,self.posting_base=self.lexicon(self.lexicon_offset)
        if not all(self.terms) or any(left>=right for left,right in pairwise(self.terms)):
            raise ValueError('unverified event lexicon ordering/duplicate terms')
        self.sections['lexicon']={'offset':self.lexicon_offset,'bytes':self.posting_base-self.lexicon_offset}
        _,self.block_size,self.uncompressed=struct.unpack('>III',self.span(self.lexicon_offset,12))
        nblocks=(self.nterms+self.block_size-1)//self.block_size
        ends=[self.u32be(self.lexicon_offset+12+i*4) for i in range(nblocks)]
        base=self.lexicon_offset+12+nblocks*4;self.blocks=[];previous=0
        for end in ends:self.blocks.append((base+previous,end-previous));previous=end
        width=getattr(self,'directory_width',8);typed=getattr(self,'tsidx9',False) and self.flags==27
        raw=array('I' if width==4 else 'Q',
            (self.u32be(self.plo+tid*width) if width==4 else self.u64be(self.plo+tid*width) for tid in range(self.nterms)))
        if raw.itemsize!=width:raise RuntimeError('event directory requires matching unsigned array width')
        kind=lambda value:value&3 if typed else 2 if self.flags in (1,3,19) and value&1 else 0
        shift=2 if typed else 1 if self.flags in (1,3,19) else 0
        self.raw_directory=raw;self.postings=PostingSpans();self.inline_ids=[];self.reference_ids=[];self.posting_kinds=[];self.directory_end_ids=[];self.metadata=TermMetadata();last=0;total=0
        # A tagged word describes the *following* term. For an external list,
        # its end is the next untagged directory word, skipping inline terms.
        next_stored=array('I',[MISSING])*self.nterms;next_id=MISSING
        for tid in range(self.nterms-1,-1,-1):
            if kind(raw[tid])<2:next_id=tid
            next_stored[tid]=next_id
        if next_stored[-1]==MISSING:raise ValueError('missing final event posting boundary')
        for tid in range(self.nterms):
            if getattr(self,'legacy',False):
                count,earliest,latest=struct.unpack('>III',self.span(self.meta+tid*12,12))
            else:
                bit=tid*self.meta_width;start=bit//8;length=(bit%8+self.meta_width+7)//8
                packed=int.from_bytes(self.span(self.meta+start,length),'little')>>(bit%8)
                count=packed&((1<<self.count_width)-1)
                lo=(packed>>self.count_width)&((1<<self.time_width)-1)
                hi=(packed>>(self.count_width+self.time_width))&((1<<self.time_width)-1)
                earliest=self.earliest+lo;latest=self.earliest+hi
            if not 1<=count<=self.nvalues or not self.earliest<=earliest<=latest<=self.latest:
                raise ValueError('invalid event term count/time metadata')
            self.metadata.append((count,earliest,latest));total+=count
            previous=0 if tid==0 else raw[tid-1]
            tag=kind(previous) if tid else 0
            self.posting_kinds.append(('stored','bitmap','inline_single','reference')[tag])
            self.reference_ids.append(None)
            if tag==2:
                point=previous>>shift
                if count!=1 or point>=self.nvalues:raise ValueError('invalid inline event posting')
                self.postings.append((None,None));self.inline_ids.append(point);self.directory_end_ids.append(None)
            elif tag==3:
                target=previous>>shift
                if target>=self.nterms or target==tid:raise ValueError('invalid referenced event term')
                self.reference_ids[-1]=target
                self.postings.append((None,None));self.inline_ids.append(None);self.directory_end_ids.append(None)
            else:
                end_id=next_stored[tid]
                if end_id==MISSING:raise ValueError('missing event posting end')
                # For physical lists the ending boundary selects the codec.
                # Only inline/reference tags describe the following term.
                self.posting_kinds[-1]='bitmap' if kind(raw[end_id])==1 else 'stored'
                start=previous>>shift
                end=raw[end_id]>>shift
                if start!=last or end<=start or end>self.size-self.posting_base:raise ValueError('invalid event posting directory')
                self.postings.append((self.posting_base+start,end-start));self.inline_ids.append(None);self.directory_end_ids.append(end_id);last=end
        if total!=self.npostings:raise ValueError('event metadata total posting count mismatch')
        for tid,target in enumerate(self.reference_ids):
            if target is not None and (self.reference_ids[target] is not None or self.metadata[target]!=self.metadata[tid]):
                raise ValueError('unverified event reference chain or metadata mismatch')
        end=self.posting_base+last
        valid_tail=True
        if getattr(self,'legacy',False):
            if end!=self.size and (_align8(end)!=self.size or self.span(end,self.size-end)!=bytes(self.size-end)):
                valid_tail=False
        elif (self.flags==0 and (_align8(end)!=self.size or self.span(end,self.size-end)!=bytes(self.size-end))) or (self.flags!=0 and end!=self.size):
            valid_tail=False
        if not valid_tail:
            if not getattr(self,'retain_opaque_trailer',False) or not 0<self.size-end<=MAX_OPAQUE_TRAILER_BYTES:
                raise ValueError('unsupported '+('legacy ' if getattr(self,'legacy',False) else '')+'event trailer/alignment')
            self.opaque_trailer={'offset':end,'bytes':self.size-end,'sha256':hashlib.sha256(self.span(end,self.size-end)).hexdigest(),
                'interpretation':'unverified;retain_original_source;no_native_search_semantics'}
        self.sections['postings']={'offset':self.posting_base,'bytes':last,'decoded':False}
        self.sections['trailer']={'offset':end,'bytes':self.size-end}


class Lexicon:
    def __init__(self,reader,sha):self._reader=reader;self.sha256=sha;self.closed=False
    @property
    def schema(self):return SCHEMA if self._reader.flags==0 else SCHEMA_V3 if getattr(self._reader,'tsidx9',False) else SCHEMA_V2
    @property
    def decoder(self):
        base='obs-event-lexicon-tsidx9-1' if getattr(self._reader,'tsidx9',False) else 'obs-event-lexicon-splunk6-1' if getattr(self._reader,'legacy',False) else 'obs-event-lexicon-tsidx7-1' if getattr(self._reader,'tsidx7',False) else DECODER if self._reader.flags==0 else FLAGS3_DECODER
        if getattr(self._reader,'tsidx9',False) and self._reader.flags==0:base='obs-event-lexicon-tsidx9-plain-1'
        if getattr(self._reader,'tsidx9',False) and self._reader.flags==19:base='obs-event-lexicon-tsidx9-inline-1'
        return base.rsplit('-',1)[0]+'-opaque-tail-1' if hasattr(self._reader,'opaque_trailer') else base
    def _check(self):
        if self.closed:raise ValueError('event lexicon is closed')
    def report(self):
        self._check();r=self._reader
        report={'event_lexicon_schema':1 if r.flags==0 else 3 if getattr(r,'tsidx9',False) else 2,'source_format':FORMAT,'decoder':self.decoder,
            'source_sha256':self.sha256,'source_bytes':r.size,'header_wire_base64':base64.b64encode(r.header).decode(),
            'values':r.nvalues,'terms':r.nterms,'postings':r.npostings,'earliest_seconds':r.earliest,'latest_seconds':r.latest,
            'lowest_address_operand':r.lowest_address,'highest_address_operand':r.highest_address,
            'terms_per_block':r.block_size,'metadata_count_bits':r.count_width,'metadata_time_bits':r.time_width,'lexicon_uncompressed_size':r.uncompressed,'sections':json.loads(_json(r.sections)),
            'scope':'strict TSIDX8 header, complete byte lexicon, term count/time metadata and opaque span bounds',
            'original_source_required':True,'values_decoded':False,'postings_decoded':False,
            'journal_addresses_resolved':False,'indexed_search_supported':False,'pruning_supported':False}
        if getattr(r,'legacy',False):report.update(source_format='splunk6-event-lexicon',
            bounds_source='complete_physical_value_pairs;not_legacy_header_sentinels',
            metadata_encoding='count-earliest-latest-be32',metadata_count_storage_bits=32,
            scope='strict legacy event header/arrays, physical value bounds, complete byte lexicon and absolute term metadata; no posting bodies or visibility')
        if getattr(r,'tsidx7',False):report.update(source_format='splunk-tsidx7-event-lexicon',observed_flags=r.flags,
            scope='strict TSIDX7 header, physical value-pair framing, complete byte lexicon, term metadata and posting spans',
            directory_word_bytes=8,directory_tag_semantics='preceding_word_inline_single_value;next_untagged_word_external_end' if r.flags==1 else 'untagged_physical_byte_boundaries',
            inline_value_ids_decoded=True,inline_terms=sum(p is not None for p in r.inline_ids))
        if r.flags==3:report.update(observed_flags=3,inline_value_ids_decoded=True,
            inline_terms=sum(p is not None for p in r.inline_ids),directory_tag_semantics='preceding_word_inline_single_value;next_untagged_word_external_end')
        if getattr(r,'tsidx9',False):report.update(source_format='splunk-tsidx9-event-lexicon',observed_flags=r.flags,
            directory_word_bytes=r.directory_width,inline_terms=sum(p is not None for p in r.inline_ids),
            reference_terms=sum(p is not None for p in r.reference_ids),bitmap_terms=r.posting_kinds.count('bitmap'),
            inline_value_ids_decoded=True,reference_term_ids_decoded=True,
            directory_tag_semantics='untagged_physical_byte_boundaries' if r.flags==0 else 'preceding_word:2_inline_3_term_reference;external_end_word:0_stored_1_bitmap')
        if getattr(r,'tsidx9',False) and r.flags==0:report.update(
            inline_value_ids_decoded=False,reference_term_ids_decoded=False,
            scope='strict TSIDX9 flags0 header, plain value pairs, packed term metadata, complete byte lexicon and untagged posting spans')
        if getattr(r,'tsidx9',False) and r.flags==19:report.update(
            reference_term_ids_decoded=False,
            directory_tag_semantics='preceding_word_inline_single_value;next_untagged_word_external_end',
            scope='strict TSIDX9 flags19 header, packed values/metadata, complete byte lexicon, four-byte inline directory and delta/run posting spans')
        if hasattr(r,'opaque_trailer'):report.update(opaque_trailer=json.loads(_json(r.opaque_trailer)),
            complete_file_semantics_validated=False,opaque_trailer_policy='explicit_preservation;known_prefix_only',
            scope=report['scope']+'; trailing source span retained uninterpreted')
        if getattr(r,'limits',None) is not None:report['resource_limits']=r.limits.describe()
        return report
    def records(self):
        self._check();r=self._reader
        for tid,term in enumerate(r.terms):
            self._check();count,lo,hi=r.metadata[tid];offset,length=r.postings[tid];block,size=r.blocks[tid//r.block_size]
            row={'term_id':tid,'term':term,'posting_count':count,'metadata_earliest_seconds':lo,'metadata_latest_seconds':hi,
                'posting_offset':offset,'posting_bytes':length,'metadata_bit_offset':r.meta*8+tid*r.meta_width,'metadata_bit_width':r.meta_width,
                'lexicon_block_offset':block,'lexicon_block_bytes':size,'term_in_block':tid%r.block_size,
                'source_sha256':self.sha256,'source_bytes':r.size,'decoder':self.decoder}
            if r.flags!=0:
                width=getattr(r,'directory_width',8)
                start=None if tid==0 else r.plo+(tid-1)*width
                end_id=r.directory_end_ids[tid];end=None if end_id is None else r.plo+end_id*width
                row.update(posting_kind=r.posting_kinds[tid],inline_value_id=r.inline_ids[tid],
                    directory_start_operand_offset=start,directory_end_operand_offset=end,
                    directory_start_operand_wire=None if start is None else r.span(start,width),
                    directory_end_operand_wire=None if end is None else r.span(end,width))
                if getattr(r,'tsidx9',False):row['referenced_term_id']=r.reference_ids[tid]
            yield row
    def batches(self):
        schema=self.schema.with_metadata({**self.schema.metadata,PROVENANCE_KEY:_json(self.report())});pending=[]
        for row in self.records():
            pending.append(row)
            if len(pending)==1024:yield pa.RecordBatch.from_pylist(pending,schema=schema);pending=[]
        if pending:yield pa.RecordBatch.from_pylist(pending,schema=schema)


@contextmanager
def open_lexicon(path,*,experimental=False,retain_opaque_trailer=False,limits=None):
    if not experimental:raise ValueError('event lexicon access is experimental; pass --experimental')
    path=Path(path);before=path.lstat()
    if not stat.S_ISREG(before.st_mode) or not 128<=before.st_size<=MAX_FILE_BYTES:
        raise ValueError('event lexicon requires a bounded regular file without symlinks')
    value=None
    try:
        with msidx._source(path) as (data,sha):
            opened=os.fstat(data.fd)
            if (before.st_dev,before.st_ino,msidx._stable(before))!=(opened.st_dev,opened.st_ino,msidx._stable(opened)):
                raise ValueError('event lexicon source changed before open')
            value=Lexicon(reader_for(data,retain_opaque_trailer=retain_opaque_trailer,limits=limits),sha)
            yield value
            after=path.lstat()
            if (before.st_dev,before.st_ino,msidx._stable(before))!=(after.st_dev,after.st_ino,msidx._stable(after)):
                raise ValueError('event lexicon source path changed')
    finally:
        if value is not None:value.closed=True


def reader_for(data,*,retain_opaque_trailer=False,limits=None):
    if data[0:8]==b'Splunk6\0':
        from .legacy_event import Reader
        return Reader(data,retain_opaque_trailer=retain_opaque_trailer,limits=limits)
    return _Reader(data,retain_opaque_trailer=retain_opaque_trailer,limits=limits)


def inspect(path,*,experimental=False,retain_opaque_trailer=False,limits=None):
    with open_lexicon(path,experimental=experimental,retain_opaque_trailer=retain_opaque_trailer,limits=limits) as value:
        rows=[dict(row,term_base64=base64.b64encode(row['term']).decode()) for row in value.records()]
        for row in rows:
            del row['term']
            for key in ('directory_start_operand_wire','directory_end_operand_wire'):
                if key in row:row[key+'_base64']=None if row[key] is None else base64.b64encode(row[key]).decode();del row[key]
        return {**value.report(),'entries':rows}


def export(path,output,format='parquet',*,experimental=False,retain_opaque_trailer=False,limits=None):
    if format not in ('parquet','arrow'):raise ValueError('event lexicon export requires parquet or arrow')
    if Path(output).resolve().is_relative_to(Path(path).resolve().parent):raise ValueError('output must be outside source bucket')
    # Source final checks run before atomic publication. Never overwrite output.
    with atomic_output(output) as file:
        with open_lexicon(path,experimental=experimental,retain_opaque_trailer=retain_opaque_trailer,limits=limits) as value:
            report=value.report();schema=value.schema.with_metadata({**value.schema.metadata,PROVENANCE_KEY:_json(report)})
            if format=='parquet':
                with pq.ParquetWriter(file,schema,compression='zstd',write_statistics=False) as writer:
                    for batch in value.batches():writer.write_batch(batch)
            else:
                with pa.ipc.new_file(file,schema) as writer:
                    for batch in value.batches():writer.write_batch(batch)
    return {**report,'output_bytes':Path(output).stat().st_size}


def verify(path,against,format='parquet',*,experimental=False,retain_opaque_trailer=False,limits=None):
    """Ordered all-field comparison, including the complete source envelope."""
    from itertools import zip_longest
    if format not in ('parquet','arrow'):raise ValueError('event lexicon verification requires parquet or arrow')
    with open_lexicon(path,experimental=experimental,retain_opaque_trailer=retain_opaque_trailer,limits=limits) as value:
        schema=value.schema.with_metadata({**value.schema.metadata,PROVENANCE_KEY:_json(value.report())});count=0
        with Path(against).open('rb') as file:
            before=os.fstat(file.fileno())
            if format=='parquet':
                reader=pq.ParquetFile(file);actual_schema=reader.schema_arrow;batch_stream=reader.iter_batches(batch_size=1024)
            else:
                reader=pa.ipc.open_file(file);actual_schema=reader.schema;batch_stream=(reader.get_batch(i) for i in range(reader.num_record_batches))
            if not actual_schema.equals(schema,check_metadata=True):raise ValueError('event lexicon export schema/provenance mismatch')
            rows=(row for batch in batch_stream for row in batch.to_pylist())
            sentinel=object()
            for expected,actual in zip_longest(value.records(),rows,fillvalue=sentinel):
                if expected!=actual:raise ValueError('event lexicon export row mismatch')
                count+=1
            if msidx._stable(before)!=msidx._stable(os.fstat(file.fileno())):raise ValueError('event lexicon export changed during verification')
        report=value.report()
    return {**report,'verified_terms':count,'comparison':'all canonical fields and source envelope, ordered'}
