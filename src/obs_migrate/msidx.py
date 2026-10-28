"""Experimental physical MSIDX4 point scan. No vendor runtime or postings search.

Observed unpacked/packed timestamps and plain/XOR Float64 column profiles only. Original bucket
files must be retained: this projection cannot restore discarded journal bytes,
index-time metadata, ingest point identity, or search/deletion visibility.
"""
from contextlib import contextmanager
from collections import OrderedDict
from bisect import bisect_left
import hashlib
import base64
import json
import math
import os
from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.parquet as pq

from .events import EXACT_TIME
from .metric_journal import MEASUREMENT,DIMENSION
from .metric_io import MetricRecordIO

FORMAT='splunk-msidx4-physical-points'
DECODER='obs-msidx4-2'
PACKED_MS_DECODER='obs-msidx4-packed-ms-1'
MAX_ENTRIES=100_000
MAX_METADATA_BYTES=16*1024*1024
MAX_FILE_BYTES=8*1024**3
MAX_CACHED_VALUES=100_000
SCHEMA=pa.schema([
    pa.field('schema_version',pa.uint16(),False),pa.field('source_format',pa.string(),False),
    pa.field('indexed_time',EXACT_TIME,False),pa.field('journal_time',EXACT_TIME),pa.field('index_time',EXACT_TIME),
    pa.field('host',pa.binary()),pa.field('source',pa.binary()),pa.field('sourcetype',pa.binary()),
    pa.field('measurements',pa.list_(pa.field('element',MEASUREMENT)),False),
    pa.field('dimensions',pa.list_(pa.field('element',DIMENSION)),False),
    pa.field('raw',pa.binary()),pa.field('timestamp_wire',pa.binary(),False),
    pa.field('dimension_set_wire',pa.binary(),False),pa.field('preamble_wire',pa.binary(8),False),
    pa.field('storage_json',pa.string(),False),
    pa.field('index',pa.string()),pa.field('bucket_id',pa.string()),
    pa.field('source_sha256',pa.string(),False),pa.field('source_bytes',pa.uint64(),False),
    pa.field('point_ordinal',pa.uint64(),False),pa.field('dimension_set_id',pa.uint32(),False),
    pa.field('tsidx_value_index',pa.uint32(),False),pa.field('timestamp_offset',pa.uint64(),False),
    pa.field('decoder',pa.string(),False),
],metadata={b'obs_migrate.msidx_schema':b'2',b'access':b'physical_msidx4_scan;no_vendor_binary',
            b'visibility':b'physical;search_deletions_unverified',
            b'raw':b'NULL;discarded_journal_bytes_not_reconstructed',
            b'indexed_search_supported':b'false'})
_io=MetricRecordIO(SCHEMA,FORMAT,('host','source','sourcetype','raw','timestamp_wire','dimension_set_wire','preamble_wire'),
                  count_field='stored_points',schema_version=2,status={'indexed_time':'decoded_storage', 'search_visibility':'UNVERIFIED',
                                                      'indexed_search_supported':False})
json_bytes=_io.json_bytes
from_json=_io.from_json
batches=_io.batches
export=_io.export
# Schema 1 exports remain readable as their original rows. Current verification
# deliberately requires schema 2: provenance and decoder revisions differ.
_LEGACY_SCHEMA=pa.schema([pa.field('timestamp_wire',pa.binary(8),False) if f.name=='timestamp_wire' else f
                         for f in SCHEMA],metadata=SCHEMA.metadata)
_LEGACY_SCHEMA=_LEGACY_SCHEMA.with_metadata({**SCHEMA.metadata,b'obs_migrate.msidx_schema':b'1'})
_legacy_io=MetricRecordIO(_LEGACY_SCHEMA,FORMAT,_io.binary_fields,count_field='stored_points',status=_io.status)


def read_export(path,format):
    if format=='parquet':
        with pq.ParquetFile(path) as file:legacy=file.schema_arrow.equals(_LEGACY_SCHEMA,check_metadata=True)
    elif format=='ndjson':
        with open(path,'rb') as file:
            first=file.readline(MAX_METADATA_BYTES*4+1)
        if len(first)>MAX_METADATA_BYTES*4:raise MsidxError('metric export row exceeds inspection bound')
        legacy=bool(first) and json.loads(first).get('schema_version')==1
    else:raise ValueError('unknown metric record export format')
    yield from (_legacy_io if legacy else _io).read_export(path,format)

verify=_io.verify


class MsidxError(ValueError):
    pass


def _strict8(value):return (value//8+1)*8

def _count(value,label,*,zero=False):
    if not (0 if zero else 1)<=value<=MAX_ENTRIES:raise MsidxError(label+' outside supported resource bounds')
    return value


def _stable(stat):return stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns


@contextmanager
def _source(path):
    path=Path(path)
    if not path.is_file():raise MsidxError('MSIDX source must be a regular file')
    with path.open('rb') as file:
        before=os.fstat(file.fileno())
        if not 128<=before.st_size<=MAX_FILE_BYTES:raise MsidxError('MSIDX source size outside supported bounds')
        sha=hashlib.file_digest(file,'sha256').hexdigest()
        if _stable(before)!=_stable(os.fstat(file.fileno())):raise MsidxError('MSIDX source changed while hashing')
        yield _FileView(file.fileno(),before.st_size),sha
        if _stable(before)!=_stable(os.fstat(file.fileno())):raise MsidxError('MSIDX source changed during scan')


class _FileView:
    """Bounded positional reads; concurrent truncation raises instead of SIGBUS."""
    def __init__(self,fd,size):self.fd,self.size=fd,size
    def __len__(self):return self.size
    def __getitem__(self,key):
        scalar=isinstance(key,int)
        start,stop=(key,key+1) if scalar else (key.start,key.stop)
        if start<0 or stop<start or stop>self.size:raise MsidxError('invalid storage read span')
        data=os.pread(self.fd,stop-start,start)
        if len(data)!=stop-start:raise MsidxError('MSIDX source truncated during scan')
        return data[0] if scalar else data


class _Reader:
    def __init__(self,data):
        self.data=data
        self.size=len(data)
        self.block_cache=OrderedDict();self.cached_values=0
        if self.span(0,8)!=b'_MSIDX4\0':raise MsidxError('unsupported MSIDX signature; only MSIDX4 is implemented')
        self.flags=self.u64be(72)
        if self.flags not in (0,5,96,101):raise MsidxError('unsupported MSIDX4 flags/timestamp compression profile')
        if self.span(100,20)!=bytes(20):raise MsidxError('unverified MSIDX4 header extension')
        self.nvalues=_count(self.u32be(8),'value entries')
        self.nterms=_count(self.u32be(16),'terms')
        self.ndimsets=_count(self.u32be(96),'dimension sets')
        if self.ndimsets>=self.nvalues:raise MsidxError('MSIDX4 has no timestamp entries')
        self.earliest,self.latest=self.u32be(60),self.u32be(64)
        if not 0<=self.earliest<=self.latest<=2147483647:raise MsidxError('unverified timestamp bounds')
        # Sections have strict *next* 8-byte boundaries, even when already aligned.
        self.seconds_width=(self.latest-self.earliest).bit_length()
        # Native flags101 add a separate 10-bit millisecond operand after the
        # whole-second delta. Do not infer this for any other flag combination.
        self.time_width=self.seconds_width+(10 if self.flags==101 else 0)
        self.time_bytes=(self.ndimsets*8+((self.nvalues-self.ndimsets)*self.time_width+63)//64*8
                         if self.flags in (5,101) else self.nvalues*8)
        pos=_strict8(128+self.time_bytes)
        for offset in (20,24,28,32,36):
            pos=_strict8(pos+self.u32be(offset))
        plo=pos
        meta=_strict8(plo+self.nterms*8)
        nbits=self.nvalues.bit_length()+2*(self.latest-self.earliest).bit_length()
        self.lexicon_offset=meta+((self.nterms*nbits+63)//64)*8
        self.terms,lexend=self.lexicon(self.lexicon_offset)
        posting_bytes=self.u64be(plo+(self.nterms-1)*8) >> (1 if self.flags in (5,101) else 0)
        self.dimsets_offset=_strict8(lexend+posting_bytes)
        self.dimsets,self.cstore_offset=self.dimension_sets(self.dimsets_offset)
        self.footer_offset=self.cstore_offset+self.u64be(88)
        self.columns=self.column_sets(self.cstore_offset,self.footer_offset)
        # All timestamp ranges must exactly partition the physical time entries.
        ranges=[];self.groups=[]
        for did in range(self.ndimsets):
            count,start=self.preamble(did)
            if not count or start<self.ndimsets or start+count>self.nvalues:raise MsidxError('invalid dimension timestamp range')
            ranges.append((start,start+count))
            self.groups.append((start,count,did))
            ds=self.dimsets[did][1];ncols=ds[1]
            for tid in ds[-ncols:]:
                if tid not in self.columns or did not in self.columns[tid].slots or self.columns[tid].slots[did][1]!=count:
                    raise MsidxError('column/dimension timestamp count mismatch')
        end=self.ndimsets
        for start,stop in sorted(ranges):
            if start!=end:raise MsidxError('overlapping or missing timestamp range')
            end=stop
        if end!=self.nvalues:raise MsidxError('missing timestamp entries')
        expected={(tid,did) for did,(_,ds) in enumerate(self.dimsets) for tid in ds[-ds[1]:]}
        actual={(tid,did) for tid,col in self.columns.items() for did in col.slots}
        if actual!=expected:raise MsidxError('unreferenced or missing column/dimension group')

    def span(self,offset,length):
        if offset<0 or length<0 or offset>self.size or length>self.size-offset:
            raise MsidxError('storage span crosses MSIDX source boundary')
        return self.data[offset:offset+length]

    def u32be(self,offset):return struct.unpack('>I',self.span(offset,4))[0]
    def u64be(self,offset):return struct.unpack('>Q',self.span(offset,8))[0]
    def u32le(self,offset):return struct.unpack('<I',self.span(offset,4))[0]

    def varint(self,pos,bound):
        value=0
        for shift in range(0,35,7):
            if pos>=bound:raise MsidxError('truncated lexicon integer')
            byte=self.data[pos];pos+=1
            if shift==28 and byte>15:raise MsidxError('lexicon integer overflow')
            value|=(byte&127)<<shift
            if not byte&128:return value,pos
        raise MsidxError('oversized lexicon integer')

    def lexicon(self,offset):
        max_bytes=getattr(self,'max_lexicon_bytes',MAX_METADATA_BYTES)
        count,block,uncompressed=struct.unpack('>III',self.span(offset,12))
        if count!=self.nterms or not 1<=block<=4096 or uncompressed>max_bytes:
            raise MsidxError('unsupported lexicon counts/resource bounds')
        nblocks=(count+block-1)//block
        ends=[self.u32be(offset+12+i*4) for i in range(nblocks)]
        base=offset+12+nblocks*4
        if ends[-1]>max_bytes:raise MsidxError('encoded lexicon resource bound exceeded')
        self.span(base,ends[-1]);terms=[];total=0;previous_end=0
        for number,end in enumerate(ends):
            if end<=previous_end:raise MsidxError('nonmonotonic lexicon block boundary')
            pos=base+previous_end;bound=base+end;previous=b''
            for k in range(min(block,count-number*block)):
                prefix=0
                if k:prefix,pos=self.varint(pos,bound)
                if k==block-1:length=bound-pos
                else:length,pos=self.varint(pos,bound)
                if prefix>len(previous) or prefix+length>2047 or pos+length>bound:
                    raise MsidxError('invalid lexicon term bounds')
                term=previous[:prefix]+self.span(pos,length);pos+=length
                if b'\0' in term:raise MsidxError('unverified NUL lexicon term')
                total+=len(term)+1
                if total>max_bytes:raise MsidxError('decoded lexicon resource bound exceeded')
                terms.append(term);previous=term
            if pos!=bound:raise MsidxError('lexicon block has trailing bytes')
            previous_end=end
        if uncompressed not in (total,total-count):raise MsidxError('lexicon uncompressed size mismatch')
        return terms,base+ends[-1]

    def term(self,tid,prefix=None):
        if tid>=len(self.terms):raise MsidxError('lexicon identifier out of bounds')
        term=self.terms[tid]
        if not term.startswith(b' '):raise MsidxError('unverified dimension/column term namespace')
        term=term[1:]
        if prefix is None:return term
        if not term.startswith(prefix):raise MsidxError('unverified routing/measurement term role')
        return term[len(prefix):]

    def dimension_sets(self,offset):
        if self.span(offset,8)!=b'DSMAGIC0':raise MsidxError('unsupported dimension-set signature')
        count=_count(self.u32le(offset+8),'dimension count')
        words=_count(self.u32le(offset+12),'dimension words')
        if count!=self.ndimsets:raise MsidxError('dimension header count mismatch')
        ends=[self.u32le(offset+16+i*4) for i in range(count)]
        base=offset+16+count*4
        self.span(base,words*4);sets=[];last=0
        for end in ends:
            if not last+6<=end<=words or end-last>4096:raise MsidxError('invalid dimension-set word range')
            pos=base+last*4
            row=struct.unpack('<'+str(end-last)+'I',self.span(pos,(end-last)*4))
            if row[0]!=len(row) or not 1<=row[1]<=len(row)-5:raise MsidxError('unsupported dimension-set record')
            for tid in row[2:]:
                if tid!=0xffffffff and tid>=self.nterms:raise MsidxError('dimension term identifier out of bounds')
            if len(set(row[-row[1]:]))!=row[1]:raise MsidxError('duplicate metric columns in dimension set')
            sets.append((pos,row));last=end
        if last!=words:raise MsidxError('unreferenced dimension words')
        return sets,(base+words*4+7)//8*8

    def column_sets(self,base,footer):
        if self.span(footer,8)!=b'CSMAGIC1':raise MsidxError('unsupported column-store footer')
        count=_count(self.u32le(footer+8),'column count')
        block_words=self.u32le(footer+12)
        options=struct.unpack('<Q',self.span(footer+16,8))[0]
        if (options,block_words) not in ((0,0),(1,1024)) or self.span(footer+24,16)!=bytes(16):
            raise MsidxError('unsupported compressed/extended column-store footer')
        if (self.flags,options) not in ((0,0),(5,1),(96,0),(101,1)):
            raise MsidxError('unverified timestamp/numeric compression combination')
        if footer+40+count*8!=self.size:raise MsidxError('column directory does not end at source EOF')
        columns={};last=0;last_tid=-1;groups=0
        for i in range(count):
            tid,end=struct.unpack('<II',self.span(footer+40+i*8,8))
            if tid<=last_tid or not last<end<=footer-base:raise MsidxError('invalid column directory')
            self.term(tid,b'metric_name::')
            pos=base+last;header=self.span(pos,32)
            nh,nv=struct.unpack_from('<II',header)
            _count(nh,'column groups');_count(nv,'column values')
            groups+=nh
            if groups>MAX_ENTRIES:raise MsidxError('column group resource bound exceeded')
            if header[8]!=1:raise MsidxError('unsupported numeric type/column header')
            # Native structs contain opaque padding at bytes 9..27. It is not
            # interpreted as fields; preserve the full original header instead.
            blocks=self.u32le(pos+28)
            if options==0 and blocks:raise MsidxError('unexpected blocks in plain column')
            if options: _count(blocks,'numeric compression blocks')
            values=pos+32+nh*16
            size=nv*8 if options==0 else blocks*(block_words*8+4)
            if values+size!=base+end:raise MsidxError('column size does not match directory')
            slots={};previous=0
            entries=[]
            for k in range(nh):
                did,upto,a,b=struct.unpack('<IIII',self.span(pos+32+k*16,16))
                if did>=self.ndimsets or did in slots or not previous<upto<=nv:
                    raise MsidxError('invalid column group')
                if options==0 and (a or b):raise MsidxError('invalid uncompressed column group')
                slots[did]=(previous,upto-previous);entries.append((previous,upto,a,b))
                previous=upto
            if previous!=nv:raise MsidxError('unreferenced column values')
            ends=[]
            if options:
                ends=[self.u32le(values+blocks*block_words*8+j*4) for j in range(blocks)]
                last_value=0
                for block_end in ends:
                    if not last_value<block_end<=nv or block_end-last_value>block_words*64:
                        raise MsidxError('invalid compression block value range')
                    last_value=block_end
                if ends[-1]!=nv:raise MsidxError('compression blocks omit column values')
                # Directory hints must describe the groups they claim to locate.
                from bisect import bisect_right
                for first,stop,block,last_offset in entries:
                    expected=bisect_right(ends,stop-1)
                    start_block=bisect_right(ends,first)
                    start_base=ends[start_block-1] if start_block else 0
                    if block!=expected or last_offset!=first-start_base:
                        raise MsidxError('compression group block hints mismatch')
            columns[tid]=_Column(self,slots,values,ends,block_words,pos,header)
            last=end;last_tid=tid
        if base+last!=footer:raise MsidxError('column body does not reach footer')
        return columns

    def preamble(self,did):
        return struct.unpack('<II' if self.flags in (5,101) else '>II',self.span(128+(self.ndimsets-1-did)*8,8))

    def metadata(self,did):
        dimension_offset,ds=self.dimsets[did]
        _,ncols,h,src,st,*ids=ds
        routing={key:None if tid==0xffffffff else self.term(tid,key.encode()+b'::')
                 for key,tid in (('host',h),('source',src),('sourcetype',st))}
        dims=[]
        for tid in ids[:-ncols]:
            term=self.term(tid)
            if b'::' not in term:raise MsidxError('dimension term lacks key separator')
            key,value=term.split(b'::',1)
            if not key:raise MsidxError('empty dimension key')
            dims.append({'key':key,'value':value})
        names=tuple(self.term(tid,b'metric_name::') for tid in ids[-ncols:])
        if not all(names):raise MsidxError('empty metric name')
        return dimension_offset,ds,names,routing,dims

    def rows(self,sha,index,bucket_id,*,dimension_ids=None,point_ids=None,columns=None):
        wanted=set(SCHEMA.names) if columns is None else set(columns)
        if wanted-set(SCHEMA.names):raise ValueError('unknown metric projection column')
        provenance='storage_json' in wanted
        need_time=bool(wanted & {'indexed_time','timestamp_wire','storage_json'})
        need_value=provenance or 'measurements' in wanted
        ordinal=0
        for did,(dimension_offset,ds) in enumerate(self.dimsets):
            count,start=self.preamble(did)
            group_start=ordinal;group_end=ordinal+count
            if dimension_ids is not None and did not in dimension_ids:
                ordinal=group_end
                continue
            _,_,names,routing,dims=self.metadata(did)
            ncols=ds[1];ids=ds[-ncols:]
            local_ids=range(count) if point_ids is None else (
                point-group_start for point in point_ids[bisect_left(point_ids,group_start):bisect_left(point_ids,group_end)])
            for local in local_ids:
                ordinal=group_start+local
                value_index=start+local;wire=None;seconds=nanos=None
                if self.flags in (5,101):
                    bit_offset=(value_index-self.ndimsets)*self.time_width
                    offset=128+self.ndimsets*8+bit_offset//8
                    length=(bit_offset%8+self.time_width+7)//8
                    if need_time:
                        wire=self.span(offset,length)
                        delta=(int.from_bytes(wire,'little')>>(bit_offset%8))&((1<<self.time_width)-1)
                        seconds=self.earliest+(delta&((1<<self.seconds_width)-1))
                        other=(delta>>self.seconds_width)*1_000_000 if self.flags==101 else 0
                else:
                    offset=128+value_index*8
                    if need_time:wire=self.span(offset,8);seconds,other=struct.unpack('>II',wire)
                if need_time:
                    nanos=other if self.flags in (96,101) else 0
                    if not self.earliest<=seconds<=self.latest or nanos>=10**9 or (self.flags==0 and other!=did):
                        raise MsidxError('timestamp/dimension operand outside observed profile')
                fraction=('0.'+str(nanos).zfill(9)).rstrip('0') if nanos else '0'
                measurements=[];locations=[]
                if need_value:
                    for tid,name in zip(ids,names):
                        bits,location=self.columns[tid].value(did,local)
                        value=struct.unpack('<d',bits)[0]
                        if not math.isfinite(value):raise MsidxError('nonfinite column values not yet verified')
                        measurements.append({'name':name,'value_bits_le':bits,'value_float64':value})
                        locations.append(dict(location,term_id=tid))
                state={'header_flags':self.flags,'lexicon_offset':self.lexicon_offset,
                       'dimension_sets_offset':self.dimsets_offset,'dimension_set_offset':dimension_offset,
                       'preamble_offset':128+(self.ndimsets-1-did)*8,
                       'column_store_offset':self.cstore_offset,'column_footer_offset':self.footer_offset,
                       'measurements':locations,'visibility':'physical;search_deletions_unverified',
                       'source_archive_required':True,
                       'timestamp_encoding':'packed-seconds-milliseconds-lsb' if self.flags==101 else 'packed-delta-lsb' if self.flags==5 else 'seconds-nanos-be' if self.flags==96 else 'seconds-dimension-be',
                       'timestamp_bit_offset':bit_offset%8 if self.flags in (5,101) else 0,
                       'timestamp_bit_width':self.time_width if self.flags in (5,101) else 64}
                if self.flags==101:
                    state.update(timestamp_seconds_bit_width=self.seconds_width,timestamp_milliseconds_bit_width=10,
                                 timestamp_milliseconds_to_nanoseconds=1_000_000)
                row={'schema_version':2,'source_format':FORMAT,'indexed_time':{'seconds':seconds,'fraction':fraction},
                       'journal_time':None,'index_time':None,**routing,'measurements':measurements,'dimensions':dims,
                       'raw':None,'timestamp_wire':wire,
                       'dimension_set_wire':self.span(dimension_offset,len(ds)*4) if 'dimension_set_wire' in wanted else None,
                       'preamble_wire':self.span(state['preamble_offset'],8) if 'preamble_wire' in wanted else None,
                       'storage_json':json.dumps(state,sort_keys=True,separators=(',',':')) if provenance else None,
                       'index':index,'bucket_id':bucket_id,'source_sha256':sha,'source_bytes':self.size,
                       'point_ordinal':ordinal,'dimension_set_id':did,'tsidx_value_index':value_index,
                       'timestamp_offset':offset,'decoder':PACKED_MS_DECODER if self.flags==101 else DECODER}
                yield row if columns is None else {key:row[key] for key in columns}
            ordinal=group_end


def records(path,*,experimental=False,index=None,bucket_id=None):
    """Scan one immutable TSIDX file, yielding physical stored metric point rows.

    This API deliberately accepts a file, not bucket discovery: callers must not
    assume an arbitrary set of companion files is an authoritative bucket view.
    Exhaust the iterator to perform the final source-stability check.
    """
    if not experimental:raise MsidxError('MSIDX reader is experimental; pass --experimental')
    with _source(path) as (data,sha):
        reader=_Reader(data)
        yield from reader.rows(sha,index,bucket_id)


class _Bits:
    """Bounded LSB-first reader for observed native 64-bit little-endian blocks."""
    def __init__(self,wire):self.wire,self.position=wire,0
    def take(self,width):
        if not 0<=width<=64 or self.position+width>len(self.wire)*8:
            raise MsidxError('compressed numeric bit span crosses block boundary')
        first=self.position//8;shift=self.position%8
        value=(int.from_bytes(self.wire[first:(self.position+width+7)//8],'little')>>shift)&((1<<width)-1)
        self.position+=width
        return value


def _xor_values(wire,count):
    bits=_Bits(wire);previous=0;leading=trailing=0
    for _ in range(count):
        start=bits.position;before=previous;old_leading,old_trailing=leading,trailing
        if bits.take(1):
            if bits.take(1):
                width=64-leading-trailing
                xor=bits.take(width)<<trailing
            else:
                leading=bits.take(5);width=bits.take(6)+1
                if leading+width>64:raise MsidxError('invalid Float64 XOR window')
                trailing=64-leading-width
                xor=bits.take(width)<<trailing
            previous^=xor
        yield (previous.to_bytes(8,'little'),start,bits.position,before,old_leading,old_trailing)


class _Column:
    def __init__(self,reader,slots,values,ends,block_words,header_offset,header):
        self.reader,self.slots,self.values=reader,slots,values
        self.ends,self.block_bytes=ends,block_words*8
        self.header_offset,self.header=header_offset,header
        

    def value(self,did,local):
        from bisect import bisect_right
        encode=lambda value:base64.b64encode(value).decode()
        number=self.slots[did][0]+local
        state={'column_header_offset':self.header_offset,'column_header_wire_base64':encode(self.header)}
        if not self.ends:
            position=self.values+number*8
            return self.reader.span(position,8),dict(state,encoding='float64-le',offset=position)
        block=bisect_right(self.ends,number)
        base=self.ends[block-1] if block else 0
        block_offset=self.values+block*self.block_bytes
        cache=self.reader.block_cache;key=(self.header_offset,block)
        if key not in cache:
            wire=self.reader.span(block_offset,self.block_bytes)
            decoded=list(_xor_values(wire,self.ends[block]-base))
            while cache and self.reader.cached_values+len(decoded)>MAX_CACHED_VALUES:
                _,old=cache.popitem(last=False);self.reader.cached_values-=len(old)
            cache[key]=decoded;self.reader.cached_values+=len(decoded)
        else:cache.move_to_end(key)
        bits,start,stop,previous,leading,trailing=cache[key][number-base]
        offset=block_offset+start//8;length=(stop+7)//8-start//8
        state.update(encoding='xor-float64-lsb',offset=offset,encoded_wire_base64=encode(self.reader.span(offset,length)),
                     bit_offset=start%8,bit_width=stop-start,block_offset=block_offset,block_bytes=self.block_bytes,
                     block_value_index=number-base,previous_bits_le_base64=encode(previous.to_bytes(8,'little')),
                     leading_before=leading,trailing_before=trailing)
        return bits,state
