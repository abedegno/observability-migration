"""Addressed redecoding of immutable decompressed native journal records.

The full audited scan still runs once. Append-only dictionaries and compact
per-record state permit subsequent bounded decoding at proven native offsets.
Original compressed-stream random seeking is deliberately outside this API.
"""
import copy
import hashlib
import json
import struct
from types import MappingProxyType

from . import events,journal
from .wire import exact_time

STATE=struct.Struct('<QIiIIIB')
PROJECTION=struct.Struct('<IqQ')
DECODER='obs-native-journal-address-1'


DEFAULT_METADATA_CACHE_BYTES=32*1024**2
MAX_METADATA_CACHE_BYTES=64*1024**2
_METADATA_REF=struct.Struct('<I')
_METADATA_ENTRY=struct.Struct('<II')
_METADATA_ENTRY_ALLOWANCE=128


def _metadata_cache_budget(value):
    if type(value) is not int or not 0<=value<=MAX_METADATA_CACHE_BYTES:
        raise ValueError('metadata cache budget must be an integer from 0 to 64 MiB')
    return value


class _MetadataCache:
    """Bounded immutable canonical strings; missing entries require full decoding.

    Accounting covers packed buffers plus a per-entry construction allowance,
    not process RSS. The temporary exact-string lookup is discarded at finish.
    """
    def __init__(self,budget):
        self.budget=_metadata_cache_budget(budget)
        self._data=bytearray();self._entries=bytearray();self._refs=bytearray()
        self._lookup={};self._records=0;self._cached=0;self._ready=False;self._closed=False

    def _charged(self):
        return len(self._data)+len(self._entries)+len(self._refs)+len(self._entries)//_METADATA_ENTRY.size*_METADATA_ENTRY_ALLOWANCE

    def add(self,value):
        if self._ready or self._closed:raise ValueError('metadata cache capture is closed')
        if not isinstance(value,str):raise ValueError('metadata cache requires canonical strings')
        ordinal=self._records;self._records+=1
        # Keep a bounded ordinal prefix. Later records safely use canonical fallback.
        if ordinal!=len(self._refs)//_METADATA_REF.size or self._charged()+_METADATA_REF.size>self.budget:return
        self._refs.extend(_METADATA_REF.pack(0))
        reference=self._lookup.get(value)
        if reference is None:
            remaining=self.budget-self._charged()-_METADATA_ENTRY.size-_METADATA_ENTRY_ALLOWANCE
            if len(value)>remaining:return
            encoded=value.encode('utf8')
            if len(encoded)>remaining:return
            reference=len(self._entries)//_METADATA_ENTRY.size+1
            self._entries.extend(_METADATA_ENTRY.pack(len(self._data),len(encoded)))
            self._data.extend(encoded);self._lookup[value]=reference
        _METADATA_REF.pack_into(self._refs,ordinal*_METADATA_REF.size,reference)
        self._cached+=1

    def finish(self):
        if self._ready or self._closed:raise ValueError('metadata cache capture is closed')
        self._lookup=None
        self._data=bytes(self._data);self._entries=bytes(self._entries);self._refs=bytes(self._refs)
        self._ready=True

    def get(self,ordinal):
        if self._closed or not self._ready:raise ValueError('metadata cache is closed or incomplete')
        if type(ordinal) is not int or not 0<=ordinal<self._records:raise ValueError('metadata cache ordinal outside capture')
        offset=ordinal*_METADATA_REF.size
        if offset>=len(self._refs):return None
        reference=_METADATA_REF.unpack_from(self._refs,offset)[0]
        if not reference:return None
        if reference>len(self._entries)//_METADATA_ENTRY.size:raise ValueError('metadata cache reference outside capture')
        start,length=_METADATA_ENTRY.unpack_from(self._entries,(reference-1)*_METADATA_ENTRY.size)
        if start+length>len(self._data):raise ValueError('metadata cache reference span outside capture')
        try:return self._data[start:start+length].decode('utf8')
        except UnicodeDecodeError as error:raise ValueError('metadata cache reference has invalid UTF8') from error

    def report(self):
        if self._closed or not self._ready:raise ValueError('metadata cache is closed or incomplete')
        return {'budget_bytes':self.budget,'charged_bytes':self._charged(),
            'buffer_bytes':len(self._data)+len(self._entries)+len(self._refs),
            'payload_bytes':len(self._data),'reference_bytes':len(self._refs),
            'descriptor_bytes':len(self._entries),'records':self._records,
            'cached_records':self._cached,'unique_values':len(self._entries)//_METADATA_ENTRY.size,
            'encoding':'UTF8 canonical JSON;u32 offset/length entries;u32 ordinal references;zero means fallback',
            'accounting':'packed buffers plus 128-byte unique-entry allowance;not process RSS',
            'validation':'exact strings captured only during complete canonical initial scan;no persisted cache trust'}

    def close(self):
        self._closed=True;self._ready=False;self._lookup=None
        self._data=self._entries=self._refs=b''


class _Slice:
    def __init__(self,data,start,end):self.data=data;self.pos=start;self.end=end;self.bytes_read=0
    def read(self,n):
        if type(n) is not int or n<0:raise ValueError('native record reads require bounded lengths')
        end=min(self.end,self.pos+n);wire=self.data[self.pos:end]
        self.pos=end;self.bytes_read+=len(wire);return wire


class Observer:
    """Internal full-scan hook; never a shortcut around validation."""
    def __init__(self,max_bytes,max_records,*,cache_projections=False,metadata_cache_bytes=DEFAULT_METADATA_CACHE_BYTES):
        if type(max_bytes) is not int or type(max_records) is not int or min(max_bytes,max_records)<1:
            raise ValueError('native journal byte/record limits must be positive integers')
        metadata_cache_bytes=_metadata_cache_budget(metadata_cache_bytes)
        self.max_bytes=max_bytes;self.max_records=max_records
        self._metadata_cache=_MetadataCache(metadata_cache_bytes) if cache_projections else None
        self._blob=bytearray();self._states=bytearray();self._digests=bytearray()
        self._stream=None;self.ready=False;self.closed=False;self.last_json_size=0
        self.cache_projections=cache_projections;self._projections=bytearray() if cache_projections else None

    def wrap(self,stream):
        if self.ready or self.closed or self._stream is not None:raise ValueError('native journal observer cannot be reused')
        self._stream=stream;return self

    def read(self,n):
        if self.ready or self.closed:raise ValueError('native journal capture is closed')
        if type(n) is not int or n<0:raise ValueError('native journal capture requires bounded reads')
        wire=self._stream.read(min(n,self.max_bytes-len(self._blob)+1))
        if len(self._blob)+len(wire)>self.max_bytes:raise ValueError('native decompressed journal byte limit exceeded')
        self._blob.extend(wire);return wire

    def event(self,decoder,row):
        if self.ready or self.closed:raise ValueError('native journal capture is closed')
        count=len(self._states)//STATE.size
        if count>=self.max_records:raise ValueError('native journal state record limit exceeded')
        if row['record_ordinal']!=count:raise ValueError('native record access requires complete physical ordinals')
        if decoder.header['version']!=1 or decoder.header['align_bits']!=5 or not decoder.state_time_seen:
            raise ValueError('native record access requires journal v1/align5 with proven time state')
        # Capture the validated row profile alongside the existing state flags.
        flags=(int(decoder.private_seen)|(int(decoder.state_time_seen)<<1)|
               (int(row['decoder']==journal.EXTENDED_EVENT_DECODER)<<2))
        self._states.extend(STATE.pack(decoder.record_offset,len(decoder.record_wire),decoder.base_event_time,
            decoder.active_host,decoder.active_source,decoder.active_source_type,flags))
        wire=events.json_bytes(row);self.last_json_size=len(wire)
        self._digests.extend(hashlib.sha256(wire).digest())
        if self.cache_projections:
            offset=decoder.record_offset;length=len(decoder.record_wire);raw=row['raw']
            if self._blob[offset:offset+length]!=decoder.record_wire or not decoder.record_wire.endswith(raw):
                raise ValueError('native raw projection span is unverified')
            state=json.loads(row['decoder_state_json'])
            self._projections.extend(PROJECTION.pack(len(raw),row['index_time']['seconds'],state['wire_components']['subsecond_wire']))
            self._metadata_cache.add(row['metadata_json'])

    def finish(self,decoder,context,sha):
        if len(self._blob)!=decoder.reader.pos or not self._states:raise ValueError('native journal capture is incomplete')
        if set(decoder.fields)-{3,4,5,6}:raise ValueError('unverified native journal dictionary namespace')
        self._fields=MappingProxyType({k:tuple(v) for k,v in decoder.fields.items()})
        self.dictionary_bytes=sum(len(s.encode('utf8','surrogateescape')) for v in self._fields.values() for s in v)
        self.header=dict(decoder.header);self.sha256=sha;self._decoder_cls=type(decoder)
        self._context=copy.copy(context)
        self._context.deletes=MappingProxyType(dict(context.deletes));self._context.matched=frozenset()
        self._blob=bytes(self._blob);self._states=bytes(self._states);self._digests=bytes(self._digests)
        if self.cache_projections:
            self._projections=bytes(self._projections)
            if self._metadata_cache._records!=len(self._states)//STATE.size:raise ValueError('metadata cache record count mismatch')
            self._metadata_cache.finish()
        self._stream=None;self.ready=True
        self.decompressed_sha256=hashlib.sha256(self._blob).hexdigest()

    def check(self):
        if self.closed or not self.ready:raise ValueError('native journal access is closed or incomplete')

    def report(self):
        self.check()
        return {'native_journal_decoder':DECODER,'retained_native_journal_bytes':len(self._blob),
            'decompressed_journal_sha256':self.decompressed_sha256,'retained_record_state_bytes':len(self._states),
            'retained_record_digest_bytes':len(self._digests),'retained_dictionary_utf8_bytes':self.dictionary_bytes,
            'record_state_encoding':'offset-u64,length-u32,base-time-i32,host-source-stype-u32,flags-u8;little-endian',
            'record_validation':'each addressed row must match its complete-scan canonical SHA256',
            'initial_full_journal_scan':True,'decompressed_record_address_lookup':True,
            'compressed_journal_random_seek':False,'retained_arrow_bytes':0,
            **({'native_projection_cache':True,'retained_projection_cache_bytes':len(self._projections),
                'metadata_projection_cache':self._metadata_cache.report(),
                'projection_cache_encoding':'raw-length-u32,index-second-i64,subsecond-wire-u64;little-endian',
                'projection_cache_validation':'canonical initial rows and exact original raw/record spans;full-row reads retain canonical digest verification'}
               if self.cache_projections else {})}

    def record(self,ordinal,*,index=None,bucket_id=None,object_uri=None):
        self.check()
        if type(ordinal) is not int or not 0<=ordinal<len(self._states)//STATE.size:raise ValueError('native record ordinal outside capture')
        offset,length,base,host,source,stype,flags=STATE.unpack_from(self._states,ordinal*STATE.size)
        stream=_Slice(self._blob,offset,offset+length);decoder=self._decoder_cls(stream)
        decoder.reader.pos=offset;decoder.header=dict(self.header);decoder.base_index_time=self.header['base_index_time']
        decoder.base_event_time=base;decoder.active_host=host;decoder.active_source=source;decoder.active_source_type=stype
        decoder.private_seen=bool(flags&1);decoder.state_time_seen=bool(flags&2);decoder.fields=self._fields
        if not decoder.scan() or decoder.err():raise ValueError('addressed native record decode failed: '+str(decoder.err()))
        if decoder.reader.pos!=offset+length or decoder.record_wire!=self._blob[offset:offset+length]:
            raise ValueError('addressed native record span mismatch')
        context=copy.copy(self._context);context.matched=set()
        row=journal._event_row(decoder,context,self.sha256,ordinal,index=index,bucket_id=bucket_id,object_uri=object_uri)
        canonical=events.json_bytes(row)
        if hashlib.sha256(canonical).digest()!=self._digests[ordinal*32:(ordinal+1)*32]:
            raise ValueError('addressed native record canonical digest mismatch')
        return row,stream.bytes_read,len(canonical)

    def planned_work(self,ordinals):
        self.check();count=total=0;n=len(self._states)//STATE.size
        for ordinal in ordinals:
            if type(ordinal) is not int or not 0<=ordinal<n:raise ValueError('native record ordinal outside capture')
            total+=STATE.unpack_from(self._states,ordinal*STATE.size)[1];count+=1
        return {'native_records_decoded':count,'native_record_bytes_read':total}

    def location(self,ordinal):
        self.check()
        if type(ordinal) is not int or not 0<=ordinal<len(self._states)//STATE.size:raise ValueError('native record ordinal outside capture')
        offset,_,base,_,_,_,_=STATE.unpack_from(self._states,ordinal*STATE.size)
        return offset,base

    def project(self,ordinal,columns,*,index=None,bucket_id=None,object_uri=None):
        self.check();columns=tuple(columns)
        metadata=self._metadata_cache.get(ordinal) if self.cache_projections and 'metadata_json' in columns else None
        if not self.cache_projections or 'decoder_state_json' in columns or ('metadata_json' in columns and metadata is None):
            row,read,_=self.record(ordinal,index=index,bucket_id=bucket_id,object_uri=object_uri)
            return {name:row[name] for name in columns},{'native_records_decoded':1,'native_record_bytes_read':read,
                'native_cached_projection_rows':0,'native_projected_bytes_read':0}
        offset,base=self.location(ordinal)
        _,length,_,host,source,stype,flags=STATE.unpack_from(self._states,ordinal*STATE.size)
        raw_size,index_seconds,subsecond=PROJECTION.unpack_from(self._projections,ordinal*PROJECTION.size)
        if raw_size>length or offset+length>len(self._blob):raise ValueError('native cached projection span outside capture')
        needed=set(columns);row={};read=0
        if 'metadata_json' in needed:
            row['metadata_json']=metadata
            read+=len(metadata) if metadata.isascii() else len(metadata.encode('utf8'))
        constants={'schema_version':3,'bucket_disabled':self._context.disabled,'bucket_context_json':self._context.context_json,
            'index':index,'bucket_id':bucket_id,'object_uri':object_uri,'source_sha256':self.sha256,
            'record_ordinal':ordinal,'decompressed_offset':offset,
            'decoder':journal.EXTENDED_EVENT_DECODER if flags&4 else journal.DECODER}
        for name in needed&constants.keys():row[name]=constants[name]
        for name,key,active in (('host',3,host),('source',4,source),('sourcetype',5,stype)):
            if name in needed:
                row[name]=self._fields[key][active-1].encode('utf8','surrogateescape') if active else None
        if 'event_time' in needed:row['event_time']=exact_time(base,subsecond)
        if 'index_time' in needed:row['index_time']=exact_time(index_seconds)
        if 'is_deleted' in needed or 'decoder_warnings' in needed:
            state={'header':self.header,'index_time_candidate_s':index_seconds,'event_time_second_candidate':base,
                'active_host':host,'active_source':source,'active_sourcetype':stype}
            context=copy.copy(self._context);context.matched=set();deleted=context.classify(offset,state)
            if 'is_deleted' in needed:row['is_deleted']=deleted
            if 'decoder_warnings' in needed:
                row['decoder_warnings']=journal._decoder_warnings(True,state['index_candidate_within_bucket_info'],bool(flags&1))
        if 'raw' in needed or 'display_text' in needed:
            raw=self._blob[offset+length-raw_size:offset+length];read+=raw_size
            if 'raw' in needed:row['raw']=raw
            if 'display_text' in needed:row['display_text']=raw.decode('utf8','replace')
        if 'record_wire' in needed:row['record_wire']=self._blob[offset:offset+length];read+=length
        if set(row)!=needed:raise ValueError('unsupported native cached projection field')
        return row,{'native_records_decoded':0,'native_record_bytes_read':0,'native_cached_projection_rows':1,
            'native_projected_bytes_read':read}

    def close(self):
        self.closed=True;self.ready=False;self._blob=self._states=self._digests=b''
        self._projections=None
        if self._metadata_cache is not None:self._metadata_cache.close()
        self._metadata_cache=None
        self._fields=MappingProxyType({});self._stream=None;self._context=None
