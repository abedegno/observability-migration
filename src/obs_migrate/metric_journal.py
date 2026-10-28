"""Physical metric records from retained native journals, without Splunk binaries.

Observed profile only: journal v1/alignment 5, opcode 39, metadata 0/10.
Journal timestamps are preserved; indexed timestamps and visibility are unverified.
"""
import base64
import json
import math
from pathlib import Path
import struct

import pyarrow as pa

from .events import EXACT_TIME
from .journal import decoder_class,open_source,resolve,JournalError,PIN
from .wire import event_components,exact_time,signed32
from .source_identity import SourceIdentityError

FORMAT='splunk-retained-metric-journal'
DECODER='splunk-ddss-python@'+PIN+'+obs-metric-journal-1'
MEASUREMENT=pa.struct([pa.field('name',pa.binary(),False),pa.field('value_bits_le',pa.binary(8),False),
                       pa.field('value_float64',pa.float64(),False)])
DIMENSION=pa.struct([pa.field('key',pa.binary(),False),pa.field('value',pa.binary(),False)])
SCHEMA=pa.schema([
    pa.field('schema_version',pa.uint16(),False),pa.field('source_format',pa.string(),False),
    pa.field('journal_time',EXACT_TIME,False),pa.field('index_time',EXACT_TIME,False),pa.field('indexed_time',EXACT_TIME),
    pa.field('host',pa.binary()),pa.field('source',pa.binary()),pa.field('sourcetype',pa.binary()),
    pa.field('measurements',pa.list_(pa.field('element',MEASUREMENT)),False),
    pa.field('dimensions',pa.list_(pa.field('element',DIMENSION)),False),
    pa.field('raw',pa.binary(),False),pa.field('extended_storage',pa.binary(),False),pa.field('record_wire',pa.binary(),False),
    pa.field('metadata_json',pa.string(),False),pa.field('decoder_state_json',pa.string(),False),
    pa.field('index',pa.string()),pa.field('bucket_id',pa.string()),
    pa.field('source_sha256',pa.string(),False),pa.field('source_compression',pa.string(),False),
    pa.field('record_ordinal',pa.uint64(),False),pa.field('decompressed_offset',pa.uint64(),False),
    pa.field('decoder',pa.string(),False),
],metadata={b'obs_migrate.metric_journal_schema':b'1',b'access':b'physical_retained_journal;no_vendor_binary',
            b'indexed_time':b'unverified;never_substitute_journal_time',b'visibility':b'physical;indexed_deletions_unverified',
            b'float64':b'exact_extended_storage_bits;derived_float64_projection'})
BINARY_FIELDS=('host','source','sourcetype','raw','extended_storage','record_wire')


def decode_measurements(entries,extension):
    strings=[(base64.b64decode(e['key']),base64.b64decode(e['value_base64'])) for e in entries if e['representation']==0]
    names=[v for k,v in strings if k==b'metric_name']
    measured=[]
    for entry in entries:
        if entry['representation']!=10:continue
        key=base64.b64decode(entry['key']);offset=int(entry['storage_offset'])
        if offset<0 or offset+8>len(extension):raise JournalError('Float64 storage offset crosses extended block')
        bits=extension[offset:offset+8];value=struct.unpack('<d',bits)[0]
        if not math.isfinite(value):raise JournalError('nonfinite metric journal profile not yet verified')
        if key==b'_value':
            if len(names)!=1 or not names[0]:raise JournalError('ambiguous/missing legacy metric name')
            name=names[0]
        elif key.startswith(b'metric_name:') and len(key)>12:
            if names:raise JournalError('mixed legacy/named measurement profile unverified')
            name=key[12:]
        else:raise JournalError('unverified Float64 metadata role')
        measured.append({'name':name,'value_bits_le':bits,'value_float64':value})
    if not measured:raise JournalError('no metric measurements in extended record')
    dimensions=[{'key':k,'value':v} for k,v in strings if k!=b'metric_name']
    if any(k.startswith(b'metric_name:') or k==b'_value' for k,v in strings):
        raise JournalError('string-valued measurement profile unverified')
    return measured,dimensions


def records(path, *, experimental=False,index=None,bucket_id=None,expected_source_sha256=None):
    if not experimental:raise JournalError('metric journal decoder is experimental; pass --experimental')
    source=resolve(path);count=0
    try:
        cls=decoder_class(metric_storage=True)
        source_context=open_source(source) if expected_source_sha256 is None else open_source(source,expected_sha256=expected_source_sha256)
        with source_context as (stream,sha,compression,reader):
            decoder=cls(stream)
            while decoder.scan():
                if decoder.header is None or (decoder.header['version'],decoder.header['align_bits'])!=(1,5) or not decoder.state_time_seen:
                    raise JournalError('unverified metric journal time/header profile')
                event=decoder.get_event();wire=decoder.record_wire
                components=event_components(wire,allow_extended=True)
                end=len(wire)-len(event.message);length=components['extended_storage_length']
                if length!=event.extended_storage_len or length>end:raise JournalError('invalid metric extended block length')
                extension=wire[end-length:end]
                measurements,dimensions=decode_measurements(decoder.metadata_entries,extension)
                binary=lambda value:value.encode('utf8','surrogateescape')
                routing={key:binary(value) if active else None for key,value,active in (
                    ('host',event.host,decoder.active_host),('source',event.source,decoder.active_source),
                    ('sourcetype',event.sourcetype,decoder.active_source_type))}
                projected={}
                for key,value in routing.items():
                    prefix=key.encode()+b'::'
                    if value is not None and not value.startswith(prefix):raise JournalError('unverified metric routing dictionary profile')
                    projected[key]=value[len(prefix):] if value is not None else None
                state={'header':decoder.header,'base_event_time':decoder.base_event_time,'components':components,
                       'indexed_time':'UNVERIFIED','visibility':'physical;indexed_deletions_unverified',
                       'routing_dictionary_base64':{k:base64.b64encode(v).decode() if v is not None else None for k,v in routing.items()},
                       'private_data_seen':decoder.private_seen,'source_archive_required':True}
                yield {'schema_version':1,'source_format':FORMAT,
                    'journal_time':exact_time(decoder.base_event_time,components['subsecond_wire']),
                    'index_time':exact_time(decoder.base_index_time+signed32(components['index_delta_wire'])),
                    'indexed_time':None,**projected,
                    'measurements':measurements,'dimensions':dimensions,'raw':event.message,
                    'extended_storage':extension,'record_wire':wire,
                    'metadata_json':json.dumps(decoder.metadata_entries,sort_keys=True,separators=(',',':')),
                    'decoder_state_json':json.dumps(state,sort_keys=True,separators=(',',':')),
                    'index':index,'bucket_id':bucket_id,'source_sha256':sha,'source_compression':compression,
                    'record_ordinal':count,'decompressed_offset':decoder.record_offset,'decoder':DECODER}
                count+=1
            if decoder.err():raise JournalError('decode failed: '+str(decoder.err()))
            if not decoder.reader._eof or decoder.reader.buffer:raise JournalError('incomplete metric journal')
            if not count:raise JournalError('no physical metric records; a journal stub cannot recover indexed metrics. Preserve the full bucket/TSIDX')
    except SourceIdentityError:raise
    except (OSError,ValueError,RuntimeError) as error:
        raise JournalError(f'{source.name}: metric record {count}: {error}') from error


# Shared transport keeps both native storage readers on identical publication,
# batching and all-field verification behavior; the journal schema stays intact.
from .metric_io import MetricRecordIO
_io=MetricRecordIO(SCHEMA,FORMAT,BINARY_FIELDS,count_field='physical_records',
                   status={'indexed_time':'UNVERIFIED','search_visibility':'UNVERIFIED'})
json_bytes=_io.json_bytes
from_json=_io.from_json
batches=_io.batches
export=_io.export
read_export=_io.read_export
verify=_io.verify
