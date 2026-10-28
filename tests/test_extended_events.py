"""Native-generated event fixture and conventional Float64/span unit tests."""
import base64
import copy
import csv
import re
import shlex
import hashlib
import json
from pathlib import Path
import struct

import pytest
from obs_migrate import events, journal
from obs_migrate.event_bucket import EventBucket

FIXTURE=Path(__file__).parent/'fixtures/extended-events-10.6'


def test_numeric_metadata_preserves_operands_exact_bits_and_input():
    entries=[{'key':'bnVtYmVy','representation':10,'operand':'0','storage_offset':'0','wire':'AA=='}]
    entries.append({'representation':0,'key':'dGFn','value_base64':'Y2Fmw6k='})
    original=copy.deepcopy(entries);wire=struct.pack('<d',-0.0)
    actual=journal._extended_event_metadata(entries,wire)
    assert entries==original and actual[1]==entries[1]
    assert {k:v for k,v in actual[0].items() if k not in ('value_float64_bits_le_base64','value_float64_hex')}==entries[0]
    assert actual[0]['value_float64_bits_le_base64']==base64.b64encode(wire).decode()
    assert actual[0]['value_float64_hex']==(-0.0).hex()


@pytest.mark.parametrize('offsets,wire',[
    ([1],struct.pack('<d',1.25)),([-1],struct.pack('<d',1.25)),
    ([8],struct.pack('<d',1.25)),([0,0],struct.pack('<dd',1.25,2.5)),
    ([0],struct.pack('<dd',1.25,2.5)),([],b''),([0],b'1234567'),
    ([0],struct.pack('<d',float('nan'))),([0],struct.pack('<d',float('inf'))),
])
def test_unverified_numeric_storage_layouts_refuse(offsets,wire):
    entries=[{'representation':10,'storage_offset':str(offset)} for offset in offsets]
    with pytest.raises(ValueError):journal._extended_event_metadata(entries,wire)


def test_extended_metadata_representations_stay_narrow():
    with pytest.raises(ValueError):journal._extended_event_metadata([{'representation':8}],struct.pack('<d',1.25))


def rows():
    bucket=next(p for p in FIXTURE.iterdir() if p.is_dir())
    return bucket,list(journal.events(bucket,experimental=True,index_type='event'))


def test_genuine_extended_event_profile_lossless():
    manifest=json.loads((FIXTURE/'manifest.json').read_text())
    for name,digest in manifest['sha256'].items():assert hashlib.sha256((FIXTURE/name).read_bytes()).hexdigest()==digest
    bucket,actual=rows();assert len(actual)==3
    inputs=json.loads((FIXTURE/'inputs.json').read_text())
    with journal.open_source(journal.resolve(bucket)) as (stream,*_):wire=stream.read()
    for row in actual:
        original=next(p for p in inputs if p['event'].encode()==row['raw'])
        assert row['host']==b'host::fixture-host'
        state=json.loads(row['decoder_state_json']);extension=base64.b64decode(state['extended_storage_base64'],validate=True)
        assert len(extension)==24 and state['wire_components']['opcode']==39
        assert state['extended_event_profile']=='v1-align5-opcode39-float64-offsets'
        assert row['decoder']==journal.EXTENDED_EVENT_DECODER
        assert row['event_time']=={'seconds':int(original['time']),'fraction':'0.125'}
        assert wire[row['decompressed_offset']:row['decompressed_offset']+len(row['record_wire'])]==row['record_wire']
        numeric=[e for e in json.loads(row['metadata_json']) if e['representation']==10]
        assert len(numeric)==3
        for e in numeric:
            key=base64.b64decode(e['key']).decode();offset=int(e['storage_offset']);bits=base64.b64decode(e['value_float64_bits_le_base64'],validate=True)
            assert extension[offset:offset+8]==bits==struct.pack('<d',original['fields'][key])
        assert events.from_json(events.json_bytes(row))==row


@pytest.mark.parametrize('index_type',['unknown','metric'])
def test_extended_event_requires_explicit_event_declaration(index_type):
    bucket=next(p for p in FIXTURE.iterdir() if p.is_dir())
    with pytest.raises(journal.JournalError):list(journal.events(bucket,experimental=True,index_type=index_type))


def test_genuine_truncated_extended_record_never_publishes(tmp_path):
    bucket,actual=rows()
    with journal.open_source(journal.resolve(bucket)) as (stream,*_):wire=stream.read()
    row=actual[0];path=tmp_path/'truncated';path.write_bytes(wire[:row['decompressed_offset']+len(row['record_wire'])-1])
    # Direct iteration must refuse truncation, rather than accept the first event.
    from obs_migrate.export import export
    output=tmp_path/'failed.parquet'
    with pytest.raises(journal.JournalError):export(journal.events(path,experimental=True,index_type='event'),output)
    assert not output.exists() and not list(tmp_path.glob('.obs-migrate-*'))


@pytest.mark.parametrize('budget',[0,512,1048576])
def test_extended_addressed_cache_and_parquet_parity(tmp_path,budget):
    bucket,canonical=rows()
    with EventBucket(bucket,experimental=True,offline_snapshot=True,backend='journal',cache_projections=True,metadata_cache_bytes=budget) as source:
        linked=list(source.records());assert [{k:v for k,v in row.items() if k!='index_link'} for row in linked]==canonical
        columns=('raw','metadata_json','decoder_state_json','event_time','index_time')
        assert list(source.projected_records(columns))==[{k:row[k] for k in columns} for row in canonical]
    from obs_migrate.export import export,verify
    path=tmp_path/'events.parquet';export(iter(canonical),path,'parquet');result=verify(iter(canonical),path,'parquet');assert result['verified_events']==3


@pytest.mark.parametrize('mutation',['opcode','header'])
def test_other_extended_profiles_remain_refused(tmp_path,mutation):
    bucket,actual=rows()
    with journal.open_source(journal.resolve(bucket)) as (stream,*_):wire=bytearray(stream.read())
    if mutation=='opcode':wire[actual[0]['decompressed_offset']]=38
    else:
        assert wire[:3]==bytes([10,1,5]);wire[2]=0
    path=tmp_path/'unsupported';path.write_bytes(wire)
    with pytest.raises(journal.JournalError):list(journal.events(path,experimental=True,index_type='event'))


def test_native_csv_reference_agrees_with_all_genuine_events():
    bucket,actual=rows()
    with (FIXTURE/'native.csv').open(encoding='utf8',newline='') as file:native=list(csv.DictReader(file))
    assert len(native)==len(actual)==3
    for row,want in zip(actual,native,strict=True):
        assert row['raw']==want['_raw'].encode()
        for key in ('host','source','sourcetype'):assert row[key]==want[key].encode()
        index=re.search(r'(?:^| )_indextime::([0-9]+)',want['_meta']);assert index
        assert row['index_time']=={'seconds':int(index.group(1)),'fraction':'0'}
        for entry in json.loads(row['metadata_json']):
            key=base64.b64decode(entry['key']).decode()
            if entry['representation']==10:
                token=re.search(r'(?:^| )'+re.escape(key)+r'::([^ ]+)',want['_meta']);assert token
                assert struct.pack('<d',float(token.group(1)))==base64.b64decode(entry['value_float64_bits_le_base64'])
            else:assert key+'::'+base64.b64decode(entry['value_base64']).decode() in shlex.split(want['_meta'])
