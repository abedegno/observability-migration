"""Native LZ4 fixtures; transport mutations are negative cases, never formats."""
from collections import Counter
import csv
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import struct

import pytest
import lz4.frame
from obs_migrate import journal,metric_journal,msidx,buckets,bucket_catalogue
from obs_migrate.cli import main

ROOT=Path(__file__).parent/'fixtures/native-lz4-10.6'

def bucket(index='matrix_events'):
    value,=list((ROOT/'warm'/index).iterdir());return value

def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def test_native_fixture_provenance_and_hashes():
    manifest=json.loads((ROOT/'manifest.json').read_text())
    assert manifest['license']=='CC0-1.0'
    for name,wanted in manifest['files'].items():assert digest(ROOT/name)==wanted
    provenance=json.loads((ROOT/'provenance.json').read_text())
    assert provenance['offline_capture']=='daemon stopped before final copies'
    for index in provenance['indexes'].values():assert 'journalCompression = lz4' in index['effective_settings']

def test_genuine_native_events_and_independent_csv(tmp_path):
    source=bucket();stats={};actual=list(journal.events(source,experimental=True,index_type='event',stats=stats))
    with (ROOT/f'matrix_events-{source.name}.csv').open(newline='') as f:native=list(csv.DictReader(f))
    assert len(actual)==len(native)==4
    for row,want in zip(actual,native,strict=True):
        assert row['raw']==want['_raw'].encode()
        for key in ('host','source','sourcetype'):assert row[key]==want[key].encode()
        sub=re.search(r'_subsecond::([^ ]+)',want['_meta']).group(1)
        assert Decimal(row['event_time']['seconds'])+Decimal(row['event_time']['fraction'])==Decimal(want['_time'])+Decimal(sub)
        assert row['index_time']['seconds']==int(re.search(r'_indextime::([0-9]+)',want['_meta']).group(1))
        assert row['source_sha256']==digest(source/'rawdata/journal.lz4')
    assert stats['compression']=='lz4' and stats['source_bytes_read']==3*(source/'rawdata/journal.lz4').stat().st_size
    # Exactly the original decompressed payload, with all bucket context retained.
    copy=tmp_path/source.name;shutil.copytree(source,copy)
    compressed=copy/'rawdata/journal.lz4';wire=lz4.frame.decompress(compressed.read_bytes());compressed.unlink();(copy/'rawdata/journal').write_bytes(wire)
    plain=list(journal.events(copy,experimental=True,index_type='event'))
    for row,want in zip(actual,plain,strict=True):
        assert {k:v for k,v in row.items() if k not in ('source_sha256','bucket_context_json')}=={k:v for k,v in want.items() if k not in ('source_sha256','bucket_context_json')}
        assert wire[row['decompressed_offset']:row['decompressed_offset']+len(row['record_wire'])]==row['record_wire']
    assert Counter(r['raw'] for r in actual)[native[-1]['_raw'].encode()]==2

@pytest.mark.parametrize('index,points',[('metrics_retained',5),('metrics_multi',1)])
def test_genuine_lz4_metric_journal_exact_bits_match_native_index(index,points):
    source=bucket(index);actual=list(metric_journal.records(source,experimental=True))
    indexed=[row for path in source.glob('*.tsidx') for row in msidx.records(path,experimental=True)]
    def signature(row):
        return (row['host'],row['source'],row['sourcetype'],tuple(sorted((d['key'],d['value']) for d in row['dimensions'])),tuple((m['name'],m['value_bits_le']) for m in row['measurements']))
    assert len(actual)==len(indexed)==points
    assert Counter(map(signature,actual))==Counter(map(signature,indexed))
    assert all(row['source_compression']=='lz4' and row['indexed_time'] is None and row['source_sha256']==digest(source/'rawdata/journal.lz4') for row in actual)
    # Independently controlled submissions, not another decoder's value oracle.
    submitted=json.loads((ROOT/'metric-submissions.json').read_text())
    expected=[]
    for item in submitted:
        p=item['payload']
        if p['index']!=index:continue
        fields=p['fields'];measurements=([(fields['metric_name'].encode(),struct.pack('<d',fields['_value']))] if 'metric_name' in fields else [(k[12:].encode(),struct.pack('<d',v)) for k,v in fields.items() if k.startswith('metric_name:')])
        expected.append((Decimal(str(p['time'])),tuple(sorted(measurements))))
    assert Counter((Decimal(r['journal_time']['seconds'])+Decimal(r['journal_time']['fraction']),tuple(sorted((m['name'],m['value_bits_le']) for m in r['measurements']))) for r in actual)==Counter(expected)

def test_stub_retains_existing_refusal():
    with pytest.raises(journal.JournalError,match='stub|no.*metric|no records'):
        list(metric_journal.records(bucket('metrics_default'),experimental=True))

def test_local_discovery_inspect_and_atomic_cli_parquet(tmp_path):
    source=bucket();listed=buckets.inventory(source.parent)
    assert len(listed)==1 and listed[0]['journal_candidates']==1
    report=bucket_catalogue.catalogue(source.parent,offline_snapshot=True)
    entry,=report['buckets']
    assert entry['name']==source.name and entry['journals'][0]['path']=='rawdata/journal.lz4'
    assert entry['journals'][0]['sha256']==digest(source/'rawdata/journal.lz4')
    assert 'missing_or_ambiguous_journal' not in entry['evidence']
    assert bucket_catalogue.select(report,[source.name])['buckets']==[source.name]
    assert journal.inspect(source)['compression']=='lz4'
    output=tmp_path/'events.parquet'
    assert main(['buckets','export',str(source),'--experimental','--index-type','event','--out',str(output)])==0
    assert main(['buckets','verify',str(source),'--experimental','--index-type','event','--against',str(output)])==0

@pytest.mark.parametrize('kind',['truncated_header','truncated_end','corrupt_header','corrupt_block','trailing'])
def test_damaged_genuine_lz4_never_publishes(tmp_path,kind):
    wire=bytearray((bucket()/'rawdata/journal.lz4').read_bytes())
    if kind=='truncated_header':wire=wire[:5]
    elif kind=='truncated_end':wire=wire[:-1]
    elif kind=='corrupt_header':wire[6]^=1
    elif kind=='corrupt_block':wire[11]=0xff;wire[12]=0xff
    else:wire+=b'unexpected trailing bytes'
    path=tmp_path/'damaged.lz4';path.write_bytes(wire);output=tmp_path/'bad.parquet'
    assert main(['buckets','export',str(path),'--experimental','--index-type','event','--out',str(output)])==1
    assert not output.exists() and not list(tmp_path.glob('.obs-migrate-*'))

def test_lz4_budget_preflight_happens_before_first_row():
    with pytest.raises(journal.JournalError,match='decompressed.*bound|decompressed.*limit'):
        next(journal.events(bucket(),experimental=True,index_type='event',max_decompressed_bytes=1))

def test_lz4_source_change_and_trusted_hash(tmp_path):
    path=tmp_path/'journal.lz4';original=(bucket()/'rawdata/journal.lz4').read_bytes();path.write_bytes(original)
    with pytest.raises(ValueError,match='SHA|sha|digest'):
        list(journal.events(path,experimental=True,index_type='event',expected_source_sha256='0'*64))
    rows=journal.events(path,experimental=True,index_type='event');next(rows)
    path.write_bytes(original+b'changed')
    with pytest.raises(journal.JournalError,match='source changed'):
        list(rows)


def test_transport_is_magic_detected_and_fully_validated(tmp_path):
    path=tmp_path/'misleading.gz';path.write_bytes((bucket()/'rawdata/journal.lz4').read_bytes())
    with journal.open_source(path) as (stream,sha,compression,reader):
        assert compression=='lz4' and sha==digest(path)
        assert stream.read()==lz4.frame.decompress(path.read_bytes())


def test_missing_lz4_dependency_has_cli_recovery(tmp_path):
    import subprocess,sys
    output=tmp_path/'missing.parquet'
    code='''import sys,importlib.abc
class Missing(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname=='lz4' or fullname.startswith('lz4.'):
   raise ModuleNotFoundError('codec unavailable',name='lz4')
sys.meta_path.insert(0,Missing())
from obs_migrate.cli import main
raise SystemExit(main(sys.argv[1:]))
'''
    result=subprocess.run([sys.executable,'-c',code,'buckets','export',str(bucket()),'--experimental','--out',str(output)],capture_output=True,text=True)
    assert result.returncode==1 and not output.exists()
    error=json.loads(result.stderr)['error']
    assert 'required dependency lz4' in error and 'pip install' in error


def test_standard_transport_integrity_and_output_bound(tmp_path):
    # Ordinary compression data tests the codec wrapper, not a native journal.
    from obs_migrate.lz4_integrity import preflight,CHUNK_BYTES,FrameIntegrityError
    data=b'bounded transport\x00\xff'*150000
    encoded=lz4.frame.compress(data,content_checksum=True)+lz4.frame.compress(b'tail',content_checksum=True)
    report=preflight(io.BytesIO(encoded),max_output_bytes=len(data)+4)
    assert report['frames']==2 and report['decompressed_bytes']==len(data)+4
    assert report['maximum_returned_chunk_bytes']<=CHUNK_BYTES
    with pytest.raises(FrameIntegrityError,match='limit'):preflight(io.BytesIO(encoded),max_output_bytes=1)
    broken=bytearray(encoded);broken[-1]^=1
    with pytest.raises(FrameIntegrityError,match='integrity'):preflight(io.BytesIO(broken))
    path=tmp_path/'transport';path.write_bytes(encoded)
    with journal.open_source(path) as (stream,*_):assert stream.read()==data+b'tail'


def test_retained_decoder_failure_closes_codec_and_allows_reuse(monkeypatch):
    original=lz4.frame.LZ4FrameFile;streams=[]
    def tracked(*args,**kwargs):
        value=original(*args,**kwargs);streams.append(value);return value
    monkeypatch.setattr(lz4.frame,'LZ4FrameFile',tracked)
    row_builder=journal._event_row
    def fail(*args,**kwargs):raise ValueError('retained native LZ4 decoder failure')
    monkeypatch.setattr(journal,'_event_row',fail)
    with pytest.raises(journal.JournalError,match='retained native LZ4') as retained:
        list(journal.events(bucket(),experimental=True,index_type='event'))
    assert retained.value.__traceback__ and streams and all(s.closed for s in streams)
    monkeypatch.setattr(journal,'_event_row',row_builder)
    rows=journal.events(bucket(),experimental=True,index_type='event');next(rows);rows.close()
    assert all(s.closed for s in streams)
    assert len(list(journal.events(bucket(),experimental=True,index_type='event')))==4
    assert all(s.closed for s in streams)

OLDER=ROOT.parent/'native-lz4-7.0'


def test_genuine_older_lz4_events_and_native_csv():
    manifest=json.loads((OLDER/'manifest.json').read_text())
    for name,wanted in manifest['files'].items():assert digest(OLDER/name)==wanted
    source,=list((OLDER/'warm/matrix_events').iterdir());actual=list(journal.events(source,experimental=True,index_type='event'))
    with (OLDER/f'matrix_events-{source.name}.csv').open(newline='') as f:native=list(csv.DictReader(f))
    assert len(actual)==len(native)==4
    for row,want in zip(actual,native,strict=True):
        assert row['raw']==want['_raw'].encode()
        for key in ('host','source','sourcetype'):assert row[key]==want[key].encode()
        sub=re.search(r'_subsecond::([^ ]+)',want['_meta']).group(1)
        assert Decimal(row['event_time']['seconds'])+Decimal(row['event_time']['fraction'])==Decimal(want['_time'])+Decimal(sub)
        assert row['index_time']['seconds']==int(re.search(r'_indextime::([0-9]+)',want['_meta']).group(1))
        assert row['source_sha256']==digest(source/'rawdata/journal.lz4')

@pytest.mark.parametrize('index',['metrics_default','metrics_retained'])
def test_genuine_older_lz4_retained_metrics(index):
    from obs_migrate import msidx1
    source,=list((OLDER/'warm'/index).iterdir());actual=list(metric_journal.records(source,experimental=True))
    indexed=[row for p in source.glob('*.tsidx') for row in msidx1.records(p,experimental=True)]
    def signature(row):
        return (row['host'],row['source'],row['sourcetype'],tuple(sorted((d['key'],d['value']) for d in row['dimensions'])),tuple((m['name'],m['value_bits_le']) for m in row['measurements']))
    assert len(actual)==len(indexed)==5 and Counter(map(signature,actual))==Counter(map(signature,indexed))
    submitted=json.loads((OLDER/'metric-submissions.json').read_text())
    expected=Counter((Decimal(str(p['payload']['time'])),p['payload']['fields']['metric_name'].encode(),struct.pack('<d',p['payload']['fields']['_value'])) for p in submitted if p['payload']['index']==index)
    assert Counter((Decimal(r['journal_time']['seconds'])+Decimal(r['journal_time']['fraction']),r['measurements'][0]['name'],r['measurements'][0]['value_bits_le']) for r in actual)==expected
    assert all(row['source_compression']=='lz4' and row['source_sha256']==digest(source/'rawdata/journal.lz4') for row in actual)


@pytest.mark.parametrize('fixture',[ROOT,OLDER])
@pytest.mark.parametrize('backend',['arrow','journal'])
def test_lz4_bucket_backends_keep_all_canonical_fields(fixture,backend):
    from obs_migrate.event_bucket import EventBucket
    source,=list((fixture/'warm/matrix_events').iterdir())
    canonical=list(journal.events(source,experimental=True,index_type='event'))
    with EventBucket(source,experimental=True,offline_snapshot=True,backend=backend) as snapshot:
        actual=list(snapshot.records())
        assert [{k:v for k,v in r.items() if k!='index_link'} for r in actual]==canonical


def test_lz4_index_job_export_verify_and_completed_resume(tmp_path):
    from obs_migrate.index_job import InputSpec,plan_job
    from obs_migrate.job_dataset import export_job,verify_job,read_job
    source=bucket()
    plan=plan_job([InputSpec('lz4','local',source.parent,(source.name,),index='sample')],offline_snapshot=True)
    output=tmp_path/'job';receipt=export_job(plan,output)
    assert receipt['events']==4
    assert verify_job(plan,output)['verified_events']==4
    before={p.relative_to(output):p.read_bytes() for p in output.rglob('*') if p.is_file()}
    assert export_job(plan,output,resume=True)['events']==4
    assert before=={p.relative_to(output):p.read_bytes() for p in output.rglob('*') if p.is_file()}
    assert sum(1 for _ in read_job(output))==4


def test_integrity_reads_concatenated_transport_once_without_rereading():
    from obs_migrate.lz4_integrity import preflight
    class Counted(io.BytesIO):
        consumed=0
        def read(self,*args):
            value=super().read(*args);self.consumed+=len(value);return value
    # Standard compression frames containing ordinary bytes, not journal fixtures.
    encoded=b''.join(lz4.frame.compress(bytes([i])*20) for i in range(12))
    source=Counted(encoded);report=preflight(source)
    assert report['frames']==12 and report['decompressed_bytes']==240
    assert source.consumed==len(encoded)
