"""Literal native replica-uploaded masks; mutations are refusal witnesses only."""
from collections import Counter
import csv
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import pytest
from obs_migrate import event_bucket,event_index,oracle
from obs_migrate.smartstore import AcquiredObjects

FIXTURE=Path(__file__).parent/'fixtures/smartstore-foreign-deletes-10.6'
PHASES=('00-failover','01-deleted','02-evicted','03-rehydrated')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def source(phase):
    path=FIXTURE/phase/'acquired'
    acquired=AcquiredObjects(path,offline_snapshot=True,expected_inventory_sha256=sha(path/'objects.json'))
    bucket,=acquired.report()['buckets'];return acquired,bucket


def stored(values):
    return [{k:v for k,v in row.items() if k not in ('is_deleted','bucket_context_json','index_link')} for row in values]


def test_original_replica_delete_fixture_hashes_and_native_oracles():
    manifest=json.loads((FIXTURE/'manifest.json').read_text())
    assert manifest['license']=='CC0-1.0'
    assert {p.relative_to(FIXTURE).as_posix():sha(p) for p in FIXTURE.rglob('*') if p.is_file() and p!=FIXTURE/'manifest.json'}==manifest['files']
    provenance=json.loads((FIXTURE/'provenance.json').read_text())
    assert provenance['native_origin_guid']!=provenance['native_uploader_guid']
    assert (provenance['physical_events'],provenance['deleted_events'],provenance['selected_events'])==(14,3,11)
    for label,count in [('before-delete',14),('after-delete',11),('before-eviction',11),('after-rehydration',11)]:
        assert len(list(oracle.search_rows(FIXTURE/(label+'.native.jsonl'))))==count
    exported=json.loads((FIXTURE/'02-evicted/native-exports.json').read_text())
    assert len(exported)==1 and exported[0]['exit_code']==12
    assert not (FIXTURE/'02-evicted/native.csv').exists()


@pytest.mark.parametrize('phase',PHASES)
@pytest.mark.parametrize('backend',['arrow','journal'])
def test_replica_masks_preserve_every_field_and_match_native_selection(tmp_path,phase,backend):
    acquired,bucket=source(phase);baseline,original=source('00-failover')
    assert bucket['id']==original['id'] and bucket['copy_identity']==original['copy_identity']
    bodies=lambda src,b:{n:src._objects[key] for n,key in b['members'].items() if not n.startswith('rawdata/deletes/')}
    assert bodies(acquired,bucket)==bodies(baseline,original)
    tsidx,=[wire for name,wire in bodies(acquired,bucket).items() if name.endswith('.tsidx')]
    with event_index.open_bytes(tsidx,experimental=True) as index:
        assert tsidx[:8]==b'_TSIDX8\0' and index.reader.flags==0
        section=index.reader.sections['delete'];assert not any(tsidx[section['offset']:section['offset']+section['bytes']])
    before={p:sha(p) for p in (FIXTURE/phase).rglob('*') if p.is_file()}
    with baseline.event_bucket(original['id'],experimental=True,backend=backend) as snapshot:initial=snapshot.table().to_pylist()
    with acquired.event_bucket(bucket['id'],experimental=True,backend=backend) as physical:
        rows=physical.table().to_pylist();assert len(rows)==14 and stored(rows)==stored(initial)
        assert Counter(r['raw'] for r in rows)==Counter((FIXTURE/'00-failover/event-input.log').read_bytes().splitlines())
        expected=[r for r in rows if r['is_deleted'] is False]
        assert len(expected)==(14 if phase=='00-failover' else 11)
        if phase!='00-failover':
            assert len([r for r in rows if r['is_deleted']])==3
            with pytest.raises(ValueError,match='disagrees'):physical.deletion_report()
        for visibility,wanted in [('physical',rows),('undeleted',expected)]:
            with acquired.event_bucket(bucket['id'],experimental=True,backend=backend,visibility=visibility) as selected:
                assert selected.table().to_pylist()==wanted and selected.report()['fully_validated_events']==14
                if visibility=='undeleted' and phase!='00-failover':
                    report=selected.deletion_report();assert report['native_base_deleted_values']==0 and report['journal_mask_deleted_values']==3
                    assert report['search_visibility']=='UNVERIFIED' and not report['indexed_search_supported']
                for format in ('arrow','parquet'):
                    output=tmp_path/(visibility+'.'+format);event_bucket.export(selected,output,format)
                    assert event_bucket.verify(selected,output,format)['verified_events']==len(wanted)
                empty=tmp_path/(visibility+'.empty');event_bucket.export(selected,empty,term=b'__absent__')
                assert event_bucket.verify(selected,empty,term=b'__absent__')['verified_events']==0
                records=selected.records();next(records);records.close();assert selected.table().num_rows==len(wanted)
    native_path=FIXTURE/('before-delete.native.jsonl' if phase=='00-failover' else 'after-rehydration.native.jsonl' if phase=='03-rehydrated' else 'after-delete.native.jsonl')
    native=list(oracle.search_rows(native_path))
    assert Counter(oracle.expected_signature(r) for r in expected)==Counter(oracle.actual_signature(r,expected_index='cluster_events_zstd') for r in native)
    assert {r['decompressed_offset']>>5 for r in expected}=={int(r['_cd'].split(':')[1]) for r in native}
    csv_path=FIXTURE/phase/'native.csv'
    if phase!='02-evicted':
        with csv_path.open(newline='') as file:csv_rows=list(csv.DictReader(file))
        signatures=[oracle.expected_signature({'raw':r['_raw'].encode(),**{k:r[k].encode() for k in ('host','source','sourcetype')},'event_time':{'seconds':int(Decimal(r['_time'])),'fraction':re.search(r'_subsecond::([^ ]+)',r['_meta'])[1]},'index_time':{'seconds':int(re.search(r'_indextime::([0-9]+)',r['_meta'])[1])}}) for r in csv_rows]
        assert Counter(signatures)==Counter(oracle.expected_signature(r) for r in expected)
    assert all(sha(p)==digest for p,digest in before.items())


@pytest.mark.parametrize('backend',['arrow','journal'])
def test_rehydrated_native_cache_retains_strict_local_bitmap_agreement(backend):
    local,=(p for p in (FIXTURE/'03-rehydrated/local').iterdir() if p.is_dir())
    with event_bucket.EventBucket(local,experimental=True,offline_snapshot=True,backend=backend,visibility='undeleted') as snapshot:
        assert snapshot.table().num_rows==11 and snapshot.deletion_report()['deleted_values']==3


def damaged(tmp_path,damage):
    path=tmp_path/'acquired';shutil.copytree(FIXTURE/'01-deleted/acquired',path)
    entries=json.loads((path/'objects.json').read_text());mask,=[e for e in entries if '/deletes/' in e['key']]
    if damage=='missing':entries.remove(mask)
    elif damage in ('address','metadata','timestamp'):
        lines=gzip.decompress((path/mask['file']).read_bytes()).decode().splitlines();keys=lines[0].split(',');values=lines[1].split(',')
        column=keys.index({'address':'event_address','metadata':'host_id','timestamp':'timestamp'}[damage]);values[column]=str(Decimal(values[column])+1000);lines[1]=','.join(values)
        wire=gzip.compress(('\n'.join(lines)+'\n').encode(),mtime=0);(path/mask['file']).write_bytes(wire);mask.update(size=len(wire),sha256=hashlib.sha256(wire).hexdigest())
    elif damage=='wrong_uploader':mask['key']=mask['key'].replace('guidSplunk-','guidOther-')
    elif damage=='wrong_bucket':mask['key']='other/'+mask['key']
    elif damage=='extra_generation':
        for i,e in enumerate(list(entries)):
            copy=dict(e,key='other/'+e['key'],file='objects/duplicate-%02d.bin'%i);shutil.copyfile(path/e['file'],path/copy['file']);entries.append(copy)
    else:raise AssertionError(damage)
    (path/'objects.json').write_text(json.dumps(entries)+'\n');return path


@pytest.mark.parametrize('damage',['missing','address','metadata','timestamp','wrong_uploader','wrong_bucket','extra_generation'])
def test_incomplete_or_inconsistent_replica_masks_refuse_even_empty_output(tmp_path,damage):
    path=damaged(tmp_path,damage);output=tmp_path/'never.parquet'
    with pytest.raises(ValueError) as failure:
        acquired=AcquiredObjects(path,offline_snapshot=True);bucket,=acquired.report()['buckets']
        with acquired.event_bucket(bucket['id'],experimental=True,visibility='undeleted') as snapshot:event_bucket.export(snapshot,output,term=b'__absent__')
    assert not output.exists() and failure.value is not None
    acquired,bucket=source('03-rehydrated')
    with acquired.event_bucket(bucket['id'],experimental=True,visibility='undeleted') as snapshot:assert snapshot.table().num_rows==11
