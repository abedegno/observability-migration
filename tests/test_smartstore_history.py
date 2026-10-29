"""Literal native uploader replacement; derived inventories are refusal cases only."""
from collections import Counter
import csv
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import shutil

import pytest
from obs_migrate import event_bucket,oracle
from obs_migrate.smartstore import AcquiredObjects

FIXTURE=Path(__file__).parent/'fixtures/smartstore-history-10.6'
PHASES=('00-source-blocked','01-source-recovered')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def source(phase):
    path=FIXTURE/phase/'acquired'
    acquired=AcquiredObjects(path,offline_snapshot=True,expected_inventory_sha256=sha(path/'objects.json'))
    bucket,=acquired.report()['buckets']
    return acquired,bucket


def stored(rows):
    # Native links, source context and a sidecar-derived diagnostic differ.
    result=[]
    for row in rows:
        values={k:v for k,v in row.items() if k not in ('index_link','object_uri','bucket_context_json','decoder_state_json')}
        state=json.loads(row['decoder_state_json']);state.pop('index_candidate_within_bucket_info')
        result.append({**values,'decoder_state':state})
    return result


def csv_signatures(path):
    with path.open(newline='') as file:rows=list(csv.DictReader(file))
    return Counter(oracle.expected_signature({
        'raw':row['_raw'].encode(),**{k:row[k].encode() for k in ('host','source','sourcetype')},
        'event_time':{'seconds':int(Decimal(row['_time'])),'fraction':re.search(r'_subsecond::([^ ]+)',row['_meta'])[1]},
        'index_time':{'seconds':int(re.search(r'_indextime::([0-9]+)',row['_meta'])[1])}}) for row in rows)


def test_original_history_manifest_and_native_references():
    manifest=json.loads((FIXTURE/'manifest.json').read_text())
    assert manifest['license']=='CC0-1.0'
    assert {p.relative_to(FIXTURE).as_posix():sha(p) for p in FIXTURE.rglob('*') if p.is_file() and p.name!='manifest.json'}==manifest['files']
    provenance=json.loads((FIXTURE/'provenance.json').read_text())
    assert provenance['native_origin_guid']==provenance['recovered_uploader_guid']!=provenance['first_uploader_guid']
    assert provenance['scope']['concurrent_copies_qualified'] is False
    for name in ('before-failure','after-storage-recovery'):
        assert len(list(oracle.search_rows(FIXTURE/(name+'.native.jsonl'))))==14
    assert csv_signatures(FIXTURE/PHASES[0]/'native.csv')==csv_signatures(FIXTURE/PHASES[1]/'native.csv')


def test_same_bucket_and_journal_keep_distinct_receipt_and_uploader_identity():
    before,old=source(PHASES[0]);after,new=source(PHASES[1])
    assert old['id']==new['id'] and old['receipt_key']==new['receipt_key']
    assert old['receipt_sha256']!=new['receipt_sha256'] and before.inventory_sha256!=after.inventory_sha256
    origin=old['manifest']['id'].split('~')[2]
    assert old['copy_identity']=={'origin_guid':origin,'uploader_guid':old['native_user_data']['uploader_guid'],
        'relation':'observed_distinct_origin_uploader','ownership':'UNVERIFIED'}
    assert new['native_user_data']['uploader_guid']==origin and 'copy_identity' not in new
    assert old['journal_key']!=new['journal_key']
    assert before._objects[old['journal_key']]==after._objects[new['journal_key']]
    for acquired,bucket in ((before,old),(after,new)):
        assert acquired.report()['search_visibility']==bucket['search_visibility']=='UNVERIFIED'
        assert not bucket['native_content_hash_verified']
        assert all('/guidSplunk-'+bucket['native_user_data']['uploader_guid']+'/' in key for key in bucket['members'].values())


@pytest.mark.parametrize('phase',PHASES)
@pytest.mark.parametrize('backend',['arrow','journal'])
@pytest.mark.parametrize('visibility',['physical','undeleted'])
def test_historical_capture_preserves_events_provenance_and_transports(tmp_path,phase,backend,visibility):
    before={p:sha(p) for p in FIXTURE.rglob('*') if p.is_file()}
    acquired,bucket=source(phase);original,old=source(PHASES[0])
    with original.event_bucket(old['id'],experimental=True,backend=backend,visibility=visibility) as baseline:
        initial=baseline.table().to_pylist()
    with acquired.event_bucket(bucket['id'],experimental=True,backend=backend,visibility=visibility) as snapshot:
        rows=snapshot.table().to_pylist()
        assert len(rows)==14 and stored(rows)==stored(initial)
        raw=Counter(r['raw'] for r in rows)
        assert raw==Counter((FIXTURE/'event-input.log').read_bytes().splitlines())
        assert sorted(raw.values())==[1]*11+[3]
        assert {r['bucket_id'] for r in rows}=={bucket['id']}
        assert {r['object_uri'] for r in rows}=={'smartstore-object-key:'+bucket['journal_key']}
        assert {r['source_sha256'] for r in rows}=={hashlib.sha256(acquired._objects[bucket['journal_key']]).hexdigest()}
        for row in rows:
            context=json.loads(row['bucket_context_json']);state=json.loads(row['decoder_state_json'])
            if phase==PHASES[0]:
                assert context['sidecars']==[] and context['index_time_bounds_s'] is None
                assert state['index_candidate_within_bucket_info'] is None
            else:
                assert [s['name'] for s in context['sidecars']]==['bucket_info.csv']
                assert context['index_time_bounds_s']==[row['index_time']['seconds']]*2
                assert state['index_candidate_within_bucket_info'] is True
        signatures=Counter(oracle.expected_signature(r) for r in rows)
        for name in ('before-failure','after-storage-recovery'):
            assert signatures==Counter(oracle.actual_signature(r,expected_index='cluster_events_zstd') for r in oracle.search_rows(FIXTURE/(name+'.native.jsonl')))
        assert signatures==csv_signatures(FIXTURE/phase/'native.csv')
        assert snapshot.report()['fully_validated_events']==14
        assert snapshot.report()['indexed_search_supported'] is False
        for fmt in ('arrow','parquet'):
            output=tmp_path/('events.'+fmt);event_bucket.export(snapshot,output,fmt)
            assert event_bucket.verify(snapshot,output,fmt)['verified_events']==14
        records=snapshot.records();next(records);records.close()
        assert snapshot.table().num_rows==14
    assert all(sha(p)==digest for p,digest in before.items())


@pytest.mark.parametrize('fmt',['arrow','parquet'])
def test_equal_journal_does_not_make_exports_interchangeable_between_captures(tmp_path,fmt):
    before,old=source(PHASES[0]);after,new=source(PHASES[1]);output=tmp_path/('events.'+fmt)
    with before.event_bucket(old['id'],experimental=True) as snapshot:event_bucket.export(snapshot,output,fmt)
    with after.event_bucket(new['id'],experimental=True) as snapshot:
        with pytest.raises(ValueError):event_bucket.verify(snapshot,output,fmt)


@pytest.mark.parametrize('damage',['receipt_only_before','receipt_only_after','stale_receipt'])
def test_historical_receipt_requires_its_complete_original_payloads_even_for_empty_export(tmp_path,damage):
    phase=PHASES[0] if damage=='receipt_only_before' else PHASES[1]
    path=tmp_path/'acquired';shutil.copytree(FIXTURE/phase/'acquired',path)
    entries=json.loads((path/'objects.json').read_text());receipt,=[e for e in entries if e['key'].endswith('/receipt.json')]
    if damage.startswith('receipt_only'):
        for entry in entries:
            if entry is not receipt:(path/entry['file']).unlink()
        entries=[receipt]
    else:
        old_root=FIXTURE/PHASES[0]/'acquired'
        old,=[e for e in json.loads((old_root/'objects.json').read_text()) if e['key'].endswith('/receipt.json')]
        wire=(old_root/old['file']).read_bytes();(path/receipt['file']).write_bytes(wire)
        receipt.update(size=len(wire),sha256=hashlib.sha256(wire).hexdigest())
    # Only the ordinary acquisition wrapper changes; native receipt bytes remain original.
    (path/'objects.json').write_text(json.dumps(entries)+'\n');output=tmp_path/'never.parquet'
    with pytest.raises(ValueError,match='missing or wrong-size object'):
        acquired=AcquiredObjects(path,offline_snapshot=True);bucket,=acquired.report()['buckets']
        with acquired.event_bucket(bucket['id'],experimental=True) as snapshot:
            event_bucket.export(snapshot,output,term=b'__absent__')
    assert not output.exists()
    valid,bucket=source(PHASES[0])
    with valid.event_bucket(bucket['id'],experimental=True) as snapshot:assert snapshot.table().num_rows==14


def test_inventory_pin_cannot_be_reused_for_the_replacement_capture():
    expected=sha(FIXTURE/PHASES[0]/'acquired/objects.json')
    with pytest.raises(ValueError,match='SmartStore acquisition inventory'):
        AcquiredObjects(FIXTURE/PHASES[1]/'acquired',offline_snapshot=True,expected_inventory_sha256=expected)
