"""Genuine native failover namespace; corrupted copies are refusal cases only."""
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil

import pytest

from obs_migrate import event_bucket,oracle
from obs_migrate.smartstore import AcquiredObjects

FIXTURE=Path(__file__).parent/'fixtures/smartstore-foreign-uploader-10.6'


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def source():
    path=FIXTURE/'acquired'
    return AcquiredObjects(path,offline_snapshot=True,expected_inventory_sha256=digest(path/'objects.json'))


def test_genuine_failover_fixture_and_native_reference_hashes():
    manifest=json.loads((FIXTURE/'manifest.json').read_text())
    assert manifest['license']=='CC0-1.0'
    assert {p.relative_to(FIXTURE).as_posix():digest(p) for p in FIXTURE.rglob('*') if p.is_file() and p.name!='manifest.json'}==manifest['files']
    p=json.loads((FIXTURE/'provenance.json').read_text())
    assert p['native_origin_guid']!=p['native_uploader_guid']
    assert p['events']==14
    before=list(oracle.search_rows(FIXTURE/'before.native.jsonl'));after=list(oracle.search_rows(FIXTURE/'after.native.jsonl'))
    assert len(before)==len(after)==14
    assert all(r['_si'][0]=='uploader-peer-a' for r in before)
    assert all(r['_si'][0]=='uploader-peer-b' for r in after)
    stored=lambda values:{r['_cd']:{k:v for k,v in r.items() if k!='_si'} for r in values}
    assert stored(before)==stored(after)


@pytest.mark.parametrize('backend',['arrow','journal'])
def test_distinct_uploader_preserves_every_event_and_native_identity(tmp_path,backend):
    acquired=source();bucket,=acquired.report()['buckets']
    origin=bucket['id'].split('~')[2];uploader=bucket['native_user_data']['uploader_guid']
    assert origin!=uploader
    assert bucket['copy_identity']=={'origin_guid':origin,'uploader_guid':uploader,'relation':'observed_distinct_origin_uploader','ownership':'UNVERIFIED'}
    assert bucket['receipt_key'].rsplit('/',2)[-2].endswith('~'+origin)
    assert '/guidSplunk-'+uploader+'/' in bucket['journal_key']
    before={p:digest(p) for p in (FIXTURE/'acquired').rglob('*') if p.is_file()}
    with acquired.event_bucket(bucket['id'],experimental=True,backend=backend) as snapshot:
        rows=snapshot.table().to_pylist();assert len(rows)==14
        assert Counter(r['raw'] for r in rows)==Counter((FIXTURE/'event-input.log').read_bytes().splitlines())
        native=list(oracle.search_rows(FIXTURE/'after.native.jsonl'))
        assert Counter(oracle.expected_signature(r) for r in rows)==Counter(oracle.actual_signature(r,expected_index='cluster_events_zstd') for r in native)
        assert {r['decompressed_offset']>>5 for r in rows}=={int(r['_cd'].split(':')[1]) for r in native}
        assert all(r['bucket_id']==bucket['id'] and r['object_uri']=='smartstore-object-key:'+bucket['journal_key'] for r in rows)
        assert all(r['source_sha256']==hashlib.sha256(acquired._objects[bucket['journal_key']]).hexdigest() for r in rows)
        local,=(FIXTURE/'local').iterdir()
        with event_bucket.EventBucket(local,experimental=True,offline_snapshot=True,backend=backend,index=bucket['index'],bucket_id=bucket['id']) as cache:
            cached=cache.table().to_pylist()
        # Acquisition identity/object location/link observations differ; every stored
        # raw/time/metadata/wire/state/ordinal/source field must be identical.
        fields=lambda values:[{k:v for k,v in r.items() if k not in ('bucket_context_json','object_uri','index_link')} for r in values]
        assert fields(rows)==fields(cached)
        with (FIXTURE/'native.csv').open(newline='') as file:csv_rows=list(csv.DictReader(file))
        assert Counter(r['_raw'].encode() for r in csv_rows)==Counter(r['raw'] for r in rows)
        csv_signatures=[]
        for native in csv_rows:
            csv_signatures.append(oracle.expected_signature({'raw':native['_raw'].encode(),
                **{k:native[k].encode() for k in ('host','source','sourcetype')},
                'event_time':{'seconds':int(native['_time']),'fraction':re.search(r'_subsecond::([^ ]+)',native['_meta'])[1]},
                'index_time':{'seconds':int(re.search(r'_indextime::([0-9]+)',native['_meta'])[1])}}))
        assert Counter(csv_signatures)==Counter(oracle.expected_signature(r) for r in rows)
        for format in ('arrow','parquet'):
            path=tmp_path/format;event_bucket.export(snapshot,path,format)
            assert event_bucket.verify(snapshot,path,format)['verified_events']==14
        empty=tmp_path/'empty.parquet';event_bucket.export(snapshot,empty,term=b'__absent__')
        assert event_bucket.verify(snapshot,empty,term=b'__absent__')['verified_events']==0
    with acquired.event_bucket(bucket['id'],experimental=True,backend=backend,visibility='undeleted') as selected:
        assert selected.table().to_pylist()==rows
        records=selected.records();next(records);records.close()
        assert selected.table().num_rows==14
    assert all(digest(p)==wanted for p,wanted in before.items())


def mutate(tmp_path,damage):
    path=tmp_path/'acquired';shutil.copytree(FIXTURE/'acquired',path)
    entries=json.loads((path/'objects.json').read_text());entry=next(e for e in entries if e['key'].endswith('/receipt.json'))
    receipt=json.loads((path/entry['file']).read_text());origin=receipt['manifest']['id'].split('~')[2];uploader=receipt['user_data']['uploader_guid']
    if damage=='origin_syntax':receipt['manifest']['id']=receipt['manifest']['id'].rsplit('~',1)[0]+'~invalid'
    elif damage=='uploader_syntax':receipt['user_data']['uploader_guid']='invalid'
    elif damage=='path_origin':receipt['manifest']['path']=receipt['manifest']['path'].replace(origin,uploader)
    elif damage=='path_local_id':receipt['manifest']['path']=receipt['manifest']['path'].replace('_0_'+origin,'_99_'+origin)
    elif damage=='key_origin':
        for e in entries:e['key']=e['key'].replace('~'+origin,'~'+uploader)
    elif damage=='member_uploader':receipt['objects'][0]['name']=receipt['objects'][0]['name'].replace(uploader,origin)
    elif damage=='missing_member':entries.remove(next(e for e in entries if e['key'].endswith('.tsidx')))
    elif damage in ('extra_member','ambiguous_copy'):
        originals=list(entries)
        for number,e in enumerate(originals if damage=='ambiguous_copy' else originals[:1]):
            copied=dict(e);copied['file']='objects/copy-%02d.bin'%number;shutil.copyfile(path/e['file'],path/copied['file'])
            copied['key']='other/'+e['key'] if damage=='ambiguous_copy' else entry['key'].rsplit('/',1)[0]+'/unlisted.bin'
            entries.append(copied)
    elif damage=='equivalent_mask':
        # An ORIGINAL existing native mask, used only as a negative admission case.
        native=Path(__file__).parent/'fixtures/smartstore-tsidx9-10.6/03-deleted'
        original=next(e for e in json.loads((native/'objects.json').read_text()) if '/deletes/' in e['key'])
        data=(native/original['file']).read_bytes();name='objects/native-mask.bin';(path/name).write_bytes(data)
        key=entry['key'].rsplit('/',1)[0]+'/guidSplunk-'+uploader+'/rawdata/deletes/'+'f'*32+'.csv.gz'
        entries.append({'key':key,'file':name,'size':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        receipt['objects'].append({'name':'./guidSplunk-'+uploader+'/rawdata/deletes/','size':-1,'expand':True})
    else:raise AssertionError(damage)
    data=json.dumps(receipt).encode();(path/entry['file']).write_bytes(data);entry.update(size=len(data),sha256=hashlib.sha256(data).hexdigest())
    (path/'objects.json').write_text(json.dumps(entries)+'\n')
    return path


@pytest.mark.parametrize('damage',['origin_syntax','uploader_syntax','path_origin','path_local_id','key_origin','member_uploader','missing_member','extra_member','ambiguous_copy'])
def test_inconsistent_or_unqualified_identity_refuses_before_empty_output(tmp_path,damage):
    path=mutate(tmp_path,damage);output=tmp_path/'never.parquet'
    with pytest.raises(ValueError) as failure:
        acquired=AcquiredObjects(path,offline_snapshot=True);bucket,=acquired.report()['buckets']
        with acquired.event_bucket(bucket['id'],experimental=True) as snapshot:event_bucket.export(snapshot,output,term=b'__absent__')
    assert not output.exists()
    # Keep failure traceback alive through a fresh valid acquisition/scan.
    acquired=source();bucket,=acquired.report()['buckets']
    with acquired.event_bucket(bucket['id'],experimental=True) as snapshot:assert snapshot.table().num_rows==14


def test_existing_same_origin_snapshot_identity_unchanged():
    path=Path(__file__).parent/'fixtures/smartstore-tsidx9-10.6/03-deleted'
    acquired=AcquiredObjects(path,offline_snapshot=True);bucket,=acquired.report()['buckets']
    assert 'copy_identity' not in bucket
    with acquired.event_bucket(bucket['id'],experimental=True,visibility='undeleted') as snapshot:
        assert snapshot.snapshot_sha256=='0ce131a963f54739464d758d23aa42953f70c162a7725c5663593aa182c274a6'
        assert snapshot.table().num_rows==11


def test_identical_native_mask_content_is_not_an_origin_signature(tmp_path):
    # Actual masks from separate native controls contain the identical CSV tuples.
    # The receipt/inventory is caller-asserted provenance, not a signature proving
    # which producer caused a mask. Copying equal native content cannot distinguish it.
    import gzip
    old=Path(__file__).parent/'fixtures/smartstore-tsidx9-10.6/03-deleted'
    new=Path(__file__).parent/'fixtures/smartstore-foreign-deletes-10.6/01-deleted/acquired'
    def body(root):
        entry=next(e for e in json.loads((root/'objects.json').read_text()) if '/deletes/' in e['key'])
        return gzip.decompress((root/entry['file']).read_bytes())
    assert body(old)==body(new)
    path=mutate(tmp_path,'equivalent_mask');acquired=AcquiredObjects(path,offline_snapshot=True);bucket,=acquired.report()['buckets']
    assert bucket['copy_identity']['ownership']=='UNVERIFIED'
    with acquired.event_bucket(bucket['id'],experimental=True,visibility='undeleted') as snapshot:
        assert snapshot.table().num_rows==11
        assert snapshot.deletion_report()['search_visibility']=='UNVERIFIED'
