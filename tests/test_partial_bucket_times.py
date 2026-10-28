"""CSV sidecar contract tests around unchanged genuine CC0 journal bytes.

Only conventional text metadata is varied here; no proprietary binary fixture
is fabricated. Actual NAS originals and independent native references stay private.
"""
import csv
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from obs_migrate import journal
from obs_migrate.event_bucket import EventBucket

SEED=Path(__file__).parent/'fixtures/botsv3/one-event'


def copied(tmp_path,earliest,latest):
    bucket=tmp_path/'one-event';shutil.copytree(SEED,bucket)
    path=bucket/'bucket_info.csv'
    with path.open('w',newline='') as file:
        writer=csv.writer(file);writer.writerow(['indextime_et','indextime_lt']);writer.writerow([earliest,latest])
    return bucket


@pytest.mark.parametrize('missing',['earliest','latest','both'])
def test_missing_text_bounds_preserve_unknown_and_every_stored_field(tmp_path,missing):
    original,=journal.events(SEED,experimental=True);seconds=original['index_time']['seconds']
    lo=None if missing in ('earliest','both') else seconds
    hi=None if missing in ('latest','both') else seconds
    bucket=copied(tmp_path,'' if lo is None else lo,'' if hi is None else hi);stats={}
    actual,=journal.events(bucket,experimental=True,stats=stats)
    assert {k:v for k,v in actual.items() if k not in ('bucket_context_json','decoder_state_json')}=={
        k:v for k,v in original.items() if k not in ('bucket_context_json','decoder_state_json')}
    context=json.loads(actual['bucket_context_json']);assert context['index_time_bounds_s']==[lo,hi]
    sidecar=next(x for x in context['sidecars'] if x['name']=='bucket_info.csv')
    assert sidecar['sha256']==hashlib.sha256((bucket/'bucket_info.csv').read_bytes()).hexdigest()
    expected_state=json.loads(original['decoder_state_json']);expected_state['index_candidate_within_bucket_info']=None
    assert json.loads(actual['decoder_state_json'])==expected_state
    assert stats['index_candidate_bounds_match'] is None
    assert stats['index_candidates_outside_bounds']==0


@pytest.mark.parametrize('missing',['earliest','latest'])
def test_known_endpoint_violation_still_warns_without_dropping_record(tmp_path,missing):
    original,=journal.events(SEED,experimental=True);seconds=original['index_time']['seconds']
    bucket=copied(tmp_path,'' if missing=='earliest' else seconds+1,
                  seconds-1 if missing=='earliest' else '');stats={}
    row,=journal.events(bucket,experimental=True,stats=stats)
    assert row['raw']==original['raw'] and row['record_wire']==original['record_wire']
    assert json.loads(row['decoder_state_json'])['index_candidate_within_bucket_info'] is False
    assert 'index_candidate_outside_bucket_info' in row['decoder_warnings']
    assert stats['index_candidates_outside_bounds']==1
    assert stats['index_candidate_bounds_match'] is None


@pytest.mark.parametrize('earliest,latest',[('invalid',''),('','invalid'),('NaN',''),('','1.5'),('2','1')])
def test_invalid_nonempty_or_reversed_known_bounds_still_refuse(tmp_path,earliest,latest):
    bucket=copied(tmp_path,earliest,latest)
    with pytest.raises(journal.JournalError):list(journal.events(bucket,experimental=True))


def test_missing_csv_field_is_not_an_empty_value(tmp_path):
    bucket=copied(tmp_path,'0','0')
    (bucket/'bucket_info.csv').write_text('indextime_et,indextime_lt\n0\n')
    with pytest.raises(journal.JournalError):list(journal.events(bucket,experimental=True))


def test_linked_and_cached_reads_preserve_partial_context(tmp_path):
    original,=journal.events(SEED,experimental=True)
    bucket=copied(tmp_path,original['index_time']['seconds'],'')
    canonical,=journal.events(bucket,experimental=True)
    with EventBucket(bucket,experimental=True,offline_snapshot=True,backend='journal',cache_projections=True) as source:
        row,=source.records()
        assert {k:v for k,v in row.items() if k!='index_link'}==canonical
        columns=('bucket_context_json','decoder_warnings','raw','metadata_json','event_time','index_time')
        projected,=source.projected_records(columns)
        assert projected=={k:canonical[k] for k in columns}
