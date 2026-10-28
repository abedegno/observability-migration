"""Bounded access tested on unchanged genuine native XOR files."""
from dataclasses import replace
import hashlib
import importlib
import json
from pathlib import Path

import pytest
from obs_migrate import msidx

ROOT=Path(__file__).parent/'fixtures/metric-xor-9'
FILE=next(ROOT.glob('*/*.tsidx'))


def limits(**kwargs):
    module=importlib.import_module('obs_migrate.metric_limits')
    return module.MetricLimits(**kwargs)


@pytest.mark.parametrize('kwargs',[{'values':0},{'values':True},{'values':1000001},
 {'cursor_bytes':0},{'cursor_bytes':8388609},{'cursor_bytes':1.5},{'name':'bad name'}])
def test_resource_limits_are_frozen_and_finite(kwargs):
    with pytest.raises(ValueError):limits(**kwargs)


def test_large_profile_is_explicit_and_immutable():
    from dataclasses import FrozenInstanceError
    module=importlib.import_module('obs_migrate.metric_limits');large=module.LARGE_METRICS
    assert large.values==1000000 and large.cursor_bytes==8*1024**2
    with pytest.raises(FrozenInstanceError):large.values=2
    assert msidx.MAX_ENTRIES==msidx.MAX_CACHED_VALUES==100000


def test_genuine_cursor_scan_is_byte_identical_and_budgeted():
    manifest=json.loads((ROOT/'manifest.json').read_text())
    for name,digest in manifest['sha256'].items():assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==digest
    baseline=list(msidx.records(FILE,experimental=True));stats={};profile=limits()
    actual=list(msidx.records(FILE,experimental=True,limits=profile,stats=stats))
    assert len(actual)==1201 and actual==baseline
    assert b''.join(map(msidx.json_bytes,actual))==b''.join(map(msidx.json_bytes,baseline))
    assert stats['complete'] is True and stats['cursor_bytes']==0
    assert 0<stats['peak_cursor_bytes']<=profile.cursor_bytes
    assert stats['numeric_lookups']==sum(len(row['measurements']) for row in actual)
    assert stats['xor_values_decoded']<10000


def test_cursor_backward_repeat_and_wire_prestate_match_original():
    base=msidx._Reader(FILE.read_bytes());reader=msidx._Reader(FILE.read_bytes(),limits=limits())
    tid=max(base.columns,key=lambda key:base.columns[key].ends[-1]);did=min(base.columns[tid].slots,key=lambda key:base.columns[tid].slots[key][0])
    for local in (10,2,2):assert reader.columns[tid].value(did,local)==base.columns[tid].value(did,local)
    work=reader.work_report();assert work['cursor_restarts']==1 and work['xor_values_decoded']==14
    assert work['numeric_lookups']==3
    reader.close_cursors();assert reader.work_report()['cursor_bytes']==0


def test_cursor_eviction_preserves_exact_values_and_charge():
    profile=limits(cursor_bytes=2*(8192+1024));base=msidx._Reader(FILE.read_bytes());reader=msidx._Reader(FILE.read_bytes(),limits=profile)
    assert len(reader.columns)==3
    for _ in range(2):
        for tid,column in reader.columns.items():
            did=next(iter(column.slots));assert column.value(did,0)==base.columns[tid].value(did,0)
            assert reader.work_report()['cursor_bytes']<=profile.cursor_bytes
    assert reader.work_report()['cursor_evictions']>=3
    reader.close_cursors()


def test_too_small_cursor_budget_refuses_before_reading_block(monkeypatch):
    reader=msidx._Reader(FILE.read_bytes(),limits=limits(cursor_bytes=1));column=next(iter(reader.columns.values()))
    original=reader.span
    def checked(offset,length):
        assert (offset,length)!=(column.values,column.block_bytes),'block read before resource refusal'
        return original(offset,length)
    monkeypatch.setattr(reader,'span',checked)
    with pytest.raises(msidx.MsidxError,match='cursor'):column.value(next(iter(column.slots)),0)
    assert reader.work_report()['cursor_bytes']==0


def test_per_reader_admission_and_partial_close_keep_defaults():
    with pytest.raises(msidx.MsidxError):list(msidx.records(FILE,experimental=True,limits=limits(values=1)))
    stats={};rows=msidx.records(FILE,experimental=True,limits=limits(),stats=stats);next(rows);rows.close()
    assert stats['complete'] is False and stats['cursor_bytes']==0
    assert len(list(msidx.records(FILE,experimental=True)))==1201
    assert msidx.MAX_ENTRIES==100000


def test_invalid_profile_and_truncated_genuine_source_refuse(tmp_path):
    with pytest.raises(ValueError):list(msidx.records(FILE,experimental=True,limits=object()))
    bad=tmp_path/'truncated.tsidx';bad.write_bytes(FILE.read_bytes()[:-1]);stats={}
    with pytest.raises(msidx.MsidxError):list(msidx.records(bad,experimental=True,limits=limits(),stats=stats))
    assert stats['complete'] is False


def test_bucket_large_profile_preserves_source_identity_and_output(tmp_path):
    from obs_migrate import metric_bucket
    bucket=FILE.parent;profile=limits();baseline=metric_bucket.Snapshot(bucket,offline_snapshot=True)
    snapshot=metric_bucket.Snapshot(bucket,offline_snapshot=True,limits=profile)
    assert snapshot.sha256==baseline.sha256 and snapshot.inventory()==baseline.inventory()
    assert list(snapshot.records(experimental=True))==list(baseline.records(experimental=True))
    old=tmp_path/'default.parquet';new=tmp_path/'large.parquet'
    expected=metric_bucket.export(bucket,old,experimental=True,offline_snapshot=True)
    actual=metric_bucket.export(bucket,new,experimental=True,offline_snapshot=True,limits=profile)
    verified=metric_bucket.verify(bucket,new,experimental=True,offline_snapshot=True,limits=profile)
    assert actual['stored_points']==verified['verified_stored_points']==1201
    assert actual['canonical_sha256']==expected['canonical_sha256']
    assert new.read_bytes()==old.read_bytes()
    assert actual['metric_resource_profile']==verified['metric_resource_profile']==profile.describe()


@pytest.mark.parametrize('version',['7.0.0','7.3.9'])
def test_explicit_msidx4_profile_refuses_legacy_storage(tmp_path,version):
    from obs_migrate import metric_storage,metric_bucket
    file=next((ROOT.parent/'metrics'/version).glob('*/*.tsidx'))
    with pytest.raises(ValueError,match='MSIDX4'):list(metric_storage.records(file,experimental=True,limits=limits()))
    with pytest.raises(ValueError,match='MSIDX4'):metric_bucket.Snapshot(file.parent,offline_snapshot=True,limits=limits())


def test_cli_large_export_verify_and_atomic_limit_refusal(tmp_path,capsys):
    from obs_migrate.cli import main
    file=tmp_path/'points.parquet';args=['metrics','bucket-export',str(FILE.parent),'--experimental','--offline-snapshot','--resource-profile','large','--out',str(file)]
    assert main(args)==0;exported=json.loads(capsys.readouterr().out)
    assert exported['stored_points']==1201 and exported['metric_resource_profile']['values']==1000000
    assert main(['metrics','bucket-verify',str(FILE.parent),'--experimental','--offline-snapshot','--resource-profile','large','--against',str(file)])==0
    assert json.loads(capsys.readouterr().out)['verified_stored_points']==1201
    from obs_migrate import metric_bucket
    bad=tmp_path/'bad.parquet'
    with pytest.raises(msidx.MsidxError):metric_bucket.export(FILE.parent,bad,experimental=True,offline_snapshot=True,limits=limits(cursor_bytes=1))
    assert not bad.exists() and not list(tmp_path.glob('.obs-migrate-*'))


def test_cli_per_file_large_profile_and_legacy_refusal(tmp_path,capsys):
    from obs_migrate.cli import main
    output=tmp_path/'slice.parquet'
    assert main(['metrics','tsidx-export',str(FILE),'--experimental','--resource-profile','large','--out',str(output)])==0
    assert json.loads(capsys.readouterr().out)['stored_points']==1201
    assert main(['metrics','tsidx-verify',str(FILE),'--experimental','--resource-profile','large','--against',str(output)])==0
    assert json.loads(capsys.readouterr().out)['verified_stored_points']==1201
    legacy=next((ROOT.parent/'metrics/7.3.9').glob('*/*.tsidx'));bad=tmp_path/'legacy.parquet'
    assert main(['metrics','tsidx-export',str(legacy),'--experimental','--resource-profile','large','--out',str(bad)])==1
    assert not bad.exists()


def test_explicit_profile_matches_independent_native_points():
    from collections import Counter
    from decimal import Decimal
    import struct
    from obs_migrate import metrics
    def signature(row,native=False):
        time=row['time' if native else 'indexed_time']
        values=tuple(sorted((m['name'],struct.pack('<d',m['value_float64']) if native else m['value_bits_le']) for m in row['measurements']))
        dimensions=tuple(sorted((d['key'],d['value']) for d in row['dimensions']))
        return (time['seconds'],Decimal(time['fraction']),row['host'],row['source'],row['sourcetype'],dimensions,values)
    actual=Counter(signature(row) for row in msidx.records(FILE,experimental=True,limits=limits()))
    expected=Counter(signature(row,True) for row in metrics.native_points(ROOT/'native.jsonl'))
    assert actual==expected and sum(actual.values())==1201
