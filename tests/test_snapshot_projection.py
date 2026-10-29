"""Explicit full-snapshot views over unchanged genuine native metric bytes."""
from pathlib import Path
import json
import shutil
import pytest
from obs_migrate import metric_bucket,metric_limits,metric_select,msidx
ROOT=Path(__file__).parent/'fixtures';BUCKET=next((ROOT/'metric-xor-9').glob('db_*'));LARGE=metric_limits.LARGE_METRICS


@pytest.mark.parametrize('name',['metric-xor-9','metrics/7.0.0','metrics/7.3.9','metrics/10.6.0.5'])
def test_projected_snapshot_records_preserve_full_fields_and_original_envelope(name):
    root=next((ROOT/name).glob('db_*'));snapshot=metric_bucket.Snapshot(root,offline_snapshot=True)
    full=list(snapshot.records(experimental=True));columns=['storage_json','measurements','indexed_time','timestamp_wire','point_ordinal','host']
    got=list(snapshot.records(experimental=True,columns=columns))
    assert got==[{k:r[k] for k in columns} for r in full]
    assert all(list(r)==columns for r in got)
    assert sum('bucket_snapshot_manifest' in json.loads(r['storage_json']) for r in got)==1
    assert all('bucket_query_sha256' not in json.loads(r['storage_json']) for r in got)


@pytest.mark.parametrize('columns',[[],(),['host','host'],['missing'],'host',{'host'},[None],['host',3]])
def test_invalid_snapshot_projection_columns_refuse(columns):
    snapshot=metric_bucket.Snapshot(BUCKET,offline_snapshot=True)
    with pytest.raises(ValueError,match='columns'):list(snapshot.records(experimental=True,columns=columns))


def test_snapshot_scalar_emission_follows_complete_numeric_preflight(monkeypatch):
    before=[];after=[];original=metric_select._Source._emit
    def measured(source,*args,**kwargs):
        before.append(source.reader.work_report()['numeric_lookups'])
        try:yield from original(source,*args,**kwargs)
        finally:after.append(source.reader.work_report()['numeric_lookups'])
    monkeypatch.setattr(metric_select._Source,'_emit',measured)
    snapshot=metric_bucket.Snapshot(BUCKET,offline_snapshot=True,limits=LARGE)
    rows=list(snapshot.records(experimental=True,columns=['point_ordinal','host']))
    assert len(rows)==1201 and before and all(n>0 for n in before) and before==after
    tiny=metric_bucket.Snapshot(BUCKET,offline_snapshot=True,limits=metric_limits.MetricLimits(cursor_bytes=1))
    with pytest.raises(ValueError,match='cursor'):list(tiny.records(experimental=True,columns=['point_ordinal']))


def test_snapshot_omitted_numeric_fields_still_validate_before_first_row(monkeypatch):
    original=msidx._Column.value
    def nonfinite(self,*args):
        bits,location=original(self,*args)
        return bytes.fromhex('000000000000f87f'),location
    monkeypatch.setattr(msidx._Column,'value',nonfinite)
    snapshot=metric_bucket.Snapshot(BUCKET,offline_snapshot=True,limits=LARGE)
    rows=snapshot.records(experimental=True,columns=['point_ordinal'])
    with pytest.raises(ValueError,match='nonfinite'):next(rows)
    rows.close()


def test_snapshot_projected_early_close_retained_failure_and_identity_without_sha(tmp_path,monkeypatch):
    seen=[];original=metric_select._Source.__init__
    def observe(self,*a,**kw):original(self,*a,**kw);seen.append(self)
    monkeypatch.setattr(metric_select._Source,'__init__',observe)
    root=tmp_path/'bucket';shutil.copytree(BUCKET,root);snapshot=metric_bucket.Snapshot(root,offline_snapshot=True,limits=LARGE)
    stream=snapshot.records(experimental=True,columns=['point_ordinal']);next(stream);stream.close()
    assert seen[-1].closed and seen[-1].reader.work_report()['cursor_bytes']==0
    emit=metric_select._Source._emit
    def failed(self,*a,**kw):
        rows=emit(self,*a,**kw)
        try:yield next(rows);raise MemoryError('after genuine projected snapshot row')
        finally:rows.close()
    with monkeypatch.context() as patch:
        patch.setattr(metric_select._Source,'_emit',failed)
        with pytest.raises(MemoryError) as retained:list(snapshot.records(experimental=True,columns=['point_ordinal']))
    assert retained.value.__traceback__ is not None and seen[-1].closed and seen[-1].reader.work_report()['cursor_bytes']==0
    assert len(list(snapshot.records(experimental=True,columns=['point_ordinal'])))==1201
    stream=snapshot.records(experimental=True,columns=['point_ordinal']);next(stream)
    with next(root.glob('*.tsidx')).open('ab') as file:file.write(b'negative-mutation-only')
    with pytest.raises(ValueError,match='changed'):list(stream)
    assert seen[-1].closed and seen[-1].reader.work_report()['cursor_bytes']==0


def test_storage_only_full_snapshot_composed_native_slices_keep_one_envelope(tmp_path):
    # This test container combines unchanged genuine slices, not a claimed producer bucket.
    root=tmp_path/'bucket';root.mkdir()
    for path in (ROOT/'metrics/10.6.0.5',ROOT/'metric-xor-9'):
        file=next(path.glob('db_*/*.tsidx'));shutil.copyfile(file,root/file.name)
    snapshot=metric_bucket.Snapshot(root,offline_snapshot=True,limits=LARGE)
    expected=[{'storage_json':r['storage_json']} for r in snapshot.records(experimental=True)]
    actual=list(snapshot.records(experimental=True,columns=['storage_json']))
    assert actual==expected and len(actual)==1206
    assert sum('bucket_snapshot_manifest' in json.loads(r['storage_json']) for r in actual)==1


def test_snapshot_sql_cli_help_lists_columns_and_profile(capsys):
    from obs_migrate.cli import main
    with pytest.raises(SystemExit) as result:main(['metrics','snapshot-sql','--help'])
    assert result.value.code==0
    help=capsys.readouterr().out;assert '--snapshot-column' in help and '--resource-profile' in help
