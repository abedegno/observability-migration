"""Opt-in direct selection uses existing genuine MSIDX bytes and finite policy."""
from pathlib import Path
import json
import pytest
from obs_migrate import bucket_select,metric_select,metric_limits,msidx
from obs_migrate.stored_time import ticks,TimeBound,TimeRange
ROOT=Path(__file__).parent/'fixtures';BUCKET=next((ROOT/'metric-xor-9').glob('db_*'));FILE=next(BUCKET.glob('*.tsidx'));LARGE=metric_limits.LARGE_METRICS


def test_file_selection_policy_and_canonical_identity(tmp_path):
    plain=metric_select.explain(FILE,metric_select.Predicate(),experimental=True)
    explicit=metric_select.explain(FILE,metric_select.Predicate(),experimental=True,limits=LARGE)
    assert explicit.pop('metric_resource_profile')==LARGE.describe() and explicit==plain
    expected=list(metric_select.records(FILE,metric_select.Predicate(),experimental=True))
    first=expected[0];bound=TimeBound.from_ticks(ticks(**first['indexed_time']))
    predicate=metric_select.Predicate(metric_name=first['measurements'][0]['name'],dimensions=tuple((d['key'],d['value']) for d in first['dimensions']),time_range=TimeRange(bound,bound))
    wanted=[r for r in expected if predicate.matches(r)]
    assert list(metric_select.records(FILE,predicate,experimental=True,limits=LARGE))==wanted
    assert [r for b in metric_select.batches(FILE,predicate,experimental=True,limits=LARGE,max_rows=2) for r in b.to_pylist()]==wanted
    for format in ('parquet','ndjson'):
        out=tmp_path/('selected.'+format)
        result=metric_select.export(FILE,predicate,out,format,experimental=True,limits=LARGE)
        verified=metric_select.verify(FILE,predicate,out,format,experimental=True,limits=LARGE)
        assert result['metric_resource_profile']==verified['metric_resource_profile']==LARGE.describe()
        assert list(msidx.read_export(out,format))==wanted


def test_bucket_policy_keeps_rows_snapshot_and_query_identity(tmp_path):
    query=metric_select.Predicate();plain=bucket_select.Selection(BUCKET,query,experimental=True,offline_snapshot=True)
    selected=bucket_select.Selection(BUCKET,query,experimental=True,offline_snapshot=True,limits=LARGE)
    assert list(selected.records())==list(plain.records())
    report=selected.report();assert report.pop('metric_resource_profile')==LARGE.describe() and report==plain.report()
    assert selected.sha256==plain.sha256 and selected.snapshot.sha256==plain.snapshot.sha256
    for format in ('parquet','ndjson'):
        out=tmp_path/('selected-'+format+'.zip')
        result=bucket_select.export(BUCKET,query,out,format,experimental=True,offline_snapshot=True,limits=LARGE)
        verified=bucket_select.verify(BUCKET,query,out,experimental=True,offline_snapshot=True,limits=LARGE)
        assert result['metric_resource_profile']==verified['metric_resource_profile']==LARGE.describe()
        # Artifact metadata cannot grant a profile to a verifier supplied a different policy.
        with pytest.raises(ValueError,match='manifest mismatch'):bucket_select.verify(BUCKET,query,out,experimental=True,offline_snapshot=True)


@pytest.mark.parametrize('version',['7.0.0','7.3.9'])
def test_explicit_legacy_refuses_but_default_keeps_working(version,tmp_path):
    root=next((ROOT/'metrics'/version).glob('db_*'));file=next(root.glob('*.tsidx'));query=metric_select.Predicate()
    assert len(list(metric_select.records(file,query,experimental=True)))==5
    for limits in (LARGE,object()):
        with pytest.raises(ValueError):metric_select.explain(file,query,experimental=True,limits=limits)
        with pytest.raises(ValueError):bucket_select.explain(root,query,experimental=True,offline_snapshot=True,limits=limits)


def test_no_match_full_preflight_and_tiny_budget_never_publish(tmp_path):
    out=tmp_path/'bad.zip';query=metric_select.Predicate(host=b'absent');tiny=metric_limits.MetricLimits(cursor_bytes=1)
    with pytest.raises(ValueError,match='cursor'):bucket_select.export(BUCKET,query,out,experimental=True,offline_snapshot=True,limits=tiny)
    assert not out.exists() and not list(tmp_path.glob('.obs-bucket-select-*'))
    with pytest.raises(ValueError,match='cursor'):metric_select.explain(FILE,query,experimental=True,limits=tiny)
    report=bucket_select.explain(BUCKET,query,experimental=True,offline_snapshot=True,limits=LARGE)
    assert report['fully_validated_points']==1201 and report['candidate_points']==0


def test_early_close_and_retained_failure_release_owned_source(tmp_path,monkeypatch):
    seen=[];old=metric_select._Source.__init__
    def observe(self,*a,**kw):old(self,*a,**kw);seen.append(self)
    monkeypatch.setattr(metric_select._Source,'__init__',observe)
    selected=bucket_select.Selection(BUCKET,metric_select.Predicate(),experimental=True,offline_snapshot=True,limits=LARGE)
    rows=selected.records();next(rows);rows.close()
    with pytest.raises(ValueError,match='complete'):selected.report()
    assert seen[-1].closed and seen[-1].reader.work_report()['cursor_bytes']==0
    original=metric_select._Source._emit
    def interrupted(self,*a,**kw):
        rows=original(self,*a,**kw)
        try:yield next(rows);raise KeyboardInterrupt('genuine selection interruption')
        finally:rows.close()
    with monkeypatch.context() as patch:
        patch.setattr(metric_select._Source,'_emit',interrupted)
        with pytest.raises(KeyboardInterrupt) as caught:list(selected.records())
    assert caught.value.__traceback__ is not None
    assert seen[-1].closed and seen[-1].reader.work_report()['cursor_bytes']==0
    with pytest.raises(ValueError,match='complete'):selected.report()
    assert len(list(selected.records()))==1201
    assert selected.report()['candidate_points']==1201
    # Direct open_source must release cursors even when its caller retains a suspended generator.
    with metric_select.open_source(FILE,experimental=True,limits=LARGE) as source:
        stream=source.records(metric_select.Predicate());next(stream)
    assert source.closed and source.reader.work_report()['cursor_bytes']==0
    stream.close()


def test_cli_direct_selection_resource_profile(tmp_path,capsys):
    from obs_migrate.cli import main
    args=['--experimental','--resource-profile','large']
    assert main(['metrics','select-explain',str(FILE),*args])==0
    assert json.loads(capsys.readouterr().out)['metric_resource_profile']==LARGE.describe()
    out=tmp_path/'cli.zip'
    common=['--experimental','--offline-snapshot','--resource-profile','large']
    assert main(['metrics','bucket-select-export',str(BUCKET),*common,'--out',str(out)])==0
    assert json.loads(capsys.readouterr().out)['metric_resource_profile']==LARGE.describe()
    assert main(['metrics','bucket-select-verify',str(BUCKET),*common,'--against',str(out)])==0
    assert json.loads(capsys.readouterr().out)['metric_resource_profile']==LARGE.describe()


@pytest.mark.parametrize('operation',['bucket','source'])
def test_batching_stage_failure_releases_rows_while_traceback_is_retained(monkeypatch,operation):
    seen=[];original_init=metric_select._Source.__init__
    def observed(self,*args,**kwargs):original_init(self,*args,**kwargs);seen.append(self)
    monkeypatch.setattr(metric_select._Source,'__init__',observed)
    selected=bucket_select.Selection(BUCKET,metric_select.Predicate(),experimental=True,offline_snapshot=True,limits=LARGE)
    def fail(row):raise MemoryError('controlled batching failure after real native row')
    with metric_select.open_source(FILE,experimental=True,limits=LARGE) as source:
        stream=selected.batches(max_rows=2) if operation=='bucket' else source.batches(metric_select.Predicate(),max_rows=2)
        with monkeypatch.context() as patch:
            patch.setattr(msidx._io,'json_bytes',fail)
            with pytest.raises(MemoryError) as retained:next(stream)
        assert retained.value.__traceback__ is not None
        if operation=='bucket':
            assert seen[-1].closed and seen[-1].reader.work_report()['cursor_bytes']==0
            assert not selected._scan_lock.locked()
            assert len(list(selected.records()))==1201
        else:
            assert source.reader.work_report()['cursor_bytes']==0
            assert len(list(source.records(metric_select.Predicate())))==1201
        assert retained.value.__traceback__ is not None


@pytest.mark.parametrize('override',[object(),metric_limits.MetricLimits(cursor_bytes=1)])
def test_open_source_batches_rejects_unapplied_budget_overrides(override):
    with metric_select.open_source(FILE,experimental=True,limits=LARGE) as source:
        with pytest.raises(TypeError,match='limits'):
            source.batches(metric_select.Predicate(),limits=override)


def test_bucket_sql_help_exposes_explicit_resource_profile(capsys):
    from obs_migrate.cli import main
    with pytest.raises(SystemExit) as result:main(['metrics','bucket-sql','--help'])
    assert result.value.code==0
    assert '--resource-profile' in capsys.readouterr().out
