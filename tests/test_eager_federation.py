"""Optional eager DataFusion providers; no native FFI bridge is required."""
from pathlib import Path
import pytest
import pyarrow as pa
datafusion=pytest.importorskip('datafusion')
from obs_migrate.federation import BucketSession
ROOT=Path(__file__).parent/'fixtures/botsv3'


def test_large_profile_bucket_sql_cli_matches_default_and_retains_policy(tmp_path,capsys):
    from obs_migrate import metric_limits
    from obs_migrate.cli import main
    import pyarrow.parquet as pq
    bucket=next((ROOT.parent/'metric-xor-9').glob('db_*'))
    common=['metrics','bucket-sql',str(bucket),'--experimental','--offline-snapshot',
            '--sql','select point_ordinal,indexed_time,measurements from points where point_ordinal%2=0 order by point_ordinal']
    default=tmp_path/'default.parquet';large=tmp_path/'large.parquet'
    assert main([*common,'--out',str(default)])==0;capsys.readouterr()
    assert main([*common,'--resource-profile','large','--out',str(large)])==0;capsys.readouterr()
    left=pq.read_table(default);right=pq.read_table(large)
    assert left.replace_schema_metadata(None).equals(right.replace_schema_metadata(None))
    import json
    manifest=json.loads(right.schema.metadata[b'obs_migrate.sql_provenance'])
    assert manifest['sources']['points']['metric_resource_profile']==metric_limits.LARGE_METRICS.describe()
    old=json.loads(left.schema.metadata[b'obs_migrate.sql_provenance'])
    assert manifest['query_sha256']==old['query_sha256']


@pytest.mark.parametrize('failure',['input','batching','sql'])
def test_larger_bucket_sql_retained_failure_releases_and_reuses(tmp_path,monkeypatch,failure):
    from obs_migrate import metric_select,metric_limits,msidx
    from datafusion.record_batch import RecordBatchStream
    bucket=next((ROOT.parent/'metric-xor-9').glob('db_*'));session=BucketSession(max_input_bytes=1 if failure=='input' else 256*1024**2)
    observed=[];original_init=metric_select._Source.__init__
    def watch(self,*args,**kwargs):original_init(self,*args,**kwargs);observed.append(self)
    monkeypatch.setattr(metric_select._Source,'__init__',watch)
    def register():return session.register('points',bucket,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS)
    if failure in ('input','batching'):
        def failed(row):raise MemoryError('registration batching after genuine point')
        with monkeypatch.context() as patch:
            if failure=='batching':patch.setattr(msidx._io,'json_bytes',failed)
            with pytest.raises((ValueError,MemoryError)) as retained:register()
        assert retained.value.__traceback__ is not None
        assert not session._sources and session._bytes==0
        assert all(s.closed and s.reader.work_report()['cursor_bytes']==0 for s in observed)
        session.max_input_bytes=256*1024**2;register()
    else:
        register();original=RecordBatchStream.__next__
        def interrupted(stream):original(stream);raise KeyboardInterrupt('bucket SQL stream interruption')
        with monkeypatch.context() as patch:
            patch.setattr(RecordBatchStream,'__next__',interrupted)
            with pytest.raises(KeyboardInterrupt) as retained:session.sql('select * from points')
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    assert retained.value.__traceback__ is not None
    session.max_result_bytes=1
    with pytest.raises(ValueError,match='byte limit'):session.sql('select measurements from points')
    session.max_result_bytes=256*1024**2
    assert session.sql('select count(*) as n from points where point_ordinal<0').table.to_pylist()==[{'n':0}]
    assert all(s.closed and s.reader.work_report()['cursor_bytes']==0 for s in observed)


def test_large_profile_bucket_sql_exact_source_predicate_and_legacy_refusal():
    from obs_migrate import metric_select,metric_limits,bucket_select
    from obs_migrate.stored_time import ticks,TimeBound,TimeRange
    bucket=next((ROOT.parent/'metric-xor-9').glob('db_*'));rows=bucket_select.records(bucket,metric_select.Predicate(),experimental=True,offline_snapshot=True)
    try:first=next(rows)
    finally:rows.close()
    bound=TimeBound.from_ticks(ticks(**first['indexed_time']));predicate=metric_select.Predicate(metric_name=first['measurements'][0]['name'],dimensions=tuple((d['key'],d['value']) for d in first['dimensions']),time_range=TimeRange(bound,bound))
    expected=bucket_select.Selection(bucket,predicate,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS);full=list(expected.records())
    reference=datafusion.SessionContext();reference.register_record_batches('points',[pa.Table.from_pylist(full,schema=expected.reader.SCHEMA).to_batches()])
    actual=BucketSession();actual.register('points',bucket,predicate,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS)
    for sql in ('select * from points order by point_ordinal','select * from points limit 3','select count(*) as n from points',
                'select * from points where point_ordinal%2=0 order by point_ordinal','select * from points where point_ordinal<0'):
        want=reference.sql(sql).to_arrow_table();got=actual.sql(sql)
        assert got.table.to_pylist()==want.to_pylist() and got.table.schema.equals(want.schema,check_metadata=False)
        assert got.manifest['sources']['points']['query_sha256']==expected.sha256
    legacy=next((ROOT.parent/'metrics/7.0.0').glob('db_*'))
    with pytest.raises(ValueError,match='MSIDX4'):BucketSession().register('points',legacy,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS)


@pytest.mark.parametrize('version',['metric-xor-9','metrics/7.0.0','metrics/7.3.9'])
def test_projected_full_snapshot_sql_schema_residuals_and_default_identity(version):
    from obs_migrate import metric_bucket,metric_limits
    root=next((ROOT.parent/version).glob('db_*'));snapshot=metric_bucket.Snapshot(root,offline_snapshot=True)
    full=pa.Table.from_pylist(list(snapshot.records(experimental=True)),schema=snapshot.reader.SCHEMA)
    columns=['host','point_ordinal','storage_json','indexed_time','measurements']
    reference=datafusion.SessionContext();reference.register_record_batches('points',[full.select(columns).to_batches()])
    session=BucketSession();info=session.register_metric_snapshot('points',root,experimental=True,offline_snapshot=True,columns=columns,expected_snapshot_sha256=snapshot.sha256)
    assert info['rows']==full.num_rows
    for sql in ('select count(*) as n from points','select * from points order by point_ordinal',
                'select * from points where point_ordinal%2=0 order by point_ordinal','select * from points where point_ordinal<0','select * from points limit 3'):
        want=reference.sql(sql).to_arrow_table();got=session.sql(sql)
        assert got.table.to_pylist()==want.to_pylist() and got.table.schema.equals(want.schema,check_metadata=False)
        assert got.manifest['sources']['points']['query']['columns']==columns
        assert not got.manifest['query']['sql_filter_pushdown']
    with pytest.raises(ValueError):session.sql('select dimensions from points')
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':full.num_rows}]
    default=BucketSession();default.register_metric_snapshot('points',root,experimental=True,offline_snapshot=True)
    old=default.sql('select count(*) as n from points');new=session.sql('select count(*) as n from points')
    assert 'columns' not in old.manifest['sources']['points']['query'] and 'metric_resource_profile' not in old.manifest['sources']['points']
    assert old.manifest['query_sha256']!=new.manifest['query_sha256']
    if version=='metric-xor-9':
        large=BucketSession();large.register_metric_snapshot('points',root,experimental=True,offline_snapshot=True,columns=columns,limits=metric_limits.LARGE_METRICS)
        result=large.sql('select count(*) as n from points')
        assert result.manifest['query_sha256']==new.manifest['query_sha256']
        assert result.manifest['sources']['points']['metric_resource_profile']==metric_limits.LARGE_METRICS.describe()
    else:
        with pytest.raises(ValueError,match='MSIDX4'):BucketSession().register_metric_snapshot('points',root,experimental=True,offline_snapshot=True,columns=columns,limits=metric_limits.LARGE_METRICS)


@pytest.mark.parametrize('failure',['input','batching'])
def test_projected_snapshot_prepare_failure_preserves_traceback_and_session_reuse(monkeypatch,failure):
    from obs_migrate import metric_select,metric_limits,native_scan
    root=next((ROOT.parent/'metric-xor-9').glob('db_*'));seen=[];original_init=metric_select._Source.__init__
    def watch(self,*a,**kw):original_init(self,*a,**kw);seen.append(self)
    monkeypatch.setattr(metric_select._Source,'__init__',watch)
    session=BucketSession(max_input_bytes=1 if failure=='input' else 256*1024**2)
    def register():return session.register_metric_snapshot('points',root,experimental=True,offline_snapshot=True,columns=['point_ordinal'],limits=metric_limits.LARGE_METRICS)
    def broken(rows,*args):
        next(rows);raise MemoryError('snapshot batch preparation after genuine row')
        yield
    with monkeypatch.context() as patch:
        if failure=='batching':patch.setattr(native_scan,'_batches',broken)
        with pytest.raises((ValueError,MemoryError)) as retained:register()
    assert retained.value.__traceback__ is not None
    assert not session._sources and session._bytes==0 and not session._context.table_exist('points')
    assert all(s.closed and s.reader.work_report()['cursor_bytes']==0 for s in seen)
    session.max_input_bytes=256*1024**2;register()
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    assert retained.value.__traceback__ is not None


def test_projected_snapshot_shared_caps_invalid_identity_and_query_cancellation(monkeypatch):
    from obs_migrate import metric_bucket,metric_select,metric_limits
    from datafusion.record_batch import RecordBatchStream
    root=next((ROOT.parent/'metric-xor-9').glob('db_*'));session=BucketSession();options=dict(experimental=True,offline_snapshot=True,columns=['point_ordinal'],limits=metric_limits.LARGE_METRICS)
    for columns in ([],['absent'],['host','host']):
        with pytest.raises(ValueError,match='columns'):session.register_metric_snapshot('points',root,**{**options,'columns':columns})
    with pytest.raises(ValueError,match='identity'):session.register_metric_snapshot('points',root,expected_snapshot_sha256='0'*64,**options)
    assert not session._sources and session._bytes==0
    session.register_metric_snapshot('points',root,**options);size=session._bytes;session.max_input_bytes=size+1
    with pytest.raises(ValueError,match='byte limit'):session.register_metric_snapshot('other',root,**options)
    assert 'other' not in session._sources and not session._context.table_exist('other') and session._bytes==size
    original=RecordBatchStream.__next__
    def interrupted(stream):original(stream);raise KeyboardInterrupt('projected snapshot query cancellation')
    with monkeypatch.context() as patch:
        patch.setattr(RecordBatchStream,'__next__',interrupted)
        with pytest.raises(KeyboardInterrupt) as retained:session.sql('select * from points')
    session.max_result_bytes=1
    with pytest.raises(ValueError,match='byte limit'):session.sql('select * from points')
    session.max_result_bytes=256*1024**2
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    assert retained.value.__traceback__ is not None


def test_projected_snapshot_actual_cli_parquet_and_no_partial_on_refusal(tmp_path,capsys):
    from obs_migrate.cli import main
    from obs_migrate import metric_bucket
    import pyarrow.parquet as pq
    import json
    root=next((ROOT.parent/'metric-xor-9').glob('db_*'));sha=metric_bucket.Snapshot(root,offline_snapshot=True).sha256
    path=tmp_path/'count.parquet';args=['metrics','snapshot-sql',str(root),'--experimental','--offline-snapshot','--resource-profile','large','--snapshot-column','point_ordinal','--expected-snapshot-sha256',sha,'--sql','select count(*) as n from points','--out',str(path)]
    assert main(args)==0;capsys.readouterr()
    table=pq.read_table(path);assert table.to_pylist()==[{'n':1201}]
    assert json.loads(table.schema.metadata[b'obs_migrate.sql_provenance'])['sources']['points']['query']['columns']==['point_ordinal']
    bad=tmp_path/'refused.parquet';args[args.index(str(path))]=str(bad)
    assert main([*args,'--max-input-bytes','1'])==1;capsys.readouterr();assert not bad.exists()
