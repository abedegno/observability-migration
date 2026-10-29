"""Optional native DataFusion residual/projection parity on genuine small sources."""
from pathlib import Path

import pytest
import pyarrow as pa

datafusion = pytest.importorskip('datafusion')
pytest.importorskip('_native_provider')

from obs_migrate.event_bucket import EventBucket, SCHEMA
from obs_migrate.federation import BucketSession

ROOT = Path(__file__).parent/'fixtures/botsv3'


@pytest.mark.parametrize('name', ['stream-29','typed-metadata'])
@pytest.mark.parametrize('budget', [0,32*1024**2])
def test_metadata_native_sql_matches_materialized_engine_with_residuals(name, budget, monkeypatch):
    with EventBucket(ROOT/name, experimental=True, offline_snapshot=True, backend='journal',
                     cache_projections=True, metadata_cache_bytes=budget) as source:
        reference = datafusion.SessionContext()
        full = pa.Table.from_pylist(list(source.records()), schema=SCHEMA)
        reference.register_record_batches('events', [full.to_batches()])
        session = BucketSession()
        session.register_native_event_snapshot('events', source)
        if budget:
            def forbidden(*args, **kwargs):
                raise AssertionError('SQL metadata projection redecoded a full record')
            monkeypatch.setattr(source._journal, 'record', forbidden)
        queries = [
            'select record_ordinal,metadata_json from events order by record_ordinal',
            'select record_ordinal,metadata_json from events order by record_ordinal limit 3',
            "select record_ordinal,metadata_json from events where metadata_json like '%wire%' and record_ordinal%2=0 order by record_ordinal",
            "select record_ordinal from events where metadata_json='' order by record_ordinal",
            'select count(*) as n,sum(length(metadata_json)) as size from events',
            'select metadata_json,count(*) as n from events group by metadata_json order by metadata_json',
            'select a.record_ordinal,a.metadata_json as a_meta,b.metadata_json as b_meta from events a join events b on a.record_ordinal=b.record_ordinal where length(a.metadata_json)>0 and a.record_ordinal<3 order by a.record_ordinal',
            # Reuse after LIMIT/empty/join must still yield the complete source.
            'select record_ordinal,metadata_json from events order by record_ordinal',
        ]
        for sql in queries:
            expected = [row for batch in reference.sql(sql).collect() for row in batch.to_pylist()]
            result = session.sql(sql)
            assert result.table.to_pylist() == expected
            if budget:
                assert all(event['native_records_decoded']==0 for event in result.manifest['native_scans']['events'])


@pytest.mark.parametrize('limit', ['input','result'])
def test_cached_metadata_obeys_query_byte_caps_and_releases_source(limit):
    with EventBucket(ROOT/'stream-29', experimental=True, offline_snapshot=True,
                     backend='journal', cache_projections=True) as source:
        session = BucketSession(**{'max_'+limit+'_bytes':1})
        session.register_native_event_snapshot('events', source)
        with pytest.raises(ValueError, match='byte limit'):
            session.sql('select metadata_json from events')
        assert len(list(source.projected_records(('metadata_json',)))) == 29
        fresh = BucketSession()
        fresh.register_native_event_snapshot('events', source)
        assert fresh.sql('select count(*) as n from events').table.to_pylist() == [{'n':29}]


def test_interrupt_releases_native_stream_while_exception_is_retained(monkeypatch):
    import gc
    import _native_provider as bridge
    from datafusion.record_batch import RecordBatchStream
    state = {'opened':0, 'closed':0}
    schema = pa.schema([('x',pa.int64())])

    class Backend:
        def reset(self):
            pass
        def scan(self, clauses, projection):
            state['opened'] += 1
            try:
                for i in range(100):
                    yield pa.RecordBatch.from_arrays([pa.array([i])], schema=schema)
            finally:
                state['closed'] += 1

    session = BucketSession()
    backend = Backend()
    session._context.register_table('events', bridge.NativeProvider(schema, backend))
    session._sources['events'] = {'check':lambda:None, 'native':backend}
    original = RecordBatchStream.__next__

    def interrupted(self):
        original(self)
        raise KeyboardInterrupt('controlled cancellation after a native batch')

    monkeypatch.setattr(RecordBatchStream, '__next__', interrupted)
    with pytest.raises(KeyboardInterrupt) as caught:
        session.sql('select x from events')
    closed_before_discard = state['closed']
    assert caught.value.__traceback__ is not None
    # Clean up the old implementation safely after recording its failure.
    # Correctness must not depend on discarding the caller's exception or GC.
    caught.value.__traceback__ = None
    gc.collect()
    assert state['opened'] == closed_before_discard == 1


# These real sources catch lost residuals and incorrect bounded-cache fallback.
SOURCES = [
    (ROOT/'stream-29', 512),
    (ROOT/'db_1534766331_1534766040_301', 2048),
    (ROOT.parent/'current-producer-10.6.0.5/db_1792972802_1792972800_0', 2048),
]


@pytest.mark.parametrize('path,partial_budget', SOURCES, ids=['gzip-repeated','gzip-diverse','zstd'])
@pytest.mark.parametrize('coverage', ['zero','partial','full'])
@pytest.mark.parametrize('pushdown', [False,True])
def test_bounded_metadata_and_existing_candidates_preserve_all_sql_residuals(path, partial_budget, coverage, pushdown):
    budget = {'zero':0, 'partial':partial_budget, 'full':32*1024**2}[coverage]
    with EventBucket(path, experimental=True, offline_snapshot=True, backend='journal',
                     cache_projections=True, metadata_cache_bytes=budget) as source:
        rows = list(source.records())
        cache = source.report()['metadata_projection_cache']
        assert cache['charged_bytes'] <= budget
        if coverage == 'partial':
            assert 0 < cache['cached_records'] < len(rows)
        else:
            assert cache['cached_records'] == (len(rows) if coverage=='full' else 0)
        plain = datafusion.SessionContext()
        plain.register_record_batches('events', [pa.Table.from_pylist(rows, schema=SCHEMA).to_batches()])
        session = BucketSession()
        session.register_native_event_snapshot('events', source,
                                              routing_pushdown=pushdown, time_pushdown=pushdown)
        host = rows[0]['host'].hex()
        seconds = rows[len(rows)//2]['event_time']['seconds']
        queries = [
            'select record_ordinal,metadata_json from events order by record_ordinal',
            'select record_ordinal,metadata_json from events limit 1',
            "select record_ordinal,metadata_json,event_time from events where (metadata_json like '%wire%' and record_ordinal%2=0) or record_ordinal=1 order by record_ordinal",
            f"select record_ordinal,metadata_json from events where host=decode('{host}','hex') and event_time['seconds']>={seconds} order by record_ordinal",
            f"select record_ordinal,metadata_json from events where event_time['seconds']<{seconds} or record_ordinal%3=0 order by record_ordinal",
            'select record_ordinal,metadata_json from events where is_deleted is null or is_deleted order by record_ordinal',
            'select metadata_json,count(*) as n from events group by metadata_json order by metadata_json',
            'select count(*) as n,sum(length(metadata_json)) as bytes from events',
            'select record_ordinal from events where false',
            'select record_ordinal,metadata_json from events order by record_ordinal',
        ]
        saw_candidates = False
        for sql in queries:
            expected = [row for batch in plain.sql(sql).collect() for row in batch.to_pylist()]
            result = session.sql(sql)
            assert result.table.to_pylist() == expected, sql
            assert result.manifest['sources']['events']['snapshot_sha256'] == source.snapshot_sha256
            assert result.manifest['indexed_search_supported'] is False
            for scan in result.manifest['native_scans']['events']:
                saw_candidates |= scan['sql_pruning_applied']
                if pushdown:
                    assert scan['candidate_events'] <= scan['base_candidate_events']
        if pushdown:
            assert saw_candidates
        # An early LIMIT and an empty scan must not prevent complete full-row reuse.
        assert list(source.records()) == rows


def test_cross_bucket_union_join_preserves_duplicate_ordinals_and_source_provenance():
    from contextlib import ExitStack
    with ExitStack() as stack:
        session = BucketSession()
        plain = datafusion.SessionContext()
        snapshots = {}
        for name,(path,budget) in zip(('left_events','right_events'), (SOURCES[0],SOURCES[2])):
            source = stack.enter_context(EventBucket(path, experimental=True, offline_snapshot=True,
                backend='journal', cache_projections=True, metadata_cache_bytes=budget,
                index=name, bucket_id=name))
            snapshots[name] = source.snapshot_sha256
            table = pa.Table.from_pylist(list(source.records()), schema=SCHEMA)
            plain.register_record_batches(name, [table.to_batches()])
            session.register_native_event_snapshot(name, source, routing_pushdown=True, time_pushdown=True)
        queries = [
            "select bucket_id,source_sha256,record_ordinal,raw,metadata_json from left_events union all select bucket_id,source_sha256,record_ordinal,raw,metadata_json from right_events order by bucket_id,record_ordinal",
            'select a.record_ordinal,a.bucket_id as a_bucket,b.bucket_id as b_bucket,a.metadata_json as a_meta,b.metadata_json as b_meta from left_events a join right_events b on a.record_ordinal=b.record_ordinal where length(a.metadata_json)>0 order by a.record_ordinal',
            'select count(*) as n from left_events',
        ]
        for sql in queries:
            result = session.sql(sql)
            expected = [row for batch in plain.sql(sql).collect() for row in batch.to_pylist()]
            assert result.table.to_pylist() == expected
            # Provenance is conservatively all registered sources, even for count(left).
            assert {name:r['snapshot_sha256'] for name,r in result.manifest['sources'].items()} == snapshots


@pytest.mark.parametrize('cap', ['input','result'])
def test_failed_query_can_reuse_the_same_session_and_snapshot(cap):
    with EventBucket(ROOT/'stream-29', experimental=True, offline_snapshot=True,
                     backend='journal', cache_projections=True, metadata_cache_bytes=512) as source:
        session = BucketSession(**{'max_'+cap+'_bytes':512})
        session.register_native_event_snapshot('events', source)
        with pytest.raises(ValueError, match='byte limit'):
            session.sql('select metadata_json from events')
        assert session.sql('select count(*) as n from events').table.to_pylist() == [{'n':29}]
        assert session.sql('select record_ordinal from events order by record_ordinal').table.to_pylist() == [
            {'record_ordinal':i} for i in range(29)]
        assert len(list(source.records())) == 29


@pytest.mark.parametrize('budget', [0,512,32*1024**2])
def test_cancelled_query_reuses_session_while_original_exception_is_retained(budget, monkeypatch):
    """Catch a leaked session/source lock without discarding the caller's traceback."""
    from datafusion.record_batch import RecordBatchStream
    with EventBucket(ROOT/'stream-29', experimental=True, offline_snapshot=True,
                     backend='journal', cache_projections=True, metadata_cache_bytes=budget) as source:
        rows = list(source.records())
        session = BucketSession()
        session.register_native_event_snapshot('events', source)
        original = RecordBatchStream.__next__

        def interrupted(self):
            original(self)
            raise KeyboardInterrupt('controlled cancellation after a genuine event batch')

        with monkeypatch.context() as patch:
            patch.setattr(RecordBatchStream, '__next__', interrupted)
            with pytest.raises(KeyboardInterrupt) as caught:
                session.sql('select record_ordinal,raw,metadata_json from events')
        traceback = caught.value.__traceback__
        assert traceback is not None
        # No traceback clearing, garbage collection or new session before recovery.
        assert session.sql('select count(*) as n from events').table.to_pylist() == [{'n':29}]
        names = ('record_ordinal','raw','metadata_json','event_time')
        recovered = session.sql('select '+','.join(names)+' from events order by record_ordinal')
        assert recovered.table.to_pylist() == [{name:row[name] for name in names} for row in rows]
        assert caught.value.__traceback__ is traceback
        assert list(source.records()) == rows


@pytest.mark.parametrize('provider',['dataset','native'])
def test_explicit_metric_capture_residuals_and_retained_interrupt_reuse(tmp_path,monkeypatch,provider):
    from obs_migrate import capture,metric_limits,msidx
    from datafusion.record_batch import RecordBatchStream
    root=next((ROOT.parent/'metric-xor-9').glob('db_*'));path=tmp_path/'metrics.zip'
    receipt=capture.create(root,path,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS)
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=metric_limits.LARGE_METRICS) as source:
        rows=list(source.select().records());reference=datafusion.SessionContext()
        reference.register_record_batches('points',[pa.Table.from_pylist(rows,schema=msidx.SCHEMA).to_batches()])
        session=BucketSession();register=session.register_native_capture if provider=='native' else session.register_capture
        register('points',source)
        for sql in ('select count(*) as n from points',
                    'select point_ordinal,indexed_time,measurements from points where point_ordinal%2=0 order by point_ordinal',
                    'select * from points order by point_ordinal limit 3',
                    'select * from points where point_ordinal<0'):
            expected=reference.sql(sql).to_arrow_table();actual=session.sql(sql)
            assert actual.table.to_pylist()==expected.to_pylist()
            assert actual.table.schema.equals(expected.schema,check_metadata=False)
        original=RecordBatchStream.__next__
        def interrupted(stream):
            original(stream)
            raise KeyboardInterrupt('controlled cancellation after genuine metric batch')
        with monkeypatch.context() as patch:
            patch.setattr(RecordBatchStream,'__next__',interrupted)
            with pytest.raises(KeyboardInterrupt) as caught:session.sql('select measurements from points')
        assert caught.value.__traceback__ is not None
        assert all(s.reader.work_report()['cursor_bytes']==0 for s in source._sources.values())
        assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
        session.max_result_bytes=1
        with pytest.raises(ValueError,match='byte limit'):session.sql('select measurements from points')
        session.max_result_bytes=256*1024**2
        assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
        assert all(s.reader.work_report()['cursor_bytes']==0 for s in source._sources.values())


@pytest.fixture
def projected_capture(tmp_path):
    from obs_migrate import capture,metric_limits
    path=tmp_path/'projected.zip';root=next((ROOT.parent/'metric-xor-9').glob('db_*'))
    receipt=capture.create(root,path,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS)
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=metric_limits.LARGE_METRICS) as source:
        yield source


def test_projected_dataset_sql_residuals_schema_identity_and_reuse(projected_capture):
    source=projected_capture;selection=source.select();original=selection.sha256
    columns=['point_ordinal','host','indexed_time','measurements','storage_json']
    rows=list(selection.records());reference=datafusion.SessionContext()
    reference.register_record_batches('points',[pa.Table.from_pylist(rows,schema=source.reader.SCHEMA).to_batches()])
    session=BucketSession();session.register_capture('points',source,columns=columns)
    for sql in ('select count(*) as n from points','select * from points order by point_ordinal',
                'select host,indexed_time,measurements from points where point_ordinal%2=0 order by point_ordinal',
                'select * from points where point_ordinal<0','select * from points limit 3'):
        expected=reference.sql(sql.replace('select *','select '+','.join(columns))).to_arrow_table()
        actual=session.sql(sql)
        assert actual.table.to_pylist()==expected.to_pylist()
        assert actual.table.schema.equals(expected.schema,check_metadata=False)
        assert actual.manifest['query']['dataset_columns']=={'points':columns}
        assert actual.manifest['capture_datasets']['points']['columns']==columns
        assert actual.manifest['sources']['points']['query_sha256']==original
    with pytest.raises(ValueError):session.sql('select dimensions from points')
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    default=BucketSession();default.register_capture('points',source)
    sql='select count(*) as n from points';old=default.sql(sql);new=session.sql(sql)
    assert 'dataset_columns' not in old.manifest['query']
    assert old.manifest['query_sha256']!=new.manifest['query_sha256']
    assert source.select().sha256==original


def test_projected_dataset_shared_budget_failure_and_reuse(projected_capture):
    source=projected_capture
    probe=BucketSession();probe.register_capture('points',source,columns=['point_ordinal']);probe.sql('select count(*) from points')
    handler=probe._sources['points']['handler'];charged=max(handler.arrow_bytes,len(handler.wire))
    session=BucketSession(max_input_bytes=charged+1)
    session.register_capture('points',source,columns=['point_ordinal']);session.register_capture('other',source,columns=['point_ordinal'])
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    with pytest.raises(ValueError,match='byte limit'):session.sql('select count(*) from other')
    assert session._capture_bytes==charged
    assert session._sources['other']['handler'].wire is None
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    tiny=BucketSession(max_input_bytes=1);tiny.register_capture('points',source,columns=['point_ordinal'])
    with pytest.raises(ValueError,match='byte limit'):tiny.sql('select count(*) from points')
    assert tiny._capture_bytes==0 and tiny.sql('select 1 as n').table.to_pylist()==[{'n':1}]


def test_projected_dataset_retained_cancel_result_cap_and_source_failure(projected_capture,monkeypatch):
    from datafusion.record_batch import RecordBatchStream
    source=projected_capture;session=BucketSession();session.register_capture('points',source,columns=['point_ordinal','measurements'])
    native=next(iter(source._sources.values()));original_emit=native._emit
    def failed(*args,**kwargs):
        rows=original_emit(*args,**kwargs)
        try:
            yield next(rows)
            raise ValueError('controlled source read failure')
        finally:rows.close()
    with monkeypatch.context() as patch:
        patch.setattr(native,'_emit',failed)
        with pytest.raises(ValueError,match='controlled source read failure') as retained:session.sql('select count(*) from points')
    assert retained.value.__traceback__ is not None and session._capture_bytes==0
    original=RecordBatchStream.__next__
    def interrupted(stream):
        original(stream);raise KeyboardInterrupt('after genuine projected Dataset batch')
    with monkeypatch.context() as patch:
        patch.setattr(RecordBatchStream,'__next__',interrupted)
        with pytest.raises(KeyboardInterrupt) as caught:session.sql('select * from points')
    assert caught.value.__traceback__ is not None
    session.max_result_bytes=1
    with pytest.raises(ValueError,match='byte limit'):session.sql('select * from points')
    session.max_result_bytes=256*1024**2
    assert session.sql('select count(*) as n from points').table.to_pylist()==[{'n':1201}]
    assert all(s.reader.work_report()['cursor_bytes']==0 for s in source._sources.values())


def test_projected_dataset_cli_parquet_output(projected_capture,tmp_path,capsys):
    from obs_migrate.cli import main
    import pyarrow.parquet as pq
    source=projected_capture;path=tmp_path/'cli.parquet'
    # CapturedBucket retains a protected native member copy; pass original artifact.
    args=['metrics','capture-sql',str(tmp_path/'projected.zip'),'--capture-sha256',source.sha256,'--experimental',
          '--resource-profile','large','--dataset-column','point_ordinal','--sql','select count(*) as n from points','--out',str(path)]
    assert main(args)==0
    assert pq.read_table(path).to_pylist()==[{'n':1201}]
    assert 'query_sha256' in capsys.readouterr().out

