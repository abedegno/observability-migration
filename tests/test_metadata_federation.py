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
