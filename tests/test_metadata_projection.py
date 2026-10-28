"""Bounded derived caches; native coverage uses original authorised fixture bytes."""
from pathlib import Path

import pytest

from obs_migrate import journal
from obs_migrate.journal_access import Observer
from obs_migrate.event_bucket import EventBucket
from obs_migrate.native_event_scan import NativeEventScan

ROOT = Path(__file__).parent/'fixtures/botsv3'


def capture(name='stream-29', budget=32*1024**2):
    observer = Observer(16*1024**2, 1000, cache_projections=True, metadata_cache_bytes=budget)
    rows = list(journal.events(ROOT/name, experimental=True, _observer=observer))
    return observer, rows


@pytest.mark.parametrize('name', ['one-event', 'stream-29', 'typed-metadata', 'db_1534766331_1534766040_301'])
def test_genuine_metadata_projection_avoids_full_redecode(name, monkeypatch):
    observer, rows = capture(name)
    try:
        def forbidden(*args, **kwargs):
            raise AssertionError('metadata projection redecoded a record')
        monkeypatch.setattr(observer, 'record', forbidden)
        for ordinal, expected in enumerate(rows):
            for columns in [('metadata_json',), tuple(k for k in expected if k!='decoder_state_json')]:
                actual, work = observer.project(ordinal, columns)
                assert actual == {k:expected[k] for k in columns}
                assert work['native_records_decoded'] == work['native_record_bytes_read'] == 0
                copied = len(expected['metadata_json'].encode())
                copied += sum(len(expected[k]) for k in ('raw','record_wire') if k in columns)
                assert work['native_projected_bytes_read'] == copied
        report = observer.report()['metadata_projection_cache']
        assert report['cached_records'] == len(rows)
        assert report['unique_values'] == len({r['metadata_json'] for r in rows})
        assert report['buffer_bytes'] <= report['charged_bytes'] <= report['budget_bytes']
    finally:
        observer.close()


@pytest.mark.parametrize('budget', [0, 1, 4, 512])
def test_budget_exhaustion_preserves_full_canonical_fallback(budget):
    observer, rows = capture(budget=budget)
    try:
        fallbacks = 0
        for ordinal, row in enumerate(rows):
            actual, work = observer.project(ordinal, ('metadata_json','raw','event_time'))
            assert actual == {k:row[k] for k in actual}
            fallbacks += work['native_records_decoded']
            assert observer.record(ordinal)[0] == row
        assert fallbacks > 0
        assert observer.report()['metadata_projection_cache']['charged_bytes'] <= budget
        # Exhausting metadata capacity must not disable the scalar/raw cache.
        assert observer.project(0, ('raw',))[1]['native_records_decoded'] == 0
    finally:
        observer.close()


def test_decoder_state_still_requires_digest_checked_full_record():
    observer, rows = capture('typed-metadata')
    try:
        actual, work = observer.project(0, ('metadata_json','decoder_state_json'))
        assert actual == {k:rows[0][k] for k in actual}
        assert work['native_records_decoded'] == 1
    finally:
        observer.close()


@pytest.mark.parametrize('budget', [-1, True, 64*1024**2+1, 0.5, None])
def test_invalid_observer_budget_refused(budget):
    with pytest.raises(ValueError, match='metadata cache budget'):
        Observer(1000, 10, cache_projections=True, metadata_cache_bytes=budget)


def test_duplicate_strings_are_interned_and_budget_includes_references():
    from obs_migrate.journal_access import _MetadataCache
    cache = _MetadataCache(1024)
    values = ['["exact café \\u0000"]']*1000
    for value in values:
        cache.add(value)
    cache.finish()
    cached = [cache.get(i) for i in range(len(values))]
    assert cached[0] == values[0] and cached[-1] is None
    assert all(value is None or value == values[i] for i,value in enumerate(cached))
    report = cache.report()
    assert report['unique_values'] == 1
    assert report['cached_records'] == sum(value is not None for value in cached)
    assert report['buffer_bytes'] <= report['charged_bytes'] <= 1024
    with pytest.raises(ValueError):
        cache.add('after freeze')
    cache.close()
    with pytest.raises(ValueError):
        cache.get(0)


def test_incomplete_initial_scan_cannot_publish_cache():
    observer = Observer(16*1024**2, 1000, cache_projections=True, metadata_cache_bytes=1024)
    source = journal.events(ROOT/'stream-29', experimental=True, _observer=observer)
    next(source)
    source.close()
    with pytest.raises(ValueError, match='incomplete'):
        observer.project(0, ('metadata_json',))
    observer.close()


def test_bad_derived_cache_reference_refuses_instead_of_returning_metadata():
    observer, _ = capture()
    try:
        observer._metadata_cache._refs = b'\xff'*4 + observer._metadata_cache._refs[4:]
        with pytest.raises(ValueError, match='metadata cache reference'):
            observer.project(0, ('metadata_json',))
    finally:
        observer.close()


@pytest.mark.parametrize('budget', [0, 32*1024**2])
def test_linked_arrow_stream_metadata_equivalence_cancellation_and_reuse(budget):
    expected = list(journal.events(ROOT/'stream-29', experimental=True))
    with EventBucket(ROOT/'stream-29', experimental=True, offline_snapshot=True,
                     backend='journal', cache_projections=True, metadata_cache_bytes=budget) as source:
        scan = NativeEventScan(source, None, 1024**2, lambda n:None)
        cols = [scan.schema.get_field_index(k) for k in ('record_ordinal','metadata_json')]
        iterator = scan.scan([], cols)
        next(iterator)
        iterator.close()
        assert not scan.events()[-1]['completed']
        scan.reset()
        actual = [row for batch in scan.scan([], cols) for row in batch.to_pylist()]
        assert actual == [{k:r[k] for k in ('record_ordinal','metadata_json')} for r in expected]
        result = scan.events()[-1]
        assert result['completed']
        assert result['native_records_decoded'] == (len(expected) if budget == 0 else 0)


def test_cli_metadata_budget_is_applied_to_real_linked_source(capsys):
    import json
    from obs_migrate.cli import main
    assert main(['events','snapshot-inspect',str(ROOT/'stream-29'),
                 '--experimental','--offline-snapshot','--backend','journal',
                 '--cache-projections','--metadata-cache-bytes','0']) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['metadata_projection_cache']['budget_bytes'] == 0
    assert report['metadata_projection_cache']['buffer_bytes'] == 0


def test_oversized_value_falls_back_without_poisoning_later_small_entries():
    from obs_migrate.journal_access import _MetadataCache
    cache = _MetadataCache(1024)
    cache.add('x'*2048)
    cache.add('[]')
    cache.add('[]')
    cache.finish()
    assert cache.get(0) is None
    assert cache.get(1) == cache.get(2) == '[]'
    assert cache.report()['unique_values'] == 1
    assert cache.report()['charged_bytes'] <= 1024
