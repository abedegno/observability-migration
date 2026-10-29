"""One authentic TSIDX9 zero base; mutations are refusal witnesses only."""
from collections import Counter
import csv
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil

import pytest

from obs_migrate import event_bucket, event_index, oracle
from obs_migrate.smartstore import AcquiredObjects

FIXTURE = Path(__file__).parent/'fixtures/smartstore-tsidx9-10.6'
PHASES = ('00-baseline', '03-deleted', '05-deleted-rehydrated')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def acquired(phase):
    path = FIXTURE/phase
    source = AcquiredObjects(path, offline_snapshot=True, expected_inventory_sha256=sha(path/'objects.json'))
    bucket, = source.report()['buckets']
    return source, bucket


def stored(rows):
    # These three fields describe the acquisition/link/deletion observation.
    # Every original stored journal field, including wire/state/metadata, remains.
    return [{k:v for k,v in row.items() if k not in ('is_deleted','bucket_context_json','index_link')} for row in rows]


def test_original_control_hashes_and_provenance():
    manifest = json.loads((FIXTURE/'manifest.json').read_text())
    assert {p.relative_to(FIXTURE).as_posix():sha(p) for p in FIXTURE.rglob('*')
            if p.is_file() and p.name != 'manifest.json'} == manifest['files']
    provenance = json.loads((FIXTURE/'provenance.json').read_text())
    assert provenance['license'] == manifest['license'] == 'CC0-1.0'
    assert provenance['producer']['VERSION'] == '10.6.0.5'
    assert provenance['native_configuration'] == {'tsidxWritingLevel':4,'journalCompression':'zstd'}
    assert provenance['object_store']['network_internal'] and not provenance['object_store']['ports']


@pytest.mark.parametrize('phase', PHASES)
@pytest.mark.parametrize('backend', ['arrow','journal'])
def test_native_zero_base_delta_all_fields_and_transports(tmp_path, phase, backend):
    source,bucket = acquired(phase)
    baseline,original = acquired('00-baseline')
    original_bodies = {name:baseline._objects[key] for name,key in original['members'].items()}
    assert {name:source._objects[bucket['members'][name]] for name in original_bodies} == original_bodies
    key, = [key for name,key in bucket['members'].items() if name.endswith('.tsidx')]
    wire = source._objects[key]
    with event_index.open_bytes(wire, experimental=True) as index:
        assert wire[:8] == b'_TSIDX9\0' and index.reader.flags == 0
        section = index.reader.sections['delete']
        assert section['bits_per_value'] == 1
        assert not any(wire[section['offset']:section['offset']+section['bytes']])
    before = {p:sha(p) for p in (FIXTURE/phase).rglob('*') if p.is_file()}
    with baseline.event_bucket(original['id'], experimental=True, backend=backend) as old:
        original_rows = old.table().to_pylist()
    with source.event_bucket(bucket['id'], experimental=True, backend=backend) as physical:
        rows = physical.table().to_pylist()
        assert len(rows) == 14 and stored(rows) == stored(original_rows)
        assert Counter(r['raw'] for r in rows) == Counter((FIXTURE/'event-input.log').read_bytes().splitlines())
        expected = [r for r in rows if r['is_deleted'] is False]
        assert len(expected) == (14 if phase == '00-baseline' else 11)
        assert all(r['source_sha256'] == hashlib.sha256(source._objects[bucket['journal_key']]).hexdigest() for r in rows)
        assert all(r['object_uri'] == 'smartstore-object-key:'+bucket['journal_key'] for r in rows)
        # Ordinary local agreement must continue to reject the zero remote base.
        if phase != '00-baseline':
            with pytest.raises(ValueError, match='disagrees'):
                physical.deletion_report()
        with source.event_bucket(bucket['id'], experimental=True, backend=backend, visibility='undeleted') as selected:
            assert selected.table().to_pylist() == expected
            assert selected.report()['fully_validated_events'] == 14
            if phase != '00-baseline':
                observation = selected.deletion_report()
                assert observation['native_base_deleted_values'] == 0
                assert observation['journal_mask_deleted_values'] == 3
                assert observation['scope'] == 'all_zero_original_tsidx9_flags0_base_and_fully_reconciled_csv_delta;not_native_search_visibility'
                assert observation['search_visibility'] == 'UNVERIFIED'
                assert observation['indexed_search_supported'] is False
            for format in ('parquet','arrow'):
                output = tmp_path/format
                event_bucket.export(selected, output, format)
                assert event_bucket.verify(selected, output, format)['verified_events'] == len(expected)
            empty = tmp_path/'empty.parquet'
            event_bucket.export(selected, empty, term=b'__absent__')
            assert event_bucket.verify(selected, empty, term=b'__absent__')['verified_events'] == 0
    native = list(oracle.search_rows(FIXTURE/phase/'native.jsonl'))
    assert Counter(oracle.actual_signature(r, expected_index='smart_events_zstd') for r in native) == Counter(oracle.expected_signature(r) for r in expected)
    assert {int(r['_cd'].split(':')[1]) for r in native} == {r['decompressed_offset']>>5 for r in expected}
    with (FIXTURE/phase/'native.csv').open(newline='') as file:
        csv_rows = list(csv.DictReader(file))
    assert Counter(r['_raw'].encode() for r in csv_rows) == Counter(r['raw'] for r in expected)
    for row in csv_rows:
        matches = [r for r in expected if r['raw'] == row['_raw'].encode()]
        for want in matches:
            assert all(want[k] == row[k].encode() for k in ('host','source','sourcetype'))
            assert want['event_time']['seconds'] == int(row['_time'])
            assert Decimal(want['event_time']['fraction']) == Decimal(re.search(r'_subsecond::([^ ]+)',row['_meta'])[1])
            assert want['index_time']['seconds'] == int(re.search(r'_indextime::([0-9]+)',row['_meta'])[1])
    assert all(sha(p) == digest for p,digest in before.items())


@pytest.mark.parametrize('backend',['arrow','journal'])
def test_native_rehydrated_cache_keeps_strict_local_bit_agreement(backend):
    local, = (FIXTURE/'local').iterdir()
    with event_bucket.EventBucket(local, experimental=True, offline_snapshot=True, backend=backend, visibility='undeleted') as snapshot:
        assert snapshot.table().num_rows == 11
        assert snapshot.deletion_report()['deleted_values'] == 3
    source,bucket = acquired('03-deleted')
    with source.materialize(bucket['id']) as root:
        with pytest.raises(ValueError, match='disagrees'):
            event_bucket.EventBucket(root, experimental=True, offline_snapshot=True, backend=backend, visibility='undeleted')


def changed_copy(tmp_path, damage):
    path = tmp_path/'acquired'
    shutil.copytree(FIXTURE/'03-deleted',path)
    entries = json.loads((path/'objects.json').read_text())
    if damage in ('nonzero_base','unused_delete_bit','unknown_profile'):
        entry, = [e for e in entries if e['key'].endswith('.tsidx')]
        wire = bytearray((path/entry['file']).read_bytes())
        with event_index.open_bytes(bytes(wire),experimental=True) as index:
            section = index.reader.sections['delete'];count = index.reader.nvalues
        if damage == 'nonzero_base':wire[section['offset']] |= 1
        elif damage == 'unused_delete_bit':
            assert count % 8 and section['bytes'] == (count+7)//8
            wire[section['offset']+section['bytes']-1] |= 128
        else:wire[79] = 255  # Corrupted native flags: a refusal witness.
        data = bytes(wire)
    elif damage == 'missing_mask':
        entries[:] = [e for e in entries if '/rawdata/deletes/' not in e['key']]
        data = None
    else:
        entry, = [e for e in entries if '/rawdata/deletes/' in e['key']]
        lines = gzip.decompress((path/entry['file']).read_bytes()).decode().splitlines()
        keys = lines[0].split(',');values = lines[1].split(',')
        column = keys.index({'mask_address':'event_address','mask_metadata':'host_id','mask_timestamp':'timestamp'}[damage])
        values[column] = str(Decimal(values[column])+1000)
        lines[1] = ','.join(values)
        data = gzip.compress(('\n'.join(lines)+'\n').encode(),mtime=0)
    if data is not None:
        (path/entry['file']).write_bytes(data)
        entry.update(size=len(data),sha256=hashlib.sha256(data).hexdigest())
    (path/'objects.json').write_text(json.dumps(entries)+'\n')
    return path


@pytest.mark.parametrize('damage',['nonzero_base','unused_delete_bit','unknown_profile','missing_mask','mask_address','mask_metadata','mask_timestamp'])
def test_corrupt_base_and_incomplete_delta_refuse_before_even_empty_output(tmp_path, damage):
    path = changed_copy(tmp_path,damage)
    output = tmp_path/'never.parquet'
    with pytest.raises(ValueError) as failure:
        source = AcquiredObjects(path,offline_snapshot=True)
        bucket, = source.report()['buckets']
        with source.event_bucket(bucket['id'], experimental=True, visibility='undeleted') as snapshot:
            event_bucket.export(snapshot,output,term=b'__absent__')
    assert not output.exists() and failure.value is not None
    # Retain a failed reader's traceback through a fresh successful acquisition.
    source,bucket = acquired('00-baseline')
    with source.event_bucket(bucket['id'],experimental=True,visibility='undeleted') as snapshot:
        assert snapshot.table().num_rows == 14


@pytest.mark.parametrize('backend',['arrow','journal'])
def test_selected_iterator_early_close_releases_snapshot_for_reuse(backend):
    source,bucket = acquired('03-deleted')
    with source.event_bucket(bucket['id'],experimental=True,backend=backend,visibility='undeleted') as snapshot:
        records = snapshot.records()
        first = next(records)
        assert first['is_deleted'] is False
        records.close()
        assert snapshot.table().num_rows == 11
