"""Small authentic metric profiles; independent CSV and journal parity controls."""
from collections import Counter
from decimal import Decimal
import csv
import hashlib
import json
from pathlib import Path
import struct

import pytest
from obs_migrate import metric_journal, msidx, msidx1, msidx2

ROOT = Path(__file__).parent / 'fixtures/metrics'


def signature(row):
    return (row['host'], row['source'], row['sourcetype'],
            tuple(sorted((d['key'], d['value']) for d in row['dimensions'])),
            tuple((m['name'], m['value_bits_le']) for m in row['measurements']))


def instant(value):
    return Decimal(value['seconds']) + Decimal(value['fraction'])


def test_original_metric_fixture_checksums():
    for entry in json.loads((ROOT / 'manifest.json').read_text())['files']:
        assert hashlib.sha256((ROOT / entry['path']).read_bytes()).hexdigest() == entry['sha256']


@pytest.mark.parametrize('version,reader', [('7.0.0', msidx1), ('7.3.9', msidx2), ('10.6.0.5', msidx)])
def test_exact_metric_bits_and_metadata_match_retained_journals(version, reader):
    bucket, = (p for p in (ROOT / version).iterdir() if p.is_dir())
    indexed = [r for path in bucket.glob('*.tsidx') for r in reader.records(path, experimental=True)]
    journal = list(metric_journal.records(bucket, experimental=True))
    assert len(indexed) == len(journal) == 5
    assert Counter(map(signature, indexed)) == Counter(map(signature, journal))


def test_legacy_metric_native_csv_independently_checks_subseconds_and_values():
    bucket, = (p for p in (ROOT / '7.0.0').iterdir() if p.is_dir())
    indexed = [r for path in bucket.glob('*.tsidx') for r in msidx1.records(path, experimental=True)]
    expected = Counter()
    with (ROOT / '7.0.0/native.csv').open() as file:
        for row in csv.DictReader(file):
            meta = dict(term.split('::', 1) for term in row['_meta'].split())
            expected[(Decimal(row['_time']) + Decimal('0' + meta['_subsecond']),
                      row['host'].removeprefix('host::').encode(),
                      row['source'].removeprefix('source::').encode(),
                      row['sourcetype'].removeprefix('sourcetype::').encode(),
                      meta['metric_name'].encode(), meta['rack'].encode(),
                      struct.pack('<d', float(meta['_value']))) ] += 1
    actual = Counter()
    for row in indexed:
        measurement, = row['measurements']
        rack = next(d['value'] for d in row['dimensions'] if d['key'] == b'rack')
        actual[(instant(row['indexed_time']), row['host'], row['source'], row['sourcetype'],
                measurement['name'], rack, measurement['value_bits_le'])] += 1
    assert actual == expected and sum(actual.values()) == 5


@pytest.mark.parametrize('version,reader', [('7.3.9', msidx2), ('10.6.0.5', msidx)])
def test_indexed_metric_resolution_matches_independent_native_queries(version, reader):
    import datetime
    bucket, = (p for p in (ROOT / version).iterdir() if p.is_dir())
    rows = [r for path in bucket.glob('*.tsidx') for r in reader.records(path, experimental=True)]
    actual = Counter()
    for row in rows:
        measurement, = row['measurements']
        rack = next(d['value'] for d in row['dimensions'] if d['key'] == b'rack')
        assert row['indexed_time']['fraction'] == '0'
        actual[(row['indexed_time']['seconds'], measurement['name'], rack, row['host'],
                measurement['value_bits_le'])] += 1
    expected = Counter()
    for line in (ROOT / version / 'native-query.jsonl').read_text().splitlines():
        item = json.loads(line)['result']
        timestamp = item['_time'].rsplit(' ', 1)[0]
        seconds = int(datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f').replace(
            tzinfo=datetime.timezone.utc).timestamp())
        assert item['minimum'] == item['maximum']
        expected[(seconds, item['metric_name'].encode(), item['rack'].encode(), item['host'].encode(),
                  struct.pack('<d', float(item['minimum'])))] += int(item['n'])
    assert actual == expected and sum(actual.values()) == 5
