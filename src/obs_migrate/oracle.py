"""Disk-backed reconciliation against independently acquired Splunk searches.

The _cd mapping is an explicit hypothesis checked per event for the observed
version-1/alignment-5 profile, not a generic locator or TSIDX implementation.
"""
from contextlib import closing
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time

from .journal import events


def fraction(value):
    if value.startswith('.'):
        value = '0'+value
    if not re.fullmatch(r'0(?:\.[0-9]{1,128})?', value):
        raise ValueError('unsupported oracle fraction representation')
    return value.rstrip('0').rstrip('.') if '.' in value else value


def signature(raw, host, source, sourcetype, seconds, subsecond, index_time):
    """Length-delimited SHA-256; no text conversion of original payload/metadata."""
    digest = hashlib.sha256()
    for value in (raw, host, source, sourcetype, str(seconds).encode(),
                  fraction(subsecond).encode(), str(index_time).encode()):
        if value is None:
            digest.update(b'N')
        else:
            digest.update(b'V'+len(value).to_bytes(8,'big'))
            digest.update(value)
    return digest.digest()


def expected_signature(row):
    def unprefix(key):
        value = row[key]
        if value is None:
            return None
        prefix = (key+'::').encode()
        if not value.startswith(prefix):
            raise ValueError('unsupported dictionary metadata prefix')
        return value[len(prefix):]
    return signature(row['raw'], *(unprefix(k) for k in ('host','source','sourcetype')),
                     row['event_time']['seconds'], row['event_time']['fraction'], row['index_time']['seconds'])


def actual_signature(row, expected_index='botsv3'):
    if not re.fullmatch(r'[0-9]+:[0-9]+', row['_cd']):
        raise ValueError('unsupported search locator representation')
    bkt = row['_bkt'].split('~')
    if len(bkt) != 3 or bkt[0] != expected_index or bkt[1] != row['_cd'].split(':')[0]:
        raise ValueError('search bucket identity does not match locator')
    def binary(key):
        return row[key].encode('utf-8','surrogateescape') if key in row else None
    return signature(binary('_raw'), *(binary(k) for k in ('host','source','sourcetype')),
                     int(row['oracle_seconds']), row.get('_subsecond','0'), int(row['_indextime']))


def decode_envelope(line):
    """Reverse Splunk's opaque non-UTF-8 response octets without replacement.

    Some native responses have JSON syntax but invalid UTF-8 payload bytes. This
    oracle-only transport bridge is followed by surrogateescape re-encoding of
    binary fields. Toolkit exports continue to use valid JSON with Base64 bytes.
    """
    return json.loads(line.decode('utf-8','surrogateescape'))


def search_rows(path,*,allow_empty=False):
    """Read complete native results; empty controls require explicit opt-in."""
    if type(allow_empty) is not bool:raise ValueError('allow_empty must be boolean')
    opener = gzip.open if str(path).endswith('.gz') else open
    count, final = 0, False
    with opener(path, 'rb') as file:
        while line := file.readline(64*1024*1024+1):
            if len(line) > 64*1024*1024:
                raise ValueError('oracle line exceeds 64 MiB')
            if not line.strip():
                continue  # Legacy native streaming endpoints can send keepalives.
            envelope = decode_envelope(line)
            if final:raise ValueError('oracle contains frames after the final marker')
            if envelope.get('preview') is not False:
                raise ValueError('oracle contains preview/nonfinal results')
            if 'lastrow' in envelope and type(envelope['lastrow']) is not bool:
                raise ValueError('oracle final marker must be boolean')
            if any(m.get('type') in ('ERROR','FATAL') for m in envelope.get('messages',[])):
                raise ValueError('oracle contains a search error')
            if 'result' in envelope:
                if final or envelope.get('offset') != count:
                    raise ValueError('oracle offsets are not contiguous')
                count += 1
                yield envelope['result']
            final |= envelope.get('lastrow',False)
    if (not count and not allow_empty) or not final:
        raise ValueError('oracle stream is incomplete')


def compare_search(source_root, acquisition_path, search_path, database_path, *, progress=None):
    """Compare each locator and its field signature, preserving event multiplicity.

    SQLite is a derived scratch index, never an input modification or a source of
    truth. A successful result requires source EOF checks and final search frames.
    """
    start = time.monotonic()
    source_root = Path(source_root)
    acquisition = json.loads(Path(acquisition_path).read_text())
    if not all(b['journal_unchanged'] for b in acquisition['copied_inputs']):
        raise ValueError('acquired oracle has changed working-copy journals')
    path = Path(search_path)
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path,'rb') as file:
        digest = hashlib.file_digest(file,'sha256').hexdigest()
    if digest != acquisition['events']['sha256']:
        raise ValueError('acquired search checksum mismatch')
    database_path = Path(database_path)
    with database_path.open('xb'):
        pass
    expected_count = actual_count = 0
    formatted_differences = 0
    max_formatted_difference = Decimal(0)
    per_bucket = []
    ids = set()
    with closing(sqlite3.connect(database_path)) as db:
        db.execute('PRAGMA journal_mode=OFF')
        db.execute('PRAGMA synchronous=OFF')
        db.execute('PRAGMA cache_size=-32768')
        db.execute('CREATE TABLE records (locator TEXT PRIMARY KEY, expected BLOB, actual BLOB, seen INTEGER DEFAULT 0, '
                   'field_mismatches INTEGER DEFAULT 0, conflicting_occurrences INTEGER DEFAULT 0)')
        for bucket in acquisition['copied_inputs']:
            name = bucket['bucket']
            if not re.fullmatch(r'db_[0-9]+_[0-9]+_[0-9]+', name):
                raise ValueError('unsupported oracle bucket name')
            bid = name.rsplit('_',1)[1]
            if bid in ids:
                raise ValueError('oracle bucket ID collision')
            ids.add(bid)
            stats = {}
            with closing(events(source_root/name, experimental=True, visibility='undeleted', stats=stats)) as rows:
                for row in rows:
                    header = json.loads(row['decoder_state_json'])['header']
                    if header['version'] != 1 or header['align_bits'] != 5 or row['decompressed_offset'] % 32:
                        raise ValueError('oracle locator hypothesis restricted to v1/alignment5')
                    if row['source_sha256'] != bucket['journal_sha256']:
                        raise ValueError('source journal differs from acquired oracle')
                    locator = f'{bid}:{row["decompressed_offset"] >> 5}'
                    db.execute('INSERT INTO records(locator,expected) VALUES (?,?)', (locator,expected_signature(row)))
                    expected_count += 1
                    if expected_count % 50_000 == 0:
                        db.commit()
                        if progress:
                            progress({'expected_events':expected_count})
            per_bucket.append({'bucket':name, 'physical_events':stats['physical_events'],
                               'deleted_events':stats['deleted_events'], 'emitted_events':stats['emitted_events']})
        db.commit()
        for row in search_rows(search_path):
            sig = actual_signature(row)
            exact = Decimal(row['oracle_seconds'])+Decimal(fraction(row.get('_subsecond','0')))
            if 'oracle_time' in row:
                difference = abs(Decimal(row['oracle_time'])-exact)
                formatted_differences += difference != 0
                max_formatted_difference = max(max_formatted_difference,difference)
            db.execute('INSERT INTO records(locator,actual,seen) VALUES (?,?,1) '
                       'ON CONFLICT(locator) DO UPDATE SET actual=excluded.actual,seen=records.seen+1, '
                       'field_mismatches=records.field_mismatches+CASE WHEN records.expected IS NOT NULL AND records.expected!=excluded.actual THEN 1 ELSE 0 END, '
                       'conflicting_occurrences=records.conflicting_occurrences+CASE WHEN records.actual IS NOT NULL AND records.actual!=excluded.actual THEN 1 ELSE 0 END',
                       (row['_cd'],sig))
            actual_count += 1
            if actual_count % 50_000 == 0:
                db.commit()
                if progress:
                    progress({'actual_events':actual_count})
        db.commit()
        mismatch = 'expected IS NULL OR actual IS NULL OR field_mismatches != 0 OR seen != 1'
        mismatches = db.execute('SELECT count(*) FROM records WHERE '+mismatch).fetchone()[0]
        samples = [r[0] for r in db.execute('SELECT locator FROM records WHERE '+mismatch+' ORDER BY locator LIMIT 32')]
        missing = db.execute('SELECT count(*) FROM records WHERE actual IS NULL').fetchone()[0]
        unexpected = db.execute('SELECT count(*) FROM records WHERE expected IS NULL').fetchone()[0]
        duplicates = db.execute('SELECT count(*) FROM records WHERE seen>1').fetchone()[0]
        duplicate_excess = db.execute('SELECT coalesce(sum(seen-1),0) FROM records WHERE seen>1').fetchone()[0]
        field_mismatches,conflicting = db.execute('SELECT coalesce(sum(field_mismatches),0),coalesce(sum(conflicting_occurrences),0) FROM records').fetchone()
    counts_match = expected_count == actual_count == acquisition['aggregate_count'] == acquisition['events']['results']
    return {'status':'PASS' if counts_match and not mismatches else 'FAIL',
            'expected_undeleted_events':expected_count,'actual_search_events':actual_count,
            'counts_match':counts_match,'locator_or_field_mismatches':mismatches,
            'missing_locators':missing,'unexpected_locators':unexpected,'duplicate_locators':duplicates,
            'duplicate_excess_rows':duplicate_excess,'field_mismatching_occurrences':field_mismatches,
            'conflicting_duplicate_occurrences':conflicting,
            'distinct_record_content_status':'PASS' if not (missing or unexpected or field_mismatches) else 'FAIL',
            'sample_mismatch_locators':samples,'per_bucket':per_bucket,
            'field_comparison':'SHA-256 over length-delimited raw bytes, host/source/sourcetype, exact event seconds/fraction, index seconds',
            'timestamp_policy':'oracle_seconds from floor(_time); original native _subsecond, canonical decimal zeroes; display/printf time retained but not used',
            'formatted_timestamp_differences':formatted_differences,
            'max_formatted_timestamp_difference_seconds':str(max_formatted_difference),
            'locator_evidence':'every _cd checked against bucket numeric ID and decompressed opcode offset >> 5; observed v1/alignment5 only',
            'elapsed_seconds':time.monotonic()-start,'database_bytes':database_path.stat().st_size,
            'build':acquisition['build'],'search_sha256':digest,
            'scope':'isolated BOTS index, default app/admin context; not generic SPL, permissions or TSIDX implementation'}
