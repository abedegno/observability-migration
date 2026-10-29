"""Bounded evidence catalogue of a caller-acquired closed local index root.

This catalogues bytes and names, not native activity or completeness. It never
selects buckets automatically and does not decode or rewrite proprietary files.
"""
import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import stat

from .source_identity import normalize_sha256, require_sha256

MAX_FILES = 200_000
MAX_BYTES = 8 * 1024**3
MAX_DEPTH = 16
MAX_ROOT_SIDECAR = 1024**2
MAX_ROOT_SIDECARS = 4 * 1024**2
JOURNALS = ('rawdata/journal', 'rawdata/journal.gz', 'rawdata/journal.zst', 'rawdata/journal.lz4')


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def _sha(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _stamp(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns)


def _walk(root, max_files, max_bytes):
    entries = {}
    total = files = 0
    pending = [(root, 0)]
    while pending:
        path, depth = pending.pop()
        if depth > MAX_DEPTH:
            raise ValueError('catalogue directory depth limit exceeded')
        value = path.lstat()
        relative = path.relative_to(root).as_posix()
        if not stat.S_ISDIR(value.st_mode):
            raise ValueError('catalogue directories must be local directories, without symlinks')
        entries[relative] = ('directory', _stamp(value))
        for child in sorted(path.iterdir()):
            value = child.lstat()
            if stat.S_ISDIR(value.st_mode):
                pending.append((child, depth + 1))
            elif stat.S_ISREG(value.st_mode):
                entries[child.relative_to(root).as_posix()] = ('file', _stamp(value))
                files += 1
                total += value.st_size
            else:
                raise ValueError('catalogue refuses symlinks and special filesystem entries')
            if files > max_files or total > max_bytes or len(entries) + len(pending) > 2 * max_files:
                raise ValueError('catalogue file/byte/entry budget exceeded')
    return entries


def _read(path, expected, retain):
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, 'rb') as stream:
        if _stamp(os.fstat(stream.fileno())) != expected:
            raise ValueError('catalogue source changed before read')
        digest = hashlib.sha256()
        saved = bytearray() if retain else None
        count = 0
        while chunk := stream.read(1024**2):
            count += len(chunk)
            if count > expected[3]:
                raise ValueError('catalogue source grew during read')
            digest.update(chunk)
            if retain:
                saved.extend(chunk)
        if count != expected[3] or _stamp(os.fstat(stream.fileno())) != expected or _stamp(path.lstat()) != expected:
            raise ValueError('catalogue source changed during read')
    return digest.hexdigest(), bytes(saved) if retain else None


def _manifest(raw):
    """Interpret only the already observed CSV path/id fields; retain all bytes."""
    if raw is None:
        return {'status': 'absent', 'rows': []}
    try:
        reader = csv.DictReader(io.StringIO(raw.decode('utf-8'), newline=''), strict=True)
        if not reader.fieldnames or 'path' not in reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ValueError('unsupported columns')
        rows = []
        seen = set()
        for row in reader:
            name = row.get('path')
            if (None in row or any(v is None for v in row.values()) or not name
                    or name in ('.', '..') or '/' in name or '\\' in name or name in seen):
                raise ValueError('invalid manifest row')
            seen.add(name)
            rows.append(row)
            if len(rows) > 10_000:
                raise ValueError('manifest row limit exceeded')
        return {'status': 'observed_csv', 'rows': rows}
    except (UnicodeError, csv.Error, ValueError):
        return {'status': 'opaque_or_invalid', 'rows': []}


def catalogue(path, *, offline_snapshot=False, expected_sha256=None,
              max_files=MAX_FILES, max_bytes=MAX_BYTES):
    """Hash all entries; fail on observed changes and retain bounded root sidecars.

    offline_snapshot is an acquisition assertion, not a guarantee established by
    stat checks. The identity binds relative names, empty directories and bytes.
    It excludes absolute paths and filesystem timestamps, so copies are portable.
    """
    if offline_snapshot is not True:
        raise ValueError('catalogue requires an explicit offline_snapshot assertion')
    expected_sha256 = normalize_sha256(expected_sha256)
    if type(max_files) is not int or not 1 <= max_files <= MAX_FILES or type(max_bytes) is not int or not 1 <= max_bytes <= MAX_BYTES:
        raise ValueError('catalogue budgets must be positive and within hard limits')
    root = Path(path).absolute()
    # Reject symlinks in all existing ancestors, including a symlinked root.
    if any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError('catalogue root and ancestors must not be symlinks')
    initial = _walk(root, max_files, max_bytes)
    inventory = []
    sidecars = []
    retained = 0
    manifest_raw = None
    for name, (kind, stamp) in sorted(initial.items()):
        if name == '.':
            continue
        if kind == 'directory':
            inventory.append({'path': name, 'kind': kind})
            continue
        is_root = '/' not in name
        keep = is_root and stamp[3] <= MAX_ROOT_SIDECAR and retained + stamp[3] <= MAX_ROOT_SIDECARS
        digest, raw = _read(root / name, stamp, keep)
        item = {'path': name, 'kind': kind, 'bytes': stamp[3], 'sha256': digest}
        inventory.append(item)
        if is_root:
            sidecar = dict(item, interpretation='opaque')
            sidecar['bytes_base64'] = base64.b64encode(raw).decode() if raw is not None else None
            sidecars.append(sidecar)
            if raw is not None:
                retained += len(raw)
            if name == '.bucketManifest':
                manifest_raw = raw
    if initial != _walk(root, max_files, max_bytes):
        raise ValueError('catalogue source inventory changed during scan')
    manifest = _manifest(manifest_raw)
    if any(s['path'] == '.bucketManifest' for s in sidecars) and manifest_raw is None:
        manifest['status'] = 'opaque_over_retention_limit'
    manifest_rows = {r['path']: r for r in manifest['rows']}
    candidates = []
    root_dirs = [i['path'] for i in inventory if i['kind'] == 'directory' and '/' not in i['path']]
    for name in root_dirs:
        prefix = name + '/'
        contents = [dict(item, path=item['path'][len(prefix):]) for item in inventory if item['path'].startswith(prefix)]
        files = {item['path']: item for item in contents if item['kind'] == 'file'}
        journal_names = [p for p in JOURNALS if p in files]
        if not (journal_names or name.startswith(('db_', 'rb_', 'hot_', 'DISABLED-')) or any(p['path'] == 'rawdata' for p in contents)):
            continue
        evidence = []
        if name.startswith('DISABLED-'):
            evidence.append('disabled_directory_name')
        if name.startswith('hot_'):
            evidence.append('hot_directory_name')
        if len(journal_names) != 1:
            evidence.append('missing_or_ambiguous_journal')
        listed = name in manifest_rows if manifest['status'] == 'observed_csv' else None
        if listed is False:
            evidence.append('not_listed_in_observed_manifest')
        candidates.append({'name': name, 'directory_sha256': _sha(contents),
                           'manifest_listed': listed, 'manifest_row': manifest_rows.get(name),
                           'journals': [files[p] for p in journal_names],
                           'file_count': len(files), 'evidence': evidence,
                           'search_visibility': 'UNVERIFIED', 'selection': 'explicit_only'})
    journal_groups = {}
    for candidate in candidates:
        for item in candidate['journals']:
            journal_groups.setdefault(item['sha256'], []).append(candidate['name'])
    duplicates = [{'journal_sha256': sha, 'buckets': sorted(set(names))}
                  for sha, names in sorted(journal_groups.items()) if len(set(names)) > 1]
    identity = _sha(inventory)
    require_sha256(identity, expected_sha256, 'index-root catalogue')
    return {'format': 'obs-index-root-catalogue-1', 'inventory_sha256': identity,
            'acquisition': 'caller_asserted_closed_offline_root',
            'source_completeness': 'UNVERIFIED', 'search_visibility': 'UNVERIFIED',
            'inventory': inventory, 'root_sidecars': sidecars, 'manifest': manifest,
            'missing_manifest_paths': sorted(set(manifest_rows) - set(root_dirs)),
            'buckets': candidates, 'duplicate_journals': duplicates,
            'files': sum(i['kind'] == 'file' for i in inventory),
            'bytes': sum(i.get('bytes', 0) for i in inventory)}


def select(report, names, *, allow_duplicate_journals=False):
    """Bind an explicit physical selection to a freshly acquired catalogue.

    This does not authorize extraction from a changed tree, resolve recovery
    ownership or establish that these copies are the native active set.
    """
    if type(allow_duplicate_journals) is not bool:
        raise ValueError('allow_duplicate_journals must be boolean')
    if type(names) not in (list, tuple) or not names or any(type(n) is not str for n in names) or len(set(names)) != len(names):
        raise ValueError('explicit nonempty unique bucket names required')
    by_name = {b['name']: b for b in report['buckets']}
    if any(name not in by_name for name in names):
        raise ValueError('selected bucket absent from catalogue candidates')
    if any(len(by_name[name]['journals']) != 1 for name in names):
        raise ValueError('selected bucket requires exactly one journal candidate')
    conflicts = [g for g in report['duplicate_journals'] if len(set(g['buckets']) & set(names)) > 1]
    if conflicts and not allow_duplicate_journals:
        raise ValueError('selected copies share original journal bytes; explicit allow_duplicate_journals required')
    return {'format': 'obs-index-root-selection-1', 'catalogue_sha256': report['inventory_sha256'],
            'buckets': sorted(names), 'duplicate_journal_override': allow_duplicate_journals,
            'duplicate_journal_conflicts': conflicts, 'search_visibility': 'UNVERIFIED',
            'semantics': 'explicit_physical_copies_only'}
