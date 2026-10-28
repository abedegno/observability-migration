"""Read-only TSIDX/MSIDX header diagnostics, not indexed search or point decoding.

Original implementation from genuine fixture bytes and native version-predicate
observations. See docs/native-metrics-re.md for evidence and explicit limits.
"""
import base64
import hashlib
import os
from pathlib import Path
import struct

LEGACY=b'Splunk6\0'
DIMSETS={b'Msidx_2\0':'msidx-2',b'_MSIDX3\0':'msidx-3',b'_MSIDX4\0':'msidx-4'}
MODERN={b'_MSIDX4\0',b'_TSIDX8\0',b'_TSIDX9\0'}
OBSERVED={LEGACY,b'Msidx_2\0',b'_MSIDX4\0',b'_TSIDX8\0',b'_TSIDX9\0'}


def header_fields(header, *, file_size):
    """Classify a header only; recognition is not format/integrity validation."""
    if len(header)<8:raise ValueError('truncated TSIDX signature')
    magic=header[:8]
    known=magic in OBSERVED or magic in DIMSETS
    minimum=128 if magic in MODERN else 64
    if known and (len(header)<minimum or file_size<minimum):
        raise ValueError('truncated recognized TSIDX header')
    flags=struct.unpack_from('>I',header,40)[0] if magic==LEGACY else None
    metric=True if magic in DIMSETS or (magic==LEGACY and flags==0x40) else None
    profile=DIMSETS.get(magic,'unknown')
    if magic==LEGACY:profile='legacy-msidx-1' if metric else 'splunk6-unclassified'
    if magic in (b'_TSIDX8\0',b'_TSIDX9\0'):profile=magic.rstrip(b'\0').decode().lower().lstrip('_')+'-unclassified'
    return {'magic_base64':base64.b64encode(magic).decode(),
            'magic_display':magic.rstrip(b'\0').decode('ascii','backslashreplace'),
            'profile':profile,'observed_fixture_profile':magic in OBSERVED,
            'native_predicate_only':magic in DIMSETS and magic not in OBSERVED,
            'metric_header_detected':metric,'legacy_flags_be':flags,
            'column_store_location_decoded':False,
            'header_base64':base64.b64encode(header[:minimum] if known else header).decode(),
            'scope':'header diagnostics only; no complete format/integrity validation',
            'indexed_search_supported':False,'metric_point_extraction_supported':False}


def inspect_header(path):
    """Hash the original local file once; retain bounded header bytes only."""
    path=Path(path)
    if not path.is_file():raise ValueError('TSIDX source must be a regular local file')
    with path.open('rb') as file:
        before=os.fstat(file.fileno())
        header=file.read(128)
        fields=header_fields(header,file_size=before.st_size)
        file.seek(0)
        sha=hashlib.file_digest(file,'sha256').hexdigest()
        after=os.fstat(file.fileno())
        if (before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_size,after.st_mtime_ns,after.st_ctime_ns):
            raise ValueError('TSIDX source changed during inspection')
    return dict(fields,source_sha256=sha,source_bytes=before.st_size,
                source_bytes_read=before.st_size+len(header),diagnostic_schema_version=1)
