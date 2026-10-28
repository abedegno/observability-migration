"""Explicit on-disk format dispatch for independently implemented metric scans."""
from pathlib import Path

from . import msidx,msidx1,msidx2


def reader_for(path):
    path=Path(path)
    if not path.is_file():raise ValueError('metric storage source must be one regular file')
    with path.open('rb') as file:header=file.read(60)
    magic=header[:8]
    if magic==b'Splunk6\0' and len(header)==60 and int.from_bytes(header[40:44],'big')==64:return msidx1
    if magic==b'Msidx_2\0':return msidx2
    if magic==b'_MSIDX4\0':return msidx
    raise ValueError('unsupported metric storage signature; verified Float64 MSIDX1/MSIDX2/MSIDX4 profiles only')


def records(path,*,experimental=False,index=None,bucket_id=None):
    yield from reader_for(path).records(path,experimental=experimental,index=index,bucket_id=bucket_id)
