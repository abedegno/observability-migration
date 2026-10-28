"""Frozen genuine memberships and deterministic preflight call overhead.

Digests were captured from the preceding reader, not an independent format
oracle. Complete reader preflight separately checks posting times/cardinalities.
"""
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

import pytest

from obs_migrate import legacy_postings
from obs_migrate.event_index import open_index

ROOT = Path(__file__).parent/'fixtures'


@pytest.mark.parametrize('directory,digest,terms,memberships', [
    ('botsv3/db_1534766331_1534766040_301',
     '8636c6de0e62d55a07ab31ec63c63fd77f9f37a857ef99807fc11c1812f8a8a8',754,5600),
    ('botsv3/stream-29',
     '81f04fc125b445e7277824828d83ff0a56b61e14408f7b3cc1ec987807baa5ee',388,2039),
    ('current-producer-10.6.0.5/db_1792972802_1792972800_0',
     '866d7a3d6850352f4bdaa3bf8ac31de88a0d2d057c62fae12afca4e680b82f67',58,185),
])
def test_genuine_postings_preserve_membership_with_one_call_per_operand(directory,digest,terms,memberships):
    file, = (ROOT/directory).glob('*.tsidx')
    calls=Counter(); observed=hashlib.sha256();total=0
    def count_calls(frame,event,arg):
        if event=='call' and frame.f_code.co_filename==legacy_postings.__file__:
            calls[frame.f_code.co_name]+=1
    with open_index(file,experimental=True) as source:
        assert source.reader.nterms==terms
        previous=sys.getprofile()
        try:
            sys.setprofile(count_calls)
            for tid in range(terms):
                ids,runs=source._postings(tid)
                total+=len(ids)
                observed.update(json.dumps([tid,ids,runs],separators=(',',':')).encode()+b'\n')
        finally:sys.setprofile(previous)
    assert total==memberships
    assert observed.hexdigest()==digest
    # These retained lists have literal/delta operands, without run or inline
    # encodings. Count Python call events instead of asserting noisy wall time.
    assert calls['uint32']==memberships
    assert sum(calls.values()) <= terms+memberships
