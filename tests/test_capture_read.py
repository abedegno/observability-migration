"""Bounded file pinning uses authentic input bytes; mutations are refusals only."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from obs_migrate.capture import _read

SEED=Path(__file__).parent/'fixtures/botsv3/one-event/rawdata/journal.gz'


def test_small_file_read_does_not_allocate_its_configured_cap():
    # Imports happen before tracing in a fresh process. This measures Python
    # traced heap, not RSS, and allows generous overhead above this tiny file.
    code='''import hashlib,json,sys,tracemalloc
from obs_migrate.capture import _read
tracemalloc.start()
data=_read(sys.argv[1],16*1024**2)
current,peak=tracemalloc.get_traced_memory()
tracemalloc.stop()
print(json.dumps({'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data),'peak':peak}))
'''
    process=subprocess.run([sys.executable,'-c',code,str(SEED.resolve())],capture_output=True,text=True)
    assert process.returncode==0,process.stderr
    result=json.loads(process.stdout)
    assert result['sha256']==hashlib.sha256(SEED.read_bytes()).hexdigest()
    assert result['bytes']==SEED.stat().st_size
    assert result['peak']<512*1024


@pytest.mark.parametrize('change',['grow','truncate','replace','short_read'])
def test_file_changes_and_incomplete_reads_refuse_and_close(tmp_path,monkeypatch,change):
    source=tmp_path/'input';wire=SEED.read_bytes();source.write_bytes(wire)
    original_open=Path.open;closed=[]
    class DuringRead:
        def __init__(self,file):self.file=file
        def __enter__(self):return self
        def __exit__(self,*args):
            try:return self.file.__exit__(*args)
            finally:closed.append(self.file.closed)
        def fileno(self):return self.file.fileno()
        def read(self,size):
            if change=='grow':
                with original_open(source,'ab') as file:file.write(wire[:1])
            elif change=='truncate':
                with original_open(source,'r+b') as file:file.truncate(len(wire)-1)
            elif change=='replace':
                source.rename(tmp_path/'original');source.write_bytes(wire)
            data=self.file.read(size)
            return data[:-1] if change=='short_read' else data
    def opened(path,mode='r',*args,**kwargs):
        file=original_open(path,mode,*args,**kwargs)
        return DuringRead(file) if path==source and mode=='rb' else file
    monkeypatch.setattr(Path,'open',opened)
    with pytest.raises(ValueError,match='changed during read'):_read(source,len(wire)+1024)
    assert closed==[True]


def test_exact_size_cap_and_empty_companions(tmp_path):
    wire=SEED.read_bytes()
    assert _read(SEED,len(wire))==wire
    with pytest.raises(ValueError,match='byte limit'):_read(SEED,len(wire)-1)
    # A plain empty companion-file utility case, not a proprietary-format seed.
    empty=tmp_path/'empty';empty.touch()
    assert _read(empty,0,zero=True)==b''
    with pytest.raises(ValueError,match='byte limit'):_read(empty,1024)


def test_non_regular_inputs_refuse_before_read(tmp_path):
    link=tmp_path/'link';link.symlink_to(SEED.resolve())
    for source in (link,tmp_path):
        with pytest.raises(ValueError,match='regular non-symlink'):_read(source,16*1024**2)
