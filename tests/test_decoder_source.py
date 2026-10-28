"""Pinned source-only loading, no package initializer or upstream Rust import."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import pytest
from obs_migrate import decoder_source,journal
from obs_migrate.cli import main

ROOT=Path(__file__).parents[1]


def copy_source(tmp_path):
    original=decoder_source.locate();target=tmp_path/'audited-python';target.mkdir()
    for name in decoder_source.HASHES:shutil.copyfile(original/name,target/name)
    # An unreviewed package initializer must never execute.
    (target/'__init__.py').write_text("raise RuntimeError('package initializer must not execute')\n")
    return target


def test_source_loading_avoids_initializer_and_reuses_verified_original_bytes(tmp_path,monkeypatch):
    target=copy_source(tmp_path);monkeypatch.setattr(decoder_source,'locate',lambda:target)
    before=set(sys.modules);first=decoder_source.load();second=decoder_source.load()
    assert first==second and first[0].__module__.startswith('_obs_migrate_ddss_')
    created=set(sys.modules)-before
    assert {n.rsplit('.',1)[-1] for n in created if n.startswith('_obs_migrate_ddss_') and '.' in n}=={'decoder','stream'}
    assert decoder_source.describe()['upstream_rust_extension_required'] is False
    assert list(journal.events(ROOT/'tests/fixtures/botsv3/one-event',experimental=True))


@pytest.mark.parametrize('name',['decoder.py','stream.py'])
def test_any_source_drift_is_rejected_before_execution_and_before_cache_reuse(tmp_path,monkeypatch,name):
    target=copy_source(tmp_path);monkeypatch.setattr(decoder_source,'locate',lambda:target)
    decoder_source.load();path=target/name
    path.write_bytes(path.read_bytes()+b"\nraise RuntimeError('unverified bytes must not execute')\n")
    with pytest.raises(ValueError,match='differs from audited pin'):decoder_source.load()
    with pytest.raises(journal.JournalError,match='audited pin'):journal.decoder_class()


def test_joint_verification_precedes_creating_any_private_module(tmp_path,monkeypatch):
    target=copy_source(tmp_path);(target/'stream.py').write_text("raise RuntimeError('must not execute')\n")
    monkeypatch.setattr(decoder_source,'locate',lambda:target);before={n for n in sys.modules if n.startswith('_obs_migrate_ddss_')}
    with pytest.raises(ValueError,match='audited pin'):decoder_source.load()
    assert {n for n in sys.modules if n.startswith('_obs_migrate_ddss_')}==before


def test_unavailable_decoder_cli_guidance_works_for_wheel_installations(tmp_path,monkeypatch,capsys):
    monkeypatch.setattr(decoder_source,'locate',lambda:tmp_path/'absent')
    out=tmp_path/'events.parquet'
    assert main(['buckets','export',str(ROOT/'tests/fixtures/botsv3/one-event'),
                 '--experimental','--out',str(out)])==1
    report=json.loads(capsys.readouterr().err)
    assert 'obsmigrate setup-decoder --source' in report['error']
    assert 'scripts/' not in report['error']
    assert not out.exists()


def test_config_is_venv_scoped_idempotent_and_does_not_overwrite(tmp_path,monkeypatch,capsys):
    target=copy_source(tmp_path);venv=tmp_path/'venv';venv.mkdir();monkeypatch.setattr(sys,'prefix',str(venv))
    assert main(['setup-decoder','--source',str(target)])==0
    configured=json.loads(capsys.readouterr().out);assert configured['source']==str(target.resolve())
    assert decoder_source.configure(target)==configured
    assert decoder_source.locate()==target.resolve()
    assert main(['doctor'])==0
    report=json.loads(capsys.readouterr().out);assert report['decoder']['status']=='ready'
    assert not report['capabilities']['upstream_rust_decoder_required']
    other=tmp_path/'other';other.mkdir()
    for name in decoder_source.HASHES:shutil.copyfile(target/name,other/name)
    with pytest.raises(ValueError,match='refusing overwrite'):decoder_source.configure(other)
    monkeypatch.setattr(sys,'prefix',sys.base_prefix)
    with pytest.raises(ValueError,match='virtual environment'):decoder_source.configure(target)


@pytest.mark.parametrize('row',[[],None,{'decoder_source_schema':True,'source':'/tmp','pin':decoder_source.PIN},
    {'decoder_source_schema':1,'source':'relative','pin':decoder_source.PIN},
    {'decoder_source_schema':1,'source':'/tmp','pin':'wrong'}])
def test_invalid_config_is_rejected_without_falling_back(tmp_path,monkeypatch,row):
    monkeypatch.setattr(sys,'prefix',str(tmp_path));decoder_source.config_path().write_text(json.dumps(row))
    with pytest.raises(ValueError):decoder_source.locate()


def test_real_journal_decodes_in_fresh_process_with_upstream_native_import_blocked():
    code=r'''
import sys,json,hashlib,importlib.abc
class NoUpstreamNative(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname=='splunk_ddss_extractor._native':raise ImportError('native deliberately unavailable')
sys.meta_path.insert(0,NoUpstreamNative())
from obs_migrate import journal,events,decoder_source
rows=list(journal.events(sys.argv[1],experimental=True))
assert 'splunk_ddss_extractor' not in sys.modules
assert 'splunk_ddss_extractor._native' not in sys.modules
print(json.dumps({'events':len(rows),'sha256':hashlib.sha256(b''.join(map(events.json_bytes,rows))).hexdigest(),'mode':decoder_source.describe()['mode']}))
'''
    path=ROOT/'tests/fixtures/botsv3/db_1534766331_1534766040_301'
    output=subprocess.check_output([sys.executable,'-c',code,str(path)],text=True)
    row=json.loads(output);assert row['events']==57 and 'initializer_not_executed' in row['mode']
