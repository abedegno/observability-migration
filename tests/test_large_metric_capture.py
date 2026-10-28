"""Existing genuine native bytes through explicit immutable capture budgets."""
from pathlib import Path
import json
import zipfile
import pytest
from obs_migrate import capture,metric_limits,metric_select,msidx

ROOT=Path(__file__).parent/'fixtures/metric-xor-9'
BUCKET=next(ROOT.glob('db_*'))
LARGE=metric_limits.LARGE_METRICS


def make(tmp_path,**options):
    path=tmp_path/'capture.zip'
    receipt=capture.create(BUCKET,path,experimental=True,offline_snapshot=True,limits=LARGE,**options)
    return path,receipt


def test_explicit_capture_preserves_all_originals_identity_and_canonical_rows(tmp_path):
    path,receipt=make(tmp_path)
    assert receipt['metric_resource_profile']==LARGE.describe()
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True) as default, capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=LARGE) as actual:
        assert actual.manifest==default.manifest
        assert list(actual.select().records())==list(default.select().records())
        assert actual.select().sha256==default.select().sha256
        report=actual.select().report();assert report.pop('metric_resource_profile')==LARGE.describe()
        assert report==default.select().report()
        with zipfile.ZipFile(path) as z:
            for name in actual.manifest['snapshot_manifest']['files']:assert z.read('source/'+name)==(BUCKET/name).read_bytes()
        assert sum(s.total for s in actual._sources.values())==1201
        assert all(s.reader.work_report()['cursor_bytes']==0 for s in actual._sources.values())


def test_scan_close_refusal_and_reuse_release_bounded_cursors(tmp_path):
    path,receipt=make(tmp_path)
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=LARGE) as actual:
        source=next(iter(actual._sources.values()));stream=actual.select().records();next(stream)
        assert source.reader.work_report()['cursor_bytes']>0
        stream.close();assert source.reader.work_report()['cursor_bytes']==0
        assert len(list(actual.select().records()))==1201
        assert source.reader.work_report()['cursor_bytes']==0
    assert source.closed and source.reader.work_report()['cursor_bytes']==0


def test_constructor_failure_closes_original_reader_cursors(tmp_path,monkeypatch):
    path,receipt=make(tmp_path);seen=[];original=msidx._Reader.rows
    def interrupted(reader,*args,**kwargs):
        seen.append(reader)
        stream=original(reader,*args,**kwargs)
        try:yield next(stream);raise KeyboardInterrupt('genuine preflight interruption')
        finally:stream.close()
    monkeypatch.setattr(msidx._Reader,'rows',interrupted)
    with pytest.raises(KeyboardInterrupt):capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=LARGE)
    assert seen and all(r.work_report()['cursor_bytes']==0 for r in seen)


def test_too_small_explicit_budget_refuses_without_publishing_capture(tmp_path):
    out=tmp_path/'bad.zip'
    with pytest.raises(msidx.MsidxError,match='cursor'):
        capture.create(BUCKET,out,experimental=True,offline_snapshot=True,limits=metric_limits.MetricLimits(cursor_bytes=1))
    assert not out.exists() and not list(tmp_path.glob('.obs-migrate-*'))
    path,receipt=make(tmp_path)
    with pytest.raises(msidx.MsidxError):capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=metric_limits.MetricLimits(values=1))


@pytest.mark.parametrize('version',['7.0.0','7.3.9'])
def test_legacy_explicit_profile_refuses_on_create_and_reopen(tmp_path,version):
    root=next((ROOT.parent/'metrics'/version).glob('db_*'));path=tmp_path/'legacy.zip'
    with pytest.raises(ValueError,match='MSIDX4'):capture.create(root,path,experimental=True,offline_snapshot=True,limits=LARGE)
    receipt=capture.create(root,path,experimental=True,offline_snapshot=True)
    with pytest.raises(ValueError,match='MSIDX4'):capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=LARGE)


def test_cli_capture_profile_receipt_and_invalid_limits(tmp_path,capsys):
    from obs_migrate.cli import main
    path=tmp_path/'cli.zip'
    assert main(['metrics','capture-create',str(BUCKET),'--experimental','--offline-snapshot','--resource-profile','large','--out',str(path)])==0
    assert json.loads(capsys.readouterr().out)['metric_resource_profile']==LARGE.describe()
    with pytest.raises(ValueError):capture.CapturedBucket(path,expected_sha256='0'*64,experimental=True,limits=object())


@pytest.mark.parametrize('operation',['resume','close'])
def test_interleaved_native_cleanup_never_blocks_behind_caller_selection(tmp_path,operation):
    import signal
    from obs_migrate.native_scan import NativeScan
    if not hasattr(signal,'setitimer'):pytest.skip('POSIX alarm bounds the deadlock regression')
    path,receipt=make(tmp_path)
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=LARGE) as source:
        native=NativeScan(source,None,None,None,256*1024**2,lambda size:None)
        stream=native.scan((),tuple(range(len(msidx.SCHEMA))))
        assert next(stream).num_rows>0
        ordinary=source.select().records();next(ordinary)
        def timed_out(*args):raise TimeoutError('native cleanup blocked behind caller-owned selection')
        original=signal.signal(signal.SIGALRM,timed_out)
        signal.setitimer(signal.ITIMER_REAL,1)
        try:
            if operation=='close':stream.close()
            else:
                with pytest.raises(ValueError,match='active'):next(stream)
        finally:
            signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,original)
            ordinary.close();stream.close()
        assert all(s.reader.work_report()['cursor_bytes']==0 for s in source._sources.values())
        assert len(list(source.select().records()))==1201
        again=NativeScan(source,None,None,None,256*1024**2,lambda size:None).scan((),())
        try:assert sum(batch.num_rows for batch in again)==1201
        finally:again.close()
