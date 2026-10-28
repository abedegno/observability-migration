"""Operator progress uses genuine immutable source copies and leaves job data intact."""
from dataclasses import replace
import json
from pathlib import Path
import shutil

import pytest
from obs_migrate import job_dataset
from obs_migrate.cli import main
from obs_migrate.index_job import InputSpec, plan_job, manifest, identity
from obs_migrate.job_dataset import export_job, verify_job, read_job

SOURCE = Path(__file__).parent/'fixtures/botsv3/db_1534766331_1534766040_301'


@pytest.fixture
def job(tmp_path):
    inputs = []
    for i in range(2):
        root = tmp_path/f'private-root-{i}-event-token'
        root.mkdir()
        shutil.copytree(SOURCE, root/SOURCE.name)
        inputs.append(InputSpec('source'+str(i), 'local', root, (SOURCE.name,), index='sample'))
    plan = plan_job(inputs, offline_snapshot=True, allow_duplicate_journals=True)
    spec = tmp_path/'private-spec.json'
    spec.write_text(json.dumps({'spec_version':1, 'allow_duplicate_journals':True,
        'inputs':[{'input_id':s.input_id, 'kind':'local', 'root':str(s.root),
                   'index':s.index, 'selectors':list(s.selectors)} for s in inputs]}))
    return plan, spec


def progress_rows(stderr):
    return [r for line in stderr.splitlines() if (r:=json.loads(line)).get('type')=='job_progress']


def stage_pairs(rows):
    return [(r['stage'],r['status']) for r in rows]


def assert_safe(rows, plan):
    allowed = {'type','progress_schema','operation','stage','status','duration_seconds',
               'member_number','members_total','source_key','input_id','selector'}
    for row in rows:
        assert set(row) <= allowed
        assert row['type']=='job_progress' and row['progress_schema']==1
        if 'duration_seconds' in row:
            assert row['duration_seconds'] >= 0
        assert all(str(s.root) not in json.dumps(row) for s in plan.inputs)
        if 'source_key' in row:
            source = next(s for s in plan.sources if s.source_key==row['source_key'])
            assert row['input_id']==source.input_id and row['selector']==source.selector
            assert 1 <= row['member_number'] <= row['members_total']==2


def test_callback_reports_validated_members_and_preserves_exact_job_result(job,tmp_path):
    plan,_ = job
    rows = []
    result = export_job(plan,tmp_path/'observed',progress=rows.append)
    plain = export_job(plan,tmp_path/'plain')
    assert result==plain and result['events']==114
    assert ('validation','started') in stage_pairs(rows)
    assert ('export','completed') in stage_pairs(rows)
    assert stage_pairs(rows)[-1]==('job','completed')
    assert_safe(rows,plan)
    for member in plan.sources:
        stages = stage_pairs([r for r in rows if r.get('source_key')==member.source_key])
        assert stages.index(('validation','completed')) < stages.index(('export','started'))
        assert stages.index(('export','completed')) < stages.index(('checkpoint','completed'))
    assert [r.event for r in read_job(tmp_path/'observed')]==[r.event for r in read_job(tmp_path/'plain')]


def test_cancel_after_checkpoint_then_resume_revalidates_saved_members(job,tmp_path):
    plan,_ = job
    output = tmp_path/'job'
    rows = []
    def cancel(row):
        rows.append(row)
        if row['stage']=='checkpoint' and row['status']=='completed':
            raise KeyboardInterrupt('private event body must not be reported')
    with pytest.raises(KeyboardInterrupt):
        export_job(plan,output,progress=cancel)
    state = json.loads((output/'manifest.json').read_text())
    assert state['complete'] is False and len(state['members'])==1
    assert ('job','completed') not in stage_pairs(rows)
    assert ('job','cancelled') in stage_pairs(rows)
    with pytest.raises(ValueError,match='incomplete'):
        list(read_job(output))
    saved = output/state['members'][0]['path']
    stamp = (saved.read_bytes(),saved.stat().st_mtime_ns)
    rows = []
    result = export_job(plan,output,resume=True,progress=rows.append)
    assert result['complete'] and result['events']==114
    assert (saved.read_bytes(),saved.stat().st_mtime_ns)==stamp
    assert ('reuse','completed') in stage_pairs(rows) and ('export','completed') in stage_pairs(rows)
    assert stage_pairs(rows)[-1]==('job','completed')
    assert_safe(rows,plan)


@pytest.mark.parametrize('operation',['verify','completed-resume'])
def test_verification_and_complete_resume_report_work_instead_of_skipping(job,tmp_path,operation):
    plan,_ = job
    output = tmp_path/'job'
    original = export_job(plan,output)
    rows = []
    if operation=='verify':
        result = verify_job(plan,output,progress=rows.append)
        assert result['verified_events']==114
        assert ('verify','completed') in stage_pairs(rows)
    else:
        assert export_job(plan,output,resume=True,progress=rows.append)==original
        assert ('reuse','completed') in stage_pairs(rows)
        assert ('export','started') not in stage_pairs(rows)
    assert ('validation','started') in stage_pairs(rows)
    assert ('source_check','completed') in stage_pairs(rows)
    assert stage_pairs(rows)[-1]==('job','completed')
    assert_safe(rows,plan)


def test_corrupt_member_progress_identifies_validated_member_without_completing(job,tmp_path):
    plan,_ = job
    output = tmp_path/'job'
    state = export_job(plan,output)
    (output/state['members'][0]['path']).write_bytes(b'ordinary corruption, not a valid proprietary format')
    rows = []
    with pytest.raises(ValueError):
        verify_job(plan,output,progress=rows.append)
    failed = [r for r in rows if r['status']=='failed' and 'source_key' in r]
    assert failed and failed[-1]['source_key']==plan.sources[0].source_key
    assert ('job','completed') not in stage_pairs(rows)
    assert_safe(rows,plan)


def test_final_source_failure_does_not_report_or_publish_complete(job,tmp_path,monkeypatch):
    plan,_ = job
    output = tmp_path/'job'
    rows = []
    def fail(_):
        raise ValueError('private-root/event-payload')
    with monkeypatch.context() as patch:
        patch.setattr(job_dataset,'finish_inputs',fail)
        with pytest.raises(ValueError):
            export_job(plan,output,progress=rows.append)
    assert ('source_check','failed') in stage_pairs(rows)
    assert ('job','completed') not in stage_pairs(rows)
    assert json.loads((output/'manifest.json').read_text())['complete'] is False
    assert 'private-root/event-payload' not in json.dumps(rows)
    assert export_job(plan,output,resume=True)['complete']


def test_callback_failure_releases_job_lock_and_leaves_recoverable_checkpoint(job,tmp_path):
    plan,_ = job
    output = tmp_path/'job'
    def fail(row):
        if row['stage']=='validation' and row['status']=='started':
            raise RuntimeError('sink unavailable')
    with pytest.raises(RuntimeError):
        export_job(plan,output,progress=fail)
    assert not json.loads((output/'manifest.json').read_text())['complete']
    assert export_job(plan,output,resume=True)['complete']


@pytest.mark.parametrize('callback',[True,42,'private-root'])
def test_noncallable_progress_refuses_before_creating_output(job,tmp_path,callback):
    plan,_ = job
    output = tmp_path/'job'
    with pytest.raises(ValueError,match='progress'):
        export_job(plan,output,progress=callback)
    assert not output.exists()


def test_unbound_plan_cannot_disclose_unvalidated_member_labels(job,tmp_path):
    plan,_ = job
    broken = replace(plan,sources=(replace(plan.sources[0],selector='/private/secret/event'),*plan.sources[1:]))
    rows = []
    with pytest.raises(ValueError):
        export_job(broken,tmp_path/'job',progress=rows.append)
    assert not rows


@pytest.mark.parametrize('command',['plan','export','verify'])
def test_cli_progress_is_opt_in_json_on_stderr_with_unchanged_stdout(job,tmp_path,capsys,command):
    plan,spec = job
    common = ['--spec',str(spec),'--offline-snapshot']
    if command=='plan':
        options=[];plain_options=[]
    elif command=='export':
        options=['--out',str(tmp_path/'observed')];plain_options=['--out',str(tmp_path/'plain')]
    else:
        export_job(plan,tmp_path/'original')
        options=plain_options=['--against',str(tmp_path/'original')]
    assert main(['jobs',command,*common,*plain_options])==0
    plain = capsys.readouterr()
    assert not plain.err
    assert main(['jobs',command,*common,*options,'--progress'])==0
    observed = capsys.readouterr()
    assert json.loads(observed.out)==json.loads(plain.out)
    rows=progress_rows(observed.err)
    assert rows and stage_pairs(rows)[:2]==[('planning','started'),('planning','completed')]
    assert_safe(rows,plan)
    assert str(tmp_path) not in observed.err


@pytest.mark.parametrize('failure',[ValueError,KeyboardInterrupt])
def test_cli_reports_active_failure_or_cancel_without_exception_data(job,tmp_path,capsys,monkeypatch,failure):
    plan,spec = job
    secret = 'private-root-secret-original-event'
    def fail(*args,**kwargs):
        raise failure(secret)
    with monkeypatch.context() as patch:
        patch.setattr(job_dataset.event_bucket,'export',fail)
        assert main(['jobs','export','--spec',str(spec),'--offline-snapshot','--out',str(tmp_path/'job'),'--progress'])==(130 if failure is KeyboardInterrupt else 1)
    observed = capsys.readouterr()
    assert not observed.out and secret not in observed.err and str(tmp_path) not in observed.err
    rows=progress_rows(observed.err)
    assert ('export','cancelled' if failure is KeyboardInterrupt else 'failed') in stage_pairs(rows)
    assert ('job','completed') not in stage_pairs(rows)
    assert_safe(rows,plan)
    assert main(['jobs','export','--spec',str(spec),'--offline-snapshot','--out',str(tmp_path/'job'),'--resume','--progress'])==0
    assert json.loads(capsys.readouterr().out)['complete']


def test_cli_spec_failure_emits_no_unvalidated_root_or_selector(tmp_path,capsys):
    spec=tmp_path/'secret-spec.json'
    spec.write_text(json.dumps({'spec_version':1,'inputs':[{'input_id':'source','kind':'local',
        'root':str(tmp_path/'secret-root'),'selectors':['private-root/event-payload']}]}))
    assert main(['jobs','plan','--spec',str(spec),'--offline-snapshot','--progress'])==1
    observed=capsys.readouterr()
    assert not observed.out and str(tmp_path) not in observed.err and 'event-payload' not in observed.err
    rows=progress_rows(observed.err)
    assert stage_pairs(rows)==[('planning','started'),('planning','failed')]
    assert all('selector' not in r and 'source_key' not in r for r in rows)


def test_terminal_reporting_failure_does_not_replace_cancellation(job,tmp_path,monkeypatch):
    plan,_ = job
    output=tmp_path/'job'
    def cancel(*args,**kwargs):
        raise KeyboardInterrupt('original cancellation')
    def broken_terminal(row):
        if row['status']=='cancelled':
            raise RuntimeError('terminal sink failure')
    with monkeypatch.context() as patch:
        patch.setattr(job_dataset.event_bucket,'export',cancel)
        with pytest.raises(KeyboardInterrupt,match='original cancellation'):
            export_job(plan,output,progress=broken_terminal)
    assert export_job(plan,output,resume=True)['complete']


def test_rehashed_but_inconsistent_plan_does_not_disclose_member_labels(job,tmp_path):
    plan,_ = job
    broken = replace(plan,sources=(replace(plan.sources[0],selector='/private/secret/event'),*plan.sources[1:]))
    broken = replace(broken,selection_sha256=identity(manifest(broken)))
    rows=[]
    with pytest.raises(ValueError):
        export_job(broken,tmp_path/'job',progress=rows.append)
    assert not rows
    assert not (tmp_path/'job').exists()
