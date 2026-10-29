"""Explicit portable Dataset schemas over genuine immutable native captures."""
from pathlib import Path
import json
import pytest
import pyarrow as pa
from obs_migrate import capture,metric_select,metric_limits
from obs_migrate.capture_dataset import dataset

ROOT=Path(__file__).parent/'fixtures'

@pytest.fixture
def captured(tmp_path):
    root=next((ROOT/'metric-xor-9').glob('db_*'));path=tmp_path/'capture.zip'
    receipt=capture.create(root,path,experimental=True,offline_snapshot=True,limits=metric_limits.LARGE_METRICS)
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True,limits=metric_limits.LARGE_METRICS) as source:
        yield source


def test_projected_records_and_dataset_equal_full_canonical_fields(captured):
    selection=captured.select(index='test',bucket_id='original');full=list(selection.records())
    columns=['storage_json','measurements','timestamp_wire','indexed_time','host','point_ordinal']
    expected=[{key:row[key] for key in columns} for row in full]
    assert list(selection.records(columns=columns))==expected
    value,handler=dataset(selection,32*1024**2,columns=columns)
    actual=value.to_table(use_threads=False)
    assert actual.to_pylist()==expected
    assert actual.schema.equals(pa.schema([selection.reader.SCHEMA.field(key) for key in columns],metadata=selection.reader.SCHEMA.metadata),check_metadata=True)
    assert handler.materializations==1
    assert value.to_table(use_threads=False).equals(actual) and handler.materializations==1
    assert 'bucket_snapshot_manifest' in json.loads(expected[0]['storage_json'])
    assert all('bucket_snapshot_manifest' not in json.loads(row['storage_json']) for row in expected[1:])


def test_projection_avoids_numeric_decode_except_predicate_dependencies(captured):
    full=list(captured.select().records());row=full[0]
    pred=metric_select.Predicate(metric_name=row['measurements'][0]['name'],dimensions=tuple((d['key'],d['value']) for d in row['dimensions']))
    selection=captured.select(pred)
    expected=[{'point_ordinal':r['point_ordinal']} for r in selection.records()]
    assert list(selection.records(columns=['point_ordinal']))==expected
    readers=[s.reader for s in captured._sources.values()];before=sum(r.work_report()['numeric_lookups'] for r in readers)
    value,_=dataset(captured.select(),32*1024**2,columns=['point_ordinal','host','indexed_time'])
    assert value.to_table(use_threads=False).num_rows==1201
    assert sum(r.work_report()['numeric_lookups'] for r in readers)==before
    assert all(r.work_report()['cursor_bytes']==0 for r in readers)


@pytest.mark.parametrize('columns',[[],(),['point_ordinal','point_ordinal'],['absent'],['point_ordinal',1],'host',{'host'},[None]])
def test_invalid_explicit_schema_refuses(captured,columns):
    with pytest.raises(ValueError,match='columns'):
        dataset(captured.select(),1024,columns=columns)
    with pytest.raises(ValueError,match='columns'):
        list(captured.select().records(columns=columns))


def test_empty_candidates_and_budget_failure_leave_no_partial_buffer(captured):
    selection=captured.select(metric_select.Predicate(host=b'absent'))
    value,handler=dataset(selection,4096,columns=['point_ordinal'])
    assert value.to_table(use_threads=False).num_rows==0 and handler.materializations==1
    reserved=[];value,handler=dataset(captured.select(),1,reserved.append,columns=['point_ordinal'])
    with pytest.raises(Exception,match='byte limit'):value.to_table(use_threads=False)
    assert handler.wire is None and handler.materializations==0 and not reserved
    assert len(list(captured.select().records(columns=['point_ordinal'])))==1201


def test_projected_interruption_retains_traceback_and_releases_capture(captured,monkeypatch):
    source=next(iter(captured._sources.values()));original=source._emit
    def interrupted(*args,**kwargs):
        rows=original(*args,**kwargs)
        try:
            yield next(rows)
            raise KeyboardInterrupt('after genuine projected row')
        finally:rows.close()
    monkeypatch.setattr(source,'_emit',interrupted)
    value,handler=dataset(captured.select(),32*1024**2,columns=['point_ordinal'])
    with pytest.raises(KeyboardInterrupt) as retained:handler._ensure()
    assert retained.value.__traceback__ is not None
    assert handler.wire is None and handler.materializations==0
    assert source.reader.work_report()['cursor_bytes']==0
    monkeypatch.setattr(source,'_emit',original)
    assert value.to_table(use_threads=False).num_rows==1201


@pytest.mark.parametrize('version',['7.0.0','7.3.9'])
def test_legacy_projection_preserves_full_record_fields(tmp_path,version):
    path=tmp_path/'legacy.zip';root=next((ROOT/'metrics'/version).glob('db_*'))
    receipt=capture.create(root,path,experimental=True,offline_snapshot=True)
    with capture.CapturedBucket(path,expected_sha256=receipt['capture_sha256'],experimental=True) as source:
        selected=source.select();columns=['storage_json','measurements','point_ordinal','indexed_time','host']
        expected=[{k:r[k] for k in columns} for r in selected.records()]
        assert list(selected.records(columns=columns))==expected
        value,_=dataset(selected,32*1024**2,columns=columns)
        assert value.to_table(use_threads=False).to_pylist()==expected
