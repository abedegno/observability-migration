"""All-field metric Parquet transport on genuine native controls."""
from pathlib import Path
import copy
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from obs_migrate import msidx,msidx1,msidx2,metric_journal

ROOT=Path(__file__).parent/'fixtures'


def control(kind):
    if kind=='xor':
        file=next((ROOT/'metric-xor-9').glob('*/*.tsidx'));return msidx,list(msidx.records(file,experimental=True))
    version={'one':'7.0.0','two':'7.3.9','four':'10.6.0.5','journal':'10.6.0.5'}[kind]
    bucket=next(p for p in (ROOT/'metrics'/version).iterdir() if p.is_dir())
    reader={'one':msidx1,'two':msidx2,'four':msidx,'journal':metric_journal}[kind]
    if kind=='journal':return reader,list(reader.records(bucket,experimental=True))
    file=next(bucket.glob('*.tsidx'));return reader,list(reader.records(file,experimental=True))


@pytest.mark.parametrize('kind',['one','two','four','journal','xor'])
def test_every_row_group_preserves_all_native_canonical_fields(tmp_path,kind):
    reader,rows=control(kind);path=tmp_path/'points.parquet';cpus=pa.cpu_count()
    exported=reader.export(iter(rows),path,max_rows=2 if kind!='xor' else 17)
    with pq.ParquetFile(path) as file:assert file.num_row_groups>1
    actual=list(reader.read_export(path,'parquet'));assert actual==rows
    assert [reader.json_bytes(row) for row in actual]==[reader.json_bytes(row) for row in rows]
    result=reader.verify(iter(rows),path,'parquet');assert next(v for k,v in result.items() if k.startswith('verified_'))==len(rows)
    assert pa.cpu_count()==cpus


def test_metric_parquet_rejects_wrong_schema_truncation_and_field_mismatch(tmp_path):
    reader,rows=control('four');good=tmp_path/'good.parquet';reader.export(iter(rows),good,max_rows=1)
    wrong=tmp_path/'wrong.parquet';pq.write_table(pa.table({'unrelated':[1]}),wrong)
    with pytest.raises(ValueError):list(reader.read_export(wrong,'parquet'))
    truncated=tmp_path/'truncated.parquet';truncated.write_bytes(good.read_bytes()[:-1])
    with pytest.raises((ValueError,OSError,pa.ArrowException)):list(reader.read_export(truncated,'parquet'))
    bad=tmp_path/'mismatch.parquet';changed=copy.deepcopy(rows);changed[0]['index']='different-provenance-label';reader.export(iter(changed),bad)
    with pytest.raises(ValueError,match='mismatch'):reader.verify(iter(rows),bad,'parquet')


def test_closing_partial_metric_reader_releases_parquet_file(tmp_path,monkeypatch):
    reader,rows=control('four');path=tmp_path/'points.parquet';reader.export(iter(rows),path,max_rows=1)
    original=pq.ParquetFile;opened=[];closed=[]
    class Observed:
        def __init__(self,*args,**kwargs):self.file=original(*args,**kwargs);opened.append(self)
        def __getattr__(self,key):return getattr(self.file,key)
        def __enter__(self):return self
        def __exit__(self,*args):self.file.close();closed.append(self)
    monkeypatch.setattr(pq,'ParquetFile',Observed)
    iterator=reader.read_export(path,'parquet');assert next(iterator)==rows[0]
    assert len(closed)<len(opened);iterator.close();assert opened and len(closed)==len(opened)
