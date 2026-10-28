"""Shared bounded, atomic Arrow/NDJSON/Parquet transport for native metric rows."""
import base64
import hashlib
from itertools import zip_longest
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .export import atomic_output


class MetricRecordIO:
    def __init__(self, schema, source_format, binary_fields, *, count_field, status, schema_version=1):
        self.schema=schema
        self.schema_version=schema_version
        self.source_format=source_format
        self.binary_fields=binary_fields
        self.count_field=count_field
        self.status=status

    def json_bytes(self,row):
        if row.get('schema_version')!=self.schema_version or row.get('source_format')!=self.source_format or set(row)!=set(self.schema.names):
            raise ValueError('metric record schema mismatch')
        result=dict(row)
        encode=lambda value:base64.b64encode(value).decode() if value is not None else None
        for key in self.binary_fields:result[key]=encode(row[key])
        result['measurements']=[dict(m,name=encode(m['name']),value_bits_le=encode(m['value_bits_le'])) for m in row['measurements']]
        result['dimensions']=[{k:encode(v) for k,v in d.items()} for d in row['dimensions']]
        return (json.dumps(result,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

    def from_json(self,line):
        row=json.loads(line)
        if row.get('schema_version')!=self.schema_version or row.get('source_format')!=self.source_format or set(row)!=set(self.schema.names):
            raise ValueError('metric record schema mismatch')
        decode=lambda value:base64.b64decode(value,validate=True) if value is not None else None
        for key in self.binary_fields:row[key]=decode(row[key])
        for m in row['measurements']:
            m['name']=decode(m['name']);m['value_bits_le']=decode(m['value_bits_le'])
        for d in row['dimensions']:
            d['key']=decode(d['key']);d['value']=decode(d['value'])
        return row

    def batches(self,rows,max_rows=1024,max_bytes=8*1024*1024):
        if max_rows<1 or max_bytes<1:raise ValueError('metric record batch limits must be positive')
        pending=[];size=0
        for row in rows:
            amount=len(self.json_bytes(row))
            if pending and (len(pending)>=max_rows or size+amount>max_bytes):
                yield pa.RecordBatch.from_pylist(pending,schema=self.schema);pending=[];size=0
            pending.append(row);size+=amount
        if pending:yield pa.RecordBatch.from_pylist(pending,schema=self.schema)

    def export(self,rows,output,format='parquet',*,max_rows=1024,max_bytes=8*1024*1024):
        if format not in ('ndjson','parquet'):raise ValueError('metric record format must be ndjson or parquet')
        count=measurements=0;digest=hashlib.sha256()
        def counted():
            nonlocal count,measurements
            for row in rows:
                count+=1;measurements+=len(row['measurements']);digest.update(self.json_bytes(row));yield row
        try:
            with atomic_output(output) as file:
                if format=='ndjson':
                    for row in counted():file.write(self.json_bytes(row))
                else:
                    with pq.ParquetWriter(file,self.schema,compression='zstd',write_statistics=False) as writer:
                        for batch in self.batches(counted(),max_rows,max_bytes):writer.write_batch(batch)
            return {self.count_field:count,'measurements':measurements,'canonical_sha256':digest.hexdigest(),
                    'output_bytes':Path(output).stat().st_size,'native_assisted':False,
                    'source_format':self.source_format,**self.status}
        finally:
            close=getattr(rows,'close',None)
            if close:close()

    def read_export(self,path,format):
        if format=='ndjson':
            with open(path,'rb') as file:
                for line in file:yield self.from_json(line)
        elif format=='parquet':
            with pq.ParquetFile(path) as file:
                if not file.schema_arrow.equals(self.schema,check_metadata=True):raise ValueError('metric record schema mismatch')
                for group in range(file.num_row_groups):
                    for batch in file.iter_batches(batch_size=1024,row_groups=[group]):yield from batch.to_pylist()
        else:raise ValueError('unknown metric record export format')

    def verify(self,rows,path,format):
        actual=self.read_export(path,format);sentinel=object();count=0
        try:
            for expected,got in zip_longest(rows,actual,fillvalue=sentinel):
                if expected is sentinel or got is sentinel or self.json_bytes(expected)!=self.json_bytes(got):
                    raise ValueError('metric record mismatch at record '+str(count))
                count+=1
            return {'verified_'+self.count_field:count,'comparison':'all canonical fields, ordered',
                    'native_assisted':False,**self.status}
        finally:
            for iterator in (rows,actual):
                close=getattr(iterator,'close',None)
                if close:close()
