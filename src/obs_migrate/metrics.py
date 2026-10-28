"""Native-assisted metric export JSON -> Arrow/Parquet, not a TSIDX decoder.

Exact supplied JSON lines/tokens are authoritative. Float64 values are derived
projections; native export's original TSIDX floating-point fidelity is unverified.
One row is one native point, retaining multiple measurements and duplicates.
"""
import base64
from decimal import Decimal, localcontext, ROUND_FLOOR
import hashlib
from itertools import zip_longest
import json
import math
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.parquet as pq

from .events import EXACT_TIME
from .export import atomic_output

MEASUREMENT = pa.struct([pa.field('name',pa.binary(),False),pa.field('value_token',pa.string(),False),
                         pa.field('value_float64',pa.float64(),False)])
DIMENSION = pa.struct([pa.field('key',pa.binary(),False),pa.field('value',pa.binary(),False)])
SCHEMA = pa.schema([
    pa.field('schema_version',pa.uint16(),False),pa.field('source_format',pa.string(),False),
    pa.field('time_token',pa.string(),False),pa.field('time',EXACT_TIME,False),
    pa.field('measurements',pa.list_(pa.field('element',MEASUREMENT)),False),
    pa.field('dimensions',pa.list_(pa.field('element',DIMENSION)),False),
    pa.field('host',pa.binary()),pa.field('source',pa.binary()),pa.field('sourcetype',pa.binary()),
    pa.field('index',pa.string()),pa.field('native_reader',pa.string()),
    pa.field('source_sha256',pa.string(),False),pa.field('record_ordinal',pa.uint64(),False),
    pa.field('line_offset',pa.uint64(),False),pa.field('source_line',pa.binary(),False),
],metadata={b'obs_migrate.metric_schema':b'1',b'source_fidelity':b'exact_native_export_lines_and_numeric_tokens',
            b'float64_policy':b'derived_projection;original_TSidx_bits_unverified',
            b'bucket_access':b'native_assisted;no_independent_metric_TSidx_decoder'})
FORMAT='splunk-native-metric-json'
MAX_LINE=16*1024*1024


class NumberToken(str):
    pass


def strict_object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('duplicate native JSON key')
        result[key]=value
    return result


def number(token):
    if len(token)>1024:raise ValueError('native numeric token exceeds limit')
    return NumberToken(token)


def bad_constant(token):
    raise ValueError('unsupported nonfinite native JSON number: '+token)


def binary(value):
    if type(value) is not str:raise ValueError('native metric names/dimensions must be strings')
    return value.encode('utf8','surrogateescape')


def timestamp(token):
    if not isinstance(token,NumberToken):raise ValueError('native time must be a JSON number')
    value=Decimal(token)
    if not value.is_finite() or value.adjusted()>19 or value.adjusted() < -1024:
        raise ValueError('native time outside supported exact range')
    with localcontext() as context:
        context.prec=max(64,len(token)+abs(value.adjusted())+32)
        seconds=int(value.to_integral_value(rounding=ROUND_FLOOR))
        if not -(1<<63)<=seconds<(1<<63):raise ValueError('native time outside int64 seconds')
        fraction=format(value-Decimal(seconds),'f') if value != seconds else '0'
    if '.' in fraction:fraction=fraction.rstrip('0').rstrip('.')
    return {'seconds':seconds,'fraction':fraction}


def native_points(path, *, index=None,native_reader=None):
    """Read an already acquired native export, preserving its exact boundary.

    This verifies file stability/completion, not completeness of its source bucket.
    Native acquisition provenance must establish the latter independently.
    """
    path=Path(path)
    with path.open('rb') as file:
        source_sha=hashlib.file_digest(file,'sha256').hexdigest()
        file.seek(0)
        digest=hashlib.sha256();ordinal=offset=0
        while line:=file.readline(MAX_LINE+1):
            if len(line)>MAX_LINE:raise ValueError('native metric line exceeds 16 MiB')
            if not line.endswith(b'\n'):raise ValueError('native metric export has an incomplete final line')
            digest.update(line)
            point=json.loads(line.decode('utf8','surrogateescape'),parse_float=number,parse_int=number,
                             parse_constant=bad_constant,object_pairs_hook=strict_object)
            if not isinstance(point,dict) or set(point)-{'host','source','sourcetype','time','fields'}:
                raise ValueError('unsupported native metric JSON object')
            if 'time' not in point:raise ValueError('native metric time missing')
            fields=point.get('fields')
            if not isinstance(fields,dict) or len(fields)>4096:raise ValueError('native metric field map missing/oversized')
            measurements=[];dimensions=[]
            for key,value in fields.items():
                if key.startswith('metric_name:'):
                    name=key[len('metric_name:'):]
                    if not name or not isinstance(value,NumberToken):raise ValueError('native measurement requires a name and numeric value')
                    projected=float(value)
                    if not math.isfinite(projected) or (projected==0 and Decimal(value)!=0):
                        raise ValueError('native measurement outside finite Float64 range')
                    measurements.append({'name':binary(name),'value_token':str(value),'value_float64':projected})
                else:
                    dimensions.append({'key':binary(key),'value':binary(value)})
            if not measurements:raise ValueError('no native measurements; unsupported source profile')
            yield {'schema_version':1,'source_format':FORMAT,'time_token':str(point['time']),
                   'time':timestamp(point['time']),'measurements':measurements,'dimensions':dimensions,
                   **{key:binary(point[key]) if key in point else None for key in ('host','source','sourcetype')},
                   'index':index,'native_reader':native_reader,'source_sha256':source_sha,
                   'record_ordinal':ordinal,'line_offset':offset,'source_line':line}
            ordinal+=1;offset+=len(line)
        if not ordinal:raise ValueError('empty native metric export is not proof of an empty bucket')
        if digest.hexdigest()!=source_sha:raise ValueError('native metric export changed during reading')


def json_bytes(row):
    if row.get('schema_version')!=1 or row.get('source_format')!=FORMAT or set(row)!=set(SCHEMA.names):
        raise ValueError('metric schema mismatch')
    result=dict(row)
    def encode(value):return base64.b64encode(value).decode('ascii') if value is not None else None
    for key in ('host','source','sourcetype','source_line'):result[key]=encode(row[key])
    result['measurements']=[dict(m,name=encode(m['name'])) for m in row['measurements']]
    result['dimensions']=[{'key':encode(d['key']),'value':encode(d['value'])} for d in row['dimensions']]
    return (json.dumps(result,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False)+'\n').encode()


def from_json(line):
    row=json.loads(line)
    if row.get('schema_version')!=1 or row.get('source_format')!=FORMAT or set(row)!=set(SCHEMA.names):
        raise ValueError('metric schema mismatch')
    def decode(value):return base64.b64decode(value,validate=True) if value is not None else None
    for key in ('host','source','sourcetype','source_line'):row[key]=decode(row[key])
    for measurement in row['measurements']:measurement['name']=decode(measurement['name'])
    for dimension in row['dimensions']:
        dimension['key']=decode(dimension['key']);dimension['value']=decode(dimension['value'])
    return row


def batches(points,max_rows=1024,max_bytes=8*1024*1024):
    if max_rows<1 or max_bytes<1:raise ValueError('metric batch limits must be positive')
    rows=[];size=0
    for row in points:
        amount=len(json_bytes(row))
        if rows and (len(rows)>=max_rows or size+amount>max_bytes):
            yield pa.RecordBatch.from_pylist(rows,schema=SCHEMA)
            rows=[];size=0
        rows.append(row);size+=amount
    if rows:yield pa.RecordBatch.from_pylist(rows,schema=SCHEMA)


def export(points,output,format='parquet',*,max_rows=1024,max_bytes=8*1024*1024):
    if format not in ('parquet','ndjson'):raise ValueError('metric format must be parquet or ndjson')
    count=measurements=0;digest=hashlib.sha256();start=time.monotonic()
    def counted():
        nonlocal count,measurements
        for point in points:
            count+=1;measurements+=len(point['measurements']);digest.update(json_bytes(point));yield point
    try:
        with atomic_output(output) as file:
            if format=='ndjson':
                for point in counted():file.write(json_bytes(point))
            else:
                with pq.ParquetWriter(file,SCHEMA,compression='zstd',write_statistics=False) as writer:
                    for batch in batches(counted(),max_rows,max_bytes):writer.write_batch(batch)
        return {'points':count,'measurements':measurements,'canonical_sha256':digest.hexdigest(),
                'output_bytes':Path(output).stat().st_size,'elapsed_seconds':time.monotonic()-start,
                'native_assisted':True,'original_tsidx_float_precision':'UNVERIFIED'}
    finally:
        close=getattr(points,'close',None)
        if close:close()


def read_export(path,format):
    if format=='ndjson':
        with open(path,'rb') as file:
            for line in file:yield from_json(line)
    elif format=='parquet':
        with pq.ParquetFile(path) as file:
            if not file.schema_arrow.equals(SCHEMA,check_metadata=True):raise ValueError('metric schema mismatch')
            for group in range(file.num_row_groups):
                for batch in file.iter_batches(batch_size=1024,row_groups=[group]):yield from batch.to_pylist()
    else:raise ValueError('unknown metric export format')


def verify(points,path,format):
    actual=read_export(path,format);sentinel=object();count=0
    try:
        for expected,got in zip_longest(points,actual,fillvalue=sentinel):
            if expected is sentinel or got is sentinel or json_bytes(expected)!=json_bytes(got):
                raise ValueError('metric export mismatch at point '+str(count))
            count+=1
        return {'verified_points':count,'comparison':'all canonical metric fields, ordered',
                'native_assisted':True,'original_tsidx_float_precision':'UNVERIFIED'}
    finally:
        for iterator in (points,actual):
            close=getattr(iterator,'close',None)
            if close:close()
