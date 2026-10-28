"""Bounded read-only S3 transport into the existing acquired-object contract."""
from datetime import datetime,timezone
import hashlib
import json
import os
from contextlib import nullcontext
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlsplit

from .smartstore import MAX_BYTES,MAX_OBJECTS,_relative,_json
from .capture import _read
from .smartstore import MAX_INVENTORY,AcquiredObjects
from . import dataset
from .export import atomic_output
from .index_job import canonical,identity
from .job_dataset import _root,_regular,_fingerprint

MAX_PAGES=16
MAX_SECONDS=900
_FACTORY=object()


class AcquisitionError(ValueError):pass
class AcquisitionCancelled(KeyboardInterrupt):pass
# SDK retry loops catch ordinary Exception. Leave that machinery first, then
# translate this private control signal to AcquisitionError at the API boundary.
class _SdkPolicyViolation(BaseException):pass


def _sha(value):return hashlib.sha256(value).hexdigest()
def _utc():return datetime.now(timezone.utc).isoformat()


def make_client(*,profile=None,region=None,endpoint_url=None,allow_http=False,ca_bundle=None):
    """Dedicated SDK client; credentials are resolved only through the SDK chain."""
    if type(allow_http) is not bool:raise AcquisitionError('allow_http must be boolean')
    if endpoint_url is not None:
        if type(endpoint_url) is not str or len(endpoint_url)>2048:raise AcquisitionError('invalid endpoint')
        try:
            url=urlsplit(endpoint_url);port=url.port
            if (url.scheme not in ('http','https') or not url.hostname or url.username or url.password
                    or url.query or url.fragment or url.path not in ('','/') or url.scheme=='http' and not allow_http):
                raise ValueError()
        except ValueError:raise AcquisitionError('endpoint must be credential-free HTTPS; HTTP requires allow_http') from None
    if ca_bundle is not None:
        from pathlib import Path
        if type(ca_bundle) is not str or not Path(ca_bundle).is_file():raise AcquisitionError('CA bundle must be a regular local file')
    try:
        import boto3
        from botocore.config import Config
        config=Config(retries={'mode':'standard','total_max_attempts':3},connect_timeout=10,read_timeout=30,
                      s3={'addressing_style':'path' if endpoint_url else 'auto'})
        client=boto3.Session(profile_name=profile,region_name=region).client('s3',endpoint_url=endpoint_url,
                                                                           verify=ca_bundle or True,config=config)
    except ImportError:raise ImportError('S3 acquisition requires the optional obs-migrate[s3] extra') from None
    except Exception:raise AcquisitionError('S3 SDK client configuration failed') from None
    resolved=urlsplit(client.meta.endpoint_url)
    if (resolved.scheme not in ('http','https') or resolved.username or resolved.password or resolved.query or resolved.fragment
            or resolved.scheme=='http' and not allow_http):
        client.close();raise AcquisitionError('resolved SDK endpoint violates transport policy')
    client._obs_acquisition_policy=(_FACTORY,client.meta.endpoint_url,ca_bundle or True,allow_http)
    client._obs_acquisition_api=None;client._obs_acquisition_sends=0
    def operation_guard(model,**kwargs):
        active=client._obs_acquisition_api
        if active is not None and model.name!=active:
            raise _SdkPolicyViolation('implicit SDK operation is outside the acquisition contract')
    def redirect_guard(response,**kwargs):
        if client._obs_acquisition_api is None or response is None:return
        http,parsed=response;code=parsed.get('Error',{}).get('Code')
        if (http is not None and http.status_code in (301,302,307,308)
                or code in ('PermanentRedirect','AuthorizationHeaderMalformed','IllegalLocationConstraintException')):
            raise _SdkPolicyViolation('S3 region redirect refused; configure the correct source region')
    def attempt_guard(**kwargs):
        if client._obs_acquisition_api is None:return
        if client._obs_acquisition_sends>=3:raise _SdkPolicyViolation('S3 HTTP send attempt budget exhausted')
        client._obs_acquisition_sends+=1
    # Public SDK hooks precede its region probe/redirect handler. Config retry
    # bounds alone do not cover that handler's nested HeadBucket or extra retry.
    client.meta.events.register_first('before-parameter-build.s3',operation_guard)
    client.meta.events.register_first('needs-retry.s3',redirect_guard)
    client.meta.events.register_first('before-send.s3',attempt_guard)
    return client


def _client(client):
    policy=getattr(client,'_obs_acquisition_policy',())
    if len(policy)!=4 or policy[0] is not _FACTORY:raise AcquisitionError('dedicated make_client S3 client required')
    config=client.meta.config
    if (client.meta.service_model.service_name!='s3' or client.meta.endpoint_url!=policy[1]
            or client._endpoint.http_session._verify!=policy[2]
            or urlsplit(policy[1]).scheme=='http' and policy[3] is not True
            or config.retries.get('mode')!='standard' or type(config.retries.get('total_max_attempts')) is not int
            or not 1<=config.retries['total_max_attempts']<=3
            or not 0<config.connect_timeout<=10 or not 0<config.read_timeout<=30):
        raise AcquisitionError('S3 client configuration is outside supported bounds')


class _Run:
    def __init__(self,cancelled=None,stats=None):
        if cancelled is not None and not callable(cancelled):raise AcquisitionError('cancelled must be callable')
        self.cancelled=cancelled;self.started=time.monotonic();self.started_utc=_utc()
        self.stats=stats if stats is not None else {'api_calls':{},'sdk_reported_retries':0,'application_bytes_received':0,'stream_restarts':0}

    def check(self):
        if self.cancelled is not None and self.cancelled():raise AcquisitionCancelled()
        if time.monotonic()-self.started>MAX_SECONDS:raise AcquisitionError('acquisition elapsed budget exceeded')

    def call(self,client,operation,**params):
        self.check()
        if operation not in ('list_objects_v2','list_object_versions','get_bucket_versioning','get_object'):
            raise AcquisitionError('remote write operation is forbidden')
        calls=self.stats['api_calls'];calls[operation]=calls.get(operation,0)+1
        if client._obs_acquisition_api is not None:raise AcquisitionError('dedicated S3 client already has an active call')
        client._obs_acquisition_api=client.meta.method_to_api_mapping[operation];client._obs_acquisition_sends=0
        try:
            try:response=getattr(client,operation)(**params)
            finally:client._obs_acquisition_api=None
        except _SdkPolicyViolation as error:raise AcquisitionError(str(error)) from None
        except AcquisitionError:raise
        except Exception as error:
            response=getattr(error,'response',{})
            self._retries(response)
            status=response.get('ResponseMetadata',{}).get('HTTPStatusCode')
            detail=' HTTP '+str(status) if type(status) is int and 400<=status<=599 else ''
            key=' key-sha256='+_sha(params['Key'].encode())[:12] if 'Key' in params else ''
            raise AcquisitionError('S3 '+operation+' failed'+detail+key) from None
        try:self._retries(response)
        except BaseException:
            if hasattr(response.get('Body'),'close'):response['Body'].close()
            raise
        return response

    def _retries(self,response):
        value=response.get('ResponseMetadata',{}).get('RetryAttempts',0)
        if type(value) is not int or not 0<=value<=2:raise AcquisitionError('unsupported SDK retry accounting')
        self.stats['sdk_reported_retries']+=value


def _modified(value):
    if not isinstance(value,datetime) or value.tzinfo is None:raise AcquisitionError('listing requires timezone-aware last-modified')
    return value.astimezone(timezone.utc).isoformat()


def _listing(client,bucket,prefix,mode,run):
    if mode not in ('conditional','versioned'):raise AcquisitionError('unsupported version mode')
    if mode=='versioned' and run.call(client,'get_bucket_versioning',Bucket=bucket).get('Status')!='Enabled':
        raise AcquisitionError('versioned acquisition requires enabled bucket versioning')
    result={};cursor={};seen=set();total=examined=0
    operation='list_objects_v2' if mode=='conditional' else 'list_object_versions'
    for page in range(MAX_PAGES):
        response=run.call(client,operation,Bucket=bucket,Prefix=prefix,MaxKeys=1000,**cursor)
        records=[(row,False) for row in response.get('Contents' if mode=='conditional' else 'Versions',[])]
        if mode=='versioned':records.extend((row,True) for row in response.get('DeleteMarkers',[]))
        if len(records)>1000:raise AcquisitionError('listing page exceeds 1000 entries')
        examined+=len(records)
        if examined>MAX_PAGES*1000:raise AcquisitionError('listing history budget exceeded')
        for row,deleted in records:
            try:key=_relative(row['Key'])
            except (KeyError,ValueError):raise AcquisitionError('listing contains unsupported object key') from None
            if not key.startswith(prefix):raise AcquisitionError('listing key outside requested prefix')
            if mode=='versioned':
                if type(row.get('IsLatest')) is not bool:raise AcquisitionError('missing latest version evidence')
                if not row['IsLatest']:continue
            if key in result:raise AcquisitionError('duplicate latest object key')
            version=row.get('VersionId') if mode=='versioned' else None
            if mode=='versioned' and (type(version) is not str or not version or version=='null' or len(version)>1024):
                raise AcquisitionError('non-null bounded version identity required')
            size=0 if deleted else row.get('Size');etag=None if deleted else row.get('ETag')
            if type(size) is not int or size<0 or not deleted and (type(etag) is not str or not etag or len(etag)>256):
                raise AcquisitionError('listing requires bounded size and opaque ETag')
            total+=size
            result[key]={'key':key,'size':size,'etag':etag,'last_modified':_modified(row.get('LastModified')),
                         'version_id':version,'delete_marker':deleted}
            if len(result)>MAX_OBJECTS or total>MAX_BYTES:raise AcquisitionError('acquired object count/byte budget exceeded')
        truncated=response.get('IsTruncated')
        if type(truncated) is not bool:raise AcquisitionError('listing missing truncation evidence')
        if not truncated:
            if not result or all(r['delete_marker'] for r in result.values()):raise AcquisitionError('no current objects in acquisition prefix')
            return [result[k] for k in sorted(result)]
        if mode=='conditional':cursor={'ContinuationToken':response.get('NextContinuationToken')}
        else:cursor={'KeyMarker':response.get('NextKeyMarker'),'VersionIdMarker':response.get('NextVersionIdMarker')}
        if any(type(v) is not str or not v or len(v)>4096 for v in cursor.values()) or tuple(cursor.values()) in seen:
            raise AcquisitionError('missing/repeated pagination cursor')
        seen.add(tuple(cursor.values()))
    raise AcquisitionError('listing page budget exceeded')


def _file(row):return 'objects/'+_sha(row['key'].encode())+'.bin'


def _save(output,state):
    if len(canonical(state))+1>4*1024**2:raise AcquisitionError('acquisition checkpoint exceeds 4 MiB')
    # Reuse its fsync/atomic replacement, with this format's stricter size bound.
    content=canonical(state)+b'\n';target=output/'acquisition.json'
    fd,name=tempfile.mkstemp(prefix='.checkpoint-',dir=output)
    try:
        with os.fdopen(fd,'wb') as stream:stream.write(content);stream.flush();os.fsync(stream.fileno())
        os.replace(name,target);dataset._sync(output)
    finally:
        if os.path.exists(name):os.unlink(name)


def _part(output,entry):
    try:
        path=output/entry['file'];value=_regular(path)
        if value.st_size!=entry['size'] or value.st_size>MAX_BYTES:raise AcquisitionError('acquired part size mismatch')
        if _fingerprint(path)!={'bytes':entry['size'],'sha256':entry['sha256']}:raise AcquisitionError('acquired part hash mismatch')
    except (OSError,ValueError):raise AcquisitionError('acquired part size/hash validation failed') from None


def _download_once(client,bucket,row,output,run,compare):
    """One complete response, closed on every error; no native bytes transformed."""
    params={'Bucket':bucket,'Key':row['key'],'IfMatch':row['etag']}
    if row['version_id'] is not None:params['VersionId']=row['version_id']
    response=run.call(client,'get_object',**params);body=response.get('Body');sha=hashlib.sha256();count=0
    try:
        if (body is None or not hasattr(body,'read') or not hasattr(body,'close')
                or response.get('ContentLength')!=row['size'] or type(response.get('ContentLength')) is not int
                or response.get('ETag')!=row['etag']
                or row['version_id'] is not None and response.get('VersionId')!=row['version_id']):
            raise AcquisitionError('GET response differs from bound object identity')
        with (nullcontext(None) if compare else atomic_output(output/_file(row))) as stream:
            while True:
                run.check();chunk=body.read(min(1024**2,row['size']-count+1))
                if not chunk:break
                count+=len(chunk);run.stats['application_bytes_received']+=len(chunk)
                if count>row['size']:raise AcquisitionError('GET body exceeds listed size')
                sha.update(chunk)
                if stream is not None:stream.write(chunk)
            if count!=row['size']:raise AcquisitionError('GET body length mismatch')
    finally:
        if body is not None and hasattr(body,'close'):body.close()
    return {**row,'file':_file(row),'sha256':sha.hexdigest()}


def _download(client,bucket,row,output,run,compare=False):
    from botocore.exceptions import BotoCoreError,IncompleteReadError,ReadTimeoutError,ResponseStreamingError,ConnectionClosedError
    for attempt in range(3):
        try:
            entry=_download_once(client,bucket,row,output,run,compare)
            if compare:_part(output,entry)
            return entry
        except (IncompleteReadError,ReadTimeoutError,ResponseStreamingError,ConnectionClosedError,TimeoutError,ConnectionError):
            if attempt==2:raise AcquisitionError('GET streaming retry budget exhausted key-sha256='+_sha(row['key'].encode())[:12]) from None
            run.stats['stream_restarts']+=1
        except BotoCoreError:
            raise AcquisitionError('GET body verification failed key-sha256='+_sha(row['key'].encode())[:12]) from None


def _tree(output,listing):
    try:
        _regular(output,directory=True);_regular(output/'.lock');_regular(output/'objects',directory=True)
        names={p.name for p in output.iterdir()}
        if names-{'objects','.lock','acquisition.json','objects.json'}:raise ValueError()
        expected={_file(row).split('/')[1]:row for row in listing if not row['delete_marker']}
        for path in (output/'objects').iterdir():
            if path.name not in expected or _regular(path).st_size!=expected[path.name]['size']:raise ValueError()
    except (OSError,ValueError):raise AcquisitionError('acquisition directory contains missing, foreign or conflicting entries') from None


def _load(output,source):
    try:
        state=_json(_read(output/'acquisition.json',4*1024**2))
        if (type(state) is not dict or set(state)!={'format','phase','source','source_sha256','entries','started_utc','last_observed_utc','counters','inventory_sha256'}
                or state['format']!='obs-s3-acquisition-1' or state['phase'] not in ('transferring','validated')
                or state['source']!=source or state['source_sha256']!=identity(source)):
            raise ValueError()
        rows=[r for r in source['listing'] if not r['delete_marker']];entries=state['entries']
        if type(entries) is not list or len(entries)>len(rows):raise ValueError()
        for entry,row in zip(entries,rows):
            if (type(entry) is not dict or set(entry)!=set(row)|{'file','sha256'} or entry['file']!=_file(row)
                    or any(entry[k]!=row[k] for k in row) or type(entry['sha256']) is not str or not re.fullmatch('[0-9a-f]{64}',entry['sha256'])):
                raise ValueError()
        if state['phase']=='validated':
            if len(entries)!=len(rows) or state['inventory_sha256']!=_sha(canonical(entries)+b'\n'):raise ValueError()
        elif state['inventory_sha256'] is not None:raise ValueError()
        stats=state['counters']
        if type(stats) is not dict or set(stats)!={'api_calls','sdk_reported_retries','application_bytes_received','stream_restarts'}:raise ValueError()
        if type(stats['api_calls']) is not dict or stats['api_calls'].keys()-{'list_objects_v2','list_object_versions','get_bucket_versioning','get_object'}:raise ValueError()
        for value in [*stats['api_calls'].values(),*(v for k,v in stats.items() if k!='api_calls')]:
            if type(value) is not int or not 0<=value<1<<63:raise ValueError()
        for key in ('started_utc','last_observed_utc'):_modified(datetime.fromisoformat(state[key]))
        _tree(output,source['listing'])
        for entry in entries:_part(output,entry)
        return state
    except (OSError,ValueError,KeyError,TypeError):raise AcquisitionError('checkpoint, original source or acquired parts differ; resume refused') from None


def _merge(run,state):
    for key,value in state['counters'].items():
        if key=='api_calls':
            for operation,count in value.items():run.stats[key][operation]=run.stats[key].get(operation,0)+count
        else:run.stats[key]+=value
    state['counters']=run.stats


def _validate(output,entries,raw):
    """Existing reader validates a temporary hard-linked view before commit."""
    with tempfile.TemporaryDirectory(prefix='obs-s3-validation-',dir=output.parent) as directory:
        root=Path(directory);(root/'objects').mkdir()
        for entry in entries:
            _part(output,entry);os.link(output/entry['file'],root/entry['file'])
        (root/'objects.json').write_bytes(raw)
        source=AcquiredObjects(root,offline_snapshot=True,expected_inventory_sha256=_sha(raw))
        report=source.report()
        del source
    return report


def _source(client,bucket,prefix,mode,listing):
    return {'bucket':bucket,'prefix':prefix,'version_mode':mode,'region':client.meta.region_name,
            'endpoint_sha256':_sha(client.meta.endpoint_url.encode()),'listing':listing,
            'limits':{'objects':MAX_OBJECTS,'body_bytes':MAX_BYTES,'pages':MAX_PAGES,'page_entries':1000,
                      'checkpoint_bytes':4*1024**2,'inventory_bytes':MAX_INVENTORY,'seconds':MAX_SECONDS,
                      'api_attempts':3,'stream_attempts':3,'connect_timeout':10,'read_timeout':30}}


def _result(state,report,run):
    return {'format':'obs-s3-acquisition-result-1','complete':True,'source_sha256':state['source_sha256'],
            'inventory_sha256':state['inventory_sha256'],'version_mode':state['source']['version_mode'],
            'objects':report['objects'],'bytes':report['bytes'],'bucket_ids':[b['id'] for b in report['buckets']],
            'observed_window':{'started_utc':state['started_utc'],'finished_utc':_utc()},
            'counters':json.loads(json.dumps(run.stats)),
            'accounting_scope':'logical S3 API calls, reported SDK retries and received application bytes; not exact wire requests/bytes; hard kills can lose counters since checkpoint',
            'consistency':'caller_asserted_quiescent_prefix;non_atomic_observed_list_get_list_window;no_live_snapshot_guarantee'}


def acquire(client,bucket,prefix,output,*,offline_snapshot=False,resume=False,version_mode='conditional',cancelled=None):
    """Acquire a quiescent explicit prefix; objects.json is the last commit."""
    if offline_snapshot is not True:raise AcquisitionError('acquisition requires offline_snapshot assertion')
    _client(client)
    if type(bucket) is not str or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',bucket) or '..' in bucket or '.-' in bucket or '-.' in bucket:
        raise AcquisitionError('explicit DNS-style S3 bucket required')
    try:
        if type(prefix) is not str or not prefix.endswith('/'):raise ValueError()
        _relative(prefix[:-1])
    except ValueError:raise AcquisitionError('explicit safe nonempty prefix ending in slash required') from None
    if type(resume) is not bool or version_mode not in ('conditional','versioned'):raise AcquisitionError('unsupported acquisition options')
    output=_root(output)
    if resume:
        _regular(output,directory=True);_regular(output/'.lock');_regular(output/'objects',directory=True)
    else:
        try:output.mkdir(mode=0o700)
        except FileExistsError:raise AcquisitionError('new acquisition requires absent output directory') from None
    run=_Run(cancelled);state=None
    with dataset._lock(output):
        if not resume:(output/'objects').mkdir()
        listing=_listing(client,bucket,prefix,version_mode,run);source=_source(client,bucket,prefix,version_mode,listing)
        _tree(output,listing)
        if resume and (output/'acquisition.json').exists():
            state=_load(output,source);_merge(run,state)
        else:
            if resume and ({p.name for p in output.iterdir()}!={'.lock','objects'} or any((output/'objects').iterdir())):
                raise AcquisitionError('unbound acquisition directory cannot resume')
            state={'format':'obs-s3-acquisition-1','phase':'transferring','source':source,'source_sha256':identity(source),
                   'entries':[],'started_utc':run.started_utc,'last_observed_utc':run.started_utc,
                   'counters':run.stats,'inventory_sha256':None}
            prospective=[{**row,'file':_file(row),'sha256':'0'*64} for row in listing if not row['delete_marker']]
            if len(canonical(prospective))+1>MAX_INVENTORY:raise AcquisitionError('acquired inventory exceeds 2 MiB')
            _save(output,state)
        if (output/'objects.json').exists():
            raw=canonical(state['entries'])+b'\n'
            if state['phase']!='validated' or _read(output/'objects.json',MAX_INVENTORY)!=raw:
                raise AcquisitionError('final acquisition inventory differs from validated checkpoint')
            # A committed inventory is already a reader root. Validate directly
            # so complete resume never changes part ctimes/link counts.
            acquired=AcquiredObjects(output,offline_snapshot=True,expected_inventory_sha256=_sha(raw))
            report=acquired.report();del acquired
            if _listing(client,bucket,prefix,version_mode,run)!=listing:raise AcquisitionError('remote state changed during complete verification')
            for entry in state['entries']:_part(output,entry)
            run.check()
            return _result(state,report,run)
        try:
            rows=[row for row in listing if not row['delete_marker']]
            for row in rows[len(state['entries']):]:
                target=output/_file(row)
                entry=_download(client,bucket,row,output,run,compare=target.exists())
                dataset._sync(output/'objects')
                state['entries'].append(entry);state['phase']='transferring';state['inventory_sha256']=None
                _save(output,state)
            raw=canonical(state['entries'])+b'\n'
            report=_validate(output,state['entries'],raw)
            if _listing(client,bucket,prefix,version_mode,run)!=listing:raise AcquisitionError('complete remote object state changed during acquisition')
            for entry in state['entries']:_part(output,entry)
            run.check();dataset._sync(output/'objects')
            state['phase']='validated';state['inventory_sha256']=_sha(raw);state['last_observed_utc']=_utc();_save(output,state)
            result=_result(state,report,run)
            with atomic_output(output/'objects.json') as stream:stream.write(raw)
            dataset._sync(output)
            return result
        except BaseException:
            if not (output/'objects.json').exists():
                try:_save(output,state)
                except OSError:pass
            raise
