"""Lossless capture files; validation reuse only on pinned immutable bytes.

A file's mode/mtime or a persisted 'validated' marker is never trusted. Each
open pins a requested artifact SHA, checks every member and fully validates the
native profile. Queries on that object neither reopen nor trust mutable paths.
"""
import hashlib
import io
import json
import os
from pathlib import Path,PurePosixPath
import stat
import struct
import threading
from types import MappingProxyType
import zipfile

from . import bucket_select,metric_bucket,metric_select,msidx1,msidx2,msidx
from .export import atomic_output

FORMAT='obs-immutable-metric-capture'
MAX_BYTES=256*1024**2
MAX_MANIFEST=4*1024**2
MAX_MEMBERS=metric_bucket.MAX_FILES+2
MAX_CENTRAL=2*1024**2
MAX_POINTS=1_000_000
MAX_CACHED_MEMBERS=1_000_000
MODULES={m.FORMAT:m for m in (msidx1,msidx2,msidx)}


def _sha(data):return hashlib.sha256(data).hexdigest()
def _copy(value):return json.loads(metric_bucket._json(value))


def _read(path,limit,*,zero=False):
    path=Path(path);before=path.lstat()
    if not stat.S_ISREG(before.st_mode):raise ValueError('capture input must be a regular non-symlink file')
    if not (0 if zero else 1)<=before.st_size<=limit:raise ValueError('capture input byte limit exceeded')
    with path.open('rb') as file:
        opened=os.fstat(file.fileno())
        if (opened.st_dev,opened.st_ino)!=(before.st_dev,before.st_ino):raise ValueError('capture input replaced while opening')
        data=file.read(limit+1);after=os.fstat(file.fileno())
    final=path.lstat()
    stamp=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
    if len(data)>limit or stamp(before)!=stamp(after) or stamp(after)!=stamp(final):
        raise ValueError('capture input changed during read')
    return data


def _directory(data):
    # Writer emits no ZIP64, comments or arbitrary extra records. Bound stdlib
    # central-directory allocation before opening an untrusted archive.
    if len(data)<22:raise ValueError('invalid capture directory')
    sig,disk,cdisk,n,total,size,offset,comment=struct.unpack('<4s4H2LH',data[-22:])
    if (sig!=b'PK\x05\x06' or disk or cdisk or n!=total or not 2<=n<=MAX_MEMBERS or
        size>MAX_CENTRAL or offset+size!=len(data)-22 or comment):raise ValueError('unsupported capture directory')
    if len(data)>=42 and data[-42:-38]==b'PK\x06\x07':raise ValueError('ZIP64 capture is unsupported')


def create(path,output,*,experimental=False,offline_snapshot=False,include_inactive=False):
    """Atomic no-overwrite capture, with original bytes of every inventoried file."""
    if not experimental:raise ValueError('capture is experimental')
    output=Path(output)
    if output.resolve().is_relative_to(Path(path).resolve()):raise ValueError('capture output must be outside source bucket')
    if output.exists() or output.is_symlink():raise FileExistsError('output already exists; overwrite disabled')
    snapshot=metric_bucket.Snapshot(path,offline_snapshot=offline_snapshot,include_inactive=include_inactive)
    if snapshot.reader not in (msidx1,msidx2,msidx):raise ValueError('capture requires verified MSIDX1/MSIDX2/MSIDX4 profiles')
    if sum(f['bytes'] for f in snapshot.manifest['files'].values())>MAX_BYTES-MAX_MANIFEST-MAX_CENTRAL:
        raise ValueError('capture source byte limit exceeded')
    manifest={'capture_schema':1,'format':FORMAT,'source_format':snapshot.reader.FORMAT,
              'snapshot_sha256':snapshot.sha256,'snapshot_manifest':snapshot.manifest}
    encoded=metric_bucket._json(manifest)
    if len(encoded)>MAX_MANIFEST:raise ValueError('capture manifest byte limit exceeded')
    with atomic_output(output) as stream:
        total_points=0
        with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED,allowZip64=False) as archive:
            archive.writestr('manifest.json',encoded)
            for name in sorted(snapshot.manifest['files']):
                # Empty companions are valid; read them through a bounded fd too.
                file=snapshot.root/name
                data=_read(file,MAX_BYTES,zero=True)
                if {'bytes':len(data),'sha256':_sha(data)}!=snapshot.manifest['files'][name]:
                    raise ValueError('source changed during capture')
                if name in snapshot.slices:
                    total_points+=metric_select._Source(data,_sha(data),snapshot.reader).total
                    if total_points>MAX_POINTS:raise ValueError('capture point limit exceeded')
                archive.writestr('source/'+name,data)
            if snapshot.external:
                parent=_read(snapshot.external[0],metric_bucket.MAX_MANIFEST_BYTES)
                if _sha(parent)!=snapshot.manifest['parent_manifest_sha256']:raise ValueError('parent manifest changed during capture')
                archive.writestr('parent.csv',parent)
        snapshot.finish()
        if stream.tell()>MAX_BYTES:raise ValueError('capture artifact byte limit exceeded')
    wire=_read(output,MAX_BYTES)
    return {'format':FORMAT,'capture_sha256':_sha(wire),'capture_bytes':len(wire),
            'snapshot_sha256':snapshot.sha256,'source_files':len(snapshot.files)}


class CapturedBucket:
    """A validated object holding independent immutable original file bytes.

    Opening a new object always validates again. Replacing the artifact or origin
    cannot affect an existing object. Expected SHA must come from the capture
    receipt/trusted caller, not from an adjacent untrusted validation marker.
    """
    def __init__(self,path,*,expected_sha256,experimental=False):
        if not experimental:raise ValueError('captured metrics are experimental')
        if not isinstance(expected_sha256,str) or len(expected_sha256)!=64 or any(c not in '0123456789abcdef' for c in expected_sha256):
            raise ValueError('a lowercase expected capture SHA-256 is required')
        data=_read(path,MAX_BYTES)
        if _sha(data)!=expected_sha256:raise ValueError('capture SHA-256 mismatch; validation cannot be reused')
        self.sha256=expected_sha256;self.size=len(data);self.closed=False;self._lock=threading.Lock()
        _directory(data)
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                infos=archive.infolist();names=[i.filename for i in infos]
                if len(set(names))!=len(names) or archive.comment:raise ValueError('duplicate capture members')
                for info in infos:
                    mode=stat.S_IFMT(info.external_attr>>16)
                    if (info.compress_type!=zipfile.ZIP_STORED or info.file_size!=info.compress_size or
                        info.flag_bits&1 or mode not in (0,stat.S_IFREG) or info.file_size>MAX_BYTES):
                        raise ValueError('unsupported capture member profile')
                if archive.getinfo('manifest.json').file_size>MAX_MANIFEST:raise ValueError('capture manifest limit exceeded')
                manifest=json.loads(archive.read('manifest.json'),object_pairs_hook=bucket_select._object)
                if set(manifest)!={'capture_schema','format','source_format','snapshot_sha256','snapshot_manifest'} or type(manifest['capture_schema']) is not int or manifest['capture_schema']!=1 or manifest['format']!=FORMAT:
                    raise ValueError('unsupported capture manifest')
                reader=MODULES.get(manifest['source_format'])
                if reader is None:raise ValueError('unsupported capture source format')
                snapshot=manifest['snapshot_manifest']
                if not isinstance(snapshot,dict):raise ValueError('invalid capture snapshot object')
                if _sha(metric_bucket._json(snapshot))!=manifest['snapshot_sha256']:raise ValueError('capture snapshot digest mismatch')
                files=snapshot['files'];slices=snapshot['selected_slices']
                if not isinstance(files,dict) or not isinstance(slices,list) or not isinstance(snapshot['directories'],list):raise ValueError('invalid capture inventory types')
                snapshot_keys={'snapshot_schema','files','directories','selected_slices','disabled_name',
                    'parent_manifest_listed','parent_manifest_sha256','selection','origin_consistency',
                    'search_visibility','indexed_search_supported'}
                if reader is not msidx:snapshot_keys.add('slice_profiles')
                if set(snapshot)!=snapshot_keys or type(snapshot['snapshot_schema']) is not int or snapshot['snapshot_schema']!=1:
                    raise ValueError('unsupported capture snapshot profile')
                if (snapshot['indexed_search_supported'] is not False or snapshot['search_visibility']!='UNVERIFIED' or
                    snapshot['origin_consistency']!='caller_asserted_offline_snapshot;not_independently_proven'):
                    raise ValueError('unsupported capture visibility claims')
                if len(snapshot['directories'])>metric_bucket.MAX_FILES:raise ValueError('capture directory limit exceeded')
                if not 1<=len(files)<=metric_bucket.MAX_FILES or not 1<=len(slices)<=metric_bucket.MAX_SLICES:
                    raise ValueError('capture inventory limit exceeded')
                for name in files:
                    parts=PurePosixPath(name).parts
                    if not parts or name!=PurePosixPath(name).as_posix() or name.startswith('/') or any(p in ('.','..') for p in parts) or '\\' in name:
                        raise ValueError('unsafe capture source name')
                directories=snapshot['directories']
                if sorted(set(directories))!=directories:raise ValueError('invalid capture directory inventory')
                for name in list(files)+directories:
                    lower=name.lower()
                    if (name.startswith('/') or '..' in PurePosixPath(name).parts or
                        name!=PurePosixPath(name).as_posix() or '\\' in name or
                        len(name.encode('utf8','surrogateescape'))>512 or len(PurePosixPath(name).parts)>17 or
                        lower=='rawdata/deletes' or lower.startswith('rawdata/deletes/') or
                        lower.endswith(('.lock','.tmp','.pending','.merge')) or '.temp-' in lower or
                        'optimiz' in lower and name!='optimize.result'):
                        raise ValueError('unsupported capture source layout')
                for name in files:
                    if any(str(parent) not in directories for parent in PurePosixPath(name).parents if str(parent)!='.'):
                        raise ValueError('missing capture source directory')
                expected={'manifest.json'}|{'source/'+name for name in files}
                if snapshot['parent_manifest_sha256'] is not None:expected.add('parent.csv')
                if set(names)!=expected:raise ValueError('capture inventory/member mismatch')
                pinned={}
                for name,wanted in files.items():
                    wire=archive.read('source/'+name)
                    if {'bytes':len(wire),'sha256':_sha(wire)}!=wanted:raise ValueError('capture source member digest mismatch')
                    pinned[name]=wire
                if 'parent.csv' in expected and _sha(archive.read('parent.csv'))!=snapshot['parent_manifest_sha256']:
                    raise ValueError('capture parent digest mismatch')
            if sorted(slices)!=slices or len(set(slices))!=len(slices) or set(slices)!={name for name in files if '.tsidx' in name.lower()}:
                raise ValueError('invalid capture slice inventory')
            if len({files[name]['sha256'] for name in slices})!=len(slices):raise ValueError('duplicate capture slice contents')
            self.reader=reader;self._manifest=metric_bucket._json(manifest);self._files=MappingProxyType(pinned)
            self._sources={};total_points=0
            for name in slices:
                match=metric_bucket.SLICE.fullmatch(name)
                if match is None:raise ValueError('invalid capture slice name')
                source=metric_select._Source(pinned[name],files[name]['sha256'],reader)
                source._posting_cache_limit=MAX_CACHED_MEMBERS//len(slices)
                total_points+=source.total
                if total_points>MAX_POINTS:raise ValueError('capture point limit exceeded')
                if reader is msidx:
                    # Preserve the original MSIDX4 snapshot/header-bound contract;
                    # decoded extrema may be narrower than declared slice bounds.
                    bounds=(source.reader.latest,source.reader.earliest)
                    if bounds!=(int(match[1]),int(match[2])):
                        raise ValueError('capture header bounds/profile mismatch')
                else:
                    profile=snapshot['slice_profiles'][name]
                    if (source.maximum_seconds,source.minimum_seconds)!=(int(match[1]),int(match[2])) or profile!={
                        'source_format':reader.FORMAT,'physical_points':source.total,'minimum_seconds':source.minimum_seconds,
                        'maximum_seconds':source.maximum_seconds,'bounds_source':'complete_physical_scan'}:
                        raise ValueError('capture decoded bounds/profile mismatch')
                self._sources[name]=source
        except (zipfile.BadZipFile,KeyError,TypeError,UnicodeError,RecursionError,struct.error) as error:
            raise ValueError('invalid capture') from error

    @property
    def manifest(self):return json.loads(self._manifest)

    def check(self):
        if self.closed:raise ValueError('captured bucket is closed')

    def close(self):
        if not self._lock.acquire(blocking=False):raise ValueError('captured bucket scan is active')
        try:
            self.closed=True;self._sources.clear();self._files=MappingProxyType({})
        finally:self._lock.release()

    def __enter__(self):self.check();return self
    def __exit__(self,*args):self.close()

    def select(self,predicate=None,*,index=None,bucket_id=None):
        return CapturedSelection(self,metric_select.Predicate() if predicate is None else predicate,index,bucket_id)


class CapturedSelection:
    def __init__(self,capture,predicate,index,bucket_id):
        capture.check()
        if not isinstance(predicate,metric_select.Predicate):raise ValueError('selection requires an exact Predicate')
        self.capture=capture;self.reader=capture.reader;self.index=index;self.bucket_id=bucket_id
        m=capture.manifest;self.query={'query_schema':1,'snapshot_sha256':m['snapshot_sha256'],'source_format':self.reader.FORMAT,
            'predicate':predicate.describe(),'labels':{'index':index,'bucket_id':bucket_id},
            'semantics':'physical_exact_AND;full_preflight;search_visibility_unverified'}
        self.sha256=_sha(metric_bucket._json(self.query));self._plans={};slices=[]
        if not capture._lock.acquire(blocking=False):raise ValueError('captured bucket scan is active')
        try:
            capture.check()
            for name,source in capture._sources.items():
                plan=source.plan(predicate);r=source._report(plan);self._plans[name]=plan
                slices.append({'slice_file':name,'source_sha256':source.sha256,'fully_validated_points':source.total,
                    'candidate_points':len(plan.point_ids),'candidate_dimension_sets':r['candidate_dimension_sets'],
                    'candidate_strategy':r['candidate_strategy']})
        finally:capture._lock.release()
        self._report=metric_bucket._json({'bundle_schema':1,'query':self.query,'query_sha256':self.sha256,
            'snapshot_manifest':m['snapshot_manifest'],'slices':slices,
            'fully_validated_points':sum(s['fully_validated_points'] for s in slices),
            'candidate_points':sum(s['candidate_points'] for s in slices),'full_preflight_scan':True,
            'performance_claim':False,'physical_predicate_selection':True,'search_visibility':'UNVERIFIED','indexed_search_supported':False})

    def report(self):self.capture.check();return json.loads(self._report)

    def records(self):
        c=self.capture
        if not c._lock.acquire(blocking=False):raise ValueError('captured bucket scan is active')
        try:
            c.check();first=True;m=c.manifest;snapshot=m['snapshot_manifest']
            for name,plan in self._plans.items():
                for row in c._sources[name]._emit(plan,self.index,self.bucket_id):
                    state=json.loads(row['storage_json']);state.update(bucket_snapshot_sha256=m['snapshot_sha256'],slice_file=name,bucket_query_sha256=self.sha256)
                    if first:state['bucket_snapshot_manifest']=snapshot;first=False
                    row['storage_json']=metric_bucket._json(state).decode();yield row
        finally:c._lock.release()

    def batches(self):yield from self.reader.batches(self.records())
