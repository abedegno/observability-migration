"""Physical scans of explicit offline local metric bucket snapshots.

Selection follows the observed immediate native slice filename convention, with
stricter refusal of ambiguous/partial layouts. It does not establish index-wide
activity, a consistent origin snapshot, or metric query/deletion equivalence.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from . import msidx,metric_storage,metric_limits
from .buckets import _manifest

MAX_FILES=2048
MAX_SLICES=256
MAX_TOTAL_BYTES=16*1024**3
MAX_MANIFEST_BYTES=2*1024**2
SLICE=re.compile(r'([0-9]+)-([0-9]+)-([0-9]+)\.tsidx\Z')


def _json(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()


def _tree(root):
    """No symlink traversal, bounded names/counts; retain relative names only."""
    files={};directories=[]
    def walk(path,depth=0):
        if depth>16:raise ValueError('bucket directory depth exceeds limit')
        for item in sorted(path.iterdir()):
            info=item.lstat();name=item.relative_to(root).as_posix()
            if len(name.encode('utf8','surrogateescape'))>512:raise ValueError('bucket source path exceeds limit')
            if stat.S_ISLNK(info.st_mode):raise ValueError('bucket source contains a symlink')
            if stat.S_ISDIR(info.st_mode):
                directories.append(name)
                if len(directories)>MAX_FILES:raise ValueError('bucket directory limit exceeded')
                walk(item,depth+1)
            elif stat.S_ISREG(info.st_mode):
                files[name]=(item,info)
                if len(files)>MAX_FILES:raise ValueError('bucket file limit exceeded')
            else:raise ValueError('bucket source contains a special file')
    walk(root)
    if sum(info.st_size for _,info in files.values())>MAX_TOTAL_BYTES:raise ValueError('bucket byte limit exceeded')
    return files,tuple(sorted(directories))


def _fingerprint(path):
    with path.open('rb') as file:
        before=os.fstat(file.fileno());sha=hashlib.file_digest(file,'sha256').hexdigest();after=os.fstat(file.fileno())
    if (before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_size,after.st_mtime_ns,after.st_ctime_ns):
        raise ValueError('bucket source changed while hashing')
    return {'bytes':before.st_size,'sha256':sha}


class Snapshot:
    def __init__(self,path,*,offline_snapshot=False,include_inactive=False,limits=None):
        self.limits=metric_limits.validate(limits)
        if not offline_snapshot:raise ValueError('metric bucket access requires an explicit --offline-snapshot assertion')
        path=Path(path)
        if path.is_symlink() or not path.is_dir():raise ValueError('metric bucket must be a regular local directory')
        self.root=path.resolve()
        if self.root.name.startswith('hot_'):raise ValueError('hot buckets are not supported; use a closed offline snapshot')
        disabled=self.root.name.startswith('DISABLED-')
        names,external=_manifest(self.root.parent)
        listed=self.root.name in names if names is not None else None
        if (disabled or listed is False) and not include_inactive:
            raise ValueError('disabled/unlisted bucket requires --include-inactive')
        self.external=external
        self.files,self.directories=_tree(self.root)
        self._root_identity=(self.root.stat().st_dev,self.root.stat().st_ino)
        self.slices=[]
        for name,(file,info) in self.files.items():
            lower=name.lower()
            if lower.startswith('rawdata/deletes/'):
                raise ValueError('metric delete-mask profile is unsupported; event address masks are not applied')
            if '.tsidx' in lower:
                match=SLICE.fullmatch(name)
                if match is None:raise ValueError('ambiguous/nested/reduced/temporary TSIDX path: '+name)
                latest,earliest,identifier=map(int,match.groups())
                if not 0<=earliest<=latest<=2147483647 or identifier>=1<<64:
                    raise ValueError('slice filename fields outside observed bounds')
                self.slices.append(name)
            if (lower.endswith(('.lock','.tmp','.pending','.merge')) or '.temp-' in lower
                    or re.fullmatch(r'[0-9]+-[0-9]+-[0-9]+\.manifest',lower)
                    or 'optimiz' in lower and name!='optimize.result'):
                raise ValueError('bucket has possible in-progress operation marker: '+name)
        self.slices.sort()
        if not 1<=len(self.slices)<=MAX_SLICES:raise ValueError('metric slice count outside supported bounds')
        if 'rawdata/deletes' in self.directories:
            raise ValueError('metric delete directory is unverified, including empty directories')
        identities=set();hashes={};fingerprints={};profiles={};self.reader=None
        for name,(file,info) in self.files.items():
            identity=(info.st_dev,info.st_ino)
            if name in self.slices:
                if identity in identities:raise ValueError('aliased metric slices are ambiguous')
                identities.add(identity)
            fingerprints[name]=_fingerprint(file)
            if name in self.slices:
                sha=fingerprints[name]['sha256']
                if sha in hashes:raise ValueError('duplicate metric slice content; refusing to deduplicate or double count')
                hashes[sha]=name
                try:reader=metric_storage.reader_for(file)
                except ValueError as error:raise ValueError('bucket contains an unsupported/nonmetric TSIDX slice') from error
                if self.reader is not None and reader is not self.reader:
                    raise ValueError('mixed metric storage profiles in one bucket are unverified')
                metric_storage.record_options(reader,self.limits)
                self.reader=reader
                match=SLICE.fullmatch(name);expected=(int(match[1]),int(match[2]))
                if reader is msidx:
                    with file.open('rb') as stream:header=stream.read(128)
                    # Keep the verified MSIDX4 inventory/snapshot contract stable.
                    actual=(int.from_bytes(header[64:68],'big'),int.from_bytes(header[60:64],'big'))
                else:
                    # MSIDX1/2 header positions do not contain metric bounds.
                    # Validate the complete supported profile before inventory,
                    # and derive extrema from real physical point timestamps.
                    minimum=2147483647;maximum=0;count=0
                    for row in reader.records(file,experimental=True):
                        if row['source_sha256']!=sha:raise ValueError('selected slice changed during inventory')
                        seconds=row['indexed_time']['seconds'];minimum=min(minimum,seconds);maximum=max(maximum,seconds);count+=1
                    if not count:raise ValueError('empty legacy metric slice is unverified')
                    actual=(maximum,minimum)
                    profiles[name]={'source_format':reader.FORMAT,'physical_points':count,
                        'minimum_seconds':minimum,'maximum_seconds':maximum,'bounds_source':'complete_physical_scan'}
                if actual!=expected:raise ValueError('slice filename/header or decoded point time bounds disagree')
        self.manifest={'snapshot_schema':1,'files':fingerprints,'directories':list(self.directories),
                       'selected_slices':self.slices,'disabled_name':disabled,'parent_manifest_listed':listed,
                       'parent_manifest_sha256':external[1] if external else None,
                       'selection':'all immediate numeric MSIDX4 tsidx slices in explicit closed local snapshot',
                       'origin_consistency':'caller_asserted_offline_snapshot;not_independently_proven',
                       'search_visibility':'UNVERIFIED','indexed_search_supported':False}
        if profiles:
            self.manifest['selection']='all immediate numeric legacy metric tsidx slices in explicit closed local snapshot'
            self.manifest['slice_profiles']=profiles
        if len(_json(self.manifest))>MAX_MANIFEST_BYTES:raise ValueError('bucket manifest exceeds limit')
        self.sha256=hashlib.sha256(_json(self.manifest)).hexdigest()
        self.finish()

    def finish(self):
        if self.root.is_symlink() or (self.root.stat().st_dev,self.root.stat().st_ino)!=self._root_identity:
            raise ValueError('bucket directory changed during scan')
        files,directories=_tree(self.root)
        if set(files)!=set(self.files) or directories!=self.directories:raise ValueError('bucket inventory changed during scan')
        for name,(file,_) in files.items():
            if _fingerprint(file)!=self.manifest['files'][name]:raise ValueError('bucket source changed during scan')
        _,external=_manifest(self.root.parent)
        if external!=self.external:raise ValueError('parent bucket manifest changed during scan')

    def inventory(self):
        self.finish()
        return {'snapshot_sha256':self.sha256,**self.manifest}

    def records(self,*,experimental=False,index=None,bucket_id=None):
        if not experimental:raise ValueError('metric bucket decoder is experimental; pass --experimental')
        self.finish();first=True
        for name in self.slices:
            rows=self.reader.records(self.root/name,experimental=True,index=index,bucket_id=bucket_id,
                                     **metric_storage.record_options(self.reader,self.limits))
            try:
                for row in rows:
                    if row['source_sha256']!=self.manifest['files'][name]['sha256']:
                        raise ValueError('selected slice changed after snapshot')
                    state=json.loads(row['storage_json'])
                    state['bucket_snapshot_sha256']=self.sha256;state['slice_file']=name
                    if first:state['bucket_snapshot_manifest']=self.manifest;first=False
                    row['storage_json']=_json(state).decode()
                    yield row
            finally:rows.close()
        self.finish()


def export(path,output,format='parquet',*,experimental=False,offline_snapshot=False,include_inactive=False,index=None,bucket_id=None,limits=None):
    root=Path(path).resolve();output=Path(output).resolve()
    if output.is_relative_to(root):raise ValueError('output must be outside the source archive/bucket')
    snapshot=Snapshot(path,offline_snapshot=offline_snapshot,include_inactive=include_inactive,limits=limits)
    result=snapshot.reader.export(snapshot.records(experimental=experimental,index=index,bucket_id=bucket_id),output,format)
    result=dict(result,bucket_snapshot_sha256=snapshot.sha256,selected_slices=len(snapshot.slices))
    if snapshot.limits is not None:result['metric_resource_profile']=snapshot.limits.describe()
    return result


def verify(path,against,format='parquet',*,experimental=False,offline_snapshot=False,include_inactive=False,index=None,bucket_id=None,limits=None):
    snapshot=Snapshot(path,offline_snapshot=offline_snapshot,include_inactive=include_inactive,limits=limits)
    result=snapshot.reader.verify(snapshot.records(experimental=experimental,index=index,bucket_id=bucket_id),against,format)
    result=dict(result,bucket_snapshot_sha256=snapshot.sha256,selected_slices=len(snapshot.slices))
    if snapshot.limits is not None:result['metric_resource_profile']=snapshot.limits.describe()
    return result
