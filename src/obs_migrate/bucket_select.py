"""Snapshot-bound exact physical metric selection and atomic ZIP bundles.

Bundles contain exactly manifest.json and points.parquet/points.ndjson. Every
inventoried slice is fully validated, including zero-match slices. No pruning,
performance, live membership, or native search equivalence is implied.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import struct
import tempfile
import threading
import zipfile

from . import metric_bucket,metric_select,msidx,msidx1,msidx2
from .export import atomic_output

MAX_MANIFEST_BYTES=8*1024**2
MAX_RESULT_BYTES=16*1024**3
BUNDLE_FORMAT='obs-metric-bucket-selection-zip'


def _sha(path):
    with Path(path).open('rb') as file:return hashlib.file_digest(file,'sha256').hexdigest()


class Selection:
    def __init__(self,path,predicate,*,experimental=False,offline_snapshot=False,include_inactive=False,index=None,bucket_id=None):
        if not experimental:raise ValueError('bucket predicates are experimental; pass --experimental')
        if not isinstance(predicate,metric_select.Predicate):raise ValueError('selection requires an exact Predicate')
        self.snapshot=metric_bucket.Snapshot(path,offline_snapshot=offline_snapshot,include_inactive=include_inactive)
        self.reader=self.snapshot.reader
        if self.reader not in (msidx1,msidx2,msidx):raise ValueError('bucket predicates support verified MSIDX1/MSIDX2/MSIDX4 profiles only')
        self.predicate=predicate;self.index=index;self.bucket_id=bucket_id
        self.query={'query_schema':1,'snapshot_sha256':self.snapshot.sha256,'source_format':self.reader.FORMAT,
            'predicate':predicate.describe(),'labels':{'index':index,'bucket_id':bucket_id},
            'semantics':'physical_exact_AND;full_preflight;search_visibility_unverified'}
        self.sha256=hashlib.sha256(metric_bucket._json(self.query)).hexdigest()
        self.slices=[];self.complete=False;self._scan_lock=threading.Lock()

    def _scan(self,emit):
        if not self._scan_lock.acquire(blocking=False):raise ValueError('bucket selection scan is already active')
        self.slices=[];self.complete=False;first=True
        try:
            self.snapshot.finish()
            for name in self.snapshot.slices:
                with metric_select.open_source(self.snapshot.root/name,experimental=True) as source:
                    if source.sha256!=self.snapshot.manifest['files'][name]['sha256'] or source.module is not self.reader:
                        raise ValueError('selected slice changed after snapshot')
                    plan=source.plan(self.predicate);report=source._report(plan)
                    self.slices.append({'slice_file':name,'source_sha256':source.sha256,
                        'fully_validated_points':report['fully_validated_points'],'candidate_points':report['candidate_points'],
                        'candidate_dimension_sets':report['candidate_dimension_sets'],'candidate_strategy':report['candidate_strategy']})
                    if emit:
                        for row in source._emit(plan,self.index,self.bucket_id):
                            state=json.loads(row['storage_json'])
                            state.update(bucket_snapshot_sha256=self.snapshot.sha256,slice_file=name,bucket_query_sha256=self.sha256)
                            if first:state['bucket_snapshot_manifest']=self.snapshot.manifest;first=False
                            row['storage_json']=metric_bucket._json(state).decode();yield row
            self.snapshot.finish()
            self.complete=True
        finally:
            try:
                if not self.complete:self.snapshot.finish()
            finally:self._scan_lock.release()

    def records(self):
        yield from self._scan(True)

    def batches(self,*,max_rows=1024,max_bytes=8*1024*1024):
        yield from self.reader.batches(self.records(),max_rows,max_bytes)

    def report(self):
        if not self.complete or self._scan_lock.locked():raise ValueError('bucket predicate result requires complete source exhaustion')
        return {'bundle_schema':1,'query':self.query,'query_sha256':self.sha256,
            'snapshot_manifest':self.snapshot.manifest,'slices':self.slices,
            'fully_validated_points':sum(s['fully_validated_points'] for s in self.slices),
            'candidate_points':sum(s['candidate_points'] for s in self.slices),
            'full_preflight_scan':True,'performance_claim':False,'physical_predicate_selection':True,
            'search_visibility':'UNVERIFIED','indexed_search_supported':False}

    def explain(self):
        for _ in self._scan(False):pass
        return self.report()


def records(path,predicate,**options):
    yield from Selection(path,predicate,**options).records()


def batches(path,predicate,*,max_rows=1024,max_bytes=8*1024*1024,**options):
    yield from Selection(path,predicate,**options).batches(max_rows=max_rows,max_bytes=max_bytes)


def explain(path,predicate,**options):return Selection(path,predicate,**options).explain()


def _data_info(file,format,count,measurements,canonical):
    return {'file':'points.'+format,'format':format,'bytes':file.stat().st_size,'sha256':_sha(file),
        'stored_points':count,'measurements':measurements,'canonical_sha256':canonical}


def export(path,predicate,output,format='parquet',**options):
    if format not in ('parquet','ndjson'):raise ValueError('bucket selection payload must be parquet or ndjson')
    output=Path(output)
    if output.resolve().is_relative_to(Path(path).resolve()):raise ValueError('output must be outside the source archive/bucket')
    if output.exists() or output.is_symlink():raise FileExistsError('output already exists; overwrite disabled')
    selection=Selection(path,predicate,**options)
    with tempfile.TemporaryDirectory(prefix='.obs-bucket-select-',dir=output.parent) as directory:
        file=Path(directory)/('points.'+format)
        stats=selection.reader.export(selection.records(),file,format)
        if file.stat().st_size>MAX_RESULT_BYTES:raise ValueError('bucket selection payload exceeds resource bound')
        manifest={**selection.report(),'data':_data_info(file,format,stats['stored_points'],stats['measurements'],stats['canonical_sha256'])}
        encoded=metric_bucket._json(manifest)
        if len(encoded)>MAX_MANIFEST_BYTES:raise ValueError('bucket selection manifest exceeds resource bound')
        with atomic_output(output) as stream:
            with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED,allowZip64=True) as archive:
                archive.writestr('manifest.json',encoded);archive.write(file,manifest['data']['file'])
            selection.snapshot.finish()
    return {'bundle_format':BUNDLE_FORMAT,'bundle_sha256':_sha(output),'output_bytes':output.stat().st_size,
        'query_sha256':selection.sha256,'bucket_snapshot_sha256':selection.snapshot.sha256,
        'stored_points':stats['stored_points'],'selected_slices':len(selection.slices),
        'zero_match_slices':sum(s['candidate_points']==0 for s in selection.slices),
        'native_assisted':False,'indexed_search_supported':False,'search_visibility':'UNVERIFIED'}


def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('duplicate bucket selection manifest key')
        result[key]=value
    return result


def _check_directory(stream,size):
    # Our comment-free two-entry ZIP has a 22-byte EOCD, also when ZIP64 extra
    # records are present. Bound central-directory allocation before ZipFile
    # loads it; payload/manifest limits are checked separately below.
    if size<22:raise ValueError('invalid selection ZIP directory')
    stream.seek(size-22);record=stream.read(22)
    signature,disk,central_disk,local_count,total_count,central_size,_,comment=struct.unpack('<4s4H2LH',record)
    if signature!=b'PK\x05\x06' or disk or central_disk or local_count!=2 or total_count!=2 or central_size>4096 or comment:
        raise ValueError('unsupported selection ZIP directory profile')
    # ZIP64 may override the EOCD fields. Our writer emits its fixed 56-byte
    # record and 20-byte locator; validate those before the stdlib allocates CD.
    if size>=42:
        stream.seek(size-42);locator=stream.read(20)
        if locator[:4]==b'PK\x06\x07':
            _,disk_number,record_offset,disks=struct.unpack('<4sLQL',locator)
            if disk_number or disks!=1 or record_offset!=size-42-56:
                raise ValueError('unsupported selection ZIP64 directory profile')
            stream.seek(record_offset)
            sig,length,_,_,disk_number,central_disk,local_count,total_count,central_size,central_offset=struct.unpack('<4sQ2H2L4Q',stream.read(56))
            if (sig!=b'PK\x06\x06' or length!=44 or disk_number or central_disk or
                local_count!=2 or total_count!=2 or central_size>4096 or central_offset+central_size!=record_offset):
                raise ValueError('unsupported selection ZIP64 directory profile')
    stream.seek(0)


def verify(path,predicate,against,format=None,**options):
    selection=Selection(path,predicate,**options);against=Path(against)
    if against.is_symlink() or not against.is_file():raise ValueError('selection bundle must be a regular file')
    try:
        with against.open('rb') as stream:
            before=os.fstat(stream.fileno())
            if not 1<=before.st_size<=MAX_RESULT_BYTES+MAX_MANIFEST_BYTES+1024*1024:
                raise ValueError('selection bundle size exceeds resource bound')
            _check_directory(stream,before.st_size)
            bundle_sha=hashlib.file_digest(stream,'sha256').hexdigest();stream.seek(0)
            with zipfile.ZipFile(stream) as archive:
                infos=archive.infolist();names=[i.filename for i in infos]
                if len(infos)!=2 or len(set(names))!=2 or 'manifest.json' not in names or archive.comment:
                    raise ValueError('unsupported bucket selection bundle layout')
                for info in infos:
                    mode=info.external_attr>>16
                    if (info.compress_type!=zipfile.ZIP_STORED or info.compress_size!=info.file_size or info.flag_bits&1 or
                        stat.S_IFMT(mode) not in (0,stat.S_IFREG) or info.file_size<0):
                        raise ValueError('unsupported selection ZIP member profile')
                if archive.getinfo('manifest.json').file_size>MAX_MANIFEST_BYTES:raise ValueError('selection manifest exceeds resource bound')
                manifest=json.loads(archive.read('manifest.json'),object_pairs_hook=_object)
                if not isinstance(manifest,dict) or not isinstance(manifest.get('data'),dict):raise ValueError('invalid selection manifest')
                actual_format=manifest['data'].get('format')
                if actual_format not in ('parquet','ndjson') or format is not None and format!=actual_format:
                    raise ValueError('unsupported/mismatched selection payload format')
                filename='points.'+actual_format
                if set(names)!={'manifest.json',filename} or manifest['data'].get('file')!=filename:
                    raise ValueError('unsupported bucket selection payload name')
                if archive.getinfo(filename).file_size>MAX_RESULT_BYTES:raise ValueError('selection payload exceeds resource bound')
                if manifest.get('query')!=selection.query or manifest.get('query_sha256')!=selection.sha256:
                    raise ValueError('bucket selection query/snapshot identity mismatch')
                if manifest.get('snapshot_manifest')!=selection.snapshot.manifest:
                    raise ValueError('bucket selection source manifest mismatch')
                # Known flat payload name, bounded stored bytes, private temporary
                # directory: never extract untrusted archive paths or links.
                with tempfile.TemporaryDirectory(prefix='obs-bucket-select-') as directory:
                    file=Path(directory)/filename
                    with archive.open(filename) as source,file.open('wb') as dest:shutil.copyfileobj(source,dest)
                    if file.stat().st_size!=manifest['data'].get('bytes') or _sha(file)!=manifest['data'].get('sha256'):
                        raise ValueError('bucket selection payload hash/size mismatch')
                    count=measurements=0;digest=hashlib.sha256()
                    def expected():
                        nonlocal count,measurements
                        for row in selection.records():
                            count+=1;measurements+=len(row['measurements']);digest.update(selection.reader.json_bytes(row));yield row
                    selection.reader.verify(expected(),file,actual_format)
                    wanted={**selection.report(),'data':_data_info(file,actual_format,count,measurements,digest.hexdigest())}
                    if metric_bucket._json(manifest)!=metric_bucket._json(wanted):raise ValueError('bucket selection result manifest mismatch')
            if msidx._stable(before)!=msidx._stable(os.fstat(stream.fileno())):raise ValueError('selection bundle changed during verification')
    except (zipfile.BadZipFile,KeyError,TypeError,UnicodeError,RecursionError,struct.error,NotImplementedError) as error:
        raise ValueError('invalid bucket selection bundle') from error
    return {'bundle_format':BUNDLE_FORMAT,'bundle_sha256':bundle_sha,'verified_stored_points':count,
        'comparison':'query identity, complete source/result manifests, payload hash and all canonical rows',
        'query_sha256':selection.sha256,'bucket_snapshot_sha256':selection.snapshot.sha256,
        'selected_slices':len(selection.slices),'zero_match_slices':sum(s['candidate_points']==0 for s in selection.slices),
        'native_assisted':False,'indexed_search_supported':False,'search_visibility':'UNVERIFIED'}
