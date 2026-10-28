"""Bounded immutable physical event snapshots with explicit full journal scan.

This is derived Arrow access, not compressed-journal random seeking or native
search. All native bytes are pinned before validation; queries do not reopen
the source or trust persisted validation markers.
"""
from contextlib import ExitStack
from array import array
import base64
import hashlib
import json
from pathlib import Path
import re
import struct
import tempfile
import threading
from types import MappingProxyType

import pyarrow as pa
import pyarrow.parquet as pq

from . import capture, event_index, events, journal, metric_bucket, msidx,event_limits
from .buckets import _manifest
from .export import atomic_output
from .source_identity import normalize_sha256,require_sha256

MAX_SOURCE_BYTES = 256*1024**2
MAX_EVENT_BYTES = 256*1024**2
MAX_DECOMPRESSED_BYTES = 256*1024**2
MAX_EVENTS = 100_000
MAX_MEMBERSHIPS = 5_000_000
MAX_TERMS = 1_000_000
MAX_LEXICON_BYTES = 64*1024**2
PROVENANCE_KEY = b'obs_migrate.event_bucket_provenance'
LINK_STATE=struct.Struct('<HI')
LINK = pa.struct([
    pa.field('slice_file',pa.string(),False),
    *[field for field in event_index.SCHEMA if field.name != 'decoder'],
    pa.field('decoder',pa.string(),False),
])
SCHEMA = pa.schema([*events.SCHEMA,pa.field('index_link',LINK,False)],metadata={
    **events.SCHEMA.metadata,b'obs_migrate.event_bucket_schema':b'1',
    b'indexed_search_supported':b'false',b'pruning_supported':b'false'})


def _json(value):return metric_bucket._json(value)
def _sha(data):return hashlib.sha256(data).hexdigest()


class EventBucket:
    """Pin an explicit closed offline bucket and materialize linked Arrow rows.

    The caller asserts the origin is offline. Inventory/stability checks detect
    observed changes; they do not prove a coherent production snapshot.
    """
    def __init__(self,path,*,experimental=False,offline_snapshot=False,include_inactive=False,
                 index=None,bucket_id=None,object_uri=None,backend='arrow',retain_opaque_trailer=False,cache_projections=False,metadata_cache_bytes=None,limits=None,expected_snapshot_sha256=None,
                 visibility='physical',_smartstore_mask_delta=None):
        if visibility not in ('physical','undeleted'):raise ValueError('event visibility must be physical or undeleted')
        if _smartstore_mask_delta is not None and (type(_smartstore_mask_delta) is not dict or visibility!='undeleted'):
            raise ValueError('SmartStore mask delta requires receipt evidence and undeleted selection')
        self._smartstore_mask_delta=json.loads(_json(_smartstore_mask_delta)) if _smartstore_mask_delta is not None else None
        self.visibility=visibility;self._mask_selectable=None;self._undeleted_ordinals=None
        expected_snapshot_sha256=normalize_sha256(expected_snapshot_sha256)
        limits=event_limits.validate(limits)
        if limits is not None and backend!='journal':raise ValueError('explicit event resource profiles require journal backend')
        max_events=MAX_EVENTS if limits is None else limits.events
        max_terms=MAX_TERMS if limits is None else limits.bucket_terms
        max_memberships=MAX_MEMBERSHIPS if limits is None else limits.bucket_memberships
        max_decompressed=MAX_DECOMPRESSED_BYTES if limits is None else limits.decompressed_bytes
        from .journal_access import Observer,DEFAULT_METADATA_CACHE_BYTES,_metadata_cache_budget
        metadata_cache_bytes=_metadata_cache_budget(DEFAULT_METADATA_CACHE_BYTES if metadata_cache_bytes is None else metadata_cache_bytes)
        if type(cache_projections) is not bool or cache_projections and backend!='journal':
            raise ValueError('native projection cache requires journal backend and boolean option')
        if backend not in ('arrow','journal'):raise ValueError('event backend must be arrow or journal')
        self.backend=backend;self._journal=None;self._native_links=b''
        if not experimental:raise ValueError('event bucket access is experimental')
        if not offline_snapshot:raise ValueError('event bucket requires an explicit offline_snapshot assertion')
        path=Path(path)
        if path.is_symlink() or not path.is_dir():raise ValueError('event bucket requires a regular directory')
        root=path.resolve()
        self._origin_root=root
        self._root_identity=(root.stat().st_dev,root.stat().st_ino)
        if root.name.startswith('hot_'):raise ValueError('hot event buckets are unsupported')
        names,external=_manifest(root.parent)
        listed=root.name in names if names is not None else None
        disabled=root.name.startswith('DISABLED-')
        if (disabled or listed is False) and not include_inactive:raise ValueError('inactive event bucket requires include_inactive')
        files,directories=metric_bucket._tree(root)
        if 'splunk-need-optimize.dat' in directories:
            raise ValueError('pending optimization request must be an empty regular file')
        slices=[];pending_optimization=[]
        for name in files:
            if name=='splunk-need-optimize.dat':
                if files[name][1].st_size!=0:raise ValueError('unverified nonempty pending optimization request')
                pending_optimization.append(name)
                continue
            lower=name.lower()
            if '.tsidx' in lower:
                match=metric_bucket.SLICE.fullmatch(name)
                if match is None:raise ValueError('ambiguous/nested/reduced event TSIDX path')
                latest,earliest,identifier=map(int,match.groups())
                if not 0<=earliest<=latest<=2147483647 or identifier>=1<<64:raise ValueError('event slice filename fields outside verified bounds')
                slices.append(name)
            if (lower.endswith(('.lock','.tmp','.pending','.merge')) or '.temp-' in lower or
                re.fullmatch(r'[0-9]+-[0-9]+-[0-9]+\.manifest',lower) or
                'optimiz' in lower and name!='optimize.result'):
                raise ValueError('event bucket has an in-progress operation marker')
        if not 1<=len(slices)<=metric_bucket.MAX_SLICES:raise ValueError('event slice count outside supported bounds')
        parent=capture._read(external[0],metric_bucket.MAX_MANIFEST_BYTES) if external else None
        if parent is not None and _sha(parent)!=external[1]:raise ValueError('parent manifest changed during event snapshot')
        if sum(info.st_size for _,info in files.values())+(len(parent) if parent else 0)>MAX_SOURCE_BYTES:
            raise ValueError('event snapshot source byte limit exceeded')
        pinned={};pinned_bytes=len(parent) if parent else 0
        for name,(file,_) in files.items():
            remaining=MAX_SOURCE_BYTES-pinned_bytes
            wire=capture._read(file,remaining,zero=True)
            pinned_bytes+=len(wire);pinned[name]=wire
        journal_name=journal.resolve(root).relative_to(root).as_posix()
        if journal_name not in pinned:raise ValueError('event journal is outside pinned inventory')
        after,after_dirs=metric_bucket._tree(root)
        if set(after)!=set(files) or after_dirs!=directories:raise ValueError('event bucket inventory changed during pinning')
        for name,(file,info) in after.items():
            before=files[name][1]
            if (before.st_dev,before.st_ino,msidx._stable(before))!=(info.st_dev,info.st_ino,msidx._stable(info)):
                raise ValueError('event bucket source changed during pinning')
            if metric_bucket._fingerprint(file)!={'bytes':len(pinned[name]),'sha256':_sha(pinned[name])}:
                raise ValueError('event bucket source changed during pinning')
        if _manifest(root.parent)[1]!=external:raise ValueError('parent manifest changed during event snapshot')
        if root.is_symlink() or (root.stat().st_dev,root.stat().st_ino)!=self._root_identity:
            raise ValueError('event root changed during pinning')
        self._stack=ExitStack();self.closed=False;self._lock=threading.Lock()
        try:
            self._sources={};by_address={};memberships=0;profiles={};source_hashes=set();terms=lexicon_bytes=0
            self._slice_names=tuple(sorted(slices))
            for slice_id,name in enumerate(self._slice_names):
                sha=_sha(pinned[name])
                if sha in source_hashes:raise ValueError('duplicate event slice contents')
                source_hashes.add(sha)
                source=self._stack.enter_context(event_index.open_bytes(pinned[name],experimental=True,retain_opaque_trailer=retain_opaque_trailer,limits=limits))
                self._sources[name]=source;report=source.report()
                terms+=source.reader.nterms;lexicon_bytes+=sum(len(t)+1 for t in source.reader.terms)
                if terms>max_terms or lexicon_bytes>MAX_LEXICON_BYTES:raise ValueError('event snapshot aggregate lexicon limit exceeded')
                memberships+=report['fully_validated_posting_memberships']
                if memberships>max_memberships:raise ValueError('event snapshot posting limit exceeded')
                match=metric_bucket.SLICE.fullmatch(name)
                if (source.reader.latest,source.reader.earliest)!=(int(match[1]),int(match[2])):
                    raise ValueError('event slice filename/header bounds mismatch')
                profiles[name]=report
                # All values and postings have already passed full preflight.
                # Keep only their proven address and compact source/value IDs;
                # reconstruct the exact original link when its journal row arrives.
                for value_id,address in enumerate(source.addresses):
                    if address in by_address:raise ValueError('overlapping event slice addresses are unverified')
                    by_address[address]=(slice_id<<32)|value_id
                    if len(by_address)>max_events:raise ValueError('event snapshot record limit exceeded')
            manifest={'event_snapshot_schema':1,'files':{n:{'bytes':len(b),'sha256':_sha(b)} for n,b in pinned.items()},
                'directories':list(directories),'selected_slices':sorted(slices),
                'journal':journal_name,'parent_manifest_sha256':external[1] if external else None,
                'parent_manifest_listed':listed,'disabled_name':disabled,
                'origin_consistency':'caller_asserted_offline_snapshot;not_independently_proven',
                'search_visibility':'UNVERIFIED','indexed_search_supported':False,'pruning_supported':False}
            if pending_optimization:
                if any(pinned[name] for name in pending_optimization):raise ValueError('pending optimization request changed during pinning')
                manifest['pending_optimization_requests']=[{'file':name,'bytes':0,
                    'interpretation':'native_optimizer_request;closed_offline_source_required'} for name in pending_optimization]
            opaque={name:profile['catalog']['opaque_trailer'] for name,profile in profiles.items() if 'opaque_trailer' in profile['catalog']}
            if opaque:manifest['uninterpreted_index_trailers']=opaque
            self.snapshot_sha256=_sha(_json(manifest))
            require_sha256(self.snapshot_sha256,expected_snapshot_sha256,'event snapshot')
            stats={};batches=[];pending=[];encoded=0;event_count=0
            selectable=bytearray() if visibility=='undeleted' else None
            undeleted_ordinals=array('I') if visibility=='undeleted' else None
            self._ordinals={name:array('I',[0xffffffff])*source.reader.nvalues for name,source in self._sources.items()}
            if backend=='journal':
                self._journal=Observer(max_decompressed,max_events,cache_projections=cache_projections,metadata_cache_bytes=metadata_cache_bytes);self._stack.callback(self._journal.close)
                self._native_links=bytearray()
            # Reuse the audited journal decoder on private pinned bytes. Original
            # relative names and parent manifest retain bucket-context semantics.
            with tempfile.TemporaryDirectory(prefix='obs-event-snapshot-') as temp:
                clone=Path(temp)/root.name;clone.mkdir()
                for directory in directories:(clone/directory).mkdir(parents=True,exist_ok=True)
                for name,wire in pinned.items():(clone/name).write_bytes(wire)
                if parent is not None:(clone.parent/'.bucketManifest').write_bytes(parent)
                rows=journal.events(clone,experimental=True,index_type='event',index=index,bucket_id=bucket_id,
                    object_uri=object_uri,stats=stats,include_disabled=include_inactive,
                    max_decompressed_bytes=max_decompressed,_observer=self._journal)
                try:
                    for row in rows:
                        state=json.loads(row['decoder_state_json'])
                        if (state['header']['version'],state['header']['align_bits'])!=(1,5) or row['event_time'] is None:
                            raise ValueError('event snapshot requires verified journal v1/align5 time profile')
                        if row['decompressed_offset']%32:raise ValueError('unaligned event journal address')
                        address=row['decompressed_offset']>>5
                        identity=by_address.pop(address,None)
                        if identity is None:raise ValueError('journal/index address coverage mismatch')
                        slice_id,value_id=identity>>32,identity&0xffffffff
                        slice_name=self._slice_names[slice_id]
                        link={'slice_file':slice_name,**self._sources[slice_name].value(value_id)}
                        if row['event_time']['seconds']!=link['indexed_seconds']:raise ValueError('journal/index second mismatch')
                        if row['source_sha256']!=manifest['files'][journal_name]['sha256']:raise ValueError('pinned journal source mismatch')
                        encoded+=(self._journal.last_json_size if self._journal else len(events.json_bytes(row)))+len(_json({k:v for k,v in link.items() if not isinstance(v,bytes)}))+len(link['value_wire'])
                        if backend=='arrow' and encoded>MAX_EVENT_BYTES:raise ValueError('event snapshot decoded byte limit exceeded')
                        self._ordinals[slice_name][value_id]=event_count
                        if selectable is not None:
                            if type(row['is_deleted']) is not bool:raise ValueError('undeleted selection requires reconciled journal mask annotations')
                            selectable.append(not row['is_deleted'])
                            if not row['is_deleted']:undeleted_ordinals.append(event_count)
                        event_count+=1
                        if self._journal:self._native_links.extend(LINK_STATE.pack(slice_id,value_id))
                        else:pending.append({**row,'index_link':link})
                        if not self._journal and len(pending)==1024:
                            batches.append(pa.RecordBatch.from_pylist(pending,schema=SCHEMA));pending=[]
                finally:rows.close()
            if by_address:raise ValueError('index values missing from complete journal')
            if pending:batches.append(pa.RecordBatch.from_pylist(pending,schema=SCHEMA))
            self._table=pa.Table.from_batches(batches,schema=SCHEMA) if backend=='arrow' else None
            if self._journal:self._native_links=bytes(self._native_links)
            self._files=MappingProxyType(pinned);self._parent=parent
            if selectable is not None:
                self._mask_selectable=bytes(selectable);self._undeleted_ordinals=undeleted_ordinals
                deletion_observation=self.deletion_report()
            self._report=_json({'event_bucket_schema':1,'snapshot_sha256':self.snapshot_sha256,'snapshot_manifest':manifest,
                'source_profiles':profiles,'journal_scan':stats,'fully_validated_events':event_count,
                'fully_validated_posting_memberships':memberships,'initial_full_journal_scan':True,
                'repeated_query_journal_scan':False,'compressed_journal_random_seek':False,
                'journal_addresses_resolved':True,'address_profile':'v1_align5:decompressed_offset>>5;complete one-to-one coverage',
                'retained_source_bytes':sum(map(len,pinned.values()))+(len(parent) if parent else 0),
                'decoded_event_accounted_bytes':encoded,'retained_arrow_bytes':self._table.nbytes if self._table is not None else 0,
                'memory_accounting':'bounded source and serialized event budgets; not process RSS or Python object overhead' if backend=='arrow' else 'bounded native source/decompressed/state; event accounting is processed data, not retained memory or process RSS',
                'labels':{'index':index,'bucket_id':bucket_id,'object_uri':object_uri},
                **({'deletion_policy':('all_physical_records;existing_journal_mask_annotations;no_tsidx_visibility' if visibility=='physical'
                    else 'full_physical_validation;exclude_reconciled_journal_masks_only;no_tsidx_visibility')}
                   if 'rawdata/deletes' in directories or any(getattr(s.reader,'legacy',False) for s in self._sources.values()) else {}),
                **({'backend':'native_journal','retained_link_state_bytes':len(self._native_links),**self._journal.report()} if self._journal else {}),
                **({'resource_limits':limits.describe()} if limits is not None else {}),
                **({'mask_selection':{'visibility':'undeleted','semantics':'exclude_reconciled_journal_deletes_only;not_native_search_visibility',
                    'selected_events':len(undeleted_ordinals),'deleted_events_excluded':event_count-len(undeleted_ordinals),
                    'retained_selection_bytes':len(selectable)+undeleted_ordinals.itemsize*len(undeleted_ordinals),
                    'native_delete_bitmaps':deletion_observation,
                    **({'delete_evidence_policy':'immutable_smartstore_base_plus_receipt_bound_csv_delta'} if _smartstore_mask_delta is not None else {}),
                    'initial_validation':'all_physical_events_values_postings_and_mask_addresses'}} if selectable is not None else {}),
                'search_visibility':'UNVERIFIED','indexed_search_supported':False,'pruning_supported':False})
        except BaseException:
            self._stack.close();self.closed=True
            raise

    def _check(self):
        if self.closed:raise ValueError('event snapshot is closed')

    def report(self):self._check();return json.loads(self._report)
    def deletion_report(self):
        if self._smartstore_mask_delta is not None:
            from .event_deletions import validate_smartstore_delta
            return validate_smartstore_delta(self,self._smartstore_mask_delta)
        from .event_deletions import validate
        return validate(self)
    def __enter__(self):self._check();return self
    def __exit__(self,*args):self.close()
    def close(self):
        if not self._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
        try:
            self._stack.close();self._sources.clear();self._ordinals.clear()
            self._files=MappingProxyType({});self._table=None;self._parent=None;self.closed=True
            self._native_links=b''
            self._mask_selectable=None;self._undeleted_ordinals=None
        finally:self._lock.release()

    def _selection(self,term):
        self._check()
        if term is None:return None if self._mask_selectable is None else list(self._undeleted_ordinals)
        # Let the shared API enforce exact-byte term bounds even for no matches.
        selected=[]
        for name,source in self._sources.items():
            selected.extend(self._ordinals[name][i] for i in source.posting_ids(term))
        return sorted(selected) if self._mask_selectable is None else sorted(i for i in selected if self._mask_selectable[i])

    def _query_semantics(self,term):
        semantics='all_physical_events' if term is None else 'exact_physical_lexicon_membership'
        return semantics if self._mask_selectable is None else semantics+'_excluding_reconciled_deletes'

    def table(self,term=None):
        if not self._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
        try:
            ids=self._selection(term)
            native_stats={}
            if self._journal:
                pending=[];batches=[];size=0;pending_bytes=0
                for row,accounted in self._native_records(ids,native_stats):
                    pending.append(row);pending_bytes+=accounted
                    if len(pending)>=1024 or pending_bytes>=8*1024**2:
                        batch=pa.RecordBatch.from_pylist(pending,schema=SCHEMA);size+=batch.nbytes
                        if size>MAX_EVENT_BYTES:raise ValueError('native event Arrow materialization byte limit exceeded')
                        batches.append(batch);pending=[];pending_bytes=0
                if pending:
                    batch=pa.RecordBatch.from_pylist(pending,schema=SCHEMA);size+=batch.nbytes
                    if size>MAX_EVENT_BYTES:raise ValueError('native event Arrow materialization byte limit exceeded')
                    batches.append(batch)
                result=pa.Table.from_batches(batches,schema=SCHEMA)
            else:result=self._table if ids is None else self._table.take(pa.array(ids,type=pa.uint64()))
            query={'term_base64':None if term is None else base64.b64encode(term).decode(),
                'semantics':self._query_semantics(term),
                'selected_events':result.num_rows,'order':'journal_record_ordinal'}
            if self._journal:query.update(native_stats)
            envelope={**self.report(),'query':query}
            return result.replace_schema_metadata({**SCHEMA.metadata,PROVENANCE_KEY:_json(envelope)})
        finally:self._lock.release()

    def describe_selection(self,term=None):
        """Physical input plan only; no event payload decoding or actual-work claim."""
        if not self._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
        try:
            ids=self._selection(term);count=self.report()['fully_validated_events'] if ids is None else len(ids)
            query={'term_base64':None if term is None else base64.b64encode(term).decode(),
                'semantics':self._query_semantics(term),
                'selected_events':count,'order':'journal_record_ordinal'}
            if self._journal:
                work=self._journal.planned_work(range(count) if ids is None else ids)
                query.update(planned_native_records=work['native_records_decoded'],
                    planned_native_record_wire_bytes=work['native_record_bytes_read'])
            return {**self.report(),'query':query}
        finally:self._lock.release()

    def _native_records(self,ids,stats):
        self._check();ids=range(len(self._native_links)//LINK_STATE.size) if ids is None else ids
        stats.update(native_records_decoded=0,native_record_bytes_read=0)
        labels=self.report()['labels']
        for ordinal in ids:
            self._check();row,read,accounted=self._journal.record(ordinal,**labels)
            slice_id,value_id=LINK_STATE.unpack_from(self._native_links,ordinal*LINK_STATE.size)
            name=self._slice_names[slice_id];link=self._sources[name].value(value_id)
            if link['address_operand']!=row['decompressed_offset']>>5 or link['indexed_seconds']!=row['event_time']['seconds']:
                raise ValueError('addressed native event/index linkage mismatch')
            stats['native_records_decoded']+=1;stats['native_record_bytes_read']+=read
            yield {**row,'index_link':{'slice_file':name,**link}},accounted+256

    def _projected_native_records(self,ids,columns,stats):
        self._check();ids=range(len(self._native_links)//LINK_STATE.size) if ids is None else ids
        stats.update(native_records_decoded=0,native_record_bytes_read=0,native_cached_projection_rows=0,native_projected_bytes_read=0)
        labels=self.report()['labels'];base=tuple(name for name in columns if name!='index_link')
        for ordinal in ids:
            self._check();row,work=self._journal.project(ordinal,base,**labels)
            if 'index_link' in columns:
                slice_id,value_id=LINK_STATE.unpack_from(self._native_links,ordinal*LINK_STATE.size)
                name=self._slice_names[slice_id];link=self._sources[name].value(value_id)
                offset,seconds=self._journal.location(ordinal)
                if (link['address_operand'],link['indexed_seconds'])!=(offset>>5,seconds):raise ValueError('cached native event/index linkage mismatch')
                row['index_link']={'slice_file':name,**link}
            for key,value in work.items():stats[key]+=value
            yield row

    def projected_records(self,columns,term=None,*,stats=None):
        columns=tuple(columns)
        if len(set(columns))!=len(columns) or any(name not in SCHEMA.names for name in columns):
            raise ValueError('invalid native event projection columns')
        if self._journal:
            if not self._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
            try:
                ids=self._selection(term)
                yield from self._projected_native_records(ids,columns,{} if stats is None else stats)
            finally:self._lock.release()
        else:
            table=self.table(term).select(columns)
            for batch in table.to_batches(max_chunksize=1024):
                for row in batch.to_pylist():self._check();yield row

    def records(self,term=None,*,stats=None):
        if self._journal:
            if not self._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
            try:
                ids=self._selection(term)
                for row,_ in self._native_records(ids,{} if stats is None else stats):yield row
            finally:self._lock.release()
            return
        table=self.table(term)
        for batch in table.to_batches(max_chunksize=1024):
            for row in batch.to_pylist():self._check();yield row


def export(snapshot,output,format='parquet',*,term=None):
    if not isinstance(snapshot,EventBucket):raise ValueError('export requires a validated EventBucket')
    if format not in ('parquet','arrow'):raise ValueError('event snapshot export requires parquet or arrow')
    if Path(output).resolve().is_relative_to(snapshot._origin_root):raise ValueError('output must be outside source bucket')
    if snapshot._journal:return _native_transport(snapshot,output,format,term=term)
    table=snapshot.table(term)
    with atomic_output(output) as file:
        if format=='parquet':pq.write_table(table,file,compression='zstd',write_statistics=False)
        else:
            with pa.ipc.new_file(file,table.schema) as writer:writer.write_table(table)
    return json.loads(table.schema.metadata[PROVENANCE_KEY])


def verify(snapshot,against,format='parquet',*,term=None):
    if format not in ('parquet','arrow'):raise ValueError('event snapshot verify requires parquet or arrow')
    if snapshot._journal:return _native_transport(snapshot,against,format,term=term,verify=True)
    wanted=snapshot.table(term)
    with Path(against).open('rb') as file:
        import os
        before=os.fstat(file.fileno())
        if before.st_size>2*MAX_EVENT_BYTES:raise ValueError('event snapshot artifact byte limit exceeded')
        actual=pq.read_table(file) if format=='parquet' else pa.ipc.open_file(file).read_all()
        if not wanted.equals(actual,check_metadata=True):raise ValueError('event snapshot export mismatch')
        if msidx._stable(before)!=msidx._stable(os.fstat(file.fileno())):raise ValueError('event snapshot export changed')
    return {**json.loads(wanted.schema.metadata[PROVENANCE_KEY]),'verified_events':wanted.num_rows,
        'comparison':'all event/link fields and source/query envelope, ordered'}


def _native_transport(snapshot,path,format,*,term,verify=False):
    """Publish only after the planned complete-stream counters are fulfilled."""
    from itertools import zip_longest
    import os
    from .native_scan import _batches
    if not snapshot._lock.acquire(blocking=False):raise ValueError('event snapshot scan is active')
    rows=None
    try:
        ids=snapshot._selection(term)
        ordinals=range(len(snapshot._native_links)//LINK_STATE.size) if ids is None else ids
        expected=snapshot._journal.planned_work(ordinals);report=snapshot.report()
        report['query']={'term_base64':None if term is None else base64.b64encode(term).decode(),
            'semantics':snapshot._query_semantics(term),
            'selected_events':len(ordinals),'order':'journal_record_ordinal',**expected}
        schema=SCHEMA.with_metadata({**SCHEMA.metadata,PROVENANCE_KEY:_json(report)})
        work={};rows=snapshot._native_records(ids,work)
        def records():
            for row,_ in rows:yield row
        maximum=max(64*1024**2,2*report['decoded_event_accounted_bytes']+8*1024**2)
        if verify:
            with Path(path).open('rb') as file:
                before=os.fstat(file.fileno())
                if before.st_size>maximum:raise ValueError('native event artifact byte limit exceeded')
                if format=='parquet':
                    reader=pq.ParquetFile(file);actual_schema=reader.schema_arrow
                    batches=reader.iter_batches(batch_size=256)
                else:
                    reader=pa.ipc.open_file(file);actual_schema=reader.schema
                    batches=(reader.get_batch(i) for i in range(reader.num_record_batches))
                if not actual_schema.equals(schema,check_metadata=True):raise ValueError('event snapshot export schema/provenance mismatch')
                actual=(row for batch in batches for row in batch.to_pylist());sentinel=object()
                for wanted,got in zip_longest(records(),actual,fillvalue=sentinel):
                    if wanted!=got:raise ValueError('event snapshot export mismatch')
                if msidx._stable(before)!=msidx._stable(os.fstat(file.fileno())):raise ValueError('event snapshot export changed')
                if work!=expected:raise ValueError('native event stream work mismatch')
            return {**report,'verified_events':len(ordinals),'comparison':'all event/link fields and source/query envelope, ordered'}
        with atomic_output(path) as file:
            writer=pq.ParquetWriter(file,schema,compression='zstd',write_statistics=False) if format=='parquet' else pa.ipc.new_file(file,schema)
            with writer:
                for batch in _batches(records(),schema,8*1024**2):
                    writer.write_batch(batch)
                    if file.tell()>maximum:raise ValueError('native event artifact byte limit exceeded')
            if work!=expected:raise ValueError('native event stream work mismatch')
            if file.tell()>maximum:raise ValueError('native event artifact byte limit exceeded')
        return report
    finally:
        if rows is not None:rows.close()
        snapshot._lock.release()
