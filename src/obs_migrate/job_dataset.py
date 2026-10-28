"""Serial, provenance-bound multi-bucket event exports over existing leaf files.

Complete publication is the final operation. A provisional directory is never a
readable job. Source verification and artifact-integrity reading are distinct.
"""
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat

import pyarrow as pa
import pyarrow.parquet as pq

from . import dataset,event_bucket
from .index_job import MAX_MANIFEST,canonical,identity,manifest,open_member,finish_inputs,member_key
from .event_limits import LARGE_EVENTS

# Job artifacts have an additional I/O bound; leaf readers keep their original
# limits. This deliberately bounds imported files even without trusted sources.
PART_CAPS={'default':2*event_bucket.MAX_DECOMPRESSED_BYTES+8*1024**2,
           'large':2*LARGE_EVENTS.decompressed_bytes+8*1024**2}


@dataclass(frozen=True)
class JobRow:
    source_key:str
    event:dict


def _stamp(value):
    return value.st_dev,value.st_ino,value.st_mode,value.st_size,value.st_mtime_ns,value.st_ctime_ns


def _regular(path,directory=False):
    try:value=path.lstat()
    except OSError:raise ValueError('missing job entry') from None
    if not (stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)):
        raise ValueError('job entries must be regular, without symlinks')
    return value


def _root(output):
    output=Path(output).absolute()
    if any(p.is_symlink() for p in (output,*output.parents)):raise ValueError('job path contains a symlink')
    return output


def _guard_output(plan,output):
    output=_root(output).resolve()
    for spec in plan.inputs:
        source=spec.root.resolve()
        if output.is_relative_to(source) or source.is_relative_to(output):raise ValueError('job output overlaps input')


def _fingerprint(path):
    before=_regular(path);sha=hashlib.sha256();count=0
    if before.st_size>PART_CAPS['large']:raise ValueError('job artifact exceeds supported byte limit')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as stream:
        if _stamp(os.fstat(stream.fileno()))!=_stamp(before):raise ValueError('job part changed before hashing')
        while chunk:=stream.read(1024**2):
            sha.update(chunk);count+=len(chunk)
            if count>before.st_size:raise ValueError('job part grew during hashing')
        if _stamp(os.fstat(stream.fileno()))!=_stamp(before) or _stamp(_regular(path))!=_stamp(before) or count!=before.st_size:
            raise ValueError('job part changed during hashing')
    return {'bytes':count,'sha256':sha.hexdigest()}


def _pairs(items):
    result={}
    for key,value in items:
        if key in result:raise ValueError('duplicate job JSON key')
        result[key]=value
    return result


def _save(output,state):
    if len(canonical(state))+1>MAX_MANIFEST:raise ValueError('job manifest exceeds 4 MiB')
    dataset._save(output,state)


def _digest(value):return type(value) is str and re.fullmatch('[0-9a-f]{64}',value) is not None


def _selection(value):
    if type(value) is not dict or set(value)!={'format','inputs','sources','visibility','resource_profile','allow_duplicate_journals'}:
        raise ValueError('job selection shape mismatch')
    if value['format']!='obs-index-job-selection-1' or value['visibility'] not in ('physical','undeleted') or value['resource_profile'] not in ('default','large') or type(value['allow_duplicate_journals']) is not bool:
        raise ValueError('job selection policy mismatch')
    inputs=value['inputs'];sources=value['sources']
    if type(inputs) is not list or not 1<=len(inputs)<=32 or type(sources) is not list or not 1<=len(sources)<=256:
        raise ValueError('job selection count mismatch')
    by_id={};keys=[]
    for spec in inputs:
        if type(spec) is not dict or set(spec)!={'input_id','kind','selectors','index','identity_sha256','expected_identity_sha256','identity_provenance'}:
            raise ValueError('portable input shape mismatch')
        identifier=spec['input_id']
        if type(identifier) is not str or not re.fullmatch('[a-z][a-z0-9_-]{0,62}',identifier) or identifier in by_id:
            raise ValueError('portable input_id mismatch')
        if spec['kind'] not in ('local','smartstore') or not _digest(spec['identity_sha256']):raise ValueError('portable identity mismatch')
        selectors=spec['selectors']
        if type(selectors) is not list or not selectors or any(type(s) is not str or not s or len(s)>1024 for s in selectors) or selectors!=sorted(set(selectors)):
            raise ValueError('portable selector mismatch')
        expected=spec['expected_identity_sha256']
        if expected is not None and expected!=spec['identity_sha256']:raise ValueError('portable expected identity mismatch')
        if spec['identity_provenance']!=('caller_supplied_expected' if expected else 'observed_authentication_unverified'):
            raise ValueError('portable identity provenance mismatch')
        by_id[identifier]=spec
    if list(by_id)!=sorted(by_id):raise ValueError('portable inputs out of order')
    selected=set()
    for source in sources:
        if type(source) is not dict or set(source)!={'source_key','input_id','kind','selector','identity_sha256','journal_sha256','index','bucket_id'}:
            raise ValueError('portable member shape mismatch')
        spec=by_id.get(source['input_id'])
        if spec is None or source['kind']!=spec['kind'] or source['identity_sha256']!=spec['identity_sha256'] or source['selector'] not in spec['selectors']:
            raise ValueError('portable member selection mismatch')
        if source['source_key']!=member_key(source['input_id'],source['kind'],source['identity_sha256'],source['selector']) or not _digest(source['journal_sha256']):
            raise ValueError('portable source key mismatch')
        selected.add((source['input_id'],source['selector']));keys.append(source['source_key'])
    if keys!=sorted(set(keys)) or selected!={(s['input_id'],selector) for s in inputs for selector in s['selectors']}:
        raise ValueError('portable selected members mismatch')
    return sources


def _part_name(source_key,format):return 'parts/'+source_key+('.parquet' if format=='parquet' else '.arrow')


def _entries(output,state):
    _regular(output,directory=True);_regular(output/'parts',directory=True);_regular(output/'.lock')
    if {p.name for p in output.iterdir()}!={'.lock','parts','manifest.json'}:raise ValueError('foreign job root entry')
    allowed={Path(_part_name(s['source_key'],state['member_format'])).name for s in state['selection']['sources']}
    present=set()
    for path in (output/'parts').iterdir():
        _regular(path)
        if path.name not in allowed:raise ValueError('foreign job part')
        present.add('parts/'+path.name)
    required={e['path'] for e in state['members']}
    if not required<=present or state['complete'] and required!=present:raise ValueError('job member inventory mismatch')


def _load(output):
    output=_root(output);path=output/'manifest.json';before=_regular(path)
    if before.st_size>MAX_MANIFEST:raise ValueError('job manifest exceeds 4 MiB')
    with path.open('rb') as stream:raw=stream.read(MAX_MANIFEST+1)
    if len(raw)>MAX_MANIFEST or _stamp(_regular(path))!=_stamp(before):raise ValueError('job manifest changed')
    try:
        state=json.loads(raw,object_pairs_hook=_pairs)
        if type(state) is not dict or set(state)!={'format','complete','selection_sha256','selection','member_format','members','events','physical_events'}:
            raise ValueError('job manifest shape mismatch')
        if state['format']!='obs-index-job-1' or type(state['complete']) is not bool or state['member_format'] not in ('parquet','arrow'):
            raise ValueError('job format mismatch')
        sources=_selection(state['selection'])
        if state['selection_sha256']!=identity(state['selection']):raise ValueError('job selection hash mismatch')
        members=state['members']
        if type(members) is not list or len(members)>len(sources) or state['complete'] and len(members)!=len(sources):raise ValueError('job member count mismatch')
        for entry,source in zip(members,sources):
            if type(entry) is not dict or set(entry)!={'source_key','path','bytes','sha256','snapshot_sha256','index','bucket_id','events','physical_events','envelope_sha256'}:
                raise ValueError('job member entry mismatch')
            if any(entry[k]!=source[k] for k in ('source_key','index','bucket_id')) or entry['path']!=_part_name(source['source_key'],state['member_format']):
                raise ValueError('job checkpoint is not a prefix of selection')
            if any(type(entry[k]) is not int or entry[k]<0 for k in ('bytes','events','physical_events')) or entry['events']>entry['physical_events'] or entry['bytes']>PART_CAPS[state['selection']['resource_profile']]:
                raise ValueError('job member counters mismatch')
            if any(not _digest(entry[k]) for k in ('sha256','snapshot_sha256','envelope_sha256')):raise ValueError('job member digest mismatch')
        for field in ('events','physical_events'):
            if type(state[field]) is not int or state[field]!=sum(e[field] for e in members):raise ValueError('job total mismatch')
        _entries(output,state)
        return state
    except (KeyError,TypeError,UnicodeError,json.JSONDecodeError):raise ValueError('malformed job manifest') from None


def _match(plan,state):
    if state['selection_sha256']!=plan.selection_sha256 or state['selection']!=manifest(plan):raise ValueError('job selection differs from plan')


def _entry(member,path,report,stamp):
    return {'source_key':member.source_key,'path':path,**stamp,'snapshot_sha256':report['snapshot_sha256'],
            'index':member.index,'bucket_id':member.bucket_id,'events':report['query']['selected_events'],
            'physical_events':report['fully_validated_events'],'envelope_sha256':identity(report)}


def _parts(output,state):
    _entries(output,state)
    for entry in state['members']:
        size=_regular(output/entry['path']).st_size
        if size!=entry['bytes'] or size>PART_CAPS[state['selection']['resource_profile']]:raise ValueError('job member size mismatch or limit exceeded')
        if _fingerprint(output/entry['path'])!={k:entry[k] for k in ('bytes','sha256')}:raise ValueError('job member hash mismatch')


def export_job(plan,output,*,format='parquet',resume=False):
    if format not in ('parquet','arrow') or type(resume) is not bool:raise ValueError('unsupported job export options')
    output=_root(output);_guard_output(plan,output)
    if resume:
        _regular(output,directory=True);_regular(output/'.lock')
    else:
        try:output.mkdir(mode=0o700)
        except FileExistsError:raise ValueError('new job requires absent output directory') from None
    with dataset._lock(output):
        if resume and (output/'manifest.json').exists():
            state=_load(output);_match(plan,state)
            if state['member_format']!=format:raise ValueError('resume format differs from job')
            if state['complete']:
                _verify(plan,output,state)
                return state
        else:
            if resume:
                if {p.name for p in output.iterdir()}!={'.lock','parts'}:raise ValueError('unbound job directory cannot resume')
                _regular(output/'parts',directory=True)
                if any((output/'parts').iterdir()):raise ValueError('members without initial selection manifest cannot resume')
            else:(output/'parts').mkdir()
            state={'format':'obs-index-job-1','complete':False,'selection_sha256':plan.selection_sha256,
                   'selection':manifest(plan),'member_format':format,'members':[],'events':0,'physical_events':0}
            _save(output,state)
        checkpoints={entry['source_key']:entry for entry in state['members']}
        for member in plan.sources:
            relative=_part_name(member.source_key,format)
            with open_member(plan,member) as leaf:
                if (output/relative).exists():
                    if not resume:raise ValueError('job part unexpectedly exists')
                    checked=event_bucket.verify(leaf,output/relative,format)
                    report={k:v for k,v in checked.items() if k not in ('verified_events','comparison')}
                else:report=event_bucket.export(leaf,output/relative,format)
                entry=_entry(member,relative,report,_fingerprint(output/relative))
            saved=checkpoints.get(member.source_key)
            if saved is not None:
                if saved!=entry:raise ValueError('job checkpoint differs from source/output')
            else:
                state['members'].append(entry)
                state['events']+=entry['events'];state['physical_events']+=entry['physical_events']
                _save(output,state)
        finish_inputs(plan);_parts(output,state)
        state['complete']=True;_save(output,state)
        return state


def _verify(plan,output,state):
    _match(plan,state)
    if not state['complete']:raise ValueError('job is incomplete')
    _parts(output,state);total=0
    for member,entry in zip(plan.sources,state['members']):
        with open_member(plan,member) as leaf:
            checked=event_bucket.verify(leaf,output/entry['path'],state['member_format'])
            report={k:v for k,v in checked.items() if k not in ('verified_events','comparison')}
            if _entry(member,entry['path'],report,_fingerprint(output/entry['path']))!=entry:raise ValueError('job member differs from source')
            total+=checked['verified_events']
    finish_inputs(plan);_parts(output,state)
    if _load(output)!=state:raise ValueError('job manifest changed during verification')
    return {**state,'verified_events':total,'comparison':'all sources, ordered event/link fields and full member envelopes'}


def verify_job(plan,output):
    output=_root(output);_guard_output(plan,output);_regular(output,directory=True)
    _regular(output/'.lock')
    with dataset._lock(output):return _verify(plan,output,_load(output))


def read_job(output):
    """Check artifact integrity, not source authenticity; exhaust for final checks."""
    output=_root(output);state=_load(output)
    if not state['complete']:raise ValueError('job is incomplete')
    manifest_stamp=_fingerprint(output/'manifest.json');_parts(output,state)
    for entry in state['members']:
        path=output/entry['path'];initial=_regular(path);count=0;previous=-1
        with path.open('rb') as stream:
            if _stamp(os.fstat(stream.fileno()))!=_stamp(initial):raise ValueError('job member replaced before reading')
            if state['member_format']=='parquet':
                reader=pq.ParquetFile(stream);schema=reader.schema_arrow;batches=reader.iter_batches(batch_size=1024)
            else:
                reader=pa.ipc.open_file(stream);schema=reader.schema;batches=(reader.get_batch(i) for i in range(reader.num_record_batches))
            if schema.remove_metadata()!=event_bucket.SCHEMA.remove_metadata():raise ValueError('job linked schema mismatch')
            try:envelope=json.loads(schema.metadata[event_bucket.PROVENANCE_KEY],object_pairs_hook=_pairs)
            except (TypeError,KeyError,ValueError):raise ValueError('missing/invalid job member provenance') from None
            if identity(envelope)!=entry['envelope_sha256'] or envelope['snapshot_sha256']!=entry['snapshot_sha256'] or envelope['query']['selected_events']!=entry['events'] or envelope['fully_validated_events']!=entry['physical_events']:
                raise ValueError('job member envelope mismatch')
            if envelope['labels']['index']!=entry['index'] or envelope['labels']['bucket_id']!=entry['bucket_id']:raise ValueError('job member label mismatch')
            undeleted=state['selection']['visibility']=='undeleted'
            semantics='all_physical_events'+('_excluding_reconciled_deletes' if undeleted else '')
            query=envelope['query']
            if query.get('term_base64') is not None or query.get('order')!='journal_record_ordinal' or query.get('semantics')!=semantics:
                raise ValueError('job member is not the declared full visibility selection')
            if undeleted:
                selection=envelope.get('mask_selection',{})
                if selection.get('visibility')!='undeleted' or selection.get('selected_events')!=entry['events'] or selection.get('deleted_events_excluded')!=entry['physical_events']-entry['events']:
                    raise ValueError('job member deletion policy mismatch')
            elif 'mask_selection' in envelope or entry['events']!=entry['physical_events']:
                raise ValueError('physical job member cannot exclude events')
            expected_limits=None if state['selection']['resource_profile']=='default' else LARGE_EVENTS.describe()
            if envelope.get('resource_limits')!=expected_limits:raise ValueError('job member resource profile mismatch')
            if envelope.get('backend')!='native_journal':raise ValueError('job member backend mismatch')
            source=next(s for s in state['selection']['sources'] if s['source_key']==entry['source_key'])
            snapshot=envelope['snapshot_manifest']
            if snapshot['files'][snapshot['journal']]['sha256']!=source['journal_sha256']:raise ValueError('job member journal identity mismatch')
            for batch in batches:
                for event in batch.to_pylist():
                    ordinal=event['record_ordinal']
                    if ordinal<=previous or ordinal>=entry['physical_events'] or count>=entry['events']:raise ValueError('job member row order/count mismatch')
                    if event['index']!=entry['index'] or event['bucket_id']!=entry['bucket_id']:raise ValueError('job event label mismatch')
                    if event['source_sha256']!=source['journal_sha256'] or undeleted and event['is_deleted'] is not False:
                        raise ValueError('job event source/visibility mismatch')
                    previous=ordinal;count+=1
                    yield JobRow(entry['source_key'],event)
            if count!=entry['events'] or _stamp(os.fstat(stream.fileno()))!=_stamp(initial) or _stamp(_regular(path))!=_stamp(initial):
                raise ValueError('job member changed or stream incomplete')
        if _fingerprint(path)!={k:entry[k] for k in ('bytes','sha256')}:raise ValueError('job member changed after reading')
    if _fingerprint(output/'manifest.json')!=manifest_stamp or _load(output)!=state:raise ValueError('job manifest changed during read')
    _parts(output,state)
