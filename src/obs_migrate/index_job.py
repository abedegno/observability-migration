"""Explicit, portable offline event selections over existing bucket readers.

Planning binds inventories, not format support or native search visibility.
Private roots never enter the portable selection. Each reader is short lived.
"""
from contextlib import contextmanager
from dataclasses import dataclass,field,asdict,replace
import hashlib
import json
from pathlib import Path
import re

from .bucket_catalogue import catalogue,select
from .event_bucket import EventBucket
from .event_limits import LARGE_EVENTS
from .smartstore import AcquiredObjects
from .source_identity import normalize_sha256
from .capture import _read

MAX_INPUTS=32
MAX_MEMBERS=256
MAX_MANIFEST=4*1024**2


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode('ascii')


def identity(value):return hashlib.sha256(canonical(value)).hexdigest()


def member_key(input_id,kind,identity_sha256,selector):
    return identity(dict(input_id=input_id,kind=kind,identity_sha256=identity_sha256,selector=selector))


@dataclass(frozen=True)
class InputSpec:
    input_id:str
    kind:str
    root:Path=field(repr=False)
    selectors:tuple[str,...]
    index:str|None=None
    expected_identity_sha256:str|None=None


@dataclass(frozen=True)
class ResolvedSource:
    source_key:str
    input_id:str
    kind:str
    selector:str
    identity_sha256:str
    journal_sha256:str
    index:str|None
    bucket_id:str|None


@dataclass(frozen=True)
class JobPlan:
    inputs:tuple[InputSpec,...]=field(repr=False)
    sources:tuple[ResolvedSource,...]
    visibility:str
    resource_profile:str
    allow_duplicate_journals:bool
    selection_sha256:str


def _input(spec):
    if not isinstance(spec,InputSpec) or type(spec.input_id) is not str or not re.fullmatch(r'[a-z][a-z0-9_-]{0,62}',spec.input_id):
        raise ValueError('input requires a unique ASCII input_id')
    if spec.kind not in ('local','smartstore'):raise ValueError('input kind must be local or smartstore')
    if type(spec.selectors) is not tuple or not spec.selectors or any(type(s) is not str or not s or len(s)>1024 or any(c in s for c in ('/','\\','\x00','\n','\r')) for s in spec.selectors):
        raise ValueError('input requires explicit bounded selectors')
    if len(spec.selectors)!=len(set(spec.selectors)):raise ValueError('duplicate selector')
    if spec.index is not None and (type(spec.index) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,255}',spec.index)):
        raise ValueError('invalid local index label')
    if spec.kind=='smartstore' and spec.index is not None:raise ValueError('SmartStore native index label cannot be overridden')
    if not isinstance(spec.root,(Path,str)) or '://' in str(spec.root) or '\x00' in str(spec.root):raise ValueError('input root must be a local path')
    return replace(spec,root=Path(spec.root).absolute(),selectors=tuple(sorted(spec.selectors)),expected_identity_sha256=normalize_sha256(spec.expected_identity_sha256))


def _resolve(spec,expected=None):
    """Inventory one root, returning metadata only; do not retain object bytes."""
    try:
        if spec.kind=='local':
            report=catalogue(spec.root,offline_snapshot=True,expected_sha256=expected or spec.expected_identity_sha256)
            select(report,spec.selectors,allow_duplicate_journals=True)
            sha=report['inventory_sha256'];by_name={b['name']:b for b in report['buckets']}
            entries=[]
            for selector in spec.selectors:
                bucket=by_name[selector];native=(bucket['manifest_row'] or {}).get('id') or None
                entries.append((selector,bucket['journals'][0]['sha256'],spec.index,native))
        else:
            acquired=AcquiredObjects(spec.root,offline_snapshot=True,expected_inventory_sha256=expected or spec.expected_identity_sha256)
            report=acquired.report();sha=acquired.inventory_sha256;by_id={b['id']:b for b in report['buckets']}
            if any(s not in by_id for s in spec.selectors):raise ValueError('unknown selected native bucket')
            entries=[(s,hashlib.sha256(acquired._objects[by_id[s]['journal_key']]).hexdigest(),by_id[s]['index'],s) for s in spec.selectors]
        return tuple(ResolvedSource(member_key(spec.input_id,spec.kind,sha,s),spec.input_id,spec.kind,s,sha,journal,index,bucket)
                     for s,journal,index,bucket in entries)
    except (OSError,ValueError,RuntimeError) as error:
        raise ValueError('input '+spec.input_id+' inventory validation failed ('+type(error).__name__+')') from None


def manifest(plan):
    inputs=[]
    for spec in plan.inputs:
        actual=next(s.identity_sha256 for s in plan.sources if s.input_id==spec.input_id)
        inputs.append({'input_id':spec.input_id,'kind':spec.kind,'selectors':list(spec.selectors),
                       'index':spec.index,'identity_sha256':actual,'expected_identity_sha256':spec.expected_identity_sha256,
                       'identity_provenance':'caller_supplied_expected' if spec.expected_identity_sha256 else 'observed_authentication_unverified'})
    return {'format':'obs-index-job-selection-1','inputs':inputs,'sources':[asdict(s) for s in plan.sources],
            'visibility':plan.visibility,'resource_profile':plan.resource_profile,'allow_duplicate_journals':plan.allow_duplicate_journals}


def plan_job(inputs,*,offline_snapshot=False,visibility='physical',resource_profile='default',allow_duplicate_journals=False):
    if offline_snapshot is not True:raise ValueError('job requires offline_snapshot assertion')
    if type(visibility) is not str or visibility not in ('physical','undeleted'):raise ValueError('unsupported job visibility')
    if type(resource_profile) is not str or resource_profile not in ('default','large'):raise ValueError('unsupported job resource profile')
    if type(allow_duplicate_journals) is not bool:raise ValueError('duplicate override must be boolean')
    if type(inputs) not in (tuple,list) or not 1<=len(inputs)<=MAX_INPUTS:raise ValueError('job requires 1..32 input roots')
    specs=tuple(sorted((_input(s) for s in inputs),key=lambda s:s.input_id))
    if len({s.input_id for s in specs})!=len(specs):raise ValueError('duplicate input_id')
    if sum(len(s.selectors) for s in specs)>MAX_MEMBERS:raise ValueError('job exceeds 256 members')
    sources=tuple(sorted((m for spec in specs for m in _resolve(spec)),key=lambda s:s.source_key))
    if len({s.source_key for s in sources})!=len(sources):raise ValueError('duplicate source key')
    if not allow_duplicate_journals and len({s.journal_sha256 for s in sources})!=len(sources):
        raise ValueError('duplicate original journal bytes require explicit override')
    result=JobPlan(specs,sources,visibility,resource_profile,allow_duplicate_journals,'')
    portable=manifest(result)
    if len(canonical(portable))>MAX_MANIFEST:raise ValueError('job selection exceeds 4 MiB')
    return replace(result,selection_sha256=identity(portable))


def load_spec(path):
    """Decode private operator JSON only; relative roots refer to the spec folder."""
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('duplicate job config key')
            result[key]=value
        return result
    try:
        path=Path(path).absolute();config=json.loads(_read(path,MAX_MANIFEST),object_pairs_hook=pairs)
    except (OSError,UnicodeError,json.JSONDecodeError):raise ValueError('job config could not be read as bounded regular JSON') from None
    if type(config) is not dict or not {'spec_version','inputs'}<=config.keys() or config.keys()-{'spec_version','inputs','visibility','resource_profile','allow_duplicate_journals'}:
        raise ValueError('job config fields mismatch')
    if type(config['spec_version']) is not int or config['spec_version']!=1:raise ValueError('unsupported job spec_version')
    inputs=config['inputs']
    if type(inputs) is not list or not 1<=len(inputs)<=MAX_INPUTS:raise ValueError('job config requires 1..32 inputs')
    specs=[]
    for value in inputs:
        if type(value) is not dict or not {'input_id','kind','root','selectors'}<=value.keys() or value.keys()-{'input_id','kind','root','selectors','index','expected_identity_sha256'}:
            raise ValueError('job config input fields mismatch')
        root=value['root'];selectors=value['selectors']
        if type(root) is not str or not root or '://' in root or any(c in root for c in ('\x00','\n','\r','$','`',';','|','&')):
            raise ValueError('job config root must be a literal local path')
        if type(selectors) is not list:raise ValueError('job config selectors must be a list')
        specs.append(_input(InputSpec(value['input_id'],value['kind'],path.parent/root,tuple(selectors),
                                     value.get('index'),value.get('expected_identity_sha256'))))
    visibility=config.get('visibility','physical');profile=config.get('resource_profile','default');override=config.get('allow_duplicate_journals',False)
    if type(visibility) is not str or visibility not in ('physical','undeleted') or type(profile) is not str or profile not in ('default','large') or type(override) is not bool:
        raise ValueError('job config policy/profile types mismatch')
    return {'inputs':tuple(specs),'visibility':visibility,'resource_profile':profile,'allow_duplicate_journals':override}


def finish_inputs(plan):
    for spec in plan.inputs:
        expected=tuple(sorted((s for s in plan.sources if s.input_id==spec.input_id),key=lambda s:s.selector))
        if _resolve(spec,expected[0].identity_sha256)!=expected:raise ValueError('input '+spec.input_id+' changed selection')


@contextmanager
def open_member(plan,member):
    if member not in plan.sources or identity(manifest(plan))!=plan.selection_sha256:raise ValueError('member/plan identity mismatch')
    spec=next(s for s in plan.inputs if s.input_id==member.input_id)
    # Whole original input must remain available, even if this leaf was pinned.
    fresh=_resolve(spec,member.identity_sha256)
    if member not in fresh:raise ValueError('input '+spec.input_id+' changed member identity')
    limits=None if plan.resource_profile=='default' else LARGE_EVENTS
    try:
        if spec.kind=='smartstore':
            acquired=AcquiredObjects(spec.root,offline_snapshot=True,expected_inventory_sha256=member.identity_sha256)
            with acquired.event_bucket(member.selector,experimental=True,backend='journal',visibility=plan.visibility,limits=limits) as leaf:
                yield leaf
        else:
            report=catalogue(spec.root,offline_snapshot=True,expected_sha256=member.identity_sha256)
            with EventBucket(spec.root/member.selector,experimental=True,offline_snapshot=True,include_inactive=True,
                             index=member.index,bucket_id=member.bucket_id,backend='journal',visibility=plan.visibility,limits=limits) as leaf:
                observed=leaf.report()['snapshot_manifest'];prefix=member.selector+'/'
                expected={i['path'][len(prefix):]:{'bytes':i['bytes'],'sha256':i['sha256']} for i in report['inventory']
                          if i['kind']=='file' and i['path'].startswith(prefix)}
                parent=next((i['sha256'] for i in report['inventory'] if i['path']=='.bucketManifest'),None)
                if observed['files']!=expected or observed['parent_manifest_sha256']!=parent:raise ValueError('leaf differs from planned catalogue')
                yield leaf
    except (OSError,ValueError,RuntimeError) as error:
        raise ValueError('input '+spec.input_id+' member validation failed ('+type(error).__name__+')') from None
