"""Load only the exact audited upstream Python modules, without its __init__.

The upstream package initializer unconditionally imports its Rust extension.
Our adapter needs only its Python decoder and stream. Verify both complete byte
snapshots before executing either, import under a private package namespace,
and never copy upstream source into this distribution.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import threading
import types
from importlib.machinery import ModuleSpec

PIN='d2885c63b36f115189f810eb0f43a888cc73c967'
HASHES={'decoder.py':'d0a7470e14a546b33a17de77dfe22c9493d4b4d56a6132ba3fd7a708220c2749',
        'stream.py':'43395e66cbf9fbf066e245ef3399653c3c2682bd3f5026161281b2d4a5440af1'}
CONFIG_NAME='obs-migrate-decoder.json'
_lock=threading.Lock();_cache={}


def config_path():return Path(sys.prefix)/CONFIG_NAME


def locate():
    config=config_path()
    if config.is_symlink():raise ValueError('invalid decoder source configuration')
    if config.exists():
        if config.is_symlink() or not config.is_file() or config.stat().st_size>16384:raise ValueError('invalid decoder source configuration')
        row=json.loads(config.read_text())
        if not isinstance(row,dict) or set(row)!={'decoder_source_schema','source','pin'} or type(row['decoder_source_schema']) is not int or row['decoder_source_schema']!=1 or row['pin']!=PIN:
            raise ValueError('decoder source configuration differs from audited pin')
        if not isinstance(row['source'],str) or not Path(row['source']).is_absolute():raise ValueError('decoder source path must be absolute')
        return Path(row['source']).resolve(strict=True)
    # Finding a top-level spec does not execute that package's initializer.
    spec=importlib.util.find_spec('splunk_ddss_extractor')
    paths=() if spec is None else tuple(spec.submodule_search_locations or ())
    if len(paths)!=1:raise ImportError('audited upstream Python source unavailable; configure a pinned source checkout')
    return Path(paths[0]).resolve(strict=True)


def verified_bytes(source):
    source=Path(source).resolve(strict=True)
    if not source.is_dir():raise ValueError('decoder source must be a directory')
    result={}
    for name,expected in HASHES.items():
        path=source/name
        if path.is_symlink() or not path.is_file() or path.stat().st_size>1024*1024:raise ValueError('decoder source requires bounded regular Python files')
        wire=path.read_bytes()
        if hashlib.sha256(wire).hexdigest()!=expected:raise ValueError('upstream decoder source differs from audited pin')
        result[name]=wire
    return source,result


def load():
    source,wire=verified_bytes(locate())
    with _lock:
        if source in _cache:return _cache[source]
        if len(_cache)>=8:raise ValueError('audited decoder source cache path limit exceeded')
        namespace='_obs_migrate_ddss_'+hashlib.sha256(str(source).encode()).hexdigest()[:16]
        package=types.ModuleType(namespace);package.__path__=[]
        package.__package__=namespace;package.__spec__=ModuleSpec(namespace,loader=None,is_package=True)
        created=[]
        try:
            if namespace in sys.modules:raise ValueError('private audited decoder namespace already occupied')
            sys.modules[namespace]=package;created.append(namespace)
            # The decoder's only relative dependency is the audited stream.
            # Compile the verified snapshots, never re-read mutable source for exec.
            for name in ('stream.py','decoder.py'):
                key=namespace+'.'+name[:-3];module=types.ModuleType(key)
                module.__file__=str(source/name);module.__package__=namespace
                module.__spec__=ModuleSpec(key,loader=None,origin=module.__file__)
                sys.modules[key]=module;created.append(key)
                exec(compile(wire[name],module.__file__,'exec'),module.__dict__)
            result=(sys.modules[namespace+'.decoder'].JournalDecoder,sys.modules[namespace+'.stream'].JournalStream)
            _cache[source]=result;return result
        except BaseException:
            for name in reversed(created):sys.modules.pop(name,None)
            raise


def describe():
    source,_=verified_bytes(locate())
    return {'decoder_source_schema':1,'source':str(source),'pin':PIN,'files_sha256':dict(HASHES),
        'mode':'verified_original_Python_modules;upstream_initializer_not_executed',
        'upstream_rust_extension_required':False,'source_bundled_in_toolkit':False}


def configure(source):
    if sys.prefix==sys.base_prefix:raise ValueError('configure the decoder inside a virtual environment')
    source,_=verified_bytes(source);path=config_path()
    row={'decoder_source_schema':1,'source':str(source),'pin':PIN};encoded=json.dumps(row,indent=2)+'\n'
    if path.exists():
        if path.is_symlink() or path.read_text()!=encoded:raise ValueError('existing decoder source configuration differs; refusing overwrite')
        return row
    # The configuration contains a path and pin only, never copied decoder code.
    with path.open('x') as file:file.write(encoded)
    return row
