"""Build the optional original Rust provider and install into the active venv.

The base toolkit stays pure Python. Cargo.lock fixes all Rust dependencies. Use
CARGO_HOME/CARGO_TARGET_DIR for cache locations and --offline after first fetch.
"""
import argparse
import importlib.machinery
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig

ROOT=Path(__file__).resolve().parents[1]


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--offline',action='store_true')
    parser.add_argument('--release',action='store_true');args=parser.parse_args()
    if sys.prefix==sys.base_prefix:parser.error('activate a virtual environment before installing the optional extension')
    manifest=ROOT/'native/obs-native/Cargo.toml';env=dict(os.environ,PYO3_PYTHON=sys.executable)
    command=['cargo','build','--locked','--manifest-path',str(manifest),'-j','4']
    if args.offline:command+=['--offline']
    if args.release:command+=['--release']
    subprocess.run(command,env=env,check=True)
    target=Path(env.get('CARGO_TARGET_DIR',manifest.parent/'target'))/('release' if args.release else 'debug')
    name='lib_native_provider.dylib' if sys.platform=='darwin' else '_native_provider.dll' if sys.platform=='win32' else 'lib_native_provider.so'
    output=Path(sysconfig.get_path('platlib'))/('_native_provider'+importlib.machinery.EXTENSION_SUFFIXES[0])
    if output.exists():
        # This process has not imported the extension; no loaded library is
        # replaced in-place. Atomic rename also leaves other processes pinned.
        temporary=output.with_name(output.name+'.new');shutil.copyfile(target/name,temporary);temporary.replace(output)
    else:shutil.copyfile(target/name,output)
    print(output)


if __name__=='__main__':main()
