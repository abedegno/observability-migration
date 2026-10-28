#!/bin/sh
# Source-only decoder setup: no upstream Rust extension or compiler build.
set -eu
cd "$(dirname "$0")/.."
if [ -x .venv/bin/python ]; then
    .venv/bin/python -I scripts/check_bootstrap_environment.py
fi
PIN=d2885c63b36f115189f810eb0f43a888cc73c967
if [ ! -d upstream/splunk_ddss_extractor/.git ]; then
    git -c core.autocrlf=false clone --no-checkout https://github.com/ponquersohn/splunk_ddss_extractor.git upstream/splunk_ddss_extractor
    git -C upstream/splunk_ddss_extractor -c core.autocrlf=false checkout --detach "$PIN"
fi
[ "$(git -C upstream/splunk_ddss_extractor rev-parse HEAD)" = "$PIN" ] || {
    echo "Upstream checkout must be at $PIN; refusing to change an existing checkout" >&2; exit 1;
}
if [ ! -x .venv/bin/python ]; then uv venv --python 3.13 .venv; fi
uv pip install --python .venv/bin/python -r requirements-python.lock
uv pip install --python .venv/bin/python --no-deps --no-build-isolation -e .
.venv/bin/obsmigrate setup-decoder --source "$PWD/upstream/splunk_ddss_extractor/src/splunk_ddss_extractor"
.venv/bin/obsmigrate doctor
