"""The optional resource gate consumes canonical Parquet, never invented journals."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from obs_migrate.export import export

SCRIPT = Path(__file__).resolve().parents[1]/'scripts/benchmark_parquet_read.py'


@pytest.mark.parametrize('gate', ['pass', 'count', 'digest', 'memory'])
def test_optional_parquet_resource_gate(tmp_path, row, gate):
    rows = [dict(row, record_ordinal=i, raw=bytes([i,255]), record_wire=bytes([0,i])) for i in range(3)]
    path = tmp_path/'events.parquet'
    export(iter(rows), path, max_rows=1)
    digest = hashlib.sha256()
    for item in rows:
        for key in ('raw','record_wire'):
            value = item[key]
            digest.update(len(value).to_bytes(8,'little'))
            digest.update(value)
    args = [sys.executable, str(SCRIPT), str(path), '--expected-events', '2' if gate=='count' else '3',
            '--expected-raw-wire-sha256', '0'*64 if gate=='digest' else digest.hexdigest()]
    if gate=='memory':
        args += ['--max-rss-mib', '0.001']
    result = subprocess.run(args, capture_output=True, text=True, cwd=tmp_path)
    assert result.stdout, result.stderr
    report = json.loads(result.stdout)
    assert report['events'] == 3
    assert report['raw_wire_sha256'] == digest.hexdigest()
    assert report['peak_rss_mib'] > 0
    assert result.returncode == (0 if gate=='pass' else 1)
    assert report['status'] == ('PASS' if gate=='pass' else 'FAIL')
    assert report['failed_gates'] == ([] if gate=='pass' else [gate])
