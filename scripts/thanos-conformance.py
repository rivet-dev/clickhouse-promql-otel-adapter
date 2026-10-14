#!/usr/bin/env python3
"""Verify Rust dedup against pinned Thanos; --regenerate replaces the golden corpus."""
import argparse
import gzip
import json
import os
from pathlib import Path
import shutil
import subprocess

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--regenerate', action='store_true')
args = parser.parse_args()
if args.regenerate:
    commit = json.loads((root / 'docs/upstream-pins.json').read_text())['thanos']['commit']
    source = root / '.research/thanos'
    if not source.exists():
        subprocess.run(['git', 'clone', 'https://github.com/thanos-io/thanos.git', str(source)], check=True)
        subprocess.run(['git', 'checkout', commit], cwd=source, check=True)
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip()
    if actual != commit:
        raise SystemExit(f'Expected pinned Thanos {commit}; found {actual}')
    fixture = source / 'pkg/dedup/adapter_reference_test.go'
    output = root / '.state/thanos-dedup.json'
    output.parent.mkdir(exist_ok=True)
    shutil.copyfile(root / 'tools/thanos/reference_test.go', fixture)
    try:
        subprocess.run(['go', 'test', '-run', 'Test(AdapterReference|DedupSeriesIterator|DedupSeriesSet)', '-count=1', '-timeout', '5m', './pkg/dedup'], cwd=source, env=os.environ | {'THANOS_REFERENCE_OUTPUT': str(output)}, check=True)
        (root / 'tests/conformance/thanos-dedup.json.gz').write_bytes(gzip.compress(output.read_bytes(), mtime=0))
    finally:
        fixture.unlink(missing_ok=True)
subprocess.run(['cargo', 'test', '--locked', '--lib', 'pinned_thanos_dedup_reference'], cwd=root, check=True)
