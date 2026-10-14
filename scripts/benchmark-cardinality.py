#!/usr/bin/env python3
"""Compare bounded high-cardinality queries against an isolated local ClickHouse database."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--series', type=int, default=100_000)
    parser.add_argument('--samples', type=int, default=30)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--require-success', action='store_true',
                        help='Validate selective results; broad query may hit its resource limit')
    args = parser.parse_args()
    if not 1 <= args.series <= 1_000_000 or not 2 <= args.samples <= 120:
        parser.error('series must be 1..1000000 and samples 2..120')
    database = 'bench_cardinality_' + uuid.uuid4().hex
    # Deliberately fixed to the repository's local test service, never cloud infrastructure.
    clickhouse = 'http://127.0.0.1:18123/'
    auth = base64.b64encode(b'default:local-test-only').decode()

    def sql(query):
        req = urllib.request.Request(clickhouse, data=query.encode(),
                                     headers={'Authorization': 'Basic ' + auth})
        with urllib.request.urlopen(req, timeout=120) as response:
            return response.read().decode()

    report = {'series': args.series, 'samples_per_series': args.samples,
              'rows': args.series * args.samples, 'binary': str(args.binary.resolve()),
              'queries': []}
    process = None
    try:
        sql(f'CREATE DATABASE {database}')
        schema = (ROOT / 'tests/snapshots/collector-schema.sql').read_text()
        for ddl in schema.replace('{{database}}', database).split(';'):
            if ddl.strip():
                sql(ddl)
        sql(f'''INSERT INTO {database}.otel_metrics_gauge
            (MetricName, ScopeName, Attributes, TimeUnix, Value)
            SELECT 'bench_gauge',
              'github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver',
              map('series_id', concat('series_',toString(intDiv(number,{args.samples}))),
                  'tenant', concat('tenant_',toString(intDiv(number,{args.samples}) % 1000))),
              toDateTime(1700000000 + (number % {args.samples}) * 15),
              toFloat64(number % {args.samples})
            FROM numbers({args.series * args.samples})''')
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        env = {**os.environ, 'CLICKHOUSE_URL': clickhouse, 'CLICKHOUSE_DATABASE': database,
               'CLICKHOUSE_USER': 'default', 'CLICKHOUSE_PASSWORD': 'local-test-only'}
        process = subprocess.Popen([str(args.binary.resolve()), '--host', '127.0.0.1',
                                    '--port', str(port)], env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        base = f'http://127.0.0.1:{port}'
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError('Adapter exited before readiness')
            try:
                with urllib.request.urlopen(base + '/-/ready', timeout=1):
                    break
            except (urllib.error.URLError, TimeoutError):
                time.sleep(.1)
        else:
            raise RuntimeError('Adapter readiness timed out')

        end = 1700000000 + (args.samples - 1) * 15
        def request(name, route, params):
            started = time.monotonic()
            req = urllib.request.Request(base + route, data=urllib.parse.urlencode(params).encode(),
                                         headers={'Content-Type': 'application/x-www-form-urlencoded'})
            try:
                with urllib.request.urlopen(req, timeout=125) as response:
                    body = response.read()
                    status = response.status
            except urllib.error.HTTPError as error:
                body, status = error.read(), error.code
            result = json.loads(body)
            record = {'name': name, 'seconds': round(time.monotonic() - started, 4),
                      'http_status': status, 'response_bytes': len(body)}
            if result.get('status') == 'success':
                data = result['data']
                record['results'] = len(data['result']) if isinstance(data, dict) else len(data)
                if isinstance(data, dict) and data['result']:
                    first = data['result'][0]
                    record['last_value'] = first.get('value', first.get('values', [None])[-1])
            else:
                record['error'] = result.get('error', '')[:200]
            return record

        selective = {'query': 'sum(bench_gauge{tenant="tenant_42"})', 'time': end}
        for name, route, params in [
            ('selective_cold', '/api/v1/query', selective),
            ('selective_warm', '/api/v1/query', selective),
            ('selective_regex', '/api/v1/query', {
                'query': 'sum(bench_gauge{tenant=~"tenant_42"})', 'time': end}),
            ('absent_label', '/api/v1/query', {'query': 'bench_gauge{tenant="absent"}', 'time': end}),
            ('selective_range', '/api/v1/query_range', {
                'query': 'sum(bench_gauge{tenant="tenant_42"})',
                'start': 1700000000, 'end': end, 'step': 15}),
            ('label_values', '/api/v1/label/tenant/values', {
                'match[]': 'bench_gauge{tenant="tenant_42"}', 'start': 1700000000, 'end': end}),
            ('broad_bounded', '/api/v1/query', {'query': 'sum(bench_gauge)', 'time': end}),
        ]:
            record = request(name, route, params)
            report['queries'].append(record)
            print(json.dumps(record), flush=True)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=8) as pool:
            records = list(pool.map(lambda n: request(f'concurrent_{n}', '/api/v1/query', selective), range(8)))
        report['concurrent_seconds'] = round(time.monotonic() - started, 4)
        report['concurrent'] = records
        status_text = Path(f'/proc/{process.pid}/status').read_text()
        report['peak_rss_kib'] = int(next(line.split()[1] for line in status_text.splitlines() if line.startswith('VmHWM:')))
        report['expected_selective_value'] = sum(1 for i in range(args.series) if i % 1000 == 42) * (args.samples - 1)
        if args.require_success:
            expected = report['expected_selective_value']
            for record in report['queries'] + records:
                if record['name'] == 'broad_bounded':
                    continue
                assert record['http_status'] == 200, record
                if record['name'] == 'absent_label':
                    assert record['results'] == 0, record
                elif record['name'] == 'label_values':
                    assert record['results'] == int(expected > 0), record
                elif expected:
                    assert record['results'] == 1, record
                    assert float(record['last_value'][1]) == expected, record
                else:
                    assert record['results'] == 0, record
            report['validation'] = 'passed'
        print(json.dumps({'peak_rss_kib': report['peak_rss_kib'],
                          'concurrent_seconds': report['concurrent_seconds']}), flush=True)
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        sql(f'DROP DATABASE IF EXISTS {database} SYNC')
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
