#!/usr/bin/env python3
"""Run the real collector; check snapshots by default, replace only with --update.

Each invocation owns a random ClickHouse database and collector container. No
handwritten schema is used: SHOW CREATE TABLE is captured after collector startup.
"""
import argparse
import difflib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from datasets import dataset

ROOT = Path(__file__).resolve().parent.parent
TABLES = ["otel_metrics_" + kind for kind in ("gauge", "sum", "histogram", "exponential_histogram", "summary")]


def query(sql):
    req = urllib.request.Request("http://127.0.0.1:18123/", data=sql.encode(),
                                 headers={"X-ClickHouse-User": "default", "X-ClickHouse-Key": "local-test-only"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode()


def snapshot(name, value, update):
    path = ROOT / "tests" / "snapshots" / name
    if update:
        path.write_text(value)
        print("updated", path.relative_to(ROOT))
    else:
        old = path.read_text() if path.exists() else ""
        if old != value:
            diff = "".join(difflib.unified_diff(old.splitlines(True), value.splitlines(True), str(path), "actual"))
            raise AssertionError("Snapshot differs; review then run --update:\n" + diff)
        print("matched", path.relative_to(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--precision-schema", action="store_true", help="precreate a DateTime64(9) variant and verify the unmodified collector against it")
    args = parser.parse_args()
    suffix = uuid.uuid4().hex
    database = "snapshot_" + suffix
    container = "ch-promql-snapshot-" + suffix
    config = f"""receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  clickhouse:
    endpoint: tcp://clickhouse:9000
    username: default
    password: local-test-only
    database: {database}
    create_schema: {str(not args.precision_schema).lower()}
    async_insert: false
    sending_queue:
      enabled: false
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [clickhouse]
"""
    if args.precision_schema:
        query(f"CREATE DATABASE {database}")
        ddl = (ROOT / "tests/snapshots/collector-schema.sql").read_text().replace("{{database}}", database).replace("DateTime", "DateTime64(9)")
        for statement in ddl.split(";"):
            if statement.strip():
                query(statement)
    with tempfile.TemporaryDirectory(prefix="ch-promql-") as temp:
        path = Path(temp) / "collector.yaml"
        path.write_text(config)
        os.chmod(temp, 0o755)
        try:
            subprocess.run(["docker", "run", "-d", "--name", container, "--network", "clickhouse-promql_default",
                            "-p", "127.0.0.1::4318", "-v", f"{path}:/etc/otelcol-contrib/config.yaml:ro",
                            "otel/opentelemetry-collector-contrib:0.162.0"], check=True, capture_output=True)
            port = subprocess.check_output(["docker", "port", container, "4318/tcp"], text=True).strip().split(":")[-1]
            payload = json.dumps(dataset()).encode()
            for attempt in range(60):
                try:
                    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/metrics", data=payload,
                                                 headers={"Content-Type": "application/json"})
                    with urllib.request.urlopen(req, timeout=20) as response:
                        result = json.load(response)
                        if result.get("partialSuccess", {}).get("rejectedDataPoints", "0") not in (0, "0"):
                            raise RuntimeError(result)
                    break
                except (urllib.error.URLError, ConnectionError):
                    if attempt == 59:
                        raise
                    time.sleep(0.5)
            names = query(f"SHOW TABLES FROM {database} FORMAT TabSeparated").splitlines()
            if len(names) != 5:
                raise AssertionError(f"Expected 5 metric tables, got {names}")
            schema = []
            rows = {}
            for table in sorted(names):
                ddl = query(f"SHOW CREATE TABLE {database}.{table} FORMAT TabSeparatedRaw").replace(database, "{{database}}")
                schema.append(ddl.strip()+";\n")
                raw = query(f"SELECT * FROM {database}.{table} FORMAT JSONEachRow")
                records = [json.loads(line) for line in raw.splitlines()]
                if not records:
                    raise AssertionError(f"Collector produced no rows for {table}")
                rows[table] = sorted(records, key=lambda r: json.dumps(r, sort_keys=True))
            suffix = "-precise" if args.precision_schema else ""
            snapshot(f"collector-schema{suffix}.sql", "\n".join(schema), args.update)
            snapshot(f"collector-rows{suffix}.json", json.dumps(rows, sort_keys=True, indent=2)+"\n", args.update)
            snapshot("otlp-dataset.json", json.dumps(dataset(), sort_keys=True, indent=2)+"\n", args.update)
        except Exception as error:
            if isinstance(error, subprocess.CalledProcessError) and error.stderr:
                print(error.stderr.decode() if isinstance(error.stderr, bytes) else error.stderr)
            subprocess.run(["docker", "logs", "--tail", "25", container], check=False)
            raise
        finally:
            subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True)
            query(f"DROP DATABASE IF EXISTS {database} SYNC")

if __name__ == "__main__":
    main()
