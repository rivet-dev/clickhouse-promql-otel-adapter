#!/usr/bin/env python3
"""Run the committed Prometheus reference corpus, or explicitly regenerate it."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOTS = ROOT / "tests/conformance"
STATE = ROOT / ".state/conformance"


def run(*command, cwd=ROOT, env=None):
    subprocess.run(command, cwd=cwd, env=env, check=True)


def package(source):
    cases = (source / "cases.jsonl").read_bytes()
    records = [json.loads(line) for line in cases.splitlines()]
    ids = sorted({r["dataset"] for r in records})
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, content in [("cases.jsonl", cases)] + [(f"datasets/{i}.json", (source / f"datasets/{i}.json").read_bytes()) for i in ids]:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(content))
    compressed = gzip.compress(buffer.getvalue(), mtime=0)
    (SNAPSHOTS / "prometheus.tar.gz").write_bytes(compressed)
    manifest = {
        "upstream": json.loads((ROOT / "docs/upstream-pins.json").read_text())["prometheus"],
        "sha256": hashlib.sha256(compressed).hexdigest(),
        "executions": len(records), "datasets": len(ids),
        "fixtures": sorted({r["fixture"] for r in records}),
    }
    (SNAPSHOTS / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Recorded {len(records)} executions, {len(ids)} datasets, {len(compressed)} compressed bytes.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--regenerate", action="store_true", help="run pinned Go reference tests and replace the accepted corpus")
    parser.add_argument("--package-existing", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.package_existing:
        package(args.package_existing)
        return
    if args.regenerate:
        pins = json.loads((ROOT / "docs/upstream-pins.json").read_text())["prometheus"]
        source = ROOT / ".research/prometheus"
        if not source.exists():
            source.parent.mkdir(parents=True, exist_ok=True)
            run("git", "clone", "--depth=1", "--branch", pins["version"], "https://github.com/prometheus/prometheus.git", str(source))
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
        if actual != pins["commit"]:
            raise SystemExit(f"Reference checkout is {actual}; expected {pins['commit']}. Refusing to overwrite it.")
        generated = ROOT / ".state/oracle-generated"
        env = os.environ | {"ORACLE_OUTPUT": str(generated), "PROMETHEUS_SOURCE": str(source)}
        run("go", "test", "-run", "TestGenerate", "-count=1", "-timeout", "20m", "./...", cwd=ROOT / "tools/oracle", env=env)
        package(generated)
        env |= {"PARSER_OUTPUT": str(SNAPSHOTS / "parser.json")}
        run("go", "test", "-run", "TestParserReference", "-count=1", "./...", cwd=ROOT / "tools/oracle", env=env)
        env |= {"API_OUTPUT": str(SNAPSHOTS / "http.json")}
        run("go", "test", "-run", "TestAPIReference", "-count=1", "./...", cwd=ROOT / "tools/oracle", env=env)
    manifest = json.loads((SNAPSHOTS / "manifest.json").read_text())
    data = (SNAPSHOTS / "prometheus.tar.gz").read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["sha256"]:
        raise SystemExit("Reference corpus checksum mismatch")
    STATE.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        archive.extractall(STATE, filter="data")
    run("cargo", "test", "--locked", "--test", "parser_conformance", "--test", "http_conformance")
    run("cargo", "run", "--locked", "--release", "--example", "conformance", "--", str(STATE))


if __name__ == "__main__":
    main()
