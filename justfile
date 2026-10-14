set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments := true

host_arch := if arch() == "x86_64" { "amd64" } else if arch() == "aarch64" { "arm64" } else { arch() }

default:
    @just --list

# Install the pinned Rust toolchain and check components.
setup:
    rustup toolchain install 1.97.0 --profile minimal --component rustfmt,clippy

# Rebuild committed parser source from the pinned archive and versioned patch.
vendor-regenerate *args:
    python3 vendor/promql-parser/regenerate.py "$@"

# Fail if generated parser source differs from its archive plus patch.
vendor-check *args:
    python3 vendor/promql-parser/regenerate.py --check "$@"

# Check offline regeneration, drift detection, and preservation on failure.
test-vendor:
    python3 -m unittest discover -s tests -p test_vendor.py

build:
    cargo build --locked --release

# Start the adapter; ClickHouse connection settings come from the environment.
run *args: build
    target/release/clickhouse-promql --host 0.0.0.0 "$@"

fmt:
    just --fmt --unstable
    cargo fmt --all

fmt-check:
    just --fmt --check --unstable
    cargo fmt --all -- --check

lint:
    cargo clippy --locked --all-targets -- -D warnings

check: fmt-check lint

# Start the real test ClickHouse on port 18123.
clickhouse-up:
    docker compose up -d --wait

# Stop ClickHouse; pass -v explicitly to delete its test data.
clickhouse-down *args:
    docker compose down "$@"

# Run Rust unit and integration tests, including isolated ClickHouse databases.
test: clickhouse-up
    cargo test --locked

# Run the patched parser's retained upstream tests.
test-parser:
    CARGO_TARGET_DIR="$PWD/target/parser" cargo test --manifest-path vendor/promql-parser/out/Cargo.toml --all-features

# Replay reference datasets through the real ClickHouse storage path.
test-storage: clickhouse-up
    cargo test --locked --test storage_conformance

# Run Prometheus references; pass --regenerate to refresh them using Go.
conformance *args: clickhouse-up
    python3 scripts/conformance.py "$@"

# Run Thanos dedup references; pass --regenerate to refresh them using Go.
dedup *args:
    python3 scripts/thanos-conformance.py "$@"

# Emit deterministic fake OTLP metrics as JSON.
dataset:
    python3 scripts/datasets.py

# Exercise selective queries over an isolated local high-cardinality dataset.
benchmark-cardinality *args: build clickhouse-up
    python3 scripts/benchmark-cardinality.py --binary target/release/clickhouse-promql "$@"

# Check real Collector schema/row snapshots; pass --update to accept changes.
snapshots *args: clickhouse-up
    python3 scripts/generate-snapshots.py "$@"

# Check the opt-in timestamp precision schema against the real Collector.
snapshots-precise *args: clickhouse-up
    python3 scripts/generate-snapshots.py --precision-schema "$@"

# All release validation, also runnable locally.
ci: vendor-check test-vendor check test test-parser conformance snapshots snapshots-precise

# Demo terminal 1: expose fake Prometheus metrics on port 18080.
demo-metrics: clickhouse-up
    python3 demo/metrics.py --host 0.0.0.0

# Demo terminal 2: start the adapter against the demo database.
demo-adapter: build
    CLICKHOUSE_PASSWORD=local-test-only target/release/clickhouse-promql --host 0.0.0.0 --database promql_demo

# Demo terminal 3: start the Collector and Grafana (port 13000).
demo-up: clickhouse-up
    docker compose -f compose.yaml -f demo/compose.yaml up -d --wait collector-demo grafana-demo

demo-down:
    docker compose -f compose.yaml -f demo/compose.yaml stop collector-demo grafana-demo

# Package a native Linux binary and licenses into dist/.
package architecture=host_arch: build
    #!/usr/bin/env bash
    set -euo pipefail
    architecture="$1"
    case "$architecture" in amd64|arm64) ;; *) echo 'Expected amd64 or arm64' >&2; exit 1;; esac
    test "$(uname -s)" = Linux && test "$architecture" = '{{ host_arch }}'
    version=$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["package"]["version"])')
    mkdir -p "dist/$architecture"
    cp target/release/clickhouse-promql LICENSE NOTICE "dist/$architecture/"
    strip "dist/$architecture/clickhouse-promql"
    "dist/$architecture/clickhouse-promql" --version
    tar -C "dist/$architecture" -czf "dist/clickhouse-promql-otel-adapter-v$version-linux-$architecture.tar.gz" clickhouse-promql LICENSE NOTICE

image-build architecture=host_arch: (package architecture)
    docker build --platform "linux/$1" -t clickhouse-promql-otel-adapter:local .

image-smoke architecture=host_arch: (image-build architecture)
    docker run --rm --platform "linux/$1" clickhouse-promql-otel-adapter:local --version

# Run the published image with CLICKHOUSE_* settings from your environment.
image-run *args:
    docker run --rm -p 9091:9091 -e CLICKHOUSE_URL -e CLICKHOUSE_DATABASE -e CLICKHOUSE_USER -e CLICKHOUSE_PASSWORD ghcr.io/rivet-dev/clickhouse-promql-otel-adapter:latest "$@"

# Ensure a release tag agrees with Cargo.toml before publishing anything.
release-check tag:
    #!/usr/bin/env python3
    import re, sys, tomllib
    version = tomllib.load(open("Cargo.toml", "rb"))["package"]["version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version) or sys.argv[1] != f"v{version}":
        sys.exit("Release tag must be v" + version)

# Unpack downloaded release artifacts and produce checksums / CI image metadata.
release-prepare tag: (release-check tag)
    #!/usr/bin/env bash
    set -euo pipefail
    for architecture in amd64 arm64; do
      mkdir -p "dist/$architecture"
      tar -xzf "artifacts/clickhouse-promql-otel-adapter-$1-linux-$architecture.tar.gz" -C "dist/$architecture"
    done
    (cd artifacts && sha256sum ./*.tar.gz > SHA256SUMS)
    repository="${GITHUB_REPOSITORY:-rivet-dev/clickhouse-promql-otel-adapter}"
    printf 'name=ghcr.io/%s\nversion=%s\n' "${repository,,}" "${1#v}" >> "${GITHUB_OUTPUT:-/dev/stdout}"

# Publish prepared binaries to GitHub (called after the CI image push).
release-publish tag: (release-check tag)
    gh release create "$1" artifacts/* --verify-tag --title "$1" --generate-notes

# Tag the current clean main commit and trigger the release workflow.
release tag: (release-check tag)
    #!/usr/bin/env bash
    set -euo pipefail
    test -z "$(git status --porcelain)" || { echo 'Commit all changes before releasing' >&2; exit 1; }
    test "$(git rev-parse HEAD)" = "$(git rev-parse main)" || { echo 'Release from main' >&2; exit 1; }
    git tag "$1"
    git push origin "refs/tags/$1"
