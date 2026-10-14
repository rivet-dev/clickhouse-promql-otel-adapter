The compressed corpus contains actual storage snapshots and query results from
all 21 fixture files in the pinned Prometheus release. The manifest records its
source revision, execution count, dataset count, and checksum. Inputs include
stale samples, start timestamps, native/custom histograms and reset hints.

Run `python3 scripts/conformance.py`. Regenerate explicitly with
`python3 scripts/conformance.py --regenerate` (requires Go for the reference only).
The adapter and its evaluator remain entirely Rust. See `NOTICE` and `LICENSE`.

These are captured query executions, including upstream-generated instant,
range, and @ variants, rather than a count of independent original assertions.
The runner compares values with numerical tolerance, labels exactly, sorted
function ordering, errors, warnings, and infos. All failures are counted and
written to `.state/conformance/report.json`; a failure makes the command fail.

This corpus currently tests the in-memory evaluator. ClickHouse conversion and
HTTP integration are covered separately. Query-creation/parser rejections and
all HTTP protocol details require additional coverage; passing this corpus alone
does not establish complete Prometheus/Thanos compatibility.
