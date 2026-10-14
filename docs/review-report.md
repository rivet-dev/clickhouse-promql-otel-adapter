# Independent review and corrections

Three read-only subagents reviewed semantics, storage/ingestion, and HTTP/testing
on October 14, 2026. Follow-up API and semantics reviews checked the repairs.
Findings were fixed in the shared Rust implementation and regression fixtures.

| Area | Finding and correction | Evidence |
|---|---|---|
| Native histograms | Expand straddled zero buckets before schema reduction; align aggregate inputs in the same order | `native_zero_threshold_reconciliation`, full native fixture replay |
| Regex | Translate RE2 ASCII classes, class literals, repeat limits/leading zeros, Unicode properties, flags, duplicate captures and Go replacements | 1,200 Go reference cases; HTTP label replacement cases |
| Parser | Comments inside duration/offset syntax, fractional duration precision, UTF-8 label names, topk overflow, empty count_values label validation | Review regressions, 356 parser cases, HTTP format/parse corpus |
| Annotations | Long-range count overflow and timestamps outside chrono's domain; negative fractional seconds/year formatting | 70-step annotation regression and date unit tests |
| Resource accounting | Count retained generated results/caches rather than rejecting cumulative window reads as memory | Retained-memory regression, all evaluator fixtures |
| OTel planning | Fetch info series at shifted selector time; avoid unrelated invalid metric poisoning; accept bucket-less histograms | Real ClickHouse regressions and Collector snapshots |
| Storage duplicates | Compare start timestamps as well as value/histogram before retry deduplication | Real ClickHouse conflict regression |
| SQL/discovery | Filter normalized metric names safely, preserve quoted names, use distinct label rows for discovery | Real ClickHouse bound/quoting/dense-history tests |
| HTTP | Malformed label-route panic, U__ UTF-8 escapes, metadata negative/zero limits, pretty formatting | Nine API integration tests, 1,016 pinned HTTP requests |
| Thanos | Analyze/explain fallback, optional default range step, HA options | API regressions and 800 seeded replica cases |
| Lifecycle | Slow body/admission timeouts; keep permits owned by CPU workers; search work cap; cancellation | Request deadline regression and code review |
| Backend errors | Separate storage request failures (500 internal) from execution failures (422) | Unreachable ClickHouse regression |
| Snapshot cleanup | Ensure precision schema setup errors also clean isolated database/container | Generator cleanup path review and clean default/precision runs |

The reviewers also identified real format losses: omitted zero threshold,
optional histogram presence, original metric metadata/type/unit and subsecond
precision. These cannot be fixed by query logic; [schema findings](schema-findings.md)
describe concrete exporter/schema changes. Go and Rust allocation statistics are
also inherently implementation-specific; [the API contract](api-contract.md)
defines the reported measurements instead of claiming identical numbers.

Validation commands are in the root README. Committed snapshots and reference
corpora require explicit regeneration. Full evaluator, real storage replay,
HTTP, vendor parser, formatter and strict Clippy checks were rerun after repairs.
Manual screenshots prove the live integration for the observed workflows; they
are not substituted for automated semantic tests.
