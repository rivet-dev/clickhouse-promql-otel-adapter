# Vendored parser

`promql-parser` is the Apache-2.0 GreptimeTeam crate version 0.11.0 from crates.io.
Its LICENSE and source copyright notices are retained. The upstream archive is
pinned by SHA-256 in `promql-parser/regenerate.py`. Local changes live in
[`promql-parser/patches/promql-parser-0.11.0-prometheus-compat.patch`](promql-parser/patches/promql-parser-0.11.0-prometheus-compat.patch).
`promql-parser/out/` is generated and committed so builds do not require a separate
download step. Cargo uses that directory through `[patch.crates-io]`.

Edit the patch, then run these from the repository root:

```sh
just vendor-regenerate
just vendor-check
just test-parser
just conformance
```

Do not edit `out/` or Cargo's registry cache. Commit both the patch and regenerated
source. Regeneration verifies the release checksum, applies the patch with zero
fuzz in a temporary directory, and replaces `out/` only after success. CI runs
`just vendor-check` to catch any missing, changed or extra source files. Both
recipes accept `--archive /path/to/promql-parser-0.11.0.crate` for offline use;
the checksum is still required. Upstream's `Cargo.lock` and registry marker are
excluded; local Cargo build artifacts are ignored by the drift check.

## Compatibility changes

Local changes add anchored
and smoothed range-selector AST flags and grammar rules, already reserved by the
upstream lexer. The adapter handles contextual duration expressions in its Rust
frontend before passing the normalized query to this parser.

Additional local changes implement histogram trimming operators, delayed validation
of empty selectors for info() metadata matchers, byte-accurate Go string escapes,
and a parser token limit before AST construction. Tests were updated where new
semantics differ from the original crate. Run the retained suite with
`just test-parser`.

RE2 compatibility is implemented in `out/src/label/regex.rs`: ASCII Perl classes,
Go flag/property validation, repeat limits, literal class-set operators,
duplicate/numbered capture names, and Go replacement expansion. Unicode property
names are generated alongside the regex reference corpus into `.state/`, then
compared with the committed parser. If they change, update the patch explicitly;
reference regeneration never rewrites `out/`. Fractional duration
formatting and unary plus pretty printing are also patched for the pinned
Prometheus API. All runtime code remains Rust.
