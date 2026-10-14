# Vendored parser

`promql-parser` is the Apache-2.0 GreptimeTeam crate version 0.11.0 from crates.io.
Its LICENSE and source copyright notices are retained. Local changes add anchored
and smoothed range-selector AST flags and grammar rules, already reserved by the
upstream lexer. The adapter handles contextual duration expressions in its Rust
frontend before passing the normalized query to this parser.

Additional local changes implement histogram trimming operators, delayed validation
of empty selectors for info() metadata matchers, byte-accurate Go string escapes,
and a parser token limit before AST construction. Tests were updated where new
semantics differ from the original crate. Run the retained suite with
`cargo test --manifest-path vendor/promql-parser/Cargo.toml --all-features`.

RE2 compatibility is implemented in `src/label/regex.rs`: ASCII Perl classes,
Go flag/property validation, repeat limits, literal class-set operators,
duplicate/numbered capture names, and Go replacement expansion. Unicode property
names are generated alongside the regex reference corpus. Fractional duration
formatting and unary plus pretty printing are also patched for the pinned
Prometheus API. All runtime code remains Rust.
