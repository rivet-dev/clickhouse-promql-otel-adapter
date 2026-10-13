# Vendored parser

`promql-parser` is the Apache-2.0 GreptimeTeam crate version 0.11.0 from crates.io.
Its LICENSE and source copyright notices are retained. Local changes add anchored
and smoothed range-selector AST flags and grammar rules, already reserved by the
upstream lexer. The adapter handles contextual duration expressions in its Rust
frontend before passing the normalized query to this parser.
