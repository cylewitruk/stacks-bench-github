# Agent Guidance

## Workspace Commands

ALWAYS prefer these commands over using `cargo ...` or `rustfmt ...` directly. Only
fall-back to custom tool calls if necessary.

| Command | Description |
| ------- | ----------- |
| `just build` | Build the workspace |
| `just lint` | Lint the workspace (incl. `rustfmt` check) |
| `just test` | Run workspace tests |

`just test` accepts nextest filters and a few agent-friendly output modes:

- `just test <filter>` runs matching tests.
- `just test --summary <filter>` prints only the nextest header and summary.
- `just test --failures <filter>` prints failing tests and captured failure
  output.
- `just test --results <filter>` prints per-test pass/fail statuses without
  captured success output.
- Add `--no-sccache` when the sandbox blocks the configured compiler cache.
  This is supported by `just build`, `just lint`, `just fix`, and `just test`.

## Database Tests

Integration tests that need Postgres pick one of two helpers in
`sbgh-core::db::test_support`:

- `setup_pg_db()` (default) — a fresh, migrated database on a shared,
  compose-managed server; the returned guard drops it on teardown. Use for
  schema-isolated tests.
- `setup_pg()` — a dedicated testcontainers Postgres per test, for suites
  needing full cluster isolation. The `sbgh-cli` suites still use it, but
  since the Phase 6 role collapse they no longer touch cluster-global
  objects and could move to `setup_pg_db()`.

`just test` auto-starts the shared server. Both require Docker.

## Coding Style

Write clear, concise, idiomatic and best-practice-driven code for any given
language.

## Documentation

Follow these rules when writing any documentation, both in code and dedicated
files:

- Avoid long, drawn-out and overexplained documentation; be clear but concise,
  focusing on the important details of the item being documented.
