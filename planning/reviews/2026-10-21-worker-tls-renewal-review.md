# Resident Worker TLS Renewal — Review Handoff

## Scope and identity

Base: `d5dd5128b1253b3c9cab87b077e29629eafe3f08`. Review the uncommitted
candidate against the [file manifest](2026-10-21-worker-tls-renewal.sha256),
including the new worker TLS test module. This handoff and the manifest are
review metadata, not hashed candidate files. Verify identity before and after;
report drift. Do not edit, stage, commit, push or contact production.

```bash
git rev-parse HEAD
git status --short
shasum -a 256 -c planning/reviews/2026-10-21-worker-tls-renewal.sha256
git diff HEAD
git ls-files --others --exclude-standard
```

The [host evidence](../qualification/2026-10-18-v35-rollout.md#scenario-a-blocked-by-aged-worker-tls-wrapper--2026-10-21)
shows a resident worker unable to reconnect after daemon restart. Its one-hour
X.509 proof wrapper was generated at transport construction and reused forever.
The enrolled identity is its P-256 public key, not the disposable certificate.

## Change and review questions

- Worker transport retains its loaded identity key and trust roots, but builds
  a fresh TLS-1.3-only client configuration and one-hour wrapper for every new
  connection. There is no timer, key-file reread, fleet re-registration or change
  to lease deadlines. Check reconnect/clone behavior and fail-closed issuance.
- Daemon verification uses the TLS handshake time and distinguishes expiry,
  future validity and invalid purpose from malformed encoding. Existing size,
  lifetime, canonical P-256, EKU, signature and registry authorization boundaries
  must remain intact. Check that diagnostics do not weaken authentication.
- The controlled-clock regression exchanges bytes over real TLS, restarts its
  listener, rejects a stale wrapper, then reconnects twice with the unchanged
  connector/key after two-hour clock advances. It uses a test verifier, not the
  production daemon. Separate daemon tests cover the production verifier and
  validity boundaries; the pinned runtime checks exercise production mTLS/RPC.
- `x509-parser` is a test-only worker dependency already present in the workspace;
  the lockfile adds only that dependency edge. No migrations or protocol changes.

Prioritize correctness, identity continuity and missing regression coverage.
Report tests you run separately from the author evidence. Do not reopen waived
restore rehearsals or deferred physical multi-worker qualification.

## Author verification

- `just test --no-sccache --results transport`: 14 passed, including the aged
  reconnect and missing-clock tests.
- `just test --no-sccache --results fleet::tls`: five passed, including typed
  validity failures, unchanged wrapper constraints and existing signature tests.
- `just test --no-sccache --summary`: 1,118 passed, 26 skipped; one nextest
  child-process leak warning. No test failures.
- Full rerun with per-test results: 1,118 passed, 26 skipped, no leak warning.
  The intermittent warning was not attributed or fixed by this change.
- `just lint --no-sccache`: passed before the documentation update.
- `just build --no-sccache`: passed.
- `just qualify-protocol-runtime`: both pinned runtime/schema checks passed.
  An initial attempt was blocked by sandbox Docker access before tests ran;
  the successful rerun had approved access to the local test database.
- `just check-docs` and `git diff --check`: passed after the documentation update.

## Deployment boundary

Not deployed; scenario A remains open. After review and the operator's signed
commit, use the existing controlled update procedures for daemon and worker.
The worker change repairs reconnect; the daemon change improves diagnosis.
Retain protocol floor and enrollment. Keep admission paused during replacement
so the already-queued B does not run before checks are captured. Do not stop an
active attempt merely to deploy this fix.

Verify running binary hashes, worker preflight, session identity and empty fleet
debt. Finish queued-B resume/admission evidence and receipt replay checks. An
aged-worker reconnect after more than one hour, without restarting that worker,
is the relevant host confirmation; do not substitute a freshly started worker
for that check. Then finish the already-scoped scenario B and artifact/sign-off
work. No host operation or v35 closure is claimed by local test success.
