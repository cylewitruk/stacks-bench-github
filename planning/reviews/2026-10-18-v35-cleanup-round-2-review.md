# v35 Cleanup Round 2 — Opus Delta Review

## Identity and scope

Review the focused cleanup of the [original candidate](2026-10-18-v35-release-review.md),
not the entire implementation again. No commit or deployment has occurred.

- HEAD: `0f18950ad086799015fd1af0d66a7e672a3e5e27`.
- Original 101-file manifest remains unchanged; every original file was verified
  against its staged version before freezing this round.
- Original staged diff SHA-256 (`git diff --cached --binary HEAD`):
  `a94637ef744d18cf980e3b1fde74d64da81da6103ecbbd3a826fa3c55f7f1338`.
- [Frozen cleanup patch](2026-10-18-v35-cleanup-round-2.patch): 15 files,
  SHA-256 `ae38e00695897c3b5743c0f5bc4cc7a62ac42590ead22aec2160345ae3f99ce1`.
- [Full updated candidate manifest](2026-10-18-v35-cleanup-round-2.sha256):
  102 files, SHA-256 `0661e26ac2e8ca1815d5f068bdba62388b9a893585d1c82e3626e042f62b5aed`.
- Cleanup edits remain unstaged; the new source module and round-2 review
  artifacts are untracked. Existing staging was not changed. Review metadata
  is excluded from candidate manifests.

The patch is relative to the reviewed candidate, **not HEAD**. Read it; do not
apply it. The original manifest is expected to differ from the updated working
tree. Verify the new manifest instead, before and after review:

```bash
git rev-parse HEAD
git status --short
shasum -a 256 planning/reviews/2026-10-18-v35-cleanup-round-2.{patch,sha256}
shasum -a 256 -c planning/reviews/2026-10-18-v35-cleanup-round-2.sha256
git apply --reverse --check planning/reviews/2026-10-18-v35-cleanup-round-2.patch
git diff --check
```

`--reverse --check` only validates that the patch matches the current files; it
does not reverse it. Stop on identity drift. Do not edit, stage, commit, deploy,
contact production, or add reviewers. Local fixture tests are allowed.

## Changes to review

| Original finding | Cleanup | Regression evidence |
| ---- | ---- | ---- |
| Artifact garbage blocks maintenance for the GC grace period | Private `fleet/quiescence.rs` supplies the same counters to scheduler status and protocol-floor admission. Active-attempt uploads and accepted terminal-manifest members block; unrelated retained staging is reported separately as `garbage_artifacts`. GC timing/deletion is unchanged. | Fenced and expired uploads, real cleanup acknowledgement, floor changes, accepted zero-byte promotion, late verification of an unused grant, and human/JSON CLI output. |
| Pause leaves withdrawn sessions `offered` | Lock affected registries with `FOR NO KEY UPDATE`, then offered attempts, then repair affected offered sessions to idle/draining before withdrawal/requeue. Preserve accepted work and offline sessions. | Offer → pause → deregister; real expiry sweep interleaving; existing acceptance/pause race tests. |
| Cleanup admission is worker-only | Worker-wide pending cleanup blocks new and retained offers and first acceptance. The already-accepted retry path remains idempotent. | Successor with predecessor cleanup and another queued job; committed cleanup debt racing with poll/accept of a retained offer. |
| Test-only bulk drain bypass | Remove `set_all_workers_draining` from trait/implementation; test the audited lifecycle operation instead. | Existing idle-drain regression. |
| Misleading freeze message | Mention scheduler admission as well as submission changes. | Build/lint and coordinator tests. |
| Updater retry with stopped worker and cleanup | Document failure inspection, preflight/registration, resident-only cordoned restart, and the legacy-worker limitation. No automatic service-start or policy bypass added. | Runbook review; existing updater tests pass. |

Important review details:

- Terminal manifest membership, not merely `verified`, determines retained
  promotion debt. The test exercises late verification through the production
  store after terminal acceptance. No upload grants or artifact rows are deleted
  to obtain quiescence.
- The expiry test initially assumed the sweep would wait at its explicit session
  update. PostgreSQL actually waits earlier during foreign-key checks in the
  attempt update. Pause's registry lock must coexist with those key-share locks;
  its attempt locks must precede session repair. Please scrutinize both orders.
- The retained-offer admission test deliberately seeds a pending obligation after
  offer delivery under the registry lock. It tests the server invariant even if
  a client retains an offer; it is not evidence of that sequence occurring on the
  deployed worker. The predecessor-cleanup test uses actual session replacement.
- The shared lock-wait test helper now reports actual waiting SQL on failure.
- No migration, worker wire contract, artifact GC policy, updater script, or
  deployment qualification claim changed in this round.

## Author verification — final candidate

- `just build --no-sccache`: passed.
- `just lint --no-sccache`: passed, including documentation/architecture checks,
  script tests, clippy and formatting (7 updater tests; 17 progress tests).
- Full nextest run `1080a1d2-cf26-4b95-8f52-71e2cdcd6c8e`: **1,114 passed,
  26 skipped**, 38.027s. No leak warnings on this run.
- `just qualify-protocol-runtime`: **2 passed**, run
  `96e92a6f-2c26-477b-9cfc-2e736f6c1805`, 39.491s.
- Buf 1.69.0 breaking check against `fixtures/fleet/revision-1/proto`: passed
  in a read-only, network-disabled container with an ephemeral cache.
- Diff whitespace, patch reverse-check and all 102 candidate hashes: passed.

Earlier evidence is not discarded: full run
`9da02d59-d8d4-4880-9e42-a577c90e18d6` had 1,113 passes, one new lock-wait test
failure, and two leak warnings. Focused retries identified the foreign-key lock
waits; the final regression passes. Leak names were not emitted by that run's
failure-only output mode. The historical intermittent leak warning remains
unresolved; the final clean run does not establish a fix. Initial Docker sandbox
denial and a read-only Buf cache error were setup failures, not passing tests.

Final native probe identities (macOS, not Hetzner qualification):

| Probe | SHA-256 |
| ---- | ---- |
| Previous worker, source `0f18950` | `bf284e0adc18d3b382af65f66ce1d02d9949a3d9e62d17bc875c2d766f713615` |
| Previous schema | `d0c66edbc3300c7f4440e4a0f6633186acab3fa4c35123711a3a79fa23589e6d` |
| Current worker | `b2e2669b6bb0f7efe9b96ea5b1826b58feb2b6e399ee8b36b4582f36d92439b4` |

## Requested verdict

Confirm resolution of each original finding, then report any new actionable
defect with file/line, triggering sequence and focused fix/test. Prioritize
artifact ownership, pause/expiry lock ordering, and cleanup admission races.
State tests actually run and remaining gaps separately from author evidence.

Give a commit-readiness verdict and confirm the new manifest still matches.
Server-first deployment, host qualification, backup/restore drill, second worker
and full v35 closure remain open. Decision 0006 is approved; rollback remains
matching DB/binaries/configuration restore-only, with roll-forward preferred.
Do not reopen those decisions or infer permission to raise the protocol floor.
