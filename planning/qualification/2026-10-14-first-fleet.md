# First Fleet Qualification — Reconciled 2026-10-14

Evidence record for [v34](../iterations/v34-first-fleet-deployment-readiness.md).
This is a partial qualification, not a release sign-off or a live host audit.

## Evidence Basis

September–October evidence below comes from operator-pasted commands, result
JSON, and GitHub screenshots in the fleet-resumption conversation. August
evidence remains in v34's [dated record](../iterations/v34-first-fleet-deployment-readiness.md#real-host-progress--2026-08-07).
Those historical checks are retained, not rerun or independently reverified.
Host paths and artifact keys are retrieval references; their current existence
and checksums have not been rechecked for this reconciliation.

The repository reviewed was `e8417f5` plus the outstanding planning edits.
That is not proof of the currently deployed release. There was no host mutation
or new canary during this documentation pass.

## Deployment Identity

| Field | Recorded value |
| ---- | ---- |
| Topology | Separate daemon and worker processes on one physical `stacks-bench` host |
| Worker | `11db06ad-fe64-457b-ac2b-b495c6628989` / `combined-fsn1-01` |
| Profile | `hetzner-i7-8700-isolated-2vcpu-v1` |
| Capabilities | `benchmark`, `block_validation`, `build_only` |
| Transport | Production connector reported `grpc+tls1.3`, authorized registration, protocol revision 1 |
| Qualification directories | `/var/lib/sbgh/qualification/20260922T114642Z` and `/var/lib/sbgh-worker/qualification/20260922T114642Z` |
| Regular origin | `vg0/mainnet-2026-09-21`; `mainnet-` prefix and read-only origin verified on October 5 |
| Latest confirmed return to service | October 5: online, undrained, idle; attempts/cleanup/gaps/staged bytes all zero |

On October 12 the operator reported the worker offline and drained again.
Undrain/restart instructions were supplied, but no subsequent status was pasted.
The cause and current service state are therefore unconfirmed.

## Release, Storage, and Recovery Evidence

- **September 22 backup restore: passed.** Backup
  `sbgh-sbgh-20260922T010418Z.tar.zst` restored into a scratch database. The
  corrected restore checker validated 142 submissions, 143 specs, 166 jobs,
  143 build steps, and 107 run steps, with all nine consistency checks passing.
  The earlier checker incorrectly classified the schema as pre-v14; that first
  output alone was not a successful schema validation.
- **September 22 update: passed while quiescent.** Daemon, CLI, and worker
  installed hashes matched the release binaries. The update created backup
  `sbgh-sbgh-20260922T114635Z.tar.zst`, passed preflight/transport checks, and
  returned the worker online. This does not prove active-attempt restart
  survival, emergency rollback, or a restore of the newly created backup.
- **September chainstate import: passed.** `ripcat` streamed 257,080,129,528
  compressed bytes; the script verified SHA-256
  `794b32cf2f6cab97f744346a2744a7f8f51e17fdd61afd163f00b9fcaa1d6aeb`
  before retaining the read-only `mainnet-2026-09-21` origin. September 23's
  two-snapshot isolation script completed and removed both test LVs. Report:
  `/var/lib/sbgh-worker/qualification/20260922T114642Z/lvm-isolation-mainnet-2026-09-21.md`.
- **September 23 worker identity: recorded.** At checkout
  `fd01b1b2d691dcdcc87a13397b2ab9509244a6ac`, the running worker executable and
  `/usr/local/bin/sbgh-worker` both hashed to
  `ea454480f46923d5036fc4ba3b53a3442521fdb084e8e23cbbca923e517055a0`.
  This is stronger than checkout identity alone, but identifies only that
  worker observation.
- **September 23 artifact access: passed.** The combined-host update at
  checkout `58f7f78` completed. `jobs artifacts ls/cat` retrieved the promoted
  schema-v3 validation result for job `b045c950-05b8-41f6-b1ef-e1ca512ad7a0`.
  It does not replace the standalone S3 put/HEAD/get/checksum/delete ceremony.
- **Canonical experimental import: transfer verified.** The operator reported
  `PORTABILITY.json: OK`, `SHA256SUMS: OK`, 186,175 checksum entries, and
  `data_checksums_exit=0`, then a read-only
  `vg0/bench-canonical-2026-09-29`. This is transfer evidence, not distributed
  snapshot inventory, canonical-ledger, or cross-architecture qualification.

## Controlled Validation on the September Origin

Both successful jobs used `stacks-network/stacks-core` commit
`e2b39b01c70fde056b7018cd1e7ce83d022e7f15`, checked one block, and reported
`valid` on `vg0/mainnet-2026-09-21`.

| Submission | Job | Attempt |
| ---- | ---- | ---- |
| `2ab4198e-6008-4059-a371-994e0ca23aa2` | `b4350d58-c73d-48f7-befb-3b4d8a26e36a` | `22064da9-e5eb-435a-9edc-4e3670143e21` |
| `4356e126-b2bd-421c-a606-3486d6f8a25b` | `b045c950-05b8-41f6-b1ef-e1ca512ad7a0` | `b45a2f53-d4ce-47b6-8847-062a62c97dfa` |

Observed counts were 185,630 pre-Nakamoto and 8,865,006 Nakamoto entries;
global range `9050635..=9050635` mapped to `naka-index-range 8865005 8865006`.
For the second job, the operator confirmed the attempt directory was removed,
only base chainstates remained, and fleet attempts/cleanup/gaps/staged bytes
were zero. The artifact path is
`b045c950-05b8-41f6-b1ef-e1ca512ad7a0/block-validation/block-validation-result.json`.

Keep the preceding failure: job `cc716545-4ca2-4977-926d-d4358ed218d6` used
commit `1ed2021d9209f1ba7d4d9c9a763296c41f9194bb` and failed because the tool
expected chainstate schema 13 but received 14. It is compatibility-failure
evidence, not an invalid-block verdict. Neither successful one-block run
qualifies full-history validation or measures distributed speedup.

## GitHub Benchmark Evidence

Provider: [cylewitruk/stacks-core PR #6](https://github.com/cylewitruk/stacks-core/pull/6).
The GitHub benchmark journey passes on the September 24/25 completed comments
and artifacts. Exact benchmark comment/Check Run IDs and benchmark attempt IDs
were not pasted and remain evidence-retrieval work.

| Job | Code commit | Observed result |
| ---- | ---- | ---- |
| `a6fd1207-fc32-4b76-9fc7-61204bebdd93` | `bf5e18cf82126365b344aabdb1bf00786b4e0632` | September 24 bot completion screenshot: 4,000 measured + 1,000 warmup, 20,969 transactions; artifact listing and idle/zero-debt fleet afterward |
| `3ceb7209-4a8e-4564-ac04-4c4b464cf0ef` | `75826048d8265fb837899a621e1f353042cf3eb8` | September 25 bot completion screenshot: 90 measured + 10 warmup, 596 transactions; artifact listing and idle/zero-debt fleet afterward |
| `810ec2fa-73a2-4635-be7e-ec59e734f736` | `4bf5000855399a68fb206c2add91530c79120868` | `develop2` push baseline completed: 4,000 measured + 1,000 warmup, inline calibration, retained DB and JSONL |
| `14cee62e-f848-4e5e-a792-96b2a0690481` | `848ce3cf24fd8240baaf0e837c67bf7160a8f039` | Long `develop2` baseline completed; stored origin `vg0/mainnet-2026-09-21` |
| `88ed079a-b617-49a0-902d-217f23284ee6` | `55fc068f024412cfa06214c99438a86db7b945ae` | Long optimized PR run completed; `bench-canonical-` was the configured prefix, but its terminal origin field was not pasted |

The first two artifact listings include `appdata/stacks-bench.db`, `run.json`,
`run.progress.jsonl`, `phase.log`, console output, and the executed binary.
Listing JSONL proves retention, not correct live attribution, debounce,
calibration/repeat handling, or terminal precedence. Those `0027` gates remain
open.

The long pair used these identical effective arguments:

```text
--network mainnet --no-baseline --no-profiler-kv --bench-spans-only
--tip 7539c6da0f2eafcb8f9f9b12c1733d8970758e59bd383b85091620f4f071ecb1
--start-at 8694809 --end-at 8719808 --warmup 5000
```

Both summaries reported success, 25,000 entries, 20,000 measured, 5,000 warmup,
60,702 transactions, Clarity runtime 405,897,124,502, read length
164,510,130,322, and write length 2,773,781,402. Replay durations were
18,901.697873619 seconds (baseline) and 7,383.230683924 seconds (optimized).
These were separate submissions with different prepared chainstates. They do
not qualify `0039`'s automated multi-variant/repeat/calibration workflow, ABBA,
or automatic PR-baseline selection. Instructions to download both databases
were supplied; local receipt and checksum verification were not reported.

## Explicit Follow-Up Schedule

Unchecked means not demonstrated; rescheduling does not mark a gate passed.
v34/`0080` remains `in_progress`. v35 is the next implementation priority;
its lifecycle work may begin while the following bounded evidence work remains.

| Remaining gate | Owner and next checkpoint |
| ---- | ---- |
| Raw evidence completion | v34: retrieve benchmark submission/attempt/provider identities, exact deployed release/image/tool versions, and artifact checksums; retain host reports and operator sign-off before final qualification |
| Direct S3 ceremony | v34: record put/HEAD/get/checksum/delete before final qualification; existing artifact reads are partial evidence |
| Slack benchmark journey | v34: one small natural-language benchmark with one canonical report and cleanup evidence; not proven by Slack parser tests |
| Fine benchmark progress (`0027`) | v34 fast-follow: inspect live guest-to-provider progress and archived JSONL, then calibration/repeat/debounce/fallback cases |
| Integrated comparison (`0039`) | v34 fast-follow: two refs on one compatible snapshot/worker, then clean repeats and per-variant calibration/DB carry-forward; combine restart portion with v35 |
| Full-history validation | v34: schedule an explicit maintenance window and retain complete coverage/reduction/artifact/cleanup evidence before final qualification |
| Active/post-terminal daemon restart, worker loss, cleanup recovery | Rescheduled to v35 Phases 2–3 and 7; test the new lifecycle and retain revision-1 behavior as the reference, not as a passed transparent-restart gate |
| Identity rotation/revocation, capability/profile changes, policy persistence | Rescheduled to v35 Phases 6–7; retain the original v34 Phase 8 acceptance checks |
| Deployed revision-1 semantic-digest vectors | v35 Phase 1 prerequisite to protocol changes; pin the deployed baseline before Phase 5 negotiation |
| Second physical host | Explicitly deferred from v34; commission by v35 Phase 6's two-worker maintenance gate, then use v36's snapshot-bootstrap contract; no multi-machine claim yet |
| Validation initialization/progress distinction | v34 follow-up; retain the observed scan delay as v36 timing input, not a measured placement model |

## Next Implementation Checkpoint

Start [v35 Phase 1](../iterations/v35-seamless-fleet-lifecycle.md#phase-1-lifecycle-contract-and-failure-matrix):
review Decision 0006, capture deployed heartbeat/lease/session timing and the
revision-1 protocol baseline, and freeze the failure matrix. Then implement
resident-session/cleanup recovery server-first, followed by process-local
daemon shutdown. Existing manual undrain/restart operations are evidence of the
problem, not evidence that these new semantics have shipped.

Decisions 0006/0007 remain drafts. No v35–v37 implementation gates are checked
by this reconciliation, and neither a canonical ledger nor cross-worker
validation has been demonstrated.
