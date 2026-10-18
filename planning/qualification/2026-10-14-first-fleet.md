# First Fleet Qualification — Reconciled 2026-10-14

Evidence record for [v34](../archive/superseded/v34-first-fleet-deployment-readiness.md)
and its [v35 continuation](../iterations/v35-seamless-fleet-lifecycle.md).
This is a partial qualification, not a release sign-off or a live host audit.

## Evidence Basis

September–October evidence below comes from operator-pasted commands, result
JSON, and GitHub screenshots in the fleet-resumption conversation. August
evidence remains in v34's [dated record](../archive/superseded/v34-first-fleet-deployment-readiness.md#real-host-progress--2026-08-07).
Those historical checks are retained, not rerun or independently reverified.
Host paths and artifact keys are retrieval references; their current existence
and checksums have not been rechecked for this reconciliation.

The repository reviewed on October 14 was `e8417f5` plus the outstanding planning edits.
That is not proof of the currently deployed release. There was no host mutation
or new canary during that documentation pass. The October 15 addendum below
records subsequent operator-executed deployment and canaries, not an independent
host audit.

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
| Latest confirmed return to service | October 16 at 09:49:30 UTC: online, undrained, idle; attempts/cleanup/gaps/staged bytes all zero |

On October 12 the operator reported the worker offline and drained again.
The cause was not established. The October 15 observations below supersede that
service-state uncertainty, without attributing the earlier drain.

## v35 Deployment and Restart Qualification — 2026-10-15

Evidence: operator-pasted installation, API, systemd, executable-hash, and job
report output in this conversation. Times below are UTC. This qualifies one
combined host and one CLI block-validation attempt, not provider convergence or
multi-host operation.

### Release and Rollback Provenance

- Clean host checkout: `d0b6852` (`feat(fleet): add resident recovery and
  non-destructive daemon shutdown`); `just build --release` succeeded.
- Quiescent server-first installation: explicitly drain/stop the worker, back
  up, install/restart the daemon and CLI, then install/preflight the worker.
  Both unit verification checks passed. The worker was explicitly undrained
  and started once after installation; no worker intervention occurred during
  the subsequent restart canaries.
- Installed binaries matched the build. Running daemon and worker executable
  hashes also matched after deployment.

| Binary | SHA-256 |
| ---- | ---- |
| `sbgh-daemon` | `972e36fae3c51ae460fcf39edce1c5ef76096200e024682d9f6362b7a59bd78f` |
| `sbgh-cli` | `0a5159c575bbe6c1fa325da1c5f567bdd0ca4d61a6e0a3bf979ac32a67fde935` |
| `sbgh-worker` | `e533946ccb450de0580ed62e570e26e89864f1edb8bc2ed9f442e961fa6a89fb` |

Rollback archive:
`/var/lib/sbgh/backups/pre-v35.hZJvSnOm/installed-files.tar`, SHA-256
`ef87fe25e6fb6ca78d686a8a0758dcabe6378c7c7300c9f8b86c556caafc2aaa`.
Postgres backup service succeeded and produced
`/var/lib/sbgh/backups/sbgh-sbgh-20261015T081703Z.tar.zst`.
Neither restoration of this backup nor rollback was exercised.

Reported timing configuration: heartbeat 10s, lease 45s, session 90s, offer
30s, long poll 20s, request timeout 120s. These are operator-reported settings,
not an independent effective-runtime configuration dump. Preflight passed;
transport reported authorized TLS 1.3, protocol 1. The worker retained the
`mainnet-` chainstate prefix.

### Restart Results

Shared worker identity throughout the captured restart checks:

| Field | Value |
| ---- | ---- |
| Session | `a7c111fc-9269-41df-a9b8-56d91204b7a6` |
| PID | `437078` |
| systemd invocation | `4f8e6cc4828a40ff8d1d4288eef39a80` |
| Automatic restarts | `0` |

1. **Short idle restart: passed.** Daemon restart start/return timestamps both
   read `08:24:50` (whole-second resolution). At `08:25:15` the API was healthy
   and worker online, idle, undrained, with unchanged process/invocation and
   zero backlog. The subsequent session observation retained the original UUID.
2. **Expired idle session: passed.** The operator ran the prescribed daemon
   stop / 110-second wait / start procedure, exceeding the reported 90-second
   session TTL. Exact stop/start timestamps were not supplied. At `08:56:49`
   the same session UUID was idle with fresh heartbeat and expiry `08:58:19`;
   PID, invocation and restart count were unchanged. No manual worker recovery
   was needed.
3. **Active validation restart through completion: passed.** Before restart,
   the attempt below was validating with heartbeat `09:06:41`. After restart,
   daemon PID `448539` was active and the API responded; at `09:11:38` the same
   attempt was still validating with a fresh fleet heartbeat. Worker PID,
   invocation and restart count were unchanged. The terminal report retained
   that attempt and reported `completed` / `done`, `valid`, one checked block,
   no invalid blocks and no failure. At `09:30:33` the worker was idle and
   undrained with attempts/cleanup/gaps/staged bytes all zero. Exact active
   restart timing was not captured, so this is continuity evidence, not a
   measured maximum outage or startup-grace boundary test.

| Active canary identity | Value |
| ---- | ---- |
| Submission | `82d3b27d-7f0d-4cff-8f96-224f9271be68` |
| Job | `9b79b6a0-1efa-4725-be6a-67dcedfdb096` |
| Attempt | `c64efab2-b901-4a53-9108-7bed38572c8f` |
| Source | CLI; `stacks-network/stacks-core` at `e2b39b01c70fde056b7018cd1e7ce83d022e7f15` |
| Selection | Recent 1; global index `9050635`, Nakamoto local index `8865005` |
| Origin | `vg0/mainnet-2026-09-21` |

During inspection, the operator reported an apparently slow command block and
supplied local libvirt monitoring logs. A bounded API check returned success,
and the subsequent report confirmed a fresh authoritative fleet heartbeat.
The apparent inspection delay was not attributed; local VM log messages alone
were not treated as proof of lease renewal.

These checks do not qualify active lease-loss teardown/requeue, successor
fencing on the host, duplicate cleanup acknowledgement, post-terminal provider
replay, forced shutdown, rollback, or previous-version interoperability. No
fresh host filesystem/VM cleanup inventory or terminal artifact checksum was
supplied for this canary; zero cleanup counters are the recorded cleanup evidence.

## Active Lease-Expiry Canary — 2026-10-16: Resident Recovery Failed

Operator-pasted journals and inventories for deployed `d0b6852` show safe local
cancellation and subsequent cleanup-gated replacement, but **not resident
recovery**. Preserve this failed gate; a successful replacement does not erase
the worker restart.

| Identity | Value |
| ---- | ---- |
| Submission | `e3f8a643-8a60-48a2-865b-f6875c16cdf6` |
| Job | `d54835c2-2ae6-4006-8ab2-1487b28510a5` |
| Original attempt | `a26e0166-9299-408d-b790-157d397b40fa` |
| Replacement attempt | `10f824c2-21cf-477f-a6b1-fcdb805ab03f` |
| Source | Same upstream commit and recent-one-block selection as October 15 |

Observed timeline (UTC):

- `07:12:24`: daemon stopped; worker PID `437078` remained running.
- `07:13:09.782`: worker logged lease authority loss and cancelled execution.
- `07:13:11`: driver reported aborted execution and performed cleanup; worker
  resumed retrying `ListCleanup` while the daemon was unavailable.
- `07:13:24` and `07:14:15`: resource snapshots showed no canary VM, thin
  snapshots, or attempt mounts. The worker still had its original PID and
  invocation, with zero systemd restarts.
- `07:14:15`: daemon returned. The earlier CLI refusal at `07:14:12` preceded
  the scheduled restart; it was not evidence that automatic restoration failed.
- `07:14:15.580`: `ListCleanup` returned `Unknown: transport error`; worker
  classified it as permanent and exited with status 1.
- `07:14:25`: systemd restarted the worker: PID `784068`, invocation
  `8cf9b9862ef94571a4f9d6f894f9cafa`, `NRestarts=1`.
- `07:14:26`: new session `8e326949-d983-4e84-b921-b13558a4bece` registered.
  Cleanup obligation `9` had reason `worker process registered a successor
  session`; acknowledgement at `.298` preceded the new offer at `.311` and
  acceptance at `.546`.
- `07:18:16`: replacement attempt was validating with a fresh heartbeat.
- `07:27:04.249`: replacement attempt finished; the subsequent report showed
  `completed` / `done`, one of one jobs complete, and no failure. The validation
  verdict/artifact was not included in this excerpt.
- `07:28:41`: worker online, undrained and idle, all fleet backlog counters zero,
  and no queued jobs. This establishes successful replacement completion, not
  same-process recovery of the interrupted attempt.

The daemon also logged a TLS `BadEncoding` rejection during reconnect. The
underlying handshake cause is not established. The actionable worker defect
is classifying an unstructured gRPC `Unknown` transport failure as permanent.
Fix and test that retry classification without weakening typed authorization,
supersession, or lease-expiry rejection. Then repeat this host canary on the
corrected build. Durable attempt/cleanup query results remain outstanding.

Local follow-up: unstructured `Unknown` now uses the transport retry fallback;
structured fleet errors retain their explicit retry policy. Authorization and
protocol rejections remain non-retryable, and lease deadlines are unchanged.
Before the fix, both the classification regression and TLS loopback lease-loss
test reproduced the permanent `ListCleanup` exit. After the fix, all 67 worker
tests passed, including same-UUID recovery and cleanup acknowledgement before
the next poll. That loopback test cancels a stalled credential RPC and uses
stubbed host tools; it does not qualify real VM teardown or reproduce the
underlying TLS handshake failure.

Full nextest run `108e65ce-abff-482b-8870-18d72e5651bd`: **1,029 passed,
24 skipped** (`just test --no-sccache --failures`). `just lint --no-sccache`,
documentation checks, and `git diff --check` passed. Deployment and the repeat
canary are recorded below; the original failed run remains failure evidence.

## Active Lease-Expiry Repeat — 2026-10-16: Passed

The operator deployed `0f18950` using the worker-only update workflow. Preflight
and production registration checks passed; the daemon was not updated. Build,
installed, and running worker executable SHA-256 all matched:
`9e5e484de7ba032de8674c35c9e1309fa2a046be744d6ccb8c2518e083154501`.

| Identity | Value |
| ---- | ---- |
| Submission | `c8921af3-347c-417d-940e-fefe53fdaaab` |
| Job | `2eb775c1-a50b-4e72-b13b-2cdaaea3d2bf` |
| Expired attempt / fence | `9a21026b-8814-4eef-bb27-c379226651ac` / 1 |
| Completed attempt / fence | `ae71051f-7c38-4148-be71-811db2c26de3` / 2 |
| Session, unchanged | `08f3e493-2990-462c-93ef-8f7234fbcef1` |
| Worker PID / restarts | `808295` / `0` |
| systemd invocation, unchanged | `08039929315e4ce0a27613ddae48e812` |
| Source | `stacks-network/stacks-core` at `e2b39b01c70fde056b7018cd1e7ce83d022e7f15`; recent 1 |

Operator-pasted resource inventories and the read-only recovery query establish
this sequence (UTC):

- `07:55:38`: daemon stopped; the attempt's last persisted lease expiry was
  `07:56:19.758673` (later than the earlier pre-outage query's expiry).
- `07:56:38` and `07:57:28`: no canary VM, snapshots, or attempt mounts remained.
  Worker PID, invocation, and restart count were unchanged. These observations
  bound when resources were absent, not the exact cancellation instant.
- `07:57:28`: daemon restarted after the 110-second outage.
- `07:57:48.376425`: original attempt became `expired`; its terminal timestamp,
  reliable sequence and digest remained null. Obligation `10` was created for
  lease expiry with `requeue_job=true`.
- `07:57:51.961475`: obligation `10` completed.
- `07:57:51.975259`: replacement attempt created, then accepted at
  `07:57:52.196205`, after cleanup acknowledgement and under the same session.
- `08:10:08.768803`: replacement attempt and job completed. Terminal reliable
  sequence was `8`, digest
  `9d271c1cdc95c91a57d6598c0f25276cc0498d1c0863f66b5c3d0097f46be56f`.
- `09:49:30`: same worker process and session idle, undrained, with zero fleet
  backlog counters. Session `ended_at` remained null. The query contained
  exactly two attempts and one completed cleanup obligation for this job.

Both attempts retained execution generation 1; the fencing generation advanced
from 1 to 2. This is a retry within the same execution, not a new submission or
execution generation. No systemd restart or manual cleanup was needed.

This passes the observed active lease-loss, physical cleanup, same-session
recovery, cleanup-gated single retry, and replacement completion path. It does
not qualify injected duplicate acknowledgements, concurrent successor races,
provider replay, or a universal teardown timing bound. The repeated run's
validation verdict and artifact contents were not included in the supplied SQL.

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
v34 is closed as superseded. Its unfinished `0080`, `0027`, and `0039` checks
now belong to [v35's carried qualification](../iterations/v35-seamless-fleet-lifecycle.md#carried-qualification),
with their existing item statuses preserved. Lifecycle implementation comes first;
closure requires the carried evidence as well.

| Remaining gate | Owner and next checkpoint |
| ---- | ---- |
| Raw evidence completion | v35 carried qualification: retrieve benchmark submission/attempt/provider identities, exact deployed release/image/tool versions, and artifact checksums; retain host reports and operator sign-off before final qualification |
| Direct S3 ceremony | v35 carried qualification: record put/HEAD/get/checksum/delete before final qualification; existing artifact reads are partial evidence |
| Slack benchmark journey | v35 carried qualification: one small natural-language benchmark with one canonical report and cleanup evidence; not proven by Slack parser tests |
| Fine benchmark progress (`0027`) | v35 carried qualification: inspect live guest-to-provider progress and archived JSONL, then calibration/repeat/debounce/fallback cases |
| Integrated comparison (`0039`) | v35 carried qualification: two refs on one compatible snapshot/worker, then clean repeats and per-variant calibration/DB carry-forward; combine restart portion with v35 |
| Full-history validation | v35 carried qualification: schedule an explicit maintenance window and retain complete coverage/reduction/artifact/cleanup evidence before final qualification |
| Post-terminal daemon restart, worker loss, cleanup recovery | October 15 restart checks and October 16 corrected active lease-loss/cleanup/retry passed above; provider replay and the remaining v35 Phases 2–3/7 failure matrix stay open |
| Identity rotation/revocation, capability/profile changes, policy persistence | Rescheduled to v35 Phases 6–7; retain the original v34 Phase 8 acceptance checks |
| Previous-protocol runtime qualification | The deployed `0f18950` schema/digests/hash and source-pinned local TLS/RPC probes pass; real-task mixed-cohort and host qualification remain open. Operator approved backup/restore-only daemon rollback on 2026-10-17, preferring roll-forward; host restore/reconciliation remains unqualified |
| Second physical host | Explicitly deferred from v34; commission by v35 Phase 6's two-worker maintenance gate, then use v36's snapshot-bootstrap contract; no multi-machine claim yet |
| Validation initialization/progress distinction | Locally implemented and tested, including conservative processing-only ETA; live guest/provider qualification remains open. Retain the scan delay as v36 timing input, not a measured placement model |

## Next Implementation Checkpoint

The first v35 implementation now has local evidence for conservative lease
deadlines, resident session/cleanup recovery, and process-local daemon shutdown.
See the [failure matrix and validation record](../design/0082-fleet-lifecycle-failure-matrix.md#local-implementation--2026-10-14).
The October 15 restart checks and October 16 corrected lease-expiry repeat now
provide host evidence for idle recovery, active-attempt continuity, and resident
cleanup-gated retry. Audited scheduler pause/resume (v35 Phase 4) is now locally
implemented, not deployed or host-qualified. Negotiation, resident maintenance,
audited abort and preflight provenance are also implemented locally, with
production-store races, source-pinned previous/current worker probes, real
daemon process signals and local MinIO recovery tests. Validation initialization
and conservative ETA reporting now have local coverage. Latest full nextest run
`911b6e1d-cff3-4cfb-8829-97e4dcbd5869`: 1,107 passed, 26 skipped; build and lint
passed. The [failure matrix](../design/0082-fleet-lifecycle-failure-matrix.md)
records scope and limitations of each local evidence set.

Next: consolidated release review and a verified recovery package, then
operator-run server-first host qualification. The old migrator rejects the new
ledger. On 2026-10-17 the operator separately approved backup/restore-only daemon
rollback, preferring roll-forward fixes. Restore must include the matching
database, binaries and configuration; post-backup work/provider effects need
explicit reconciliation. Local isolated restore passed, but the host drill and
sign-off remain open. The earlier approval of `0f18950` as the previous worker
contract still excludes incompatible pre-v33 layouts; worker compatibility is
not relaxed by the daemon recovery decision.

The second-worker VM location is awaiting operator selection. A same-host VM
can qualify multi-worker function but does not close the second-physical-host
gate. Provider/progress/comparison, identity-policy and production rollback
ceremonies remain open. No pending local release has been deployed by the agent.

Decision 0006 was accepted on 2026-10-14; Decision 0007 remains draft.
Local v35 tests remain separate from the bounded host evidence above; v35 as a
whole is not qualified. Neither a canonical ledger nor cross-worker validation
has been demonstrated.
