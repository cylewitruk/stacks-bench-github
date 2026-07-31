# v32: Task-Aware Slack Submission

Successor to
[v31](../archive/completed/0077-worker-identity-and-config-simplification.md).
Resolve authorized Slack natural language into typed benchmark or
block-validation intent and submit either task through the existing
task-submission kernel.

> **Status:** in_progress — local implementation and validation are complete;
> real Slack/provider/worker canaries remain deployment gates.
>
> v32 is creation-only. It makes intent resolution and Slack intake task-aware
> without adding cancel, restart, replace, scheduling, or worker-selection
> behavior. Slack lifecycle controls remain in
> [`0070`](../backlog.md).

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0069-task-aware-intent-resolution` | primary: provider-neutral typed intent | in_progress |
| `0078-slack-task-submission` | co-primary: Slack benchmark/validation creation | in_progress |

## 0069 — Task-Aware Intent Resolution

- **id:** `0069-task-aware-intent-resolution`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0020-llm-intent-resolution`,
  `0059-intent-resolution-boundary`
- **relates_to:** `0036-pr-comment-llm-intent`,
  `0064-task-submission-kernel`, `0065-job-lifecycle-controls`
- **unblocks:** `0036-pr-comment-llm-intent`,
  `0078-slack-task-submission`
- **source:** block-validation user-story audit and v32 sequencing review
  (2026-07)

**Problem:** `sbgh-intent` is provider-separated but semantically
benchmark-only. `IntentOutcome::Resolved` contains `BenchmarkRequest`, the
OpenAI prompt speaks only about benchmarks, and the structured schema cannot
represent block-validation creation.

**Scope:** Replace the benchmark-only resolved outcome with a
provider-neutral, strictly validated creation intent. Use exhaustive task
variants rather than `TaskKind` plus optional task fields. Preserve the
existing deterministic benchmark parser and add a deterministic validation
form before provider fallback. Extract source selectors but leave
authorization, reference resolution, immutable commit freezing, and execution
policy to daemon application services.

**Acceptance:**

- Deterministic and provider-backed input converge on the same typed creation
  intent for both benchmarks and block validation.
- Benchmark behavior, comparison requests, limits, invalid diagnostics, and
  existing fixtures remain compatible.
- Ambiguous action/task/source requests fail closed rather than guessing.
- Provider JSON cannot select a worker or choose shard, concurrency, timeout,
  scheduling, persistence, or reporting behavior.

**Deferred / non-goals:** No lifecycle actions, agent tools, autonomous job
enumeration, provider-issued side effects, or provider-owned authorization.

## 0078 — Slack Task Submission

- **id:** `0078-slack-task-submission`
- **status:** `in_progress`
- **priority:** `high`
- **depends_on:** `0064-task-submission-kernel`,
  `0066-task-aware-reporting`, `0069-task-aware-intent-resolution`
- **relates_to:** `0067-github-block-validation-submission`,
  `0070-slack-block-validation-controls`
- **unblocks:** `0070-slack-block-validation-controls`
- **source:** Slack natural-language block-validation requirement (2026-07)

**Problem:** Slack intake, its narrow queue port, request summaries, and
submission adapter are benchmark-specific. An authorized user can ask the LLM
for a benchmark but cannot create block-validation demand without another
surface.

**Scope:** Make Slack creation task-aware. Resolve the configured repository
and requested revision to a full immutable commit in daemon composition,
select server-owned block-validation policy, and submit through `0064`. Reuse
the existing request-stable reporting identity, reaction lifecycle, and
snapshot reconciliation. Make benchmark and block-validation creation
independently authorizable, with unauthorized callers rejected before any
provider call when they have no task entitlement.

**Acceptance:**

- Authorized natural-language benchmark and block-validation requests each
  create exactly one immutable submission and one canonical Slack snapshot.
- Socket Mode redelivery and daemon restart reuse the same producer/reporting
  identity and do not duplicate work or messages.
- Mutable, abbreviated, missing, or ambiguous revisions are resolved or
  rejected by the daemon; model text never becomes a commit directly.
- Slack and intent crates perform no SQL, worker selection, GitHub side effect,
  or task-plan persistence.

**Deferred / non-goals:** No cancel/restart/replace (`0070`), arbitrary
repository selection, App Home controls, watched-ref automation, or GitHub
natural-language intake.

## Outcome

```text
authorized Slack mention
        |
        v
deterministic task parser
        |
        +-- no match --> sbgh-intent provider
                           |
                           v
                 validated UserIntent::Create
                    |                 |
                    v                 v
              Benchmark          BlockValidation
                    \                 /
                     v               v
               daemon source + policy resolution
                           |
                           v
                  task-submission kernel
                           |
                           v
                 pull scheduler / worker fleet
```

The LLM classifies and extracts bounded user intent. It does not authorize,
resolve, schedule, or execute work.

## Design Rules

- **Use an exhaustive intent sum type.** The application contract is shaped as
  `UserIntent::Create(TaskCreationIntent)`, with distinct `Benchmark` and
  `BlockValidation` variants. Do not use a task discriminator beside a bag of
  nullable fields.
- **Keep model DTOs at the provider edge.** Strict model-facing JSON may
  represent invalid or incomplete output. It must validate and convert before
  becoming `UserIntent`; it never reaches persistence or a surface adapter
  directly.
- **Keep creation separate from lifecycle mutation.** v32 accepts submission
  intent only. Text resembling cancel, restart, replace, or supersede is
  rejected with a bounded diagnostic. `0065`, `0072`, and `0070` own those
  operations.
- **Authorize before spending where the task is known.** Workspace membership
  and the union of task creation permissions are checked before an LLM call.
  The resolved task is then checked against its specific permission before any
  source resolution or submission. A user entitled to one task may spend a
  rate-limited provider call that resolves to the other task, but cannot submit
  it; a user entitled to neither task causes no provider call.
- **Parse deterministic forms first.** Existing benchmark flag syntax remains
  the fast path. Add an explicit validation grammar so scripts and precise
  users do not require a provider.
- **Freeze source identity in the daemon.** Intent carries only an optional
  repository/revision selector. Daemon composition restricts it to configured
  policy, resolves it through GitHub, and freezes the full commit before
  constructing a submission command.
- **Keep execution policy server-owned.** The intent may select the default
  validation plan or provide a bounded epoch/range override allowed by policy.
  Worker placement, shard count, concurrency, timeout, and fleet capability
  remain application policy.
- **One block-validation submission service.** Move validation execution
  defaults out of the GitHub namespace into task-owned daemon policy. Existing
  GitHub and new Slack adapters feed the same validation planner/kernel
  boundary; neither reconstructs payload defaults.
- **Preserve idempotency at the aggregate.** Slack's opaque reporting identity
  remains the producer key. A retry returns the canonical receipt; conflicting
  content under the same identity fails closed.
- **Render snapshots from typed state.** The immediate queued view and durable
  reporter use task-aware data. Block validation must not call benchmark
  comparison, metric, or profiler paths.
- **Keep integration ownership narrow.** `sbgh-slack` owns Slack orchestration
  and a consumer-defined submission port. Daemon composition owns GitHub
  resolution and application policy. `sbgh-intent` owns extraction and
  validation. `sbgh-core` continues to own submission contracts.
- **Do not invent a generic workflow engine.** Two explicit task variants are
  enough. Adding a future task is a compiler-checked enum extension, not a
  registry, plugin system, or untyped map.

## Stable Intent Shape

The internal contract is conceptually:

```rust
enum UserIntent {
    Create(TaskCreationIntent),
}

enum TaskCreationIntent {
    Benchmark(BenchmarkRequest),
    BlockValidation(BlockValidationIntent),
}

struct BlockValidationIntent {
    source: RequestedSource,
    selection: ValidationSelection,
}

enum ValidationSelection {
    DefaultPlan,
    Range {
        epoch: ValidationEpochIntent,
        start: u64,
        end: u64,
    },
}
```

`RequestedSource` contains only user-facing repository/revision text. It is not
a resolved commit and carries no installation or database identity.

The exact provider JSON can differ where Structured Outputs requires a
schema-friendly representation, but conversion must enforce:

- exactly one task variant;
- one creation action;
- benchmark-only fields absent for validation and validation-only fields
  absent for benchmarks;
- non-empty bounded selectors;
- inclusive ranges with `start <= end`;
- known validation epochs only;
- existing benchmark repetition/comparison bounds; and
- no unknown fields.

## Stable Slack Grammar

Existing benchmark flags remain accepted. Add a deterministic validation form:

```text
validate [--rev <ref>]
         [--epoch pre-nakamoto|nakamoto --start-at <height> --end-at <height>]
```

Omitting all range fields selects the configured default validation plan.
Providing any range field requires all three. Partial ranges fail before an
LLM call.

Natural-language examples include:

```text
@BenchBot benchmark block 185700 on abc123
@BenchBot compare block 185700 between abc123 and def456
@BenchBot run block validation on commit abc123
@BenchBot validate Nakamoto blocks 185700 through 186000 on abc123
```

“Validate this change,” a bare “run it,” mixed benchmark/validation wording,
or lifecycle wording is invalid unless the task and required source/selection
are unambiguous under configured defaults.

## Shared Block-Validation Policy

The daemon currently stores execution defaults under
`[github.block_validation]`, even though they are task policy. v32 moves them
to one task-owned configuration used by all producers:

```toml
[tasks.block_validation]
default_epoch = "nakamoto"
default_range_start = 185630
default_range_end = 186000
requested_shards = 32
max_concurrency = 24
timeout_secs = 86400
allow_range_override = true
```

This is request policy, not worker capacity or chainstate identity. Workers
still advertise local maxima and may decline an incompatible offer before
acceptance. The scheduler chooses a compatible worker.

The parser requires the entire default plan when block validation is enabled.
Unknown, partial, inverted, protocol-invalid, or over-policy ranges fail at
daemon startup or before submission. The checked-in daemon example remains
parse-tested.

## Slack Authorization

Keep the existing benchmark allowlist and add an optional block-validation
override:

```toml
[slack]
allowed_team_ids = ["T0123ABCD"]
# Existing benchmark entitlement.
allowed_user_ids = ["U0123ABCD"]
# Optional. Omit to inherit allowed_user_ids; set [] to disable validation.
block_validation_user_ids = ["U0123ABCD"]
```

The effective benchmark set is `allowed_user_ids`. The effective validation set
is `block_validation_user_ids` when present, otherwise `allowed_user_ids`.
Their union is the pre-provider admission set. After resolution the exact task
set is authoritative. An allowed benchmark user may consume a bounded provider
call that resolves to validation, but cannot submit validation unless present
in its effective set.

This preserves the current single-operator configuration without duplicating a
user ID, while allowing an explicit validation-only list or explicit disable
when needed. Update the canonical example, optional environment-variable
projection, setup guide, and parse tests together.

## Phases

### Phase 1: Freeze Existing Benchmark Behavior

**Goal:** Establish a reviewable behavioral baseline before widening the
contract.

**Scope:**

- Record deterministic and provider-backed benchmark fixtures for singleton,
  block range, txids, repetitions, revision override, and comparison.
- Record Slack authorization, rate limit, redelivery, reaction, producer-key,
  queued snapshot, and submission receipt behavior.
- Identify all benchmark-shaped names at the intent/Slack seam.

**Status:**

- [ ] Baseline tests added or confirmed
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] Existing accepted benchmark inputs produce identical normalized
  `BenchmarkRequest` values.
- [ ] Existing invalid diagnostics remain bounded and do not expose provider
  errors.
- [ ] Existing Slack benchmark submission remains exactly-once.

### Phase 2: Introduce the Task-Aware Intent Contract

**Goal:** Make extraction provider-neutral and compiler-exhaustive.

**Scope:**

- Add `UserIntent`, `TaskCreationIntent`, `RequestedSource`, and typed
  block-validation selection.
- Change `IntentOutcome::Resolved` to carry `UserIntent`.
- Replace the benchmark-only provider DTO/schema conversion with task-aware
  strict validation.
- Preserve benchmark domain types rather than duplicating their semantics in a
  second intent hierarchy.

**Status:**

- [ ] Core implementation
- [ ] Unit/schema tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] Cross-task fields, unknown variants, partial ranges, zero/overflow
  values, ambiguous actions, and lifecycle actions fail closed.
- [ ] Model DTOs are not exported as application contracts.
- [ ] `sbgh-intent` remains independent of daemon, Slack, PostgreSQL, and
  generated fleet protocol types.

### Phase 3: Add Deterministic and Provider-Backed Resolution

**Goal:** Resolve both task kinds without making the provider authoritative.

**Scope:**

- Dispatch explicit benchmark and validation grammars before provider fallback.
- Update the OpenAI prompt and Structured Outputs schema for task-aware
  submission.
- Extend the eval corpus with benchmark/validation disambiguation, default and
  explicit validation plans, abbreviated revisions, ambiguous “validate,”
  malformed ranges, and destructive wording.
- Retain input size, timeout, per-user rate, logging, and error-redaction
  controls.

**Status:**

- [ ] Core implementation
- [ ] Unit/eval/provider-adapter tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] Natural-language benchmark and validation examples resolve to the
  expected typed variants.
- [ ] Deterministic requests make no provider call.
- [ ] Provider output cannot introduce raw CLI arguments or execution policy.

### Phase 4: Consolidate Validation Submission Policy

**Goal:** Give GitHub and Slack one task-owned validation planner.

**Scope:**

- Replace `GitHubBlockValidationConfig` with task-owned validation submission
  policy under `[tasks.block_validation]`.
- Add a daemon application service that validates plan selection, constructs
  `BlockValidationPayload`, and calls the task-submission kernel.
- Adapt the existing GitHub `/validate-blocks` queue without changing its
  explicit range behavior.
- Add Slack provenance/actor/producer-key inputs without coupling the service
  to Slack API types.

**Status:**

- [ ] Core implementation
- [ ] Unit/PostgreSQL/config tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] GitHub and Slack validation submission produce the same payload for the
  same resolved source and plan.
- [ ] The service performs no scheduling or worker selection.
- [ ] Submission plus provenance remains one transaction under `0064`.
- [ ] Existing `/validate-blocks` redelivery remains idempotent.

### Phase 5: Make Slack Creation Task-Aware

**Goal:** Submit either task from one Slack connector.

**Scope:**

- Replace `BenchmarkQueue` with a consumer-owned typed task-submission port.
- Make request summaries, queued snapshots, reactions, diagnostics, and
  logging exhaustive over both creation variants.
- Resolve configured repository/revision and freeze the full commit in daemon
  composition.
- Enforce task-specific creation permissions and reuse the existing reporting
  identity for idempotency/reconciliation.

**Status:**

- [ ] Core implementation
- [ ] Unit/integration tests
- [ ] Reviewed
- [ ] Validated

**Acceptance & Validation:**

- [ ] Authorized natural-language benchmark and validation mentions create
  their expected immutable submissions.
- [ ] A user authorized for only one task cannot submit the other.
- [ ] A caller with no task permission causes no provider, GitHub, database, or
  Slack side effect beyond the bounded rejection path.
- [ ] A partially authorized caller may consume only a rate-limited provider
  call before the task-specific check rejects the other task; no source
  resolution, submission, or public Slack message follows.
- [ ] Duplicate Socket Mode delivery returns/adopts the canonical receipt and
  snapshot without duplicate work.
- [ ] Block-validation queued/running/terminal snapshots use task-aware
  reporting and never enter benchmark-only result paths.

### Phase 6: Ratchets, Documentation, and Rollout Proof

**Goal:** Make the new boundary durable and operator-usable.

**Scope:**

- Update architecture, Slack setup, daemon setup/config examples, and command
  examples.
- Add source/DAG checks preventing benchmark-only intent/queue contracts from
  returning at the shared seam.
- Add checked-in config parse tests, validation-permission default/override
  tests, and stale-key rejection for the retired GitHub policy fields.
- Run a real Slack benchmark canary and block-validation canary.

**Status:**

- [ ] Documentation updated
- [ ] Ratchets added
- [ ] Workspace validation green
- [ ] Real Slack canaries recorded

**Acceptance & Validation:**

- [ ] Docs describe current task-aware behavior without lifecycle promises.
- [ ] `sbgh-slack` and `sbgh-intent` contain no SQL or provider credentials
  outside their owned adapters.
- [ ] The canonical daemon example loads and old policy keys fail loudly.
- [ ] Real Slack messages produce one benchmark and one block-validation
  submission, each reaching its correct report surface.

## Test Matrix

| Area | Required coverage |
| ---- | ----------------- |
| Intent validation | exhaustive task variants, cross-task field rejection, default/explicit validation selection, invalid diagnostics |
| Deterministic parsing | existing benchmark grammar, validation grammar, partial/ambiguous input |
| Provider adapter | strict schema, malformed output, task ambiguity, timeout/rate/input bounds, secret-safe errors |
| Policy | default plan, explicit override allow/deny, protocol limits, startup rejection |
| Source resolution | branch/tag/full/abbreviated commit, missing and ambiguous ref, configured repository restriction |
| Authorization | benchmark-only, validation-only, both, neither; zero-entitlement pre-provider rejection; partial-entitlement post-resolution rejection |
| Submission | benchmark group, validation singleton, provenance, digest conflict, retry/redelivery |
| Slack | acknowledgement/reaction, task-aware queued snapshot, search-before-create, unchanged snapshot suppression |
| Regression | benchmark comparison, cache gate, reporting, fleet dispatch, worker execution unchanged |

## Final Validation

- [ ] `just build --no-sccache`
- [ ] `just lint --no-sccache`
- [ ] `just test --summary --no-sccache`
- [ ] `git diff --check`
- [ ] Package metadata still matches the documented dependency graph.
- [ ] Existing benchmark deterministic/LLM/Slack golden behavior remains
  compatible except for the explicit authorization/config migration.
- [ ] Validation intent cannot choose scheduling, worker, resource, or
  credential fields.
- [ ] GitHub and Slack validation creation share one application planner and
  task-submission kernel.
- [ ] Socket Mode redelivery/restart cannot duplicate either task or its Slack
  report message.
- [ ] One real natural-language benchmark and one real natural-language
  block-validation request complete through Slack.

## Rollout

1. Configure `[tasks.block_validation]` with a bounded default plan. Leave
   `block_validation_user_ids` omitted to inherit the benchmark allowlist, or
   set an explicit validation list.
2. Deploy with LLM resolution disabled; verify deterministic benchmark and
   validation commands plus existing GitHub `/validate-blocks`.
3. Enable the configured provider and run invalid/ambiguous probes, confirming
   no work is submitted.
4. Run one natural-language benchmark and one natural-language validation
   canary from an authorized user.
5. Redeliver each Socket Mode envelope and restart the daemon; verify one
   submission and one Slack reporting message per request.

Rollback disables Slack/LLM intake and redeploys the prior binary/config. The
iteration adds no worker protocol or database schema migration.

## Deferred / Follow-Ups

- `0065` defines task-neutral cancel/restart/replace.
- `0072` projects lifecycle terminals that occur before a running attempt.
- `0070` exposes deterministic Slack lifecycle controls after those services
  exist.
- `0067` adds policy-backed GitHub `/validate`.
- `0036` reuses `UserIntent` for natural-language GitHub PR comments.
- `0068` adds watched-ref benchmark/validation fan-out.
