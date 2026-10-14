# v37: Community Worker Validation Trust

Successor to [v36](v36-distributed-block-validation.md). Extend distributed
block validation from operator-trusted hosts to independently operated
community capacity without treating authenticated worker identity as proof of
correct execution.

> **Status:** planned — deliberately split from v36 so trusted-fleet
> distribution can ship and provide real evidence before trust/adjudication
> policy is finalized.

## Items

| Item | Role | Status |
| ---- | ---- | ------ |
| `0086-community-worker-validation-trust` | primary: result authority, corroboration, and discrepancy handling | planned |

## 0086 — Community Worker Validation Trust

- **id:** `0086-community-worker-validation-trust`
- **status:** `planned`
- **priority:** `medium`
- **depends_on:** `0085-distributed-block-validation-orchestration`
- **source:** [Decision 0005](../decisions/0005-public-key-worker-identities.md),
  [Decision 0007](../decisions/0007-daemon-owned-distributed-validation-plans.md)

**Problem:** Authentication proves which enrolled worker submitted a result,
not that an independently operated host executed faithfully. A community worker
must not unilaterally determine an authoritative verdict merely because its
snapshot tip is canonical and its key is enrolled.

**Scope:** Add explicit worker trust tiers and versioned result-authority
policy. Bind typed shard-result digests to worker, canonical generation, plan,
shard, execution slices, attempt, and fence. Support configurable trusted
execution, redundant validation, deterministic sampling, discrepancy
quarantine, adjudication, and operator audit while preserving useful progress
from non-authoritative capacity.

**Acceptance:** An untrusted worker cannot unilaterally determine a submission
verdict; corroborating results converge deterministically; disagreements retain
all evidence, quarantine affected workers/ranges, and fail closed; trust-policy
changes are audited; and operator/provider output distinguishes execution from
authoritative acceptance.

**Deferred / non-goals:** No remote hardware attestation, token/reward system,
anonymous enrollment, or claim of Byzantine fault tolerance beyond the stated
corroboration policy.

## Design Rules

- Identity authenticates evidence; policy determines whether it is
  authoritative.
- Canonical snapshot compatibility remains necessary but never sufficient for
  result trust.
- Every policy decision is bound to an immutable policy generation recorded on
  the shard allocation and result.
- Redundant placement uses independent workers and failure domains; two results
  from the same operator are not independent corroboration unless policy says
  so explicitly.
- Disagreement never resolves by arrival order or majority among an unbounded
  cohort. It enters a typed, durable adjudication state.
- A trust-policy change affects future allocations only. Existing evidence
  remains interpretable under the generation that admitted it.

## Persistence and Migration

- Add worker trust tier, operator/failure-domain identity, and audited policy
  generation.
- Add typed result attestations/digests and corroboration relationships keyed by
  canonical generation, plan shard, execution slices, attempt, and fence.
- Add durable discrepancy, quarantine, adjudication, and release records.
- Keep v36 trusted-worker semantics as the fail-closed default until a current
  policy explicitly admits community evidence.

## Phases

### Phase 1: Threat Model and Authority Policy

- [ ] Define attacker, compromise, collusion, replay, withholding, and
  availability assumptions.
- [ ] Pin trust tiers, independence domains, sampling/redundancy rules, and
  deterministic terminal states.
- [ ] Review Decision 0007 consequences against real v36 evidence.
- [ ] Reviewed (Codex)

### Phase 2: Typed Evidence and Policy Generation

- [ ] Bind result digests to every semantic and fencing identity.
- [ ] Persist audited trust policy and allocation-time policy generation.
- [ ] Prove replay, stale-fence, cross-generation, and cross-shard substitution
  are rejected.
- [ ] Reviewed (Codex)

### Phase 3: Corroboration and Discrepancy Workflow

- [ ] Implement redundant/sampled placement and deterministic reduction.
- [ ] Implement quarantine and operator adjudication without deleting evidence.
- [ ] Project authoritative, pending-corroboration, and disputed states clearly
  to CLI/GitHub/Slack.
- [ ] Reviewed (Codex)

### Phase 4: Community Fleet Qualification

- [ ] Enroll at least one independently operated worker with a canonically
  verified snapshot.
- [ ] Prove matching corroboration, injected disagreement, worker loss,
  cancellation, and policy-generation rollover.
- [ ] Confirm community evidence cannot change a verdict outside policy.
- [ ] Validate cleanup, artifacts, audit rows, and provider convergence.

## Final Validation

v37 is complete when community capacity contributes useful distributed
validation evidence while no independently operated worker can unilaterally
produce an authoritative verdict, and both agreement and disagreement converge
through a deterministic, audited policy.
