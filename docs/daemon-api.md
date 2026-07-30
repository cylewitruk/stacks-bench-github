# Daemon API

`sbgh-daemon` exposes two independent network surfaces:

- the operator and webhook API under `/api`, authenticated with bearer tokens;
- the `sbgh.fleet.v1.WorkerFleetService` protobuf/gRPC control plane on a separate
  HTTP/2, TLS 1.3 mutual-X.509 listener.

This document covers `/api`. Worker wire messages live in `sbgh-proto`,
transport-neutral fleet values live in `sbgh-fleet`, and workers do not use
operator cookies.

## Topology

The API is intended only for the daemon host and the local Docker bridge:

```text
sbgh-handler ── ingest token ──┐
sbgh-cli ───── admin cookie ───┼──> sbgh-daemon ──> PostgreSQL
```

Bind loopback for `sbgh-cli` and the Docker bridge gateway for
`sbgh-handler`. Do not expose `/api` on a public interface. The daemon is the
sole database client and applies pending migrations before it begins serving.

The operator API uses HTTP/1.1 and JSON. Request and response types are defined
in `sbgh-api`, which also provides the typed Rust client used by the handler
and CLI. This does not describe the protobuf/gRPC worker listener.

## Authentication

Pass tokens as:

```text
Authorization: Bearer <token>
```

| Scope | Holder | Access |
| --- | --- | --- |
| `ingest` | webhook handler | `POST /api/webhooks` only |
| `read` | reserved scope | authenticated `GET` routes |
| `admin` | local operator CLI | all read and mutation routes |

Missing or invalid credentials return `401`. A valid token with insufficient
scope returns `403`.

The daemon creates a random admin cookie at `[api].cookie_path` on every boot,
owned by its service user and mode `0600`. `sbgh-cli` reads
`/etc/sbgh/daemon/.cookie` by default, so normal operator commands run as
`sbgh`:

```bash
sudo -u sbgh sbgh-cli status
```

The handler's static ingest token is supplied through
`SBGH_API_INGEST_TOKEN` to both the handler and daemon. Do not put it in TOML.
The authorization model contains a read-only scope, but the production
configuration does not currently provision a standalone read token. The admin
cookie satisfies all read routes.

`GET /api/health` is intentionally unauthenticated and returns only liveness.

## Routes

### Health and identity

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/health` | none | Liveness |
| `GET` | `/api/whoami` | `read` | Return the resolved caller scope |

### Webhooks

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `POST` | `/api/webhooks` | `ingest` | Persist one HMAC-verified GitHub delivery |
| `GET` | `/api/webhooks` | `read` | List inbox rows |

The handler forwards the raw JSON body and relevant `X-GitHub-*` headers after
verifying `X-Hub-Signature-256`. The daemon owns the supported-event allowlist.
An unsupported event returns `200` with `result: "ignored"` and is not stored.
Supported delivery IDs are idempotent:

```json
{ "result": "recorded", "id": 12345 }
```

or:

```json
{ "result": "duplicate" }
```

List filters are `event_type`, `status`, and `limit`. Payload bodies are not
returned.

### GitHub installations and identity resolution

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/installations` | `read` | List known App installations |
| `GET` | `/api/resolve?owner=O&repo=R` | `read` | Resolve a known slug to installation/repository IDs |

Resolution uses daemon-owned installation and repository state. It returns
`404` for unknown identities and `409` for a suspended installation.

### Installer and repository allowlists

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/installers` | `read` | List installer accounts |
| `POST` | `/api/installers` | `admin` | Add or re-enable an installer |
| `POST` | `/api/installers/disable` | `admin` | Soft-disable an installer |
| `GET` | `/api/repos` | `read` | List supported repository roots |
| `POST` | `/api/repos` | `admin` | Add or re-enable a repository root |
| `POST` | `/api/repos/disable` | `admin` | Soft-disable a repository root |

Human-readable GitHub logins and repository names are resolved by the daemon.
Numeric IDs remain available to the CLI as an emergency path.

### Execution policy

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/policies/target` | `read` | List target-repository policy |
| `POST` | `/api/policies/target` | `admin` | Allow or re-enable a target |
| `POST` | `/api/policies/target/disable` | `admin` | Soft-disable a target |
| `GET` | `/api/policies/source` | `read` | List source-repository trust |
| `POST` | `/api/policies/source` | `admin` | Allow or re-enable a source |
| `POST` | `/api/policies/source/disable` | `admin` | Soft-disable a source |
| `GET` | `/api/policies/triggers` | `read` | List push/tag triggers |
| `POST` | `/api/policies/triggers` | `admin` | Add a trigger |
| `POST` | `/api/policies/triggers/{id}/disable` | `admin` | Soft-disable a trigger |
| `POST` | `/api/policies/triggers/{id}/pin` | `admin` | Set or clear its binary-cache pin |

Target and source policy accepts `install_id` plus `repo_id`. The CLI's
`--on owner/repo` form resolves those values first.

Trigger kinds are `branch_push` and `tag_created`. Branch matching accepts an
exact branch or trailing `*` prefix glob; tag matching uses a regular
expression. Optional benchmark arguments are frozen when a matching event is
submitted.

### Users and roles

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/users` | `read` | List known GitHub users |
| `GET` | `/api/roles` | `read` | List role grants |
| `POST` | `/api/roles` | `admin` | Grant a role |
| `POST` | `/api/roles/revoke` | `admin` | Revoke a role |

Roles are:

- `admin`;
- `trigger_pr_benchmark`;
- `view_results`.

A grant may apply to an installation or one repository within it.

### Jobs and reports

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/jobs` | `read` | List jobs by status and limit |
| `GET` | `/api/submissions/{id}/report` | `read` | Read the canonical typed submission report |
| `POST` | `/api/jobs/block-validation` | `admin` | Submit block-validation demand |

The report response is the same provider-neutral snapshot used by GitHub and
Slack rendering. It contains aggregate identity/lifecycle, artifacts, and one
exhaustive tagged task detail:

```json
{
  "identity": {
    "submission_id": "…",
    "current_job_id": "…",
    "current_attempt_id": "…",
    "task_kind": "block_validation",
    "source": "cli",
    "repository": "owner/repo",
    "commit": "0123abcd"
  },
  "lifecycle": {
    "state": "completed",
    "phase": null,
    "completed_jobs": 1,
    "total_jobs": 1,
    "failure": null
  },
  "task": {
    "kind": "block_validation",
    "detail": {
      "requested_range": { "start": 100, "end": 200 },
      "observed_range": { "start": 100, "end": 200 },
      "verdict": "valid",
      "checked_blocks": 101,
      "chainstate_origin": "mainnet-2026-07-29",
      "invalid_blocks": []
    }
  },
  "artifacts": []
}
```

The `read` scope is daemon-wide trusted-operator access, not a
repository/tenant credential.

Block-validation submission requires:

```json
{
  "idempotency_key": "operator/request-123",
  "install_id": 1,
  "repo_id": 2,
  "commit": "40-or-64-character-hex-object-id",
  "worker_id": null,
  "epoch": "nakamoto",
  "range_start": 100,
  "range_end": 200,
  "requested_shards": 16,
  "max_concurrency": 8,
  "timeout_secs": 21600
}
```

`worker_id` is an optional placement constraint. Submission does not require a
connected worker. A successful response identifies the canonical
`submission_id`, disposition (`created` or `already_submitted`), and initial
job IDs. Reusing the same idempotency key for different executable demand
returns a conflict.

### Fleet operation

| Method | Path | Scope | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/fleet` | `read` | Worker/session/resource, lease, attempt, trace, and cleanup state |
| `GET` | `/api/fleet/metrics` | `read` | Prometheus text |
| `POST` | `/api/fleet/workers` | `admin` | Create a disabled, drained worker policy |
| `GET` | `/api/fleet/workers/{id}` | `read` | Inspect policy, session, and identity metadata |
| `PATCH` | `/api/fleet/workers/{id}` | `admin` | Update a drained worker policy or enable/disable it |
| `POST` | `/api/fleet/workers/{id}/identities` | `admin` | Validate and authorize a public P-256 identity key |
| `DELETE` | `/api/fleet/workers/{id}/identities/{identity}` | `admin` | Revoke an identity under normal lifecycle guards |
| `POST` | `/api/fleet/workers/{id}/emergency-disable` | `admin` | Withdraw worker authorization and expire its leases |
| `POST` | `/api/fleet/workers/{id}/identities/{identity}/emergency-revoke` | `admin` | Revoke an identity and expire sessions using it |
| `POST` | `/api/fleet/workers/{id}/drain` | `admin` | Set or clear durable drain |
| `POST` | `/api/fleet/jobs/{id}/cancel` | `admin` | Request cancellation |
| `POST` | `/api/fleet/submissions/{id}/recover` | `admin` | Create a fresh execution generation |

Recovery starts again at the submission's first specification/run. An optional
compatible worker constraint can pin the new generation; older results remain
auditable and do not enter the new comparison.

Worker creation, policy updates, and identity operations return canonical
worker detail. Identity requests contain one bounded P-256 `PUBLIC KEY` PEM.
The daemon canonicalizes its SPKI and returns only the lowercase SHA-256 digest
and timestamps. Private keys and certificate wrappers are never accepted.

Normal capability/profile/disable and final-identity changes require a
drained, quiescent worker. Emergency operations immediately withdraw
authorization and expire relevant leases; the existing expiry coordinator
then owns fencing, cleanup, and safe requeue.

## Error contract

Errors use:

```json
{ "error": { "code": "snake_case_code", "message": "human readable" } }
```

| Status | Meaning |
| --- | --- |
| `400` | Malformed or invalid request |
| `401` | Missing or invalid token |
| `403` | Insufficient scope |
| `404` | Unknown identity |
| `409` | State, constraint, or idempotency conflict |
| `502` | Required GitHub resolution failed |
| `503` | A required backing service is unavailable |

Webhook redelivery and idempotent administrative upserts are safe to retry.
Mutation callers that create executable demand must reuse a stable
idempotency key for the same logical request.

## CLI

`sbgh-cli` is the supported operator interface and mirrors the routes above:

```bash
sudo -u sbgh sbgh-cli --help
sudo -u sbgh sbgh-cli installer --help
sudo -u sbgh sbgh-cli repo --help
sudo -u sbgh sbgh-cli policy --help
sudo -u sbgh sbgh-cli user --help
sudo -u sbgh sbgh-cli jobs --help
sudo -u sbgh sbgh-cli fleet --help
```

Worker enrollment and identity commands print the canonical worker policy
as JSON so setup and rotation scripts can consume UUIDs, identity digests, and
timestamps without parsing log text.

The defaults are `http://127.0.0.1:8787` and
`/etc/sbgh/daemon/.cookie`; use global `--api-url` and `--cookie` only for a
deliberately different local deployment.
