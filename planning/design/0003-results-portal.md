# Design 0003: Results portal (web UI + GitHub login)

- **id:** `0003-results-portal`
- **status:** `backlog`
- **depends_on:** `0001-artifact-store`
- **review:** Codex signed off (design)
- **source:** roadmap-v11

A permissioned **web portal** to browse benchmark runs, watch the queue, and
deep-inspect a run's profile — with **GitHub login** mapped to the users/roles
sbgh already tracks. It reads artifacts from the shared artifact store (`0001`),
which both this portal and the Slack surface (`0002`) consume.

**Goal:** a self-serve way to find a run, see the queue, and open its profiler
trace — **without porting** stacks-bench's complex trace-viewer. Reuse the
**upstream stacks-bench profiler-explorer** (version-matched, proxied behind the
portal's auth); the portal owns navigation/catalog/auth, not the trace UI. It
inherits the worker-fleet (`0004`) "orchestrator is sole DB client" discipline —
the portal is an API client, never a second DB client.

## Why

- **Discovery + navigation.** Today a run's results live as a local archive on
  the orchestrator. There's no way to browse runs, watch the queue, or share a
  profile link. A portal is the obvious home.
- **Deep profiling is already solved upstream.** `stacks-bench` ships a
  profiler-explorer (UI + backend API reading a run's SQLite directly). Re-use
  it; don't re-implement the hard part.
- **It shares a foundation with Slack.** Both want artifacts in object storage
  rather than orchestrator-local disk — build that layer once.

## The crux: reuse the upstream explorer, don't port the viewer

**Decision (proposed): reuse `stacks-bench`'s explorer, proxied + version-matched
— do not port its trace-viewer components into the portal.**

| | Port the viewer into the portal | Reuse upstream explorer (proposed) |
| --- | --- | --- |
| Trace/flamegraph UI | Re-own the complex viewer | Upstream maintains it |
| SQLite schema | Re-own schema-reading; track drift | Upstream owns it; evolves with stacks-bench |
| Version drift across runs | Must handle N schema versions in one viewer | **Version-matched** explorer per run — drift is a non-issue |
| Auth | Native (one app) | Portal **proxies** the (auth-less) explorer behind its session |
| UX | One unified app | Portal shell + embedded/proxied deep view |

The deciding factors are **schema ownership** and **version drift**: you already
archive the per-run `stacks-bench` binary *because* the schema changes across
runs, so a ported viewer would have to track every schema version. A
version-matched upstream explorer sidesteps that entirely. The auth objection to
reuse dissolves with proxying (the explorer binds localhost/private-net; the
portal reverse-proxies authenticated requests only).

**The split:** the **portal owns** GitHub login, the run catalog, the queue,
search/filter, per-run metadata, and artifact links (all simple, all *yours*).
For deep inspection it **hands off** to a per-run, version-matched explorer
instance.

## Architecture

- **Shared artifact store (the foundation) — [`0001`](../archive/completed/0001-artifact-store.md).**
  An `ArtifactStore` (local-FS today → **Hetzner object storage**, S3-compatible),
  shipping each run's bundle (SQLite, `run.json`, flamegraph, binary) on
  completion, with signed-URL fetch. **Free intra-Hetzner egress** keeps
  orchestrator → store → portal-machine cheap. The portal reads SQLite from it;
  it also backs [`0002`](../archive/completed/0002-slack-adhoc-profiling.md)'s Slack links. Built first, as
  its own slice.
- **Portal is an API client of the orchestrator, not a second DB client**
  (consistent with v9's "orchestrator is sole DB client"). The orchestrator
  exposes **read** endpoints (runs / queue / run-detail / artifact pointers);
  the portal backend consumes them. The portal **never touches Postgres**.
- **Auth: GitHub OAuth → existing roles → *scoped visibility*.** The portal
  backend runs the GH OAuth web flow, resolves the GH user, and maps to the
  users/roles sbgh already tracks. **Login is authentication, not authorization**
  (Codex): every run/artifact read is **scoped server-side to the repos/
  installations the user may see**, by role — a user never lists or fetches
  artifacts for repos they lack access to. Distinct from the operator `/api`
  cookie/token auth.
- **Separate, internet-facing machine.** The portal runs on a smaller Hetzner
  cloud server (it's public for GH login + serves the UI + fetches SQLite +
  proxies the explorer). The orchestrator stays **private and lean** — it's busy
  running benchmarks and shouldn't host a public user surface.
- **Per-run explorer launch.** On "open profiler", the portal backend fetches the
  run's SQLite from object storage, starts an **ephemeral explorer instance
  pinned to the run's `stacks-bench` version**, and reverse-proxies the user to
  it through the portal session; idle-timeout teardown.

```text
browser ──GH OAuth/session──▶ portal backend ──read API──▶ orchestrator (sole DB client)
                                   │  └─proxy──▶ ephemeral version-matched stacks-bench explorer
                                   └─fetch SQLite/flamegraph──▶ Hetzner object storage ◀──ship── orchestrator
```

## Phases (rough — backlog)

### Phase 1: Shared artifact store (object storage)

**Extracted to its own slice — [`0001`](../archive/completed/0001-artifact-store.md).** The
`ArtifactStore` (local + S3-compatible, ship-on-completion, signed URLs) is a
shared foundation for both this portal and v10, so it's tracked separately and
built **first**. v11 depends on it: the portal fetches a run's SQLite from object
storage via a signed URL.

### Phase 2: Orchestrator read-API

Read endpoints for runs / queue / run-detail / artifact pointers, with an auth
mode the portal can use (service token / mTLS). Keeps the DB behind the
orchestrator.

**Visibility is per-installation/repo/role, not just "authenticated" (Codex
Medium).** The portal is public-facing and artifacts can include **private-repo**
build output + profiler data, so "logged in" is *not* authorization. Every read
endpoint must **scope results to what the GitHub user may see** — filtered by
their installation/repo membership and role (the existing authz model), enforced
**server-side in the orchestrator**, not in the portal UI. A user only ever lists
/ opens / fetches artifacts for runs on repos they have access to; the
signed-URL/download path inherits the same check (no enumerating other repos'
artifacts by key).

### Phase 3: Portal backend + frontend

GH OAuth → existing roles; run list, queue view, run detail, artifact links from
object storage. The "navigation" MVP — useful before any deep profiler view.

### Phase 4: Profiler-explorer integration

Fetch the run SQLite, launch the version-matched ephemeral explorer, reverse-proxy
it behind the portal session. The deep-inspection payoff.

## Decisions (proposed)

1. **Reuse the upstream explorer, proxied + version-matched — don't port the
   viewer.** Schema ownership + cross-run version drift make a port a permanent
   fork; reuse makes drift a non-issue.
2. **One shared artifact store** (object storage) backs both Slack (v10) and the
   portal — build it once, first.
3. **Portal is an orchestrator API client, never a second DB client** (v9
   boundary discipline).
4. **GitHub OAuth maps to existing users/roles, and reads are visibility-scoped**
   — login authenticates; the orchestrator enforces per-installation/repo/role
   visibility on every run/artifact read (private-repo data is at stake).
5. **Separate internet-facing machine**; the orchestrator stays private/lean.

## Open questions

1. **Explorer packaging + drift.** Can the explorer be pointed at an arbitrary
   SQLite path/URL? Does it tolerate older DBs, or do we genuinely need
   version-matched ephemeral instances (the per-run-binary archive suggests yes)?
   How is it built/distributed for the portal machine to launch?
2. **Portal ↔ orchestrator transport/auth.** Service token vs. mTLS vs.
   private-network-only; how much run metadata comes from the live API vs. an
   index shipped to object storage.
3. **Artifact retention/lifecycle** in object storage (and who serves the Slack
   links — same store).
4. **Does this want the v9 fleet first?** No hard dependency — it reads
   results, which exist regardless of where execution ran. But the artifact store
   (Phase 1) should land before v10 Phase 4.

## Relationship to the other roadmaps

- **Shares the artifact store ([`0001`](../archive/completed/0001-artifact-store.md)) with v10** —
  build v12 first; both this portal and v10's Slack links consume it.
- **Independent of v6 and v9** — it reads results, agnostic to task kind or
  execution backend. Rides the v8 artifact seam.
- **Inherits v9's boundary discipline** — orchestrator owns the DB; the portal,
  like a worker, talks to it over an API rather than reaching into Postgres.
