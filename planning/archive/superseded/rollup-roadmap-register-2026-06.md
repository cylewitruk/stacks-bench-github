# Roadmap register (pre-planning-system)

- **rollup:** `roadmap-register-2026-06`
- **status:** `superseded`
- **archive kind:** historical roadmap register
- **current register:** [planning/index.md](../../index.md),
  [planning/backlog.md](../../backlog.md), [planning/iterations/](../../iterations/)

> **Superseded by the planning system.** The `docs/roadmap-vN.md` docs were the
> pre-planning-system roadmaps — each mixed durable decisions, design, phased
> execution, and status into one file (which is why they read as a backlog). They
> were migrated into `planning/`: shipped work → `archive/completed/`; active work
> → `backlog.md` + `design/`; durable rules → `decisions/`. The old `docs/` files
> are tombstones. This file is the crosswalk.

## Crosswalk (old doc → planning item(s))

| Old doc | Was | Migrated to |
| ---- | ---- | ---- |
| `roadmap-v2` | inbox/job pipeline + v2 schema cutover | `archive/completed/0011` (+ `0013`) |
| `roadmap-v3` | API-fronted daemon + role/config collapse | `archive/completed/0012` (+ `0013`) |
| `roadmap-v4` | check-run surface | `archive/completed/0007` (+ `0014`) |
| `roadmap-v5` | execution architecture | `archive/completed/0008` (+ `0014`/`0015`/`0016`/`0017`) |
| `roadmap-v6` | multi-task platform + block validation | `design/0005` (platform) + `design/0019` (recipe) |
| `roadmap-v7` | change-impact reporting | `archive/completed/0009` (+ `0018`) |
| `roadmap-v8` | Driver seam + AWS backend | `archive/completed/0010` (Phase 1) + `0006` (cloud, parked) |
| `roadmap-v9` | worker fleet | `design/0004-worker-fleet.md` (`0004`) |
| `roadmap-v10` | Slack ad-hoc profiling | `design/0002-slack-adhoc-profiling.md` (`0002`) |
| `roadmap-v11` | results portal | `design/0003-results-portal.md` (`0003`) |
| `roadmap-v12` | artifact store | `0001` / iteration `v1-artifact-store` (converted) |
| `block-validation-taskspec` | block-val seam sketch | `design/0019` |

## Notes

- **All twelve old docs are tombstones.** v9/v10/v11 were converted to
  `design/0004`/`0002`/`0003` (their backlog items); shipped work to
  `archive/completed/`; v6 to `design/0005`+`0019`. `backlog.md` + `index.md` are
  the roadmap now; the `docs/roadmap-vN.md` tombstones are slated for deletion.
- v8's cloud-phase text that v9 superseded is historical; `0006` (parked) carries the
  live intent, gated on cost/variance/hydration data.
- Durable architectural rules from these roadmaps are catalogued as ADR-extraction
  candidates in [decisions/README](../../decisions/README.md); the artifact-store
  ones (`0001`–`0003`) are already extracted.
