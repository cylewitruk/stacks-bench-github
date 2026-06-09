# Index

The single registry of every planning artifact: its stable **ID**, current
**status**, and current **location**. This is the one mutable lookup table —
other docs reference an item by its stable ID and resolve it here, so they never
need editing when an item's status or location changes.

> **Maintenance rule.** Update this table whenever an item changes status or
> moves files. It is the one place that *must* change on every transition.
> Statuses come from the [README](README.md#statuses) vocabulary; `Type` is
> `item` (`NNNN`), `decision` (`000N`), or `iteration` (`vN`).

| ID | Name | Type | Status | Location |
| ---- | ---- | ---- | ------ | -------- |
| `0001-artifact-store` | Artifact store | item | `candidate` | [backlog.md](backlog.md) |
| `0002-slack-adhoc-profiling` | Slack ad-hoc profiling | item | `candidate` | [backlog.md](backlog.md) |
| `0003-results-portal` | Results portal | item | `backlog` | [backlog.md](backlog.md) |
| `0004-worker-fleet` | Worker fleet (`remote-daemon`) | item | `backlog` | [backlog.md](backlog.md) |
| `0005-block-validation` | Block validation (2nd task kind) | item | `backlog` | [backlog.md](backlog.md) |
| `0006-aws-cloud-backend` | AWS / cloud backend | item | `parked` | [backlog.md](backlog.md) |
| `0007-check-run-surface` | Check-run surface | item | `shipped` | [docs/roadmap-v4.md](../docs/roadmap-v4.md) |
| `0008-execution-architecture` | Execution architecture | item | `shipped` | [docs/roadmap-v5.md](../docs/roadmap-v5.md) |
| `0009-change-impact-reporting` | Change-impact reporting | item | `shipped` | [docs/roadmap-v7.md](../docs/roadmap-v7.md) |
| `0010-driver-seam` | Task-agnostic `Driver` seam | item | `shipped` | [docs/roadmap-v8.md](../docs/roadmap-v8.md) |

> `shipped` items still point at their `docs/roadmap-vN.md` (pre-planning-system);
> they move to `archive/completed/<id>.md` when converted, and this table updates
> then. No `decision` (`000N`) or `iteration` (`vN`) artifacts exist yet.
