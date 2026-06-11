# Index

The single registry of every planning artifact: its stable **ID**, current
**status**, and current **location**. This is the one mutable lookup table —
other docs reference an item by its stable ID and resolve it here, so they never
need editing when an item's status or location changes.

> **Maintenance rule.** Update this table whenever an item changes status or
> moves files. It is the one place that *must* change on every transition.
> `Type` is `item` (`NNNN`), `decision` (`000N`), or `iteration` (`vN`).
> **Item/iteration** statuses come from the [README](README.md#statuses);
> **decision** statuses (`draft`/`accepted`/`superseded`) come from
> [decisions/README](decisions/README.md). Item and decision namespaces are
> independent, so `0001` exists as both an item and a decision — `Type`
> disambiguates.

## Items

| ID | Name | Status | Location |
| ---- | ---- | ------ | -------- |
| `0001-artifact-store` | Artifact store | `shipped` | [archive/completed/0001-artifact-store.md](archive/completed/0001-artifact-store.md) |
| `0002-slack-adhoc-profiling` | Slack ad-hoc profiling | `shipped` | [archive/completed/0002-slack-adhoc-profiling.md](archive/completed/0002-slack-adhoc-profiling.md) |
| `0003-results-portal` | Results portal | `backlog` | [backlog.md](backlog.md) |
| `0004-worker-fleet` | Worker fleet (`remote-daemon`) | `backlog` | [backlog.md](backlog.md) |
| `0005-task-kind-platform` | Task-kind platform | `backlog` | [backlog.md](backlog.md) |
| `0006-aws-cloud-backend` | AWS / cloud backend | `parked` | [backlog.md](backlog.md) |
| `0007-check-run-surface` | Check-run surface | `shipped` | [archive/completed/0007-check-run-surface.md](archive/completed/0007-check-run-surface.md) |
| `0008-execution-architecture` | Execution architecture | `shipped` | [archive/completed/0008-execution-architecture.md](archive/completed/0008-execution-architecture.md) |
| `0009-change-impact-reporting` | Change-impact reporting | `shipped` | [archive/completed/0009-change-impact-reporting.md](archive/completed/0009-change-impact-reporting.md) |
| `0010-driver-seam` | Task-agnostic `Driver` seam | `shipped` | [archive/completed/0010-driver-seam.md](archive/completed/0010-driver-seam.md) |
| `0011-job-pipeline-cutover` | Job pipeline + v2 cutover | `shipped` | [archive/completed/0011-job-pipeline-cutover.md](archive/completed/0011-job-pipeline-cutover.md) |
| `0012-api-fronted-daemon` | API-fronted daemon | `shipped` | [archive/completed/0012-api-fronted-daemon.md](archive/completed/0012-api-fronted-daemon.md) |
| `0013-drop-legacy-jobs-table` | Drop legacy `jobs` table | `backlog` | [backlog.md](backlog.md) |
| `0014-preclaim-placeholder-checks` | Pre-claim placeholder checks | `backlog` | [backlog.md](backlog.md) |
| `0015-resource-aware-admission` | Resource-aware admission | `backlog` | [backlog.md](backlog.md) |
| `0016-db-enforced-same-sha-dedup` | DB-enforced same-SHA dedup | `parked` | [backlog.md](backlog.md) |
| `0017-generic-phase-events` | Generic phase-event enum | `backlog` | [backlog.md](backlog.md) |
| `0018-auto-rerun-confidence-gate` | Auto-rerun confidence gate | `parked` | [backlog.md](backlog.md) |
| `0019-block-validation-recipe` | Block-validation recipe | `backlog` | [backlog.md](backlog.md) |
| `0020-llm-intent-resolution` | LLM intent resolution (Slack) | `backlog` | [backlog.md](backlog.md) |
| `0021-slack-live-timeline` | Slack live timeline | `in_progress` | [iterations/v6-slack-live-timeline.md](iterations/v6-slack-live-timeline.md) |

## Iterations

| ID | Name | Status | Location |
| ---- | ---- | ------ | -------- |
| `v4-artifact-store` | Artifact store | `shipped` | [archive/completed/0001-artifact-store.md](archive/completed/0001-artifact-store.md) |
| `v5-slack-adhoc-profiling` | Slack ad-hoc profiling | `shipped` | [archive/completed/0002-slack-adhoc-profiling.md](archive/completed/0002-slack-adhoc-profiling.md) |
| `v6-slack-live-timeline` | Slack live timeline | `in_progress` | [iterations/v6-slack-live-timeline.md](iterations/v6-slack-live-timeline.md) |

## Decisions

| ID | Name | Status | Location |
| ---- | ---- | ------ | -------- |
| `0001-artifact-urls-s3-only` | Artifact URLs are S3-only | `accepted` | [decisions/0001-artifact-urls-s3-only.md](decisions/0001-artifact-urls-s3-only.md) |
| `0002-artifact-refs-are-store-keys` | Artifact refs are store keys | `accepted` | [decisions/0002-artifact-refs-are-store-keys.md](decisions/0002-artifact-refs-are-store-keys.md) |
| `0003-artifact-export-failure-not-benchmark-failure` | Export-fail ≠ bench-fail | `accepted` | [decisions/0003-artifact-export-failure-not-benchmark-failure.md](decisions/0003-artifact-export-failure-not-benchmark-failure.md) |

> Migration complete: every `docs/roadmap-vN.md` is a tombstone. `0002`/`0003`/
> `0004` design now lives in `design/000N-*.md`; they promote to an iteration when
> selected.
