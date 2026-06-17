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
| `0005-task-kind-platform` | Job-model decomposition + task-kind platform | `shipped` | [archive/completed/0005-task-kind-platform.md](archive/completed/0005-task-kind-platform.md) |
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
| `0020-llm-intent-resolution` | LLM intent resolution (Slack) | `shipped` | [archive/completed/0020-llm-intent-resolution.md](archive/completed/0020-llm-intent-resolution.md) |
| `0021-slack-live-timeline` | Slack live timeline | `shipped` | [archive/completed/0021-slack-live-timeline.md](archive/completed/0021-slack-live-timeline.md) |
| `0022-report-surface-trait` | Reporting-surface trait | `shipped` | [archive/completed/0022-report-surface-trait.md](archive/completed/0022-report-surface-trait.md) |
| `0023-slack-card-redesign` | Slack card redesign (live queue + rich results) | `shipped` | [archive/completed/0023-slack-card-redesign.md](archive/completed/0023-slack-card-redesign.md) |
| `0024-slack-card-stage-timings` | Slack card stage timings | `shipped` | [archive/completed/0024-slack-card-stage-timings.md](archive/completed/0024-slack-card-stage-timings.md) |
| `0025-baseline-binary-cache` | Release-baseline binary cache | `shipped` | [archive/completed/0025-baseline-binary-cache.md](archive/completed/0025-baseline-binary-cache.md) |
| `0026-central-block-index-cache` | Central block/tx index cache | `backlog` | [backlog.md](backlog.md) |
| `0027-fine-grained-progress` | Fine-grained bench progress | `backlog` | [backlog.md](backlog.md) |
| `0028-results-summary-restructure` | Results-summary restructure | `backlog` | [backlog.md](backlog.md) |
| `0029-per-block-timing-detail` | Per-block / per-tx timing detail | `backlog` | [backlog.md](backlog.md) |
| `0030-results-qa-agent` | Results Q&A agent | `backlog` | [backlog.md](backlog.md) |
| `0031-reusable-build-jobs` | Reusable build jobs (artifact + target axis) | `shipped` | [archive/completed/0031-reusable-build-jobs.md](archive/completed/0031-reusable-build-jobs.md) |
| `0032-supersede-stale-pr-head-runs` | Supersede stale PR-head benchmarks | `backlog` | [backlog.md](backlog.md) |
| `0033-slack-streamed-plan-updates` | Slack streamed plan updates | `shipped` | [archive/completed/0033-slack-streamed-plan-updates.md](archive/completed/0033-slack-streamed-plan-updates.md) |
| `0034-historical-stable-toolchain` | Historical stable toolchain resolution | `backlog` | [backlog.md](backlog.md) |
| `0035-slack-app-home-status` | Slack App Home status dashboard | `backlog` | [backlog.md](backlog.md) |
| `0036-pr-comment-llm-intent` | PR-comment LLM intent resolution | `backlog` | [backlog.md](backlog.md) |
| `0037-benchmark-group-run-model` | Benchmark group/run model | `shipped` | [archive/completed/0037-benchmark-group-run-model.md](archive/completed/0037-benchmark-group-run-model.md) |
| `0038-isolated-benchmark-repetitions` | Isolated benchmark repetitions | `in_progress` | [iterations/v15-isolated-benchmark-repetitions.md](iterations/v15-isolated-benchmark-repetitions.md) |
| `0039-multi-variant-benchmark-comparisons` | Multi-variant benchmark comparisons | `backlog` | [backlog.md](backlog.md) |
| `0040-slack-queue-receipt-before-stream` | Slack queue receipt before claimed stream | `backlog` | [backlog.md](backlog.md) |
| `0041-shared-benchmark-calibration` | Shared benchmark calibration pass | `backlog` | [backlog.md](backlog.md) |
| `0042-cache-hit-minimal-source-disk` | Cache-hit minimal source disk | `in_progress` | [iterations/v16-cache-hit-minimal-source-disk.md](iterations/v16-cache-hit-minimal-source-disk.md) |
| `0043-slack-plan-ts-race` | Slack plan-ts race fix (double card) | `in_progress` | [iterations/v17-slack-reporting-robustness.md](iterations/v17-slack-reporting-robustness.md) |
| `0044-slack-reaction-lifecycle` | Slack reaction lifecycle (ack/queued/running) | `in_progress` | [iterations/v17-slack-reporting-robustness.md](iterations/v17-slack-reporting-robustness.md) |
| `0045-slack-llm-observability` | Slack/LLM observability logging | `in_progress` | [iterations/v17-slack-reporting-robustness.md](iterations/v17-slack-reporting-robustness.md) |
| `0046-slack-reaction-state-from-api` | Read reaction state via `reactions.list` instead of brute-force removal | `backlog` | [backlog.md](backlog.md) |
| `0047-slack-reporting-session` | Group-scoped Slack reporting session (surface lifetime = trigger, not run) | `in_progress` | [iterations/v18-slack-reporting-session.md](iterations/v18-slack-reporting-session.md) |
| `0048-slack-stream-error-classification` | Transient vs permanent stream-append errors (don't abandon streaming on a blip) | `backlog` | [backlog.md](backlog.md) |
| `0049-libvirt-pure-driver-spike` | Direct libvirt RPC driver spike (`libvirt-pure`) | `backlog` | [backlog.md](backlog.md) |
| `0050-stacks-bench-schema-v1-native` | Adopt `stacks-bench` schema-v1 JSON natively | `backlog` | [backlog.md](backlog.md) |

## Iterations

| ID | Name | Status | Location |
| ---- | ---- | ------ | -------- |
| `v4-artifact-store` | Artifact store | `shipped` | [archive/completed/0001-artifact-store.md](archive/completed/0001-artifact-store.md) |
| `v5-slack-adhoc-profiling` | Slack ad-hoc profiling | `shipped` | [archive/completed/0002-slack-adhoc-profiling.md](archive/completed/0002-slack-adhoc-profiling.md) |
| `v6-slack-live-timeline` | Slack live timeline | `shipped` | [archive/completed/0021-slack-live-timeline.md](archive/completed/0021-slack-live-timeline.md) |
| `v7-reporting-surface` | Reporting-surface trait | `shipped` | [archive/completed/0022-report-surface-trait.md](archive/completed/0022-report-surface-trait.md) |
| `v8-slack-card-redesign` | Slack card redesign (live queue + rich results) | `shipped` | [archive/completed/0023-slack-card-redesign.md](archive/completed/0023-slack-card-redesign.md) |
| `v9-baseline-binary-cache` | Release-baseline binary cache | `shipped` | [archive/completed/0025-baseline-binary-cache.md](archive/completed/0025-baseline-binary-cache.md) |
| `v10-job-model-decomposition` | Job-model decomposition (source/intent/task/target axes) | `shipped` | [archive/completed/0005-task-kind-platform.md](archive/completed/0005-task-kind-platform.md) |
| `v11-reusable-build-jobs` | Reusable build jobs (pin warming) | `shipped` | [archive/completed/0031-reusable-build-jobs.md](archive/completed/0031-reusable-build-jobs.md) |
| `v12-slack-streamed-plan` | Slack streamed plan updates | `shipped` | [archive/completed/0033-slack-streamed-plan-updates.md](archive/completed/0033-slack-streamed-plan-updates.md) |
| `v13-llm-intent-resolution` | LLM intent resolution | `shipped` | [archive/completed/0020-llm-intent-resolution.md](archive/completed/0020-llm-intent-resolution.md) |
| `v14-benchmark-group-run-model` | Benchmark group/run model | `shipped` | [archive/completed/0037-benchmark-group-run-model.md](archive/completed/0037-benchmark-group-run-model.md) |
| `v15-isolated-benchmark-repetitions` | Isolated benchmark repetitions | `in_progress` | [iterations/v15-isolated-benchmark-repetitions.md](iterations/v15-isolated-benchmark-repetitions.md) |
| `v16-cache-hit-minimal-source-disk` | Cache-hit minimal source disk | `in_progress` | [iterations/v16-cache-hit-minimal-source-disk.md](iterations/v16-cache-hit-minimal-source-disk.md) |
| `v17-slack-reporting-robustness` | Slack reporting robustness, reactions & observability | `in_progress` | [iterations/v17-slack-reporting-robustness.md](iterations/v17-slack-reporting-robustness.md) |
| `v18-slack-reporting-session` | Group-scoped Slack reporting session | `in_progress` | [iterations/v18-slack-reporting-session.md](iterations/v18-slack-reporting-session.md) |

## Decisions

| ID | Name | Status | Location |
| ---- | ---- | ------ | -------- |
| `0001-artifact-urls-s3-only` | Artifact URLs are S3-only | `accepted` | [decisions/0001-artifact-urls-s3-only.md](decisions/0001-artifact-urls-s3-only.md) |
| `0002-artifact-refs-are-store-keys` | Artifact refs are store keys | `accepted` | [decisions/0002-artifact-refs-are-store-keys.md](decisions/0002-artifact-refs-are-store-keys.md) |
| `0003-artifact-export-failure-not-benchmark-failure` | Export-fail ≠ bench-fail | `accepted` | [decisions/0003-artifact-export-failure-not-benchmark-failure.md](decisions/0003-artifact-export-failure-not-benchmark-failure.md) |

> Migration complete: every `docs/roadmap-vN.md` is a tombstone. `0002`/`0003`/
> `0004` design now lives in `design/000N-*.md`; they promote to an iteration when
> selected.
