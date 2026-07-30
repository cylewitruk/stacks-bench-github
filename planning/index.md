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
| `0004-worker-fleet` | Worker fleet (`remote-daemon`) | `shipped` | [archive/completed/0004-worker-fleet.md](archive/completed/0004-worker-fleet.md) |
| `0005-task-kind-platform` | Job-model decomposition + task-kind platform | `shipped` | [archive/completed/0005-task-kind-platform.md](archive/completed/0005-task-kind-platform.md) |
| `0006-aws-cloud-backend` | AWS / cloud backend | `parked` | [backlog.md](backlog.md) |
| `0007-check-run-surface` | Check-run surface | `shipped` | [archive/completed/0007-check-run-surface.md](archive/completed/0007-check-run-surface.md) |
| `0008-execution-architecture` | Execution architecture | `shipped` | [archive/completed/0008-execution-architecture.md](archive/completed/0008-execution-architecture.md) |
| `0009-change-impact-reporting` | Change-impact reporting | `shipped` | [archive/completed/0009-change-impact-reporting.md](archive/completed/0009-change-impact-reporting.md) |
| `0010-driver-seam` | Task-agnostic `Driver` seam | `shipped` | [archive/completed/0010-driver-seam.md](archive/completed/0010-driver-seam.md) |
| `0011-job-pipeline-cutover` | Job pipeline + v2 cutover | `shipped` | [archive/completed/0011-job-pipeline-cutover.md](archive/completed/0011-job-pipeline-cutover.md) |
| `0012-api-fronted-daemon` | API-fronted daemon | `shipped` | [archive/completed/0012-api-fronted-daemon.md](archive/completed/0012-api-fronted-daemon.md) |
| `0013-drop-legacy-jobs-table` | Drop legacy `jobs` table | `backlog` | [backlog.md](backlog.md) |
| `0014-preclaim-placeholder-checks` | PR placeholder and skipped checks | `backlog` | [backlog.md](backlog.md) |
| `0015-resource-aware-admission` | Resource-aware admission | `backlog` | [backlog.md](backlog.md) |
| `0016-db-enforced-same-sha-dedup` | DB-enforced same-SHA dedup | `parked` | [backlog.md](backlog.md) |
| `0017-generic-phase-events` | Task-neutral durable worker events | `shipped` | [archive/completed/0017-generic-phase-events.md](archive/completed/0017-generic-phase-events.md) |
| `0018-auto-rerun-confidence-gate` | Auto-rerun confidence gate | `parked` | [backlog.md](backlog.md) |
| `0019-block-validation-recipe` | Block-validation recipe | `shipped` | [archive/completed/0019-block-validation-recipe.md](archive/completed/0019-block-validation-recipe.md) |
| `0020-llm-intent-resolution` | LLM intent resolution (Slack) | `shipped` | [archive/completed/0020-llm-intent-resolution.md](archive/completed/0020-llm-intent-resolution.md) |
| `0021-slack-live-timeline` | Slack live timeline | `shipped` | [archive/completed/0021-slack-live-timeline.md](archive/completed/0021-slack-live-timeline.md) |
| `0022-report-surface-trait` | Reporting-surface trait | `shipped` | [archive/completed/0022-report-surface-trait.md](archive/completed/0022-report-surface-trait.md) |
| `0023-slack-card-redesign` | Slack card redesign (live queue + rich results) | `shipped` | [archive/completed/0023-slack-card-redesign.md](archive/completed/0023-slack-card-redesign.md) |
| `0024-slack-card-stage-timings` | Slack card stage timings | `shipped` | [archive/completed/0024-slack-card-stage-timings.md](archive/completed/0024-slack-card-stage-timings.md) |
| `0025-baseline-binary-cache` | Release-baseline binary cache | `shipped` | [archive/completed/0025-baseline-binary-cache.md](archive/completed/0025-baseline-binary-cache.md) |
| `0026-central-block-index-cache` | Central chainstate index ledger | `parked` | [iterations/v23-central-block-tx-index-cache.md](iterations/v23-central-block-tx-index-cache.md) |
| `0027-fine-grained-progress` | Fine-grained bench progress | `parked` | [iterations/v20-fine-grained-bench-progress.md](iterations/v20-fine-grained-bench-progress.md) |
| `0028-results-summary-restructure` | Results-summary restructure | `backlog` | [backlog.md](backlog.md) |
| `0029-per-block-timing-detail` | Per-block / per-tx timing detail | `backlog` | [backlog.md](backlog.md) |
| `0030-results-qa-agent` | Results Q&A agent | `backlog` | [backlog.md](backlog.md) |
| `0031-reusable-build-jobs` | Reusable build jobs (artifact + target axis) | `shipped` | [archive/completed/0031-reusable-build-jobs.md](archive/completed/0031-reusable-build-jobs.md) |
| `0032-supersede-stale-pr-head-runs` | Supersede stale PR-head runs | `backlog` | [backlog.md](backlog.md) |
| `0033-slack-streamed-plan-updates` | Slack streamed plan updates | `shipped` | [archive/completed/0033-slack-streamed-plan-updates.md](archive/completed/0033-slack-streamed-plan-updates.md) |
| `0034-historical-stable-toolchain` | Historical stable toolchain resolution | `backlog` | [backlog.md](backlog.md) |
| `0035-slack-app-home-status` | Slack App Home status dashboard | `backlog` | [backlog.md](backlog.md) |
| `0036-pr-comment-llm-intent` | PR-comment task-intent resolution | `backlog` | [backlog.md](backlog.md) |
| `0037-benchmark-group-run-model` | Benchmark group/run model | `shipped` | [archive/completed/0037-benchmark-group-run-model.md](archive/completed/0037-benchmark-group-run-model.md) |
| `0038-isolated-benchmark-repetitions` | Isolated benchmark repetitions | `shipped` | [archive/completed/0038-isolated-benchmark-repetitions.md](archive/completed/0038-isolated-benchmark-repetitions.md) |
| `0039-multi-variant-benchmark-comparisons` | Multi-variant benchmark comparisons | `parked` | [iterations/v22-multi-variant-benchmark-comparisons.md](iterations/v22-multi-variant-benchmark-comparisons.md) |
| `0040-slack-queue-receipt-before-stream` | Slack queue receipt before claimed stream | `superseded` | [archive/superseded/0040-slack-stream-followups.md](archive/superseded/0040-slack-stream-followups.md) |
| `0041-shared-benchmark-calibration` | Shared benchmark calibration pass | `shipped` | [archive/completed/0041-shared-benchmark-calibration.md](archive/completed/0041-shared-benchmark-calibration.md) |
| `0042-cache-hit-minimal-source-disk` | Cache-hit minimal source disk | `shipped` | [archive/completed/0042-cache-hit-minimal-source-disk.md](archive/completed/0042-cache-hit-minimal-source-disk.md) |
| `0043-slack-plan-ts-race` | Slack plan-ts race fix (double card) | `shipped` | [archive/completed/0043-slack-reporting-robustness.md](archive/completed/0043-slack-reporting-robustness.md) |
| `0044-slack-reaction-lifecycle` | Slack reaction lifecycle (ack/queued/running) | `shipped` | [archive/completed/0043-slack-reporting-robustness.md](archive/completed/0043-slack-reporting-robustness.md) |
| `0045-slack-llm-observability` | Slack/LLM observability logging | `shipped` | [archive/completed/0043-slack-reporting-robustness.md](archive/completed/0043-slack-reporting-robustness.md) |
| `0046-slack-reaction-state-from-api` | Read reaction state via `reactions.list` instead of brute-force removal | `backlog` | [backlog.md](backlog.md) |
| `0047-slack-reporting-session` | Group-scoped Slack reporting session (surface lifetime = trigger, not run) | `shipped` | [archive/completed/0047-slack-reporting-session.md](archive/completed/0047-slack-reporting-session.md) |
| `0048-slack-stream-error-classification` | Transient vs permanent stream-append errors (don't abandon streaming on a blip) | `superseded` | [archive/superseded/0040-slack-stream-followups.md](archive/superseded/0040-slack-stream-followups.md) |
| `0049-libvirt-pure-driver-spike` | Direct libvirt RPC driver spike (`libvirt-pure`) | `backlog` | [backlog.md](backlog.md) |
| `0050-stacks-bench-schema-v1-native` | Adopt `stacks-bench` schema-v1 JSON natively | `parked` | [iterations/v21-stacks-bench-schema-v1-native.md](iterations/v21-stacks-bench-schema-v1-native.md) |
| `0051-slack-progress-sections-as-plan-tasks` | Slack progress sections as first-class plan tasks | `superseded` | [archive/superseded/0040-slack-stream-followups.md](archive/superseded/0040-slack-stream-followups.md) |
| `0052-managed-stacks-node-chainstate-producer` | Managed stacks-node chainstate producer | `backlog` | [backlog.md](backlog.md) |
| `0053-repository-workspace-cleanup` | Repository and workspace truth cleanup | `shipped` | [archive/completed/0053-workspace-architecture-cleanup.md](archive/completed/0053-workspace-architecture-cleanup.md) |
| `0054-application-crate-boundaries` | Application crate and dependency boundaries | `shipped` | [archive/completed/0053-workspace-architecture-cleanup.md](archive/completed/0053-workspace-architecture-cleanup.md) |
| `0055-execution-boundary-preparation` | In-process execution-boundary preparation | `shipped` | [archive/completed/0053-workspace-architecture-cleanup.md](archive/completed/0053-workspace-architecture-cleanup.md) |
| `0056-compiler-enforced-execution-boundaries` | Compiler-enforced worker and backend boundaries | `shipped` | [archive/completed/0056-compiler-enforced-crate-boundaries.md](archive/completed/0056-compiler-enforced-crate-boundaries.md) |
| `0057-core-adapter-boundaries` | Dependency-light core and concrete adapter boundaries | `shipped` | [archive/completed/0056-compiler-enforced-crate-boundaries.md](archive/completed/0056-compiler-enforced-crate-boundaries.md) |
| `0058-github-integration-boundary` | Consolidated GitHub contract and adapter boundary | `shipped` | [archive/completed/0058-github-intent-boundaries.md](archive/completed/0058-github-intent-boundaries.md) |
| `0059-intent-resolution-boundary` | Provider-backed request intent boundary | `shipped` | [archive/completed/0058-github-intent-boundaries.md](archive/completed/0058-github-intent-boundaries.md) |
| `0060-slack-snapshot-reporting` | Single-message Slack snapshot reporting | `shipped` | [archive/completed/0060-slack-snapshot-reporting.md](archive/completed/0060-slack-snapshot-reporting.md) |
| `0061-slack-integration-boundary` | Extracted Slack integration boundary | `shipped` | [archive/completed/0060-slack-snapshot-reporting.md](archive/completed/0060-slack-snapshot-reporting.md) |
| `0062-sandboxed-worker-execution` | Sandboxed worker execution invariant | `shipped` | [archive/completed/0062-sandboxed-worker-execution.md](archive/completed/0062-sandboxed-worker-execution.md) |
| `0063-libvirt-block-validation` | Libvirt-isolated block validation | `shipped` | [archive/completed/0062-sandboxed-worker-execution.md](archive/completed/0062-sandboxed-worker-execution.md) |
| `0064-task-submission-kernel` | Task-submission kernel | `shipped` | [archive/completed/0064-task-submission-kernel.md](archive/completed/0064-task-submission-kernel.md) |
| `0065-job-lifecycle-controls` | Task-neutral job lifecycle controls | `candidate` | [backlog.md](backlog.md) |
| `0066-task-aware-reporting` | Task-aware reporting and validation results | `shipped` | [archive/completed/0066-task-aware-reporting.md](archive/completed/0066-task-aware-reporting.md) |
| `0067-github-block-validation-submission` | GitHub block-validation submission | `candidate` | [backlog.md](backlog.md) |
| `0068-watched-ref-task-actions` | Watched-ref task actions and webhook fan-out | `candidate` | [backlog.md](backlog.md) |
| `0069-task-aware-intent-resolution` | Task-aware intent resolution | `candidate` | [backlog.md](backlog.md) |
| `0070-slack-block-validation-controls` | Slack block-validation and lifecycle controls | `candidate` | [backlog.md](backlog.md) |
| `0071-github-job-lifecycle-controls` | GitHub job lifecycle controls | `candidate` | [backlog.md](backlog.md) |
| `0072-pre-attempt-terminal-projection` | Pre-attempt terminal projection | `candidate` | [backlog.md](backlog.md) |
| `0073-task-neutral-submission-model` | Task-neutral submission model rename | `shipped` | [archive/completed/0073-task-neutral-submission-model.md](archive/completed/0073-task-neutral-submission-model.md) |
| `0074-protobuf-fleet-protocol` | Protobuf worker protocol | `shipped` | [archive/completed/0074-protobuf-fleet-protocol.md](archive/completed/0074-protobuf-fleet-protocol.md) |
| `0075-rolling-worker-protocol-compatibility` | Rolling worker protocol compatibility | `candidate` | [backlog.md](backlog.md) |

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
| `v15-isolated-benchmark-repetitions` | Isolated benchmark repetitions | `shipped` | [archive/completed/0038-isolated-benchmark-repetitions.md](archive/completed/0038-isolated-benchmark-repetitions.md) |
| `v16-cache-hit-minimal-source-disk` | Cache-hit minimal source disk | `shipped` | [archive/completed/0042-cache-hit-minimal-source-disk.md](archive/completed/0042-cache-hit-minimal-source-disk.md) |
| `v17-slack-reporting-robustness` | Slack reporting robustness, reactions & observability | `shipped` | [archive/completed/0043-slack-reporting-robustness.md](archive/completed/0043-slack-reporting-robustness.md) |
| `v18-slack-reporting-session` | Group-scoped Slack reporting session | `shipped` | [archive/completed/0047-slack-reporting-session.md](archive/completed/0047-slack-reporting-session.md) |
| `v19-shared-benchmark-calibration` | Shared benchmark calibration | `shipped` | [archive/completed/0041-shared-benchmark-calibration.md](archive/completed/0041-shared-benchmark-calibration.md) |
| `v20-fine-grained-bench-progress` | Fine-grained bench progress | `parked` | [iterations/v20-fine-grained-bench-progress.md](iterations/v20-fine-grained-bench-progress.md) |
| `v21-stacks-bench-schema-v1-native` | Native `stacks-bench` schema-v1 JSON | `parked` | [iterations/v21-stacks-bench-schema-v1-native.md](iterations/v21-stacks-bench-schema-v1-native.md) |
| `v22-multi-variant-benchmark-comparisons` | Multi-variant benchmark comparisons | `parked` | [iterations/v22-multi-variant-benchmark-comparisons.md](iterations/v22-multi-variant-benchmark-comparisons.md) |
| `v23-central-block-tx-index-cache` | Central chainstate index ledger | `parked` | [iterations/v23-central-block-tx-index-cache.md](iterations/v23-central-block-tx-index-cache.md) |
| `v24-workspace-architecture-cleanup` | Workspace and architecture cleanup | `shipped` | [archive/completed/0053-workspace-architecture-cleanup.md](archive/completed/0053-workspace-architecture-cleanup.md) |
| `v24.1-compiler-enforced-crate-boundaries` | Compiler-enforced crate boundaries | `shipped` | [archive/completed/0056-compiler-enforced-crate-boundaries.md](archive/completed/0056-compiler-enforced-crate-boundaries.md) |
| `v24.2-github-intent-boundaries` | GitHub and intent integration boundaries | `shipped` | [archive/completed/0058-github-intent-boundaries.md](archive/completed/0058-github-intent-boundaries.md) |
| `v24.3-slack-snapshot-reporting` | Slack snapshot reporting and integration boundary | `shipped` | [archive/completed/0060-slack-snapshot-reporting.md](archive/completed/0060-slack-snapshot-reporting.md) |
| `v25-worker-fleet-block-validation` | First worker fleet and dedicated block validation | `shipped` | [archive/completed/0004-worker-fleet.md](archive/completed/0004-worker-fleet.md) |
| `v26-sandboxed-worker-execution` | Sandboxed worker execution and block validation | `shipped` | [archive/completed/0062-sandboxed-worker-execution.md](archive/completed/0062-sandboxed-worker-execution.md) |
| `v27.1-task-neutral-submission-model` | Task-neutral submission model rename | `shipped` | [archive/completed/0073-task-neutral-submission-model.md](archive/completed/0073-task-neutral-submission-model.md) |
| `v27.2-task-submission-kernel` | Task-submission kernel | `shipped` | [archive/completed/0064-task-submission-kernel.md](archive/completed/0064-task-submission-kernel.md) |
| `v28-task-aware-reporting` | Task-aware reporting and validation results | `shipped` | [archive/completed/0066-task-aware-reporting.md](archive/completed/0066-task-aware-reporting.md) |
| `v29-protobuf-fleet-protocol` | Protobuf worker protocol | `shipped` | [archive/completed/0074-protobuf-fleet-protocol.md](archive/completed/0074-protobuf-fleet-protocol.md) |

## Decisions

| ID | Name | Status | Location |
| ---- | ---- | ------ | -------- |
| `0001-artifact-urls-s3-only` | Artifact URLs are S3-only | `accepted` | [decisions/0001-artifact-urls-s3-only.md](decisions/0001-artifact-urls-s3-only.md) |
| `0002-artifact-refs-are-store-keys` | Artifact refs are store keys | `accepted` | [decisions/0002-artifact-refs-are-store-keys.md](decisions/0002-artifact-refs-are-store-keys.md) |
| `0003-artifact-export-failure-not-benchmark-failure` | Export-fail ≠ bench-fail | `accepted` | [decisions/0003-artifact-export-failure-not-benchmark-failure.md](decisions/0003-artifact-export-failure-not-benchmark-failure.md) |
| `0004-protobuf-fleet-protocol` | Protobuf/gRPC worker fleet protocol | `accepted` | [decisions/0004-protobuf-fleet-protocol.md](decisions/0004-protobuf-fleet-protocol.md) |

> Migration complete: every `docs/roadmap-vN.md` is a tombstone. Living designs
> remain under `design/`; shipped records live in the completed archive.
