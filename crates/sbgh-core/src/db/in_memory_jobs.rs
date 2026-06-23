//! In-memory `JobStore` for unit tests. Single Mutex serialises
//! access; mirrors the Postgres semantics for queued-state
//! invariants, claim handoff, stuck-claim sweep, and the
//! conditional `(id, claim_token)` writes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use uuid::Uuid;

use crate::Result;
use crate::db::jobs::{
    BaselineAnchor, BaselineMatch, BaselineSelection, BenchmarkRunMetric, CreatedJob,
    JobCompletion, JobCreationOutcome, JobFailure, JobStore, NewBenchmarkSpec, PendingBenchmarkRun,
};
use crate::models::{
    BenchmarkGroup, BenchmarkSpec, BenchmarkStepKind, BenchmarkWorkflowStep, GithubPullRequestJob,
    GithubUserJob, GithubWebhookJob, Job, JobCreationRequest, JobEvent, JobEventKind,
    JobEventStatus, JobMetric, JobResult, JobStatus, NewJob, NewJobEvent, QueuedEventDetail,
    ResolvedCommit, TaskKind, TerminalJobStatus, uses_shared_calibration,
};

#[derive(Default)]
pub struct InMemoryJobStore {
    state: Mutex<State>,
    next_event_id: AtomicI64,
    /// Test knob: when set, `create_unlinked_job` returns an error so callers
    /// can exercise their post-create failure handling.
    fail_create_unlinked: AtomicBool,
}

#[derive(Default)]
struct State {
    groups: HashMap<Uuid, BenchmarkGroup>,
    specs: HashMap<Uuid, BenchmarkSpec>,
    steps: Vec<BenchmarkWorkflowStep>,
    jobs: HashMap<Uuid, Job>,
    /// Insertion order — `claim_next_queued` walks in (created_at, id)
    /// order, so we sort on demand.
    insertion_order: Vec<Uuid>,
    events: Vec<JobEvent>,
    metrics: HashMap<Uuid, JobMetric>,
    results: HashMap<Uuid, JobResult>,
    webhook_links: Vec<GithubWebhookJob>,
    user_links: Vec<GithubUserJob>,
    pr_links: Vec<GithubPullRequestJob>,
}

impl InMemoryJobStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            next_event_id: AtomicI64::new(1),
            fail_create_unlinked: AtomicBool::new(false),
        }
    }

    /// Test knob: make the next (and subsequent) `create_unlinked_job` calls
    /// fail, so a caller's post-create cleanup path can be exercised.
    pub fn fail_create_unlinked_job(&self) {
        self.fail_create_unlinked
            .store(true, Ordering::SeqCst);
    }

    /// Test-only accessor: returns true iff a `github_webhook_job`
    /// link exists for the given job_id right now. Used by the
    /// concurrent-visibility regression test (slice 8 second-pass
    /// review fix) to assert that when a parallel `claim_next_queued`
    /// observed a job, the corresponding webhook link was also
    /// already visible — i.e. there was no partial-commit window.
    pub fn has_webhook_link_for_job(&self, job_id: Uuid) -> bool {
        self.state
            .lock()
            .unwrap()
            .webhook_links
            .iter()
            .any(|l| l.job_id == job_id)
    }

    /// Test-only accessor: snapshot of all `job` rows. Slice 9 handler
    /// unit tests assert how many jobs an accept path created and
    /// inspect their subject identity / commit fields.
    pub fn all_jobs(&self) -> Vec<Job> {
        self.state
            .lock()
            .unwrap()
            .jobs
            .values()
            .cloned()
            .collect()
    }

    /// Test-only accessor: snapshot of all `job_event` rows (slice 9
    /// asserts the queued event + its provenance detail).
    pub fn all_events(&self) -> Vec<JobEvent> {
        self.state
            .lock()
            .unwrap()
            .events
            .clone()
    }

    /// Test-only accessor: snapshot of all `github_user_job` links.
    pub fn user_links(&self) -> Vec<GithubUserJob> {
        self.state
            .lock()
            .unwrap()
            .user_links
            .clone()
    }

    /// Test-only accessor: snapshot of all `github_pull_request_job` links.
    pub fn pr_links(&self) -> Vec<GithubPullRequestJob> {
        self.state
            .lock()
            .unwrap()
            .pr_links
            .clone()
    }

    /// Test-only accessor for the modeled benchmark spec row.
    pub fn spec(&self, id: Uuid) -> Option<BenchmarkSpec> {
        self.state
            .lock()
            .unwrap()
            .specs
            .get(&id)
            .cloned()
    }

    /// Test-only accessor for all specs in one benchmark group.
    pub fn specs_for_group(&self, group_id: Uuid) -> Vec<BenchmarkSpec> {
        let mut specs: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .specs
            .values()
            .filter(|spec| spec.benchmark_group_id == group_id)
            .cloned()
            .collect();
        specs.sort_by_key(|spec| spec.spec_index);
        specs
    }

    pub fn steps_for_group(&self, group_id: Uuid) -> Vec<BenchmarkWorkflowStep> {
        let mut steps: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .steps
            .iter()
            .filter(|step| step.benchmark_group_id == group_id)
            .cloned()
            .collect();
        steps.sort_by_key(|step| step.step_index);
        steps
    }
}

/// Build a [`BaselineMatch`] from an in-memory job + its metric (mirrors the
/// Postgres `BaselineRow::into_match`).
fn make_match(job: &Job, metric: &JobMetric, selection: BaselineSelection) -> BaselineMatch {
    BaselineMatch {
        anchor: BaselineAnchor {
            job_id: job.id,
            github_repo_id: job.github_repo_id,
            commit: job
                .git_commit_hash
                .clone()
                .unwrap_or_default(),
            git_ref_display: job.git_ref_display.clone(),
            committed_at: job.git_committed_at,
            selection,
        },
        metric: metric.clone(),
    }
}

fn group_specs(
    specs: &[NewBenchmarkSpec],
    now: chrono::DateTime<Utc>,
) -> Result<(BenchmarkGroup, Vec<BenchmarkSpec>, Vec<BenchmarkWorkflowStep>)> {
    let Some(first) = specs.first() else {
        return Err(crate::Error::Other(anyhow::anyhow!(
            "benchmark group needs at least one spec"
        )));
    };
    let first_job = &first.new_job;
    let group_id = Uuid::new_v4();
    let group = BenchmarkGroup {
        id: group_id,
        github_installation_id: first_job.github_installation_id,
        github_repo_id: first_job.github_repo_id,
        source: first_job.axes.source,
        intent: first_job.axes.intent,
        artifact_prefix: group_id.to_string(),
        host_key: None,
        created_at: now,
        updated_at: now,
    };

    let mut out_specs = Vec::with_capacity(specs.len());
    let mut steps = Vec::new();
    let mut step_index = 0i32;
    let group_measured_run_count = specs
        .iter()
        .map(NewBenchmarkSpec::measured_run_count)
        .sum::<i32>();
    for (spec_index, requested) in specs.iter().enumerate() {
        let new = &requested.new_job;
        if new.github_installation_id != first_job.github_installation_id
            || new.axes.source != first_job.axes.source
            || new.axes.intent != first_job.axes.intent
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "benchmark group specs must share installation, source, and intent"
            )));
        }
        let spec_id = Uuid::new_v4();
        let requested_run_count = requested
            .requested_run_count
            .max(1);
        let spec = BenchmarkSpec {
            id: spec_id,
            benchmark_group_id: group_id,
            spec_index: spec_index as i32,
            requested_run_count,
            baseline_calibration_id: requested.baseline_calibration_id,
            github_repo_id: new.github_repo_id,
            task_kind: new.axes.task_kind,
            build_target: new.axes.build_target,
            git_ref_kind: new.git_ref_kind,
            git_ref_display: new.git_ref_display.clone(),
            git_commit_hash: new.git_commit_hash.clone(),
            git_committed_at: new.git_committed_at,
            workload_key: new.workload_key.clone(),
            created_at: now,
            updated_at: now,
        };
        steps.push(BenchmarkWorkflowStep {
            id: Uuid::new_v4(),
            benchmark_group_id: group_id,
            step_index,
            step_kind: BenchmarkStepKind::Build,
            benchmark_spec_id: Some(spec_id),
            created_at: now,
        });
        step_index += 1;

        if uses_shared_calibration(
            new.axes.task_kind,
            new.axes.build_target,
            group_measured_run_count,
        ) {
            steps.push(BenchmarkWorkflowStep {
                id: Uuid::new_v4(),
                benchmark_group_id: group_id,
                step_index,
                step_kind: BenchmarkStepKind::Calibrate,
                benchmark_spec_id: Some(spec_id),
                created_at: now,
            });
            step_index += 1;
        }
        if new.axes.task_kind != TaskKind::BuildOnly {
            steps.push(BenchmarkWorkflowStep {
                id: Uuid::new_v4(),
                benchmark_group_id: group_id,
                step_index,
                step_kind: BenchmarkStepKind::Run,
                benchmark_spec_id: Some(spec_id),
                created_at: now,
            });
            step_index += 1;
        }
        out_specs.push(spec);
    }

    Ok((group, out_specs, steps))
}

fn singleton_group_spec(
    new: &NewJob,
    now: chrono::DateTime<Utc>,
    requested_run_count: i32,
) -> Result<(BenchmarkGroup, BenchmarkSpec, Vec<BenchmarkWorkflowStep>)> {
    let specs = [NewBenchmarkSpec::singleton(new.clone(), requested_run_count)];
    let (group, mut specs, steps) = group_specs(&specs, now)?;
    let spec = specs
        .pop()
        .expect("singleton creation returns one spec");
    Ok((group, spec, steps))
}

fn requested_run_count_from_detail(detail: &serde_json::Value) -> i32 {
    match serde_json::from_value::<QueuedEventDetail>(detail.clone()) {
        Ok(QueuedEventDetail::SlackAdhoc { clean_repetitions, .. }) => {
            clean_repetitions.max(1) as i32
        }
        _ => 1,
    }
}

fn next_run_from_prior(prior: &Job, next_index: i32, now: chrono::DateTime<Utc>) -> Job {
    Job {
        id: Uuid::new_v4(),
        benchmark_group_id: prior.benchmark_group_id,
        benchmark_spec_id: prior.benchmark_spec_id,
        benchmark_run_index: next_index,
        github_installation_id: prior.github_installation_id,
        github_repo_id: prior.github_repo_id,
        status: JobStatus::Queued,
        source: prior.source,
        intent: prior.intent,
        task_kind: prior.task_kind,
        build_target: prior.build_target,
        git_ref_kind: prior.git_ref_kind,
        git_ref_display: prior.git_ref_display.clone(),
        git_commit_hash: prior.git_commit_hash.clone(),
        git_committed_at: prior.git_committed_at,
        workload_key: prior.workload_key.clone(),
        claim_token: None,
        claimed_at: None,
        created_at: now,
        updated_at: now,
    }
}

fn initial_run_from_spec(
    group: &BenchmarkGroup,
    spec: &BenchmarkSpec,
    id: Uuid,
    now: chrono::DateTime<Utc>,
) -> Job {
    Job {
        id,
        benchmark_group_id: group.id,
        benchmark_spec_id: spec.id,
        benchmark_run_index: 0,
        github_installation_id: group.github_installation_id,
        github_repo_id: spec.github_repo_id,
        status: JobStatus::Queued,
        source: group.source,
        intent: group.intent,
        task_kind: spec.task_kind,
        build_target: spec.build_target,
        git_ref_kind: spec.git_ref_kind,
        git_ref_display: spec.git_ref_display.clone(),
        git_commit_hash: spec.git_commit_hash.clone(),
        git_committed_at: spec.git_committed_at,
        workload_key: spec.workload_key.clone(),
        claim_token: None,
        claimed_at: None,
        created_at: now,
        updated_at: now,
    }
}

#[async_trait]
impl JobStore for InMemoryJobStore {
    async fn insert_job(&self, new: &NewJob) -> Result<Job> {
        let now = Utc::now();
        let id = Uuid::new_v4();
        let (group, spec, steps) = singleton_group_spec(new, now, 1)?;
        // v10 (0005): jobs carry the axes natively — set by the handler.
        let job = Job {
            id,
            benchmark_group_id: group.id,
            benchmark_spec_id: spec.id,
            benchmark_run_index: 0,
            github_installation_id: new.github_installation_id,
            github_repo_id: new.github_repo_id,
            status: JobStatus::Queued,
            source: new.axes.source,
            intent: new.axes.intent,
            task_kind: new.axes.task_kind,
            build_target: new.axes.build_target,
            git_ref_kind: new.git_ref_kind,
            git_ref_display: new.git_ref_display.clone(),
            git_commit_hash: new.git_commit_hash.clone(),
            git_committed_at: new.git_committed_at,
            workload_key: new.workload_key.clone(),
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        let mut s = self.state.lock().unwrap();
        s.groups
            .insert(group.id, group);
        s.specs.insert(spec.id, spec);
        s.steps.extend(steps);
        s.jobs.insert(id, job.clone());
        s.insertion_order.push(id);
        Ok(job)
    }

    async fn create_job_with_links(
        &self,
        request: &JobCreationRequest,
    ) -> Result<JobCreationOutcome> {
        // Post-second-review M1 fix: take the mutex ONCE for the entire
        // creation so concurrent in-memory tests can't observe (or
        // claim) a partially-created job between the sub-inserts.
        // Mirrors the all-or-nothing semantic of the Postgres single
        // transaction.
        //
        // Build all the rows locally first, THEN — under a single mutex
        // acquisition — re-check the slice-9 `UNIQUE (github_webhook_id)`
        // idempotency guard and commit every mutation. The check must
        // hold the SAME lock as the commit so a concurrent re-claim
        // can't slip a second job in between (mirrors the Postgres
        // `ON CONFLICT (github_webhook_id)` race-safety).
        let now = Utc::now();
        let job_id = Uuid::new_v4();
        let (group, spec, steps) = singleton_group_spec(&request.new_job, now, 1)?;

        // v10 (0005): jobs carry the axes natively — set by the handler.
        // Build all the rows locally.
        let job = Job {
            id: job_id,
            benchmark_group_id: group.id,
            benchmark_spec_id: spec.id,
            benchmark_run_index: 0,
            github_installation_id: request
                .new_job
                .github_installation_id,
            github_repo_id: request.new_job.github_repo_id,
            status: JobStatus::Queued,
            source: request.new_job.axes.source,
            intent: request.new_job.axes.intent,
            task_kind: request.new_job.axes.task_kind,
            build_target: request
                .new_job
                .axes
                .build_target,
            git_ref_kind: request.new_job.git_ref_kind,
            git_ref_display: request
                .new_job
                .git_ref_display
                .clone(),
            git_commit_hash: request
                .new_job
                .git_commit_hash
                .clone(),
            git_committed_at: request
                .new_job
                .git_committed_at,
            workload_key: request
                .new_job
                .workload_key
                .clone(),
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        let webhook_link = GithubWebhookJob {
            github_webhook_id: request.github_webhook_id,
            job_id,
            created_at: now,
        };
        let user_link = request
            .triggering_user_id
            .map(|user_id| GithubUserJob {
                github_user_id: user_id,
                job_id,
                created_at: now,
            });
        let pull_request_link = request
            .pull_request_link
            .as_ref()
            .map(|pr| GithubPullRequestJob {
                job_id,
                github_pull_request_id: pr.github_pull_request_id,
                triggering_comment_id: pr.triggering_comment_id,
                created_at: now,
            });
        let queued_event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Queued,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: request
                .queued_event_detail
                .clone()
                .map(sqlx::types::Json),
        };

        // Commit: single mutex acquisition for ALL mutations (and the
        // idempotency check). Nothing is visible to concurrent readers
        // until this block returns.
        let mut s = self.state.lock().unwrap();
        // Slice 9 idempotency guard — mirror Postgres
        // `UNIQUE (github_webhook_id)`: if this webhook already has a
        // job, this is a reprocessed delivery; write nothing.
        if s.webhook_links
            .iter()
            .any(|l| l.github_webhook_id == request.github_webhook_id)
        {
            return Ok(JobCreationOutcome::AlreadyEnqueued);
        }
        s.jobs
            .insert(job_id, job.clone());
        s.groups
            .insert(group.id, group);
        s.specs.insert(spec.id, spec);
        s.steps.extend(steps);
        s.insertion_order.push(job_id);
        s.webhook_links
            .push(webhook_link.clone());
        if let Some(ref link) = user_link {
            s.user_links
                .push(link.clone());
        }
        if let Some(ref link) = pull_request_link {
            s.pr_links.push(link.clone());
        }
        s.events
            .push(queued_event.clone());

        Ok(JobCreationOutcome::Created(Box::new(CreatedJob {
            job,
            webhook_link,
            user_link,
            pull_request_link,
            queued_event,
        })))
    }

    async fn create_unlinked_job(
        &self,
        job_id: Uuid,
        new_job: &NewJob,
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> Result<Job> {
        if self
            .fail_create_unlinked
            .load(Ordering::SeqCst)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "injected create_unlinked_job failure"
            )));
        }
        let now = Utc::now();
        let requested_run_count = requested_run_count_from_detail(queued_event_detail);
        let (group, spec, steps) = singleton_group_spec(new_job, now, requested_run_count)?;
        // v10 (0005): jobs carry the axes natively — set by the caller.
        let job = Job {
            id: job_id,
            benchmark_group_id: group.id,
            benchmark_spec_id: spec.id,
            benchmark_run_index: 0,
            github_installation_id: new_job.github_installation_id,
            github_repo_id: new_job.github_repo_id,
            status: JobStatus::Queued,
            source: new_job.axes.source,
            intent: new_job.axes.intent,
            task_kind: new_job.axes.task_kind,
            build_target: new_job.axes.build_target,
            git_ref_kind: new_job.git_ref_kind,
            git_ref_display: new_job
                .git_ref_display
                .clone(),
            git_commit_hash: new_job
                .git_commit_hash
                .clone(),
            git_committed_at: new_job.git_committed_at,
            workload_key: new_job.workload_key.clone(),
            claim_token: None,
            claimed_at: None,
            created_at: now,
            updated_at: now,
        };
        let queued_event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Queued,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: Some(sqlx::types::Json(queued_event_detail.clone())),
        };
        let mut s = self.state.lock().unwrap();
        s.jobs
            .insert(job_id, job.clone());
        s.groups
            .insert(group.id, group);
        s.specs.insert(spec.id, spec);
        s.steps.extend(steps);
        s.insertion_order.push(job_id);
        s.events.push(queued_event);
        // Record the pre-posted plan card's `ts` alongside the queued event.
        if let Some(ts) = plan_message_ts {
            s.events.push(JobEvent {
                id: self
                    .next_event_id
                    .fetch_add(1, Ordering::SeqCst),
                job_id,
                event_kind: JobEventKind::PlanMessageSent,
                event_status: JobEventStatus::Success,
                occurred_at: now,
                github_comment_id: None,
                github_check_run_id: None,
                github_check_run_url: None,
                remark: None,
                detail: Some(sqlx::types::Json(serde_json::json!({ "plan_message_ts": ts }))),
            });
        }
        Ok(job)
    }

    async fn create_unlinked_benchmark_group(
        &self,
        first_job_id: Uuid,
        specs: &[NewBenchmarkSpec],
        queued_event_detail: &serde_json::Value,
        plan_message_ts: Option<&str>,
    ) -> Result<Job> {
        if self
            .fail_create_unlinked
            .load(Ordering::SeqCst)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "injected create_unlinked_job failure"
            )));
        }
        let now = Utc::now();
        let (group, specs, steps) = group_specs(specs, now)?;
        let first_spec = specs
            .first()
            .expect("group_specs rejects empty input");
        let job = initial_run_from_spec(&group, first_spec, first_job_id, now);
        let queued_event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: first_job_id,
            event_kind: JobEventKind::Queued,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: Some(sqlx::types::Json(queued_event_detail.clone())),
        };
        let mut s = self.state.lock().unwrap();
        s.groups
            .insert(group.id, group);
        for spec in specs {
            s.specs.insert(spec.id, spec);
        }
        s.steps.extend(steps);
        s.jobs
            .insert(first_job_id, job.clone());
        s.insertion_order
            .push(first_job_id);
        s.events.push(queued_event);
        if let Some(ts) = plan_message_ts {
            s.events.push(JobEvent {
                id: self
                    .next_event_id
                    .fetch_add(1, Ordering::SeqCst),
                job_id: first_job_id,
                event_kind: JobEventKind::PlanMessageSent,
                event_status: JobEventStatus::Success,
                occurred_at: now,
                github_comment_id: None,
                github_check_run_id: None,
                github_check_run_url: None,
                remark: None,
                detail: Some(sqlx::types::Json(serde_json::json!({ "plan_message_ts": ts }))),
            });
        }
        Ok(job)
    }

    async fn append_next_benchmark_run(&self, completed_job_id: Uuid) -> Result<Option<Job>> {
        let mut s = self.state.lock().unwrap();
        let Some(prior) = s
            .jobs
            .get(&completed_job_id)
            .cloned()
        else {
            return Ok(None);
        };
        let Some(spec) = s
            .specs
            .get(&prior.benchmark_spec_id)
            .cloned()
        else {
            return Ok(None);
        };
        if prior.status != JobStatus::Completed || prior.task_kind == TaskKind::BuildOnly {
            return Ok(None);
        }
        if s.jobs.values().any(|j| {
            j.benchmark_group_id == prior.benchmark_group_id
                && matches!(j.status, JobStatus::Queued | JobStatus::Claimed | JobStatus::Running)
        }) {
            return Ok(None);
        }
        let max_index = s
            .jobs
            .values()
            .filter(|j| j.benchmark_spec_id == prior.benchmark_spec_id)
            .map(|j| j.benchmark_run_index)
            .max()
            .unwrap_or(0);
        if max_index != prior.benchmark_run_index {
            return Ok(None);
        }
        let next_index = max_index + 1;
        let now = Utc::now();
        let job = if next_index < spec.requested_run_count {
            next_run_from_prior(&prior, next_index, now)
        } else {
            let Some(next_spec) = s
                .specs
                .values()
                .filter(|candidate| {
                    candidate.benchmark_group_id == prior.benchmark_group_id
                        && candidate.spec_index > spec.spec_index
                })
                .min_by_key(|candidate| candidate.spec_index)
                .cloned()
            else {
                return Ok(None);
            };
            if s.jobs
                .values()
                .any(|j| j.benchmark_spec_id == next_spec.id)
            {
                return Ok(None);
            }
            let Some(group) = s
                .groups
                .get(&prior.benchmark_group_id)
                .cloned()
            else {
                return Ok(None);
            };
            initial_run_from_spec(&group, &next_spec, Uuid::new_v4(), now)
        };
        let detail = s
            .events
            .iter()
            .rev()
            .find(|e| e.job_id == prior.id && e.event_kind == JobEventKind::Queued)
            .and_then(|e| e.detail.clone());
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: job.id,
            event_kind: JobEventKind::Queued,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail,
        };
        let plan_event = s
            .events
            .iter()
            .rev()
            .find(|e| e.job_id == prior.id && e.event_kind == JobEventKind::PlanMessageSent)
            .cloned()
            .map(|prior_event| JobEvent {
                id: self
                    .next_event_id
                    .fetch_add(1, Ordering::SeqCst),
                job_id: job.id,
                event_kind: prior_event.event_kind,
                event_status: prior_event.event_status,
                occurred_at: now,
                github_comment_id: None,
                github_check_run_id: None,
                github_check_run_url: None,
                remark: prior_event.remark,
                detail: prior_event.detail,
            });
        s.insertion_order.push(job.id);
        s.events.push(event);
        if let Some(plan_event) = plan_event {
            s.events.push(plan_event);
        }
        s.jobs
            .insert(job.id, job.clone());
        Ok(Some(job))
    }

    async fn resume_pending_benchmark_runs(&self) -> Result<Vec<Job>> {
        let mut out = Vec::new();
        for pending in self
            .pending_completed_benchmark_runs()
            .await?
        {
            if let Some(job) = self
                .append_next_benchmark_run(pending.completed_job_id)
                .await?
            {
                out.push(job);
            }
        }
        Ok(out)
    }

    async fn pending_completed_benchmark_runs(&self) -> Result<Vec<PendingBenchmarkRun>> {
        let s = self.state.lock().unwrap();
        let mut pending = Vec::new();
        for group in s.groups.values() {
            if s.jobs.values().any(|j| {
                j.benchmark_group_id == group.id
                    && matches!(
                        j.status,
                        JobStatus::Queued | JobStatus::Claimed | JobStatus::Running
                    )
            }) {
                continue;
            }
            let latest = s
                .jobs
                .values()
                .filter(|j| j.benchmark_group_id == group.id)
                .filter_map(|job| {
                    s.specs
                        .get(&job.benchmark_spec_id)
                        .map(|spec| (spec, job))
                })
                .max_by_key(|(spec, job)| (spec.spec_index, job.benchmark_run_index));
            let Some((spec, job)) = latest else {
                continue;
            };
            if spec.task_kind == TaskKind::BuildOnly || job.status != JobStatus::Completed {
                continue;
            }
            let has_next_same_spec = job.benchmark_run_index + 1 < spec.requested_run_count;
            let has_next_spec = s
                .specs
                .values()
                .any(|candidate| {
                    candidate.benchmark_group_id == group.id
                        && candidate.spec_index > spec.spec_index
                        && !s
                            .jobs
                            .values()
                            .any(|job| job.benchmark_spec_id == candidate.id)
                });
            if !has_next_same_spec && !has_next_spec {
                continue;
            }
            pending.push(PendingBenchmarkRun {
                completed_job_id: job.id,
                benchmark_group_id: job.benchmark_group_id,
                benchmark_spec_id: job.benchmark_spec_id,
                benchmark_run_index: job.benchmark_run_index,
                requested_run_count: spec.requested_run_count,
                artifact_prefix: group.artifact_prefix.clone(),
            });
        }
        pending.sort_by_key(|p| p.completed_job_id);
        Ok(pending)
    }

    async fn benchmark_run_metrics(
        &self,
        benchmark_spec_id: Uuid,
    ) -> Result<Vec<BenchmarkRunMetric>> {
        let s = self.state.lock().unwrap();
        let mut rows: Vec<_> = s
            .jobs
            .values()
            .filter(|j| j.benchmark_spec_id == benchmark_spec_id)
            .filter_map(|j| {
                s.metrics
                    .get(&j.id)
                    .cloned()
                    .map(|metric| BenchmarkRunMetric {
                        benchmark_run_index: j.benchmark_run_index,
                        metric,
                    })
            })
            .collect();
        rows.sort_by_key(|row| row.benchmark_run_index);
        Ok(rows)
    }

    async fn lookup_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .jobs
            .get(&job_id)
            .cloned())
    }

    async fn lookup_benchmark_group(&self, group_id: Uuid) -> Result<Option<BenchmarkGroup>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .groups
            .get(&group_id)
            .cloned())
    }

    async fn lookup_benchmark_spec(&self, spec_id: Uuid) -> Result<Option<BenchmarkSpec>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .specs
            .get(&spec_id)
            .cloned())
    }

    async fn lookup_benchmark_specs(&self, group_id: Uuid) -> Result<Vec<BenchmarkSpec>> {
        let mut specs: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .specs
            .values()
            .filter(|spec| spec.benchmark_group_id == group_id)
            .cloned()
            .collect();
        specs.sort_by_key(|spec| spec.spec_index);
        Ok(specs)
    }

    async fn completed_event_detail(&self, job_id: Uuid) -> Result<Option<serde_json::Value>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .events
            .iter()
            .rev()
            .find(|e| {
                e.job_id == job_id
                    && e.event_kind == JobEventKind::Completed
                    && e.event_status == JobEventStatus::Success
            })
            .and_then(|e| {
                e.detail
                    .as_ref()
                    .map(|d| d.0.clone())
            }))
    }

    async fn claim_next_queued(&self, claim_token: Uuid) -> Result<Option<Job>> {
        let mut s = self.state.lock().unwrap();
        // Walk in insertion order (FIFO; mirrors the Postgres index ordering).
        let pick = s
            .insertion_order
            .iter()
            .find(|id| {
                s.jobs
                    .get(id)
                    .is_some_and(|j| j.status == JobStatus::Queued)
            })
            .copied();
        let Some(id) = pick else {
            return Ok(None);
        };
        let job = s.jobs.get_mut(&id).unwrap();
        job.status = JobStatus::Claimed;
        job.claim_token = Some(claim_token);
        job.claimed_at = Some(Utc::now());
        job.updated_at = Utc::now();
        Ok(Some(job.clone()))
    }

    async fn mark_running(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        resolved_commit: Option<ResolvedCommit>,
    ) -> Result<bool> {
        let mut s = self.state.lock().unwrap();
        let Some(job) = s.jobs.get_mut(&job_id) else {
            return Ok(false);
        };
        if job.status != JobStatus::Claimed || job.claim_token != Some(claim_token) {
            return Ok(false);
        }
        job.status = JobStatus::Running;
        if let Some(rc) = resolved_commit {
            job.git_commit_hash = Some(rc.hash);
            // `None` committed_at leaves the existing value (mirrors the
            // Postgres COALESCE).
            if let Some(ts) = rc.committed_at {
                job.git_committed_at = Some(ts);
            }
        }
        job.updated_at = Utc::now();
        Ok(true)
    }

    async fn mark_terminal(
        &self,
        job_id: Uuid,
        claim_token: Uuid,
        terminal_status: TerminalJobStatus,
    ) -> Result<bool> {
        let mut s = self.state.lock().unwrap();
        let Some(job) = s.jobs.get_mut(&job_id) else {
            return Ok(false);
        };
        if job.status != JobStatus::Running || job.claim_token != Some(claim_token) {
            return Ok(false);
        }
        job.status = terminal_status.into();
        job.updated_at = Utc::now();
        Ok(true)
    }

    async fn sweep_stuck_claims(&self, lease: Duration) -> Result<u64> {
        let cutoff = Utc::now() - lease;
        let mut s = self.state.lock().unwrap();
        let mut recovered = 0u64;
        for job in s.jobs.values_mut() {
            if job.status == JobStatus::Claimed
                && job
                    .claimed_at
                    .map(|t| t < cutoff)
                    .unwrap_or(false)
            {
                job.status = JobStatus::Queued;
                job.claim_token = None;
                job.claimed_at = None;
                job.updated_at = Utc::now();
                recovered += 1;
            }
        }
        Ok(recovered)
    }

    async fn complete_job(&self, completion: &JobCompletion) -> Result<bool> {
        // Mirror the Postgres transaction under a single lock: guarded
        // running→completed, then result (+ optional metric) + completed
        // event, all-or-nothing. The status guard prevents a double
        // finish, so the write-once companions can't collide here.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&completion.job_id) {
            Some(j)
                if j.status == JobStatus::Running
                    && j.claim_token == Some(completion.claim_token) => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: completion.job_id,
            event_kind: JobEventKind::Completed,
            event_status: JobEventStatus::Success,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: None,
            detail: completion
                .event_detail
                .clone()
                .map(sqlx::types::Json),
        };
        let job = s
            .jobs
            .get_mut(&completion.job_id)
            .unwrap();
        job.status = JobStatus::Completed;
        job.updated_at = now;
        s.results
            .insert(completion.result.job_id, completion.result.clone());
        if let Some(m) = completion.metric.as_ref() {
            s.metrics
                .insert(m.job_id, m.clone());
        }
        if let Some(calibration_id) = completion.baseline_calibration_id
            && let Some(spec_id) = s
                .jobs
                .get(&completion.job_id)
                .map(|job| job.benchmark_spec_id)
            && let Some(spec) = s.specs.get_mut(&spec_id)
        {
            spec.baseline_calibration_id = Some(calibration_id);
            spec.updated_at = now;
        }
        s.events.push(event);
        Ok(true)
    }

    async fn fail_job(&self, failure: &JobFailure) -> Result<bool> {
        // `fail_job` accepts `claimed` OR `running` (a job can fail
        // before it starts — preflight resolution / comment posting),
        // unlike `complete_job` which is running-only. This is what lets
        // a persistent preflight failure terminalize instead of looping
        // via the stuck-claim sweep.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&failure.job_id) {
            Some(j)
                if matches!(j.status, JobStatus::Claimed | JobStatus::Running)
                    && j.claim_token == Some(failure.claim_token) => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: failure.job_id,
            event_kind: JobEventKind::Failed,
            event_status: JobEventStatus::Fail,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: Some(failure.remark.clone()),
            detail: failure
                .event_detail
                .clone()
                .map(sqlx::types::Json),
        };
        let job = s
            .jobs
            .get_mut(&failure.job_id)
            .unwrap();
        job.status = JobStatus::Failed;
        job.updated_at = now;
        if let Some(r) = failure.result.as_ref() {
            s.results
                .insert(r.job_id, r.clone());
        }
        s.events.push(event);
        Ok(true)
    }

    async fn cancel_job(&self, job_id: Uuid, claim_token: Uuid, remark: &str) -> Result<bool> {
        // Mirror `fail_job` (claimed OR running, claim-token guarded) but
        // transition to `cancelled` + a `cancelled` event, no forensics result.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&job_id) {
            Some(j)
                if matches!(j.status, JobStatus::Claimed | JobStatus::Running)
                    && j.claim_token == Some(claim_token) => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Cancelled,
            event_status: JobEventStatus::Fail,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: Some(remark.to_string()),
            detail: None,
        };
        let job = s
            .jobs
            .get_mut(&job_id)
            .unwrap();
        job.status = JobStatus::Cancelled;
        job.updated_at = now;
        s.events.push(event);
        Ok(true)
    }

    async fn running_job_ids(&self) -> Result<Vec<Uuid>> {
        let s = self.state.lock().unwrap();
        Ok(s.jobs
            .values()
            .filter(|j| j.status == JobStatus::Running)
            .map(|j| j.id)
            .collect())
    }

    async fn find_active_job(
        &self,
        github_repo_id: i64,
        commit: &str,
        source: crate::models::JobSource,
        workload_key: &str,
    ) -> Result<Option<Uuid>> {
        let s = self.state.lock().unwrap();
        Ok(s.jobs
            .values()
            .find(|j| {
                j.github_repo_id == github_repo_id
                    && j.git_commit_hash.as_deref() == Some(commit)
                    && j.source == source
                    && j.workload_key.as_deref() == Some(workload_key)
                    && matches!(
                        j.status,
                        JobStatus::Queued | JobStatus::Claimed | JobStatus::Running
                    )
            })
            .map(|j| j.id))
    }

    async fn recent_build_attempt(
        &self,
        github_repo_id: i64,
        commit: &str,
        build_target: crate::models::BuildTarget,
        failed_since: chrono::DateTime<Utc>,
    ) -> Result<Option<Uuid>> {
        let s = self.state.lock().unwrap();
        Ok(s.jobs
            .values()
            .find(|j| {
                j.github_repo_id == github_repo_id
                    && j.git_commit_hash.as_deref() == Some(commit)
                    && j.task_kind == crate::models::TaskKind::BuildOnly
                    && j.build_target == build_target
                    && (matches!(
                        j.status,
                        JobStatus::Queued | JobStatus::Claimed | JobStatus::Running
                    ) || (matches!(j.status, JobStatus::Failed | JobStatus::Cancelled)
                        && j.updated_at >= failed_since))
            })
            .map(|j| j.id))
    }

    async fn find_baseline_for(
        &self,
        merge_base_sha: &str,
        base_ref: &str,
        merge_base_committed_at: Option<chrono::DateTime<chrono::Utc>>,
        workload_key: &str,
    ) -> Result<Option<BaselineMatch>> {
        let s = self.state.lock().unwrap();
        // Eligible = completed baseline, same workload, with a recorded metric.
        // v10 (0005): keys on the `intent` axis (`baseline_benchmark`).
        let eligible = |j: &&Job| {
            j.intent == crate::models::JobIntent::BaselineBenchmark
                && j.status == JobStatus::Completed
                && j.workload_key.as_deref() == Some(workload_key)
                && s.metrics.contains_key(&j.id)
        };

        // 1. Exact hit at the merge-base SHA (repo-agnostic). Deterministic tie:
        //    freshest measurement, then job id (mirrors the SQL ORDER BY).
        if let Some(j) = s
            .jobs
            .values()
            .filter(eligible)
            .filter(|j| j.git_commit_hash.as_deref() == Some(merge_base_sha))
            .max_by_key(|j| (s.metrics[&j.id].created_at, j.id))
        {
            return Ok(Some(make_match(j, &s.metrics[&j.id], BaselineSelection::Exact)));
        }

        // 2. Nearest-before on the target branch (needs a fork-point timestamp).
        let Some(ts) = merge_base_committed_at else {
            return Ok(None);
        };
        let nearest = s
            .jobs
            .values()
            .filter(eligible)
            .filter(|j| {
                j.git_ref_display == base_ref
                    && j.git_committed_at
                        .is_some_and(|c| c <= ts)
            })
            // Newest commit ≤ fork-point; ties break to freshest measurement,
            // then job id (mirrors the SQL ORDER BY).
            .max_by_key(|j| (j.git_committed_at, s.metrics[&j.id].created_at, j.id));
        Ok(nearest.map(|j| make_match(j, &s.metrics[&j.id], BaselineSelection::NearestBefore)))
    }

    async fn queued_jobs_ordered(&self) -> Result<Vec<Job>> {
        let s = self.state.lock().unwrap();
        let mut queued: Vec<Job> = s
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Queued)
            .cloned()
            .collect();
        // Match `claim_next_queued`'s `ORDER BY created_at, id`.
        queued.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then(a.id.cmp(&b.id))
        });
        Ok(queued)
    }

    async fn cancel_orphan(&self, job_id: Uuid, remark: &str) -> Result<bool> {
        // Mirror the Postgres path: unconditional running→cancelled (no claim
        // guard) + a `cancelled` event, idempotent on a re-run.
        let mut s = self.state.lock().unwrap();
        match s.jobs.get(&job_id) {
            Some(j) if j.status == JobStatus::Running => {}
            _ => return Ok(false),
        }
        let now = Utc::now();
        let event = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id,
            event_kind: JobEventKind::Cancelled,
            event_status: JobEventStatus::Fail,
            occurred_at: now,
            github_comment_id: None,
            github_check_run_id: None,
            github_check_run_url: None,
            remark: Some(remark.to_string()),
            detail: None,
        };
        let job = s
            .jobs
            .get_mut(&job_id)
            .unwrap();
        job.status = JobStatus::Cancelled;
        job.updated_at = now;
        s.events.push(event);
        Ok(true)
    }

    async fn queued_event(&self, job_id: Uuid) -> Result<Option<JobEvent>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .find(|e| e.job_id == job_id && e.event_kind == JobEventKind::Queued)
            .cloned())
    }

    async fn pull_request_link(&self, job_id: Uuid) -> Result<Option<GithubPullRequestJob>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .pr_links
            .iter()
            .find(|l| l.job_id == job_id)
            .cloned())
    }

    async fn latest_comment_id(&self, job_id: Uuid) -> Result<Option<i64>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .filter(|e| {
                e.job_id == job_id
                    && e.github_comment_id.is_some()
                    && matches!(
                        e.event_kind,
                        JobEventKind::CommentPosted | JobEventKind::CommentUpdated
                    )
            })
            // Highest event id == most recent (monotonic counter).
            .max_by_key(|e| e.id)
            .and_then(|e| e.github_comment_id))
    }

    async fn latest_check_run(&self, job_id: Uuid) -> Result<Option<(i64, Option<String>)>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .filter(|e| {
                e.job_id == job_id
                    && e.github_check_run_id
                        .is_some()
                    && matches!(e.event_kind, JobEventKind::CheckRunCreated)
            })
            .max_by_key(|e| e.id)
            .map(|e| {
                (
                    e.github_check_run_id
                        .unwrap_or_default(),
                    e.github_check_run_url.clone(),
                )
            }))
    }

    async fn latest_plan_message_ts(&self, job_id: Uuid) -> Result<Option<String>> {
        let s = self.state.lock().unwrap();
        Ok(s.events
            .iter()
            .filter(|e| e.job_id == job_id && matches!(e.event_kind, JobEventKind::PlanMessageSent))
            .max_by_key(|e| e.id)
            .and_then(|e| {
                e.detail
                    .as_ref()
                    .and_then(|d| {
                        d.0.get("plan_message_ts")
                            .and_then(|v| v.as_str())
                            .map(str::to_string)
                    })
            }))
    }

    async fn insert_event(&self, new: &NewJobEvent) -> Result<JobEvent> {
        let row = JobEvent {
            id: self
                .next_event_id
                .fetch_add(1, Ordering::SeqCst),
            job_id: new.job_id,
            event_kind: new.event_kind,
            event_status: new.event_status,
            occurred_at: Utc::now(),
            github_comment_id: new.github_comment_id,
            github_check_run_id: new.github_check_run_id,
            github_check_run_url: new
                .github_check_run_url
                .clone(),
            remark: new.remark.clone(),
            detail: new
                .detail
                .clone()
                .map(sqlx::types::Json),
        };
        self.state
            .lock()
            .unwrap()
            .events
            .push(row.clone());
        Ok(row)
    }

    async fn record_metric(&self, metric: &JobMetric) -> Result<()> {
        // Slice 8 (post-review L1 fix): mirror the Postgres write-once
        // PK enforcement. The original impl silently overwrote on
        // duplicate, which could mask double-write bugs in later
        // tests that exercise the in-memory store.
        let mut s = self.state.lock().unwrap();
        if s.metrics
            .contains_key(&metric.job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "job_metric write-once: duplicate row for job_id={}",
                metric.job_id
            )));
        }
        s.metrics
            .insert(metric.job_id, metric.clone());
        Ok(())
    }

    async fn record_result(&self, result: &JobResult) -> Result<()> {
        // Same write-once semantics as `record_metric`.
        let mut s = self.state.lock().unwrap();
        if s.results
            .contains_key(&result.job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "job_result write-once: duplicate row for job_id={}",
                result.job_id
            )));
        }
        s.results
            .insert(result.job_id, result.clone());
        Ok(())
    }

    async fn link_to_webhook(&self, webhook_id: i64, job_id: Uuid) -> Result<GithubWebhookJob> {
        // Slice 8 (post-review L1 fix): mirror the Postgres
        // `UNIQUE (job_id)` on github_webhook_job. Postgres rejects
        // a second link for the same job; the in-memory mirror does
        // the same so the test surfaces the bug rather than silently
        // accumulating duplicate links. Slice 9 added
        // `UNIQUE (github_webhook_id)` — mirror that too.
        let mut s = self.state.lock().unwrap();
        if s.webhook_links
            .iter()
            .any(|l| l.job_id == job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_webhook_job UNIQUE(job_id): duplicate link for job_id={job_id}"
            )));
        }
        if s.webhook_links
            .iter()
            .any(|l| l.github_webhook_id == webhook_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_webhook_job UNIQUE(github_webhook_id): duplicate link for \
                 webhook_id={webhook_id}"
            )));
        }
        let row = GithubWebhookJob {
            github_webhook_id: webhook_id,
            job_id,
            created_at: Utc::now(),
        };
        s.webhook_links
            .push(row.clone());
        Ok(row)
    }

    async fn link_to_user(&self, user_id: i64, job_id: Uuid) -> Result<GithubUserJob> {
        // Mirrors `UNIQUE (job_id)` on github_user_job.
        let mut s = self.state.lock().unwrap();
        if s.user_links
            .iter()
            .any(|l| l.job_id == job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_user_job UNIQUE(job_id): duplicate link for job_id={job_id}"
            )));
        }
        let row = GithubUserJob {
            github_user_id: user_id,
            job_id,
            created_at: Utc::now(),
        };
        s.user_links.push(row.clone());
        Ok(row)
    }

    async fn link_to_pull_request(
        &self,
        pull_request_id: i64,
        job_id: Uuid,
        triggering_comment_id: Option<i64>,
    ) -> Result<GithubPullRequestJob> {
        // Mirrors `PRIMARY KEY (job_id)` on github_pull_request_job.
        let mut s = self.state.lock().unwrap();
        if s.pr_links
            .iter()
            .any(|l| l.job_id == job_id)
        {
            return Err(crate::Error::Other(anyhow::anyhow!(
                "github_pull_request_job PK(job_id): duplicate link for job_id={job_id}"
            )));
        }
        let row = GithubPullRequestJob {
            job_id,
            github_pull_request_id: pull_request_id,
            triggering_comment_id,
            created_at: Utc::now(),
        };
        s.pr_links.push(row.clone());
        Ok(row)
    }
}
