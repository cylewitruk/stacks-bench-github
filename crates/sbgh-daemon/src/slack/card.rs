//! The v8 Slack benchmark card render (item `0023`, iteration v8).
//!
//! A single [`Card`] renders to the message `blocks`: a 4-row `plan` timeline
//! (Job → Build → Run → Finalize), each row a **tense-progressing** title with
//! an italic [`details`](CardRow::details) line while it's pending/in-progress
//! that clears into a plain [`output`](CardRow::output) on complete. Once the
//! run finishes successfully ([`Card::results`] is `Some`), a `markdown`
//! results table and a **primary** "Download Profiler Data" button render
//! beneath the plan (the plan stays above — Block Kit has no API to force it
//! collapsed).
//!
//! The stage model + card builders ([`queued_card`] / [`running_card`] /
//! [`completed_card`] / [`failed_card`]) live here so both
//! [`SlackTimeline`](crate::slack::timeline)
//! (running state) and the pre-claim Slack connector (the queued card) share
//! **one** visual language without duplicating row strings. A [`CardCtx`]
//! carries the per-job bits; `commit`/`commit_url` are `None` pre-claim.

use std::cmp::Ordering;

use serde_json::{Value, json};

use crate::bench_summary::{self, DB_LINK_TTL_HUMAN, PlanTaskStatus, RunResult};

/// An optional per-row link rendered as the task's `source` (e.g. "View
/// commit").
pub struct CardLink {
    pub url: String,
    pub text: String,
}

/// One row of the 4-row live timeline.
pub struct CardRow {
    /// Tense-progressing title (future → present → past) per `status`.
    pub title: String,
    pub status: PlanTaskStatus,
    /// Italic "what's happening now / what it's waiting for". The render layer
    /// shows it **only** on a non-terminal row (pending/in-progress) and
    /// suppresses it once the row is terminal — the caller needn't clear it.
    pub details: Option<String>,
    /// Plain summary (or failure reason). The render layer shows it **only** on
    /// a terminal row (complete/error).
    pub output: Option<String>,
    pub source: Option<CardLink>,
}

/// The completion-only results, appended below the plan on terminal success.
pub struct Results<'a> {
    pub metrics: Option<&'a RunResult>,
    /// Presigned `stacks-bench.db` URL — S3-only, **presence-gated by the
    /// caller** (a dead link is never offered).
    pub db_url: Option<&'a str>,
}

/// A full render of the Slack card: the `plan` timeline, plus — when `results`
/// is `Some` (terminal success) — the markdown results table + download button.
pub struct Card<'a> {
    pub title: String,
    pub job_id: &'a str,
    pub rev: &'a str,
    pub commit: Option<&'a str>,
    pub bench_args: &'a [String],
    pub rows: Vec<CardRow>,
    pub results: Option<Results<'a>>,
}

/// The four plan rows: Job → Build → Run → Finalize.
pub const STAGES: usize = 4;

/// Stable task ids for the streamed Slack `plan`.
pub const TASK_IDS: [&str; STAGES] = ["job", "build", "run", "finalize"];

/// Per-job render context shared by the card builders — the bits of the job
/// that don't change between renders. `commit`/`commit_url` are `None`
/// **pre-claim** (the connector's queued card, before the rev resolves to a
/// SHA); the timeline supplies them once the job is claimed.
pub struct CardCtx<'a> {
    pub rev: &'a str,
    pub commit: Option<&'a str>,
    pub commit_url: Option<&'a str>,
    pub job_id: &'a str,
    /// Effective workload args for this job (Slack ad-hoc/user-tunable
    /// arguments). Used only for the compact context header above the plan.
    pub bench_args: &'a [String],
    /// Set when the Build phase was served from the binary cache (item 0025,
    /// v9) — the short fingerprint digest, surfaced as the Build row's
    /// subtext ("Reused cached build · …") instead of the plain "Built
    /// benchmark binaries". `None` for a normal build (and the pre-claim /
    /// queue cards).
    pub cached_build: Option<&'a str>,
}

/// Per-row display strings — the tense-progressing titles + italic detail
/// lines.
struct StageText {
    pending_title: &'static str,
    active_title: &'static str,
    done_title: &'static str,
    pending_details: &'static str,
    active_details: &'static str,
}

const STAGE_TEXT: [StageText; STAGES] = [
    // 0 — Job (queue → start; pending/active states drive the pre-claim queued card).
    StageText {
        pending_title: "Queued",
        active_title: "Preparing job",
        done_title: "Job started",
        pending_details: "Waiting for an available slot",
        active_details: "Preparing the job",
    },
    // 1 — Build
    StageText {
        pending_title: "Build benchmark binaries",
        active_title: "Building benchmark binaries",
        done_title: "Built benchmark binaries",
        pending_details: "Waiting for the job to start",
        active_details: "Building the stacks-bench release binary",
    },
    // 2 — Run
    StageText {
        pending_title: "Run benchmark",
        active_title: "Running benchmark",
        done_title: "Benchmark run completed",
        pending_details: "Waiting for the release binaries",
        active_details: "Running the benchmark",
    },
    // 3 — Finalize
    StageText {
        pending_title: "Finalize results",
        active_title: "Publishing artifacts",
        done_title: "Benchmark completed",
        pending_details: "Waiting for the benchmark run to complete",
        active_details: "Publishing artifacts",
    },
];

/// The render state of one row, selecting which `StageText` form + status.
enum RowState<'a> {
    Pending,
    Active,
    Done,
    Errored(&'a str),
}

/// The **running** card: `stage` (1..=`STAGES`-1) is the in-progress row,
/// earlier rows complete, later pending. The pre-claim **queued** view is
/// [`queued`] (Job row pending), *not* `running(0)` (which renders Job active).
#[cfg(test)]
fn running(ctx: &CardCtx, stage: usize) -> Value {
    render(&running_card(ctx, stage))
}

/// The typed **running** card. Used by both the Block Kit fallback renderer and
/// the streaming `task_update` path.
pub fn running_card<'a>(ctx: &'a CardCtx<'a>, stage: usize) -> Card<'a> {
    let rows = (0..STAGES)
        .map(|i| {
            let state = match i.cmp(&stage) {
                Ordering::Less => RowState::Done,
                Ordering::Equal => RowState::Active,
                Ordering::Greater => RowState::Pending,
            };
            card_row(ctx, i, state)
        })
        .collect();
    Card {
        title: title(ctx, false),
        job_id: ctx.job_id,
        rev: ctx.rev,
        commit: ctx.commit,
        bench_args: ctx.bench_args,
        rows,
        results: None,
    }
}

/// The **queued** card (pre-claim): every row pending, the Job row showing
/// "Queued" with an optional live-position `detail` overriding its default
/// "Waiting for an available slot". Posted by the connector at enqueue and
/// updated by the runner's queue-position updater; the rev resolves to a commit
/// only at claim, so [`CardCtx::commit`] is `None` here.
#[cfg(test)]
fn queued(ctx: &CardCtx, detail: Option<&str>) -> Value {
    render(&queued_card(ctx, detail))
}

/// The typed **queued** card (pre-claim).
pub fn queued_card<'a>(ctx: &'a CardCtx<'a>, detail: Option<&str>) -> Card<'a> {
    let mut rows: Vec<CardRow> = (0..STAGES)
        .map(|i| card_row(ctx, i, RowState::Pending))
        .collect();
    if let Some(detail) = detail {
        rows[0].details = Some(detail.to_string());
    }
    Card {
        title: title(ctx, false),
        job_id: ctx.job_id,
        rev: ctx.rev,
        commit: ctx.commit,
        bench_args: ctx.bench_args,
        rows,
        results: None,
    }
}

/// The **completed** card: every row complete + the results table/button.
#[cfg(test)]
fn completed(ctx: &CardCtx, results: Results) -> Value {
    render(&completed_card(ctx, results))
}

/// The typed **completed** card: every row complete + the results.
pub fn completed_card<'a>(ctx: &'a CardCtx<'a>, results: Results<'a>) -> Card<'a> {
    let rows = (0..STAGES)
        .map(|i| card_row(ctx, i, RowState::Done))
        .collect();
    Card {
        title: title(ctx, true),
        job_id: ctx.job_id,
        rev: ctx.rev,
        commit: ctx.commit,
        bench_args: ctx.bench_args,
        rows,
        results: Some(results),
    }
}

/// The **failed** card: `stage` errored (carrying `reason`), earlier rows
/// complete, later pending.
#[cfg(test)]
fn failed(ctx: &CardCtx, stage: usize, reason: &str) -> Value {
    render(&failed_card(ctx, stage, reason))
}

/// The typed **failed** card.
pub fn failed_card<'a>(ctx: &'a CardCtx<'a>, stage: usize, reason: &str) -> Card<'a> {
    let rows = (0..STAGES)
        .map(|i| {
            let state = match i.cmp(&stage) {
                Ordering::Less => RowState::Done,
                Ordering::Equal => RowState::Errored(reason),
                Ordering::Greater => RowState::Pending,
            };
            card_row(ctx, i, state)
        })
        .collect();
    Card {
        title: title(ctx, true),
        job_id: ctx.job_id,
        rev: ctx.rev,
        commit: ctx.commit,
        bench_args: ctx.bench_args,
        rows,
        results: None,
    }
}

/// One [`CardRow`] for stage `i` in `state`. The render layer owns the
/// details/output contract, so we supply only the meaningful field.
fn card_row(ctx: &CardCtx, i: usize, state: RowState) -> CardRow {
    let text = &STAGE_TEXT[i];
    let (title, status, details, output) = match state {
        RowState::Pending => {
            (text.pending_title, PlanTaskStatus::Pending, Some(text.pending_details), None)
        }
        RowState::Active => {
            (text.active_title, PlanTaskStatus::InProgress, Some(text.active_details), None)
        }
        RowState::Done => (text.done_title, PlanTaskStatus::Complete, None, None),
        RowState::Errored(reason) => (text.active_title, PlanTaskStatus::Error, None, Some(reason)),
    };
    // Build row (item 0025, v9): when this run reused a cached binary, its
    // terminal subtext notes the reused build instead of an empty "Built
    // benchmark binaries".
    let output: Option<String> = if i == 1
        && matches!(status, PlanTaskStatus::Complete)
        && let Some(id) = ctx.cached_build
    {
        Some(format!("Reused cached build · {id}"))
    } else {
        output.map(str::to_string)
    };
    CardRow {
        title: title.to_string(),
        status,
        details: details.map(str::to_string),
        output,
        source: row_source(ctx, i),
    }
}

/// The Build row links the benchmarked commit (once resolved); other rows have
/// no source (the DB download is the results button, not a row link).
fn row_source(ctx: &CardCtx, i: usize) -> Option<CardLink> {
    if i == 1 {
        ctx.commit_url
            .map(|url| CardLink {
                url: url.to_string(),
                text: "View commit".to_string(),
            })
    } else {
        None
    }
}

/// The plan title — present tense while running, past tense at terminal; the
/// short commit once resolved (just the rev pre-claim).
fn title(ctx: &CardCtx, terminal: bool) -> String {
    let verb = if terminal { "Benchmark" } else { "Benchmarking" };
    match ctx.commit {
        Some(commit) => format!("{verb} {} @ {}", ctx.rev, short_commit(commit)),
        None => format!("{verb} {}", ctx.rev),
    }
}

fn short_commit(commit: &str) -> &str {
    commit
        .get(..8)
        .unwrap_or(commit)
}

/// Render `card` to the message `blocks` array. Falls back to a plain section
/// if the typed builder ever rejects the (statically valid) shape, so reporting
/// stays non-fatal.
pub fn render(card: &Card) -> Value {
    match build(card) {
        Ok(blocks) => blocks,
        Err(e) => {
            tracing::warn!(error = %e, "slack: v8 card build failed; using a plain section");
            fallback(card)
        }
    }
}

fn build(card: &Card) -> Result<Value, Box<dyn std::error::Error>> {
    let mut blocks: Vec<Value> = context_blocks(card);
    blocks.push(serde_json::to_value(build_plan(card)?)?);
    if let Some(results) = &card.results {
        blocks.extend(result_blocks(results));
    }
    Ok(Value::Array(blocks))
}

pub(crate) fn context_blocks(card: &Card) -> Vec<Value> {
    let mut blocks = Vec::new();
    let workload = workload_context(card);
    if !workload.is_empty() {
        let mut elements = Vec::with_capacity(workload.len() + 1);
        elements.push(json!({
            "type": "image",
            "image_url": "https://static.vecteezy.com/system/resources/previews/026/123/690/non_2x/benchmark-measure-icon-in-flat-style-dashboard-rating-illustration-on-white-isolated-background-progress-service-business-concept-vector.jpg",
            "alt_text": "benchmark",
        }));
        elements.extend(
            workload
                .into_iter()
                .map(|text| {
                    json!({
                        "type": "mrkdwn",
                        "text": text,
                    })
                }),
        );
        blocks.push(json!({
            "type": "context",
            "elements": elements,
        }));
    }

    let ref_label = match card.commit {
        Some(commit) => {
            format!("*{}*  _{}_", escape_mrkdwn(card.rev), escape_mrkdwn(short_commit(commit)))
        }
        None => format!("*{}*", escape_mrkdwn(card.rev)),
    };
    blocks.push(json!({
        "type": "context",
        "elements": [
            {
                "type": "image",
                "image_url": "https://images.icon-icons.com/2582/PNG/512/price_tag_icon_153994.png",
                "alt_text": "ref",
            },
            {
                "type": "mrkdwn",
                "text": ref_label,
            },
        ],
    }));
    blocks
}

fn workload_context(card: &Card) -> Vec<String> {
    let mut parts = Vec::new();
    let target = workload_target_context(card.bench_args);
    let target_unit = target
        .as_ref()
        .map(|target| target.unit)
        .unwrap_or("block");
    if let Some(target) = target {
        parts.push(target.text);
    }
    if let Some(warmup) = flag_value(card.bench_args, "--warmup") {
        parts.push(format!("*{}* {target_unit} warmup", escape_mrkdwn(&format_count(warmup))));
    } else {
        parts.push(format!("*0* {target_unit} warmup"));
    }

    if let Some(repetitions) = flag_value(card.bench_args, "--repetitions") {
        parts.push(format!(
            "*{}* {}",
            escape_mrkdwn(&format_count(repetitions)),
            pluralize("repetition", repetitions)
        ));
    } else {
        parts.push("*1* repetition".to_string());
    }
    parts
}

struct WorkloadTarget {
    text: String,
    unit: &'static str,
}

fn workload_target_context(args: &[String]) -> Option<WorkloadTarget> {
    if let (Some(start), Some(count)) =
        (flag_value(args, "--start-at"), flag_value(args, "--count"))
        && let (Ok(start_n), Ok(count_n)) = (start.parse::<u64>(), count.parse::<u64>())
        && count_n > 0
    {
        let end = start_n.saturating_add(count_n - 1);
        return Some(WorkloadTarget {
            text: format!(
                "*Measuring* blocks *{}* to *{}*",
                escape_mrkdwn(&format_count(start)),
                escape_mrkdwn(&format_count(&end.to_string()))
            ),
            unit: "block",
        });
    }

    let blocks = flag_values(args, "--block");
    if !blocks.is_empty() {
        let text = match blocks.as_slice() {
            [one] => format!("*Measuring* block *{}*", escape_mrkdwn(&format_count(one))),
            [first, last] => format!(
                "*Measuring* blocks *{}* to *{}*",
                escape_mrkdwn(&format_count(first)),
                escape_mrkdwn(&format_count(last))
            ),
            many => format!("*Measuring* *{}* blocks", many.len()),
        };
        return Some(WorkloadTarget { text, unit: "block" });
    }

    let txids = flag_values(args, "--txid");
    if !txids.is_empty() {
        let text = match txids.as_slice() {
            [one] => format!("*Measuring* tx *{}*", short_txid(one)),
            many => format!("*Measuring* *{}* txs", many.len()),
        };
        return Some(WorkloadTarget { text, unit: "tx" });
    }
    None
}

fn format_count(s: &str) -> String {
    let Ok(n) = s.parse::<u64>() else {
        return s.to_string();
    };
    let raw = n.to_string();
    let mut out = String::with_capacity(raw.len() + raw.len() / 3);
    for (i, ch) in raw.chars().enumerate() {
        if i > 0 && (raw.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

fn pluralize<'a>(singular: &'a str, count: &str) -> &'a str {
    if count == "1" { singular } else { "repetitions" }
}

fn flag_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    flag_values(args, flag)
        .into_iter()
        .next()
}

fn flag_values<'a>(args: &'a [String], flag: &str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == flag {
            if let Some(value) = iter.next() {
                values.push(value.as_str());
            }
        } else if let Some(value) = arg.strip_prefix(&format!("{flag}=")) {
            values.push(value);
        }
    }
    values
}

fn short_txid(txid: &str) -> String {
    let trimmed = txid
        .strip_prefix("0x")
        .or_else(|| txid.strip_prefix("0X"))
        .unwrap_or(txid);
    let short = trimmed
        .get(..12)
        .unwrap_or(trimmed);
    format!("{}…", escape_mrkdwn(short))
}

fn escape_mrkdwn(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The terminal result blocks appended below the plan. Used directly by
/// `chat.stopStream`, where Slack renders final `blocks` below the streamed
/// plan instead of replacing the plan itself.
pub fn result_blocks(results: &Results) -> Vec<Value> {
    let mut blocks = vec![json!({ "type": "divider" }), markdown_block(results.metrics)];
    // The primary download button — only when an in-bucket DB link exists.
    if let Some(url) = results.db_url {
        blocks.push(json!({ "type": "divider" }));
        blocks.push(download_section(url));
    }
    blocks
}

fn build_plan(card: &Card) -> Result<slack_messaging::blocks::Plan, Box<dyn std::error::Error>> {
    use slack_messaging::blocks::elements::UrlSource;
    use slack_messaging::blocks::{Plan, TaskCard};

    let mut plan = Plan::builder().title(card.title.clone());
    for (i, row) in card.rows.iter().enumerate() {
        let mut task = TaskCard::builder()
            .task_id(format!("{}-{i}", card.job_id))
            .title(row.title.clone())
            .status(to_task_status(row.status));
        // The render layer owns the contract: italic `details` show only while
        // the row is **non-terminal** (pending/in-progress); plain `output` shows
        // only on a **terminal** row (complete/error, where the v6 timeline puts
        // the summary or the failure reason). A caller can't render both at once.
        let terminal = matches!(row.status, PlanTaskStatus::Complete | PlanTaskStatus::Error);
        if !terminal && let Some(details) = &row.details {
            task = task.details(rich_text_line(details.clone(), true)?);
        }
        if terminal && let Some(output) = &row.output {
            task = task.output(rich_text_line(output.clone(), false)?);
        }
        if let Some(src) = &row.source {
            task = task.source(
                UrlSource::builder()
                    .url(src.url.clone())
                    .text(src.text.clone())
                    .build()?,
            );
        }
        plan = plan.task(task.build()?);
    }
    Ok(plan.build()?)
}

/// A one-line rich-text body (one section, one text element). `italic` styles
/// it (the task `details` line); plain for an `output` summary.
fn rich_text_line(
    text: String,
    italic: bool,
) -> Result<slack_messaging::blocks::RichText, Box<dyn std::error::Error>> {
    use slack_messaging::blocks::RichText;
    use slack_messaging::blocks::rich_text::RichTextSection;
    use slack_messaging::blocks::rich_text::types::{
        RichTextElementText, RichTextStyle, StyleTypeFour,
    };

    let mut element = RichTextElementText::builder().text(text);
    if italic {
        element = element.style(
            RichTextStyle::<StyleTypeFour>::builder()
                .italic(true)
                .build()?,
        );
    }
    Ok(RichText::builder()
        .element(
            RichTextSection::builder()
                .element(element.build()?)
                .build()?,
        )
        .build()?)
}

fn to_task_status(s: PlanTaskStatus) -> slack_messaging::blocks::TaskStatus {
    use slack_messaging::blocks::TaskStatus;
    match s {
        PlanTaskStatus::Pending => TaskStatus::Pending,
        PlanTaskStatus::InProgress => TaskStatus::InProgress,
        PlanTaskStatus::Complete => TaskStatus::Complete,
        PlanTaskStatus::Error => TaskStatus::Error,
    }
}

/// The `markdown` results block — a heading + the shared GFM metric table (or a
/// fallback note when no metrics parsed).
fn markdown_block(metrics: Option<&RunResult>) -> Value {
    json!({ "type": "markdown", "text": results_markdown(metrics) })
}

fn results_markdown(metrics: Option<&RunResult>) -> String {
    let table = metrics
        .map(bench_summary::metric_table)
        .unwrap_or_default();
    if table.is_empty() {
        "## Benchmark Results\n\n_No parsed metrics — see the daemon archive for raw output._"
            .to_string()
    } else {
        format!("## Benchmark Results\n\n{table}")
    }
}

/// The download `section`: a primary-styled URL button for the presigned DB.
fn download_section(url: &str) -> Value {
    json!({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": format!("Profiler artifacts are available for download for {DB_LINK_TTL_HUMAN}."),
        },
        // Deliberately NO `action_id`. A URL button with one makes Slack
        // dispatch a `block_actions` interaction over Socket Mode whose echoed
        // message carries this card's `plan` block — which `slack-morphism`
        // can't deserialize, so the envelope is never ACKed, Slack redelivers it,
        // and the listener churns (errors → reconnect). The download is
        // client-side (the `url`) and we handle no interactions, so the action_id
        // was pure liability.
        "accessory": {
            "type": "button",
            "text": { "type": "plain_text", "text": "Download Profiler Data", "emoji": true },
            "style": "primary",
            "url": url,
        },
    })
}

/// Minimal valid fallback if the typed builder errors — a single mrkdwn section
/// so something still posts (each row's output/details + the DB link).
fn fallback(card: &Card) -> Value {
    let mut text = format!("*{}*\n", card.title);
    for row in &card.rows {
        if let Some(out) = &row.output {
            text.push_str(&format!("*{}:* {out}\n", row.title));
        } else if let Some(details) = &row.details {
            text.push_str(&format!("*{}:* {details}\n", row.title));
        }
    }
    if let Some(Results { db_url: Some(url), .. }) = &card.results {
        text.push_str(&format!(":inbox_tray: <{url}|Download Profiler Data>\n"));
    }
    json!([{ "type": "section", "text": { "type": "mrkdwn", "text": text } }])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        title: &str,
        status: PlanTaskStatus,
        details: Option<&str>,
        output: Option<&str>,
    ) -> CardRow {
        CardRow {
            title: title.into(),
            status,
            details: details.map(Into::into),
            output: output.map(Into::into),
            source: None,
        }
    }

    /// A run RunResult that yields a non-empty metric table.
    fn run_result() -> RunResult {
        RunResult::from_bytes(
            br#"{"success":true,"duration_secs":414.0,"data":{"measured_blocks":10,
               "warmup_blocks":2,"duration_secs":251.0,"summary":{"execution_duration_us":1655583,
               "commit_duration_us":113094,"transactions":10}}}"#,
        )
        .expect("a valid run.json")
    }

    fn live_card() -> Card<'static> {
        Card {
            title: "Benchmarking feat/stacks-bench @ 56e9fcba".into(),
            job_id: "job-1",
            rev: "feat/stacks-bench",
            commit: Some("56e9fcba1234"),
            bench_args: &[],
            rows: vec![
                row("Job started", PlanTaskStatus::Complete, None, Some("Started after 17m 23s")),
                row(
                    "Building benchmark binaries",
                    PlanTaskStatus::InProgress,
                    Some("Building stacks-bench release binary"),
                    None,
                ),
                row("Run benchmark", PlanTaskStatus::Pending, Some("Waiting for binaries"), None),
                row("Finalize results", PlanTaskStatus::Pending, Some("Waiting for the run"), None),
            ],
            results: None,
        }
    }

    /// The live card renders the compact context header, then a `plan` block
    /// with four tense rows; the in-progress row's `details` carries the italic
    /// style; no results blocks.
    #[test]
    fn live_card_renders_four_italic_rows() {
        let v = render(&live_card());
        let blocks = v
            .as_array()
            .expect("a blocks array");
        assert_eq!(blocks.len(), 3, "workload context + ref context + plan: {v}");
        assert_eq!(blocks[0]["type"], "context");
        assert_eq!(blocks[1]["type"], "context");
        assert_eq!(blocks[2]["type"], "plan");
        assert_eq!(
            blocks[2]["tasks"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
        let s = v.to_string();
        assert!(s.contains("\"status\":\"in_progress\""), "{s}");
        assert!(s.contains("\"italic\":true"), "details are italic: {s}");
        assert!(!s.contains("\"type\":\"markdown\""), "no results while live: {s}");
    }

    /// The compact context header summarizes the requested workload and the
    /// ref/commit above the plan.
    #[test]
    fn context_header_summarizes_workload_and_ref() {
        let args = vec![
            "--block".to_string(),
            "8123456".to_string(),
            "--block".to_string(),
            "8200000".to_string(),
            "--warmup".to_string(),
            "1000".to_string(),
            "--repetitions=10".to_string(),
        ];
        let ctx = CardCtx {
            rev: "sb-integration/3.4.0.0.3",
            commit: Some("c3b1aad4eeff"),
            commit_url: Some("https://github.com/o/r/commit/c3b1aad4eeff"),
            job_id: "job-1",
            bench_args: &args,
            cached_build: None,
        };
        let v = queued(&ctx, None);
        let blocks = v.as_array().unwrap();
        assert_eq!(blocks[0]["type"], "context");
        assert_eq!(blocks[1]["type"], "context");
        assert_eq!(blocks[2]["type"], "plan");
        let s = v.to_string();
        assert!(s.contains("*Measuring* blocks *8,123,456* to *8,200,000*"), "{s}");
        assert!(s.contains("*1,000* block warmup"), "{s}");
        assert!(s.contains("*10* repetitions"), "{s}");
        assert!(s.contains("*sb-integration/3.4.0.0.3*  _c3b1aad4_"), "{s}");
    }

    /// A completed card appends the results: plan → divider → markdown table →
    /// divider → a primary download button.
    #[test]
    fn completed_card_appends_table_and_primary_button() {
        let r = run_result();
        let mut card = live_card();
        for row in &mut card.rows {
            row.status = PlanTaskStatus::Complete;
            row.details = None;
        }
        card.results = Some(Results {
            metrics: Some(&r),
            db_url: Some("https://s3/stacks-bench.db"),
        });

        let v = render(&card);
        let s = v.to_string();
        let types: Vec<&str> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            ["context", "context", "plan", "divider", "markdown", "divider", "section"],
            "{v}",
        );
        assert!(s.contains("## Benchmark Results"), "{s}");
        assert!(s.contains("| Metric | Value |"), "the GFM table: {s}");
        assert!(s.contains("\"style\":\"primary\""), "primary button: {s}");
        assert!(s.contains("Download Profiler Data"), "{s}");
        assert!(s.contains("https://s3/stacks-bench.db"), "the presigned url: {s}");
        // No `action_id` anywhere: a URL button with one makes Slack dispatch a
        // `block_actions` interaction echoing the `plan` block, which
        // slack-morphism can't deserialize (un-acked envelope → redelivery +
        // reconnect churn). The card carries no interactive elements we handle.
        assert!(!s.contains("action_id"), "card must carry no interactive action_id: {s}");
    }

    /// No in-bucket DB link → the markdown table renders but the button section
    /// is omitted (never a dead link).
    #[test]
    fn completed_card_without_db_link_omits_button() {
        let r = run_result();
        let mut card = live_card();
        card.results = Some(Results {
            metrics: Some(&r),
            db_url: None,
        });
        let v = render(&card);
        let types: Vec<&str> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            ["context", "context", "plan", "divider", "markdown"],
            "no button section: {v}",
        );
        assert!(
            !v.to_string()
                .contains("\"style\":\"primary\"")
        );
    }

    /// The render layer owns the contract: a **complete** row shows its plain
    /// `output` and suppresses any leftover italic `details`, so a caller can't
    /// accidentally render both on one row.
    #[test]
    fn complete_row_suppresses_stale_details() {
        let card = Card {
            title: "t".into(),
            job_id: "j",
            rev: "develop",
            commit: None,
            bench_args: &[],
            rows: vec![row(
                "Built benchmark binaries",
                PlanTaskStatus::Complete,
                Some("Building stacks-bench release binary"), // stale in-progress detail
                Some("Built in 1m 45s"),
            )],
            results: None,
        };
        let s = render(&card).to_string();
        assert!(!s.contains("\"italic\":true"), "complete row drops italic details: {s}");
        assert!(s.contains("Built in 1m 45s"), "shows the output: {s}");
        assert!(!s.contains("Building stacks-bench release binary"), "stale details gone: {s}");
    }

    /// An **error** row is terminal too: it shows its plain `output` (the
    /// reason), never italic details.
    #[test]
    fn error_row_shows_output_not_details() {
        let card = Card {
            title: "t".into(),
            job_id: "j",
            rev: "develop",
            commit: None,
            bench_args: &[],
            rows: vec![row(
                "Run benchmark",
                PlanTaskStatus::Error,
                Some("Running benchmark"),
                Some("Failed: VM died"),
            )],
            results: None,
        };
        let s = render(&card).to_string();
        assert!(s.contains("\"status\":\"error\""), "{s}");
        assert!(s.contains("Failed: VM died"), "shows the reason: {s}");
        assert!(!s.contains("\"italic\":true"), "no italic details on a terminal error: {s}");
        assert!(!s.contains("Running benchmark"), "stale details gone: {s}");
    }

    /// No parsed metrics → the markdown block degrades to a note, not an empty
    /// table.
    #[test]
    fn results_markdown_falls_back_without_metrics() {
        let md = results_markdown(None);
        assert!(md.starts_with("## Benchmark Results"), "{md}");
        assert!(md.contains("No parsed metrics"), "{md}");
        assert!(!md.contains("| Metric |"), "no empty table: {md}");
    }

    // ─── the stage-model builders (shared by the timeline + connector) ───

    fn ctx() -> CardCtx<'static> {
        CardCtx {
            rev: "feat/stacks-bench",
            commit: Some("56e9fcba1234"),
            commit_url: Some("https://github.com/o/r/commit/56e9fcba1234"),
            job_id: "job-1",
            bench_args: &[],
            cached_build: None,
        }
    }

    /// item 0025 (v9): a cache-hit Build row carries the "Reused cached build"
    /// subtext; a normal build does not.
    #[test]
    fn build_row_notes_a_reused_cached_build() {
        let cached = CardCtx {
            rev: "feat/x",
            commit: Some("56e9fcba1234"),
            commit_url: Some("https://github.com/o/r/commit/56e9fcba1234"),
            job_id: "job-1",
            bench_args: &[],
            cached_build: Some("abc123def456"),
        };
        // Stage 2 (Run active) → the Build row is done.
        let s = running(&cached, 2).to_string();
        assert!(s.contains("Reused cached build · abc123def456"), "cached subtext: {s}");
        assert!(s.contains("Built benchmark binaries"), "Build row keeps its done title: {s}");
        // A normal build (the default ctx, no cache) has no such subtext.
        assert!(
            !running(&ctx(), 2)
                .to_string()
                .contains("Reused cached build"),
            "no cached subtext without a hit",
        );
    }

    /// `running(stage)` builds the four rows with earlier-complete / active /
    /// later-pending, the present-tense title, and the Build-row commit link.
    #[test]
    fn running_builds_four_rows_with_the_active_stage() {
        let v = running(&ctx(), 1); // Build active
        let blocks = v.as_array().unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0]["type"], "context");
        assert_eq!(blocks[1]["type"], "context");
        assert_eq!(blocks[2]["type"], "plan");
        let tasks = blocks[2]["tasks"]
            .as_array()
            .unwrap();
        assert_eq!(tasks.len(), 4);
        assert_eq!(tasks[0]["title"], "Job started"); // Job (i<1) complete
        assert_eq!(tasks[0]["status"], "complete");
        assert_eq!(tasks[1]["title"], "Building benchmark binaries"); // Build (i==1) active
        assert_eq!(tasks[1]["status"], "in_progress");
        assert_eq!(tasks[2]["status"], "pending"); // Run
        let s = v.to_string();
        assert!(s.contains("\"italic\":true"), "active/pending details are italic: {s}");
        assert!(s.contains("Benchmarking feat/stacks-bench @ 56e9fcba"), "live title: {s}");
        assert!(s.contains("View commit"), "Build row links the commit: {s}");
    }

    /// `completed` appends the results blocks and uses the past-tense title.
    #[test]
    fn completed_builds_results_card() {
        let r = run_result();
        let v = completed(
            &ctx(),
            Results {
                metrics: Some(&r),
                db_url: Some("https://s3/db"),
            },
        );
        let types: Vec<&str> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            ["context", "context", "plan", "divider", "markdown", "divider", "section"],
            "{v}",
        );
        let s = v.to_string();
        assert!(s.contains("Benchmark feat/stacks-bench @ 56e9fcba"), "terminal title: {s}");
        assert!(!s.contains("\"status\":\"in_progress\""), "all complete: {s}");
        assert!(s.contains("Download Profiler Data"), "{s}");
    }

    /// `failed(stage)` marks the errored row, earlier complete, later pending.
    #[test]
    fn failed_builds_errored_card() {
        let v = failed(&ctx(), 2, "Failed: VM died"); // Run errored
        let tasks = v.as_array().unwrap()[2]["tasks"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(tasks[1]["status"], "complete"); // Build done
        assert_eq!(tasks[2]["status"], "error"); // Run errored
        assert_eq!(tasks[3]["status"], "pending"); // Finalize pending
        assert!(
            v.to_string()
                .contains("Failed: VM died"),
            "{v}"
        );
    }

    /// Pre-claim (the connector's world): with no resolved commit, the title
    /// omits the `@ sha` and the Build row carries no "View commit" link — the
    /// property the queued card relies on (its exact Job-row state is Slice
    /// B2).
    #[test]
    fn pre_claim_card_omits_the_commit() {
        let pre = CardCtx {
            rev: "feat/x",
            commit: None,
            commit_url: None,
            job_id: "j",
            bench_args: &[],
            cached_build: None,
        };
        let s = running(&pre, 0).to_string();
        assert!(s.contains("Benchmarking feat/x"), "rev-only title: {s}");
        assert!(!s.contains(" @ "), "no commit suffix pre-claim: {s}");
        assert!(!s.contains("View commit"), "no commit link pre-claim: {s}");
    }

    /// The queued card: every row pending, the Job row "Queued" with the live
    /// position overriding its default detail.
    #[test]
    fn queued_card_shows_position_on_the_job_row() {
        let pre = CardCtx {
            rev: "feat/x",
            commit: None,
            commit_url: None,
            job_id: "j",
            bench_args: &[],
            cached_build: None,
        };
        let v = queued(&pre, Some("position 3/5, waiting 15m"));
        let tasks = v.as_array().unwrap()[2]["tasks"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(tasks.len(), 4);
        assert_eq!(tasks[0]["title"], "Queued");
        for t in &tasks {
            assert_eq!(t["status"], "pending", "every row pending while queued: {t}");
        }
        let s = v.to_string();
        assert!(s.contains("position 3/5, waiting 15m"), "live position on the Job row: {s}");
        assert!(s.contains("\"italic\":true"), "the detail is italic: {s}");
        assert!(s.contains("Benchmarking feat/x"), "rev-only title pre-claim: {s}");
    }

    /// Without a position, the Job row keeps its default queued detail.
    #[test]
    fn queued_card_without_position_uses_default_detail() {
        let pre = CardCtx {
            rev: "x",
            commit: None,
            commit_url: None,
            job_id: "j",
            bench_args: &[],
            cached_build: None,
        };
        let s = queued(&pre, None).to_string();
        assert!(s.contains("Waiting for an available slot"), "{s}");
    }
}
