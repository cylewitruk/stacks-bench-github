mod api;
mod artifact_store;
mod bench_recipe;
mod bench_summary;
mod comparison;
mod driver;
mod events;
mod job_source;
mod libvirt;
mod progress;
mod recipe;
mod reporter;
mod runner;
mod shutdown;
mod slack;
mod webhook_processor;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use sbgh_core::config::DaemonConfig;
use sbgh_core::db::{
    self, PostgresIngestStore, PostgresInstallationStore, PostgresJobStore, PostgresPolicyStore,
    PostgresPullRequestStore, PostgresRepoStore, PostgresUserStore, PostgresWebhookInbox,
};
use sbgh_core::github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::job_source::{JobSource, RunnableJobStore};
use crate::libvirt::SystemShell;
use crate::runner::Runner;
use crate::webhook_processor::{
    BasicClassifier, CreateHandler, InstallationHandler, InstallationRepositoriesHandler,
    IssueCommentHandler, ProcessorConfig, PullRequestHandler, PushHandler, WebhookProcessor,
};

#[derive(Parser, Debug)]
#[command(version, about = "stacks-bench job daemon (libvirt benchmark runner)")]
struct Args {
    /// Path to an env file to source secrets from. If omitted, `./.env` in the
    /// working directory is loaded best-effort. When this flag IS supplied, a
    /// missing or unreadable file is a fatal error rather than a silent miss.
    #[arg(long, value_name = "PATH")]
    env_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    load_env(args.env_file.as_deref())?;
    init_tracing();

    let config = DaemonConfig::load().context("loading config")?;
    // The daemon is the sole DB client (the handler and CLI became
    // API clients in Phase 4/5) and connects as the owner, so it owns schema
    // setup too: apply pending forward-only migrations at startup, before
    // serving. Single instance — no concurrent-migration race; `sqlx`
    // applies only pending *up* migrations.
    let pool = db::connect(&config.server.database_url).await?;
    db::migrate(&pool)
        .await
        .context("applying database migrations at startup")?;

    let creds =
        AppCredentials::from_pem_file(&config.github.client_id, &config.github.private_key_path)?;
    let tokens = InstallationTokenCache::new(
        creds,
        config
            .github
            .api_base_url
            .clone(),
    );
    let gh = Arc::new(OctocrabClient::new(tokens));
    let shell = Arc::new(SystemShell::new(&config.paths.sudo_binary));

    // The webhook processor runs concurrently with the job runner. Both
    // loop indefinitely; if either returns Err the daemon crashes
    // and systemd restarts it.
    //
    // The classifier is composed from per-event-type EventHandlers. The
    // set of registered handlers is what determines which event types the
    // processor will claim from the inbox (others stay `received`).
    let webhook_inbox = Arc::new(PostgresWebhookInbox::new(pool.clone()));
    let installation_store = Arc::new(PostgresInstallationStore::new(pool.clone()));
    let repo_store = Arc::new(PostgresRepoStore::new(pool.clone()));
    let policy_store = Arc::new(PostgresPolicyStore::new(pool.clone()));
    let user_store = Arc::new(PostgresUserStore::new(pool.clone()));
    let pull_request_store = Arc::new(PostgresPullRequestStore::new(pool.clone()));
    // The job store. The three job-creating handlers (issue_comment
    // /benchmark, push, create) write through it; the runner claims from it.
    let jobs_store = Arc::new(PostgresJobStore::new(pool.clone()));
    // Write-through inbox for the `/api` webhook-submit endpoint.
    let api_ingest = Arc::new(PostgresIngestStore::new(pool.clone()));
    let classifier = BasicClassifier::builder()
        .with_handler(Arc::new(
            IssueCommentHandler::new(
                repo_store.clone(),
                policy_store.clone(),
                installation_store.clone(),
                user_store.clone(),
                pull_request_store.clone(),
                gh.clone(),
                jobs_store.clone(),
            )
            .with_default_args(
                config
                    .stacks_bench
                    .default_args
                    .clone(),
            ),
        ))
        .with_handler(Arc::new(InstallationHandler::new(
            installation_store.clone(),
            repo_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(InstallationRepositoriesHandler::new(
            repo_store.clone(),
            installation_store.clone(),
            policy_store.clone(),
            user_store.clone(),
            gh.clone(),
        )))
        .with_handler(Arc::new(PullRequestHandler::new(
            repo_store.clone(),
            policy_store.clone(),
            installation_store.clone(),
            user_store,
            pull_request_store.clone(),
        )))
        .with_handler(Arc::new(
            PushHandler::new(policy_store.clone(), installation_store.clone(), jobs_store.clone())
                .with_default_args(
                    config
                        .stacks_bench
                        .default_args
                        .clone(),
                ),
        ))
        .with_handler(Arc::new(
            CreateHandler::new(policy_store, installation_store, jobs_store.clone())
                .with_default_args(
                    config
                        .stacks_bench
                        .default_args
                        .clone(),
                ),
        ))
        .build();
    let processor =
        WebhookProcessor::new(webhook_inbox, Arc::new(classifier), ProcessorConfig::default());

    // The configured artifact store (local FS, or S3 with a local mirror).
    // Built once at startup so a bad `[artifacts]` endpoint fails fast here
    // rather than per-job; the runner/reporter derive theirs from config.
    let artifact_store =
        artifact_store::build_store(&config).context("building the artifact store")?;

    // Slack ad-hoc profiling (item 0002), only when `[slack].enabled`. Resolve
    // the default repo → FK ids now (startup-fatal on misconfig, before serving)
    // and build the one Web API client shared by the reporter (terminal results)
    // and the socket connector (replies/reactions). `jobs_store` is cloned for
    // the connector before it moves into the `JobSource` below.
    let slack_runtime = if config.slack.enabled {
        let target = slack::target::resolve_target(
            &pool,
            &config
                .slack
                .default_repository,
        )
        .await
        .context("resolving [slack].default_repository")?;
        let bot_token = config
            .slack
            .bot_token
            .clone()
            .context("[slack].enabled but SBGH_SLACK_BOT_TOKEN is unset")?;
        let web_client: Arc<dyn slack::client::SlackClient> =
            Arc::new(slack::api_client::WebApiClient::new(bot_token));
        tracing::info!(
            repo = %config.slack.default_repository,
            installation_id = target.installation_id,
            repo_id = target.repo_id,
            "slack: ad-hoc profiling enabled",
        );
        Some((config.slack.clone(), target, jobs_store.clone(), web_client))
    } else {
        None
    };

    // The runner claims from the `job` family and posts PR comments for
    // `pr_comment` jobs.
    let runnable_jobs: Arc<dyn RunnableJobStore> =
        Arc::new(JobSource::new(jobs_store, repo_store, pull_request_store, artifact_store));

    // API server (roadmap-v3 Phase 2). Operator `admin` token is a cookie
    // regenerated each boot; the handler's `ingest` token comes from
    // config. Read the listen list out before `config` moves into Runner.
    let api_cookie =
        api::bootstrap_cookie(&config.api.cookie_path).context("bootstrapping api cookie")?;
    if config
        .api
        .ingest_token
        .is_none()
    {
        tracing::warn!(
            "[api].ingest_token unset — webhook submission via /api is disabled until it is set"
        );
    }
    let api_tokens = Arc::new(
        api::ApiTokens::new(
            api_cookie,
            config
                .api
                .ingest_token
                .clone(),
            None,
        )
        .context("building api tokens")?,
    );
    let api_listen = config.api.listen.clone();
    let api_state = api::ApiState {
        pool,
        ingest: api_ingest,
        gh_api_base: config
            .github
            .api_base_url
            .clone(),
    };
    let api_router = api::build_router(api_state, api_tokens);

    // The reporter posts Slack ad-hoc results through the same Web API client
    // the socket connector uses (one bot token, one client).
    let mut runner = Runner::new(config, runnable_jobs, gh, shell);
    if let Some((_, _, _, web_client)) = &slack_runtime {
        runner = runner.with_slack(web_client.clone());
    }

    // Lifecycle shutdown (roadmap-v5 Phase 4B): a `SIGINT` drains (stop
    // claiming, finish in-flight), a second `SIGINT` or `SIGTERM` aborts. The
    // coordinator owns drain completion and fires `exit`, which stops the other
    // arms so the `try_join!` returns and the process exits cleanly.
    let shutdown = shutdown::Shutdown::new();

    tokio::try_join!(
        runner.run(shutdown.clone()),
        async {
            // Stop processing webhooks once shutdown is underway; any in-flight
            // claim is reclaimed by the processor's own sweep on the next boot.
            tokio::select! {
                r = processor.run() => r.map_err(anyhow::Error::from),
                _ = shutdown.exit.cancelled() => Ok(()),
            }
        },
        api::serve(&api_listen, api_router, shutdown.exit.clone()),
        shutdown::watch_signals(shutdown.clone()),
        async {
            // Slack socket-mode receive loop (item 0002), only when enabled. It
            // stops accepting mentions at drain start; a startup failure (bad
            // app token, TLS) is logged but never crashes the daemon — Slack is
            // an optional surface. This arm then idles until full exit so it
            // never collapses the `try_join!` early.
            if let Some((cfg, target, jobs, web_client)) = slack_runtime {
                if let Err(e) =
                    slack::socket::run(cfg, target, jobs, web_client, shutdown.draining.clone())
                        .await
                {
                    tracing::error!(error = ?e, "slack: socket mode failed; continuing without Slack");
                }
            }
            shutdown
                .exit
                .cancelled()
                .await;
            Ok::<(), anyhow::Error>(())
        },
    )?;
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();
}

fn load_env(explicit: Option<&Path>) -> anyhow::Result<()> {
    match explicit {
        Some(path) => {
            dotenvy::from_path(path)
                .with_context(|| format!("loading env file from {}", path.display()))?;
        }
        None => {
            let _ = dotenvy::dotenv();
        }
    }
    Ok(())
}
