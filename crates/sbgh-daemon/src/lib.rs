pub mod api;
mod artifact_store;
mod bench_summary;
mod comparison;
mod duration;
mod fleet;
mod job_source;
#[cfg(test)]
mod pin_manager;
#[cfg(test)]
mod pin_resolver;
mod report;
mod report_event;
mod reporter;
#[cfg(test)]
#[allow(dead_code)]
mod runner;
mod shutdown;
mod slack_queue;
mod slack_report;
mod slack_target;
mod webhook_processor;

use std::sync::Arc;

use anyhow::Context;
use sbgh_core::config::{ArtifactStoreKind, DaemonConfig, LlmConfig};
use sbgh_github::{AppCredentials, InstallationTokenCache, OctocrabClient};
use sbgh_intent::{OpenAiIntentConfig, OpenAiIntentResolver};
use sbgh_postgres::{
    self as db, PostgresIngestStore, PostgresInstallationStore, PostgresJobStore,
    PostgresPolicyStore, PostgresPullRequestStore, PostgresRepoStore, PostgresUserStore,
    PostgresWebhookInbox,
};

fn execution_artifact_store_config(
    config: &DaemonConfig,
) -> anyhow::Result<artifact_store::ArtifactStoreConfig> {
    let local_root = config
        .paths
        .results_archive_dir
        .clone();
    match config.artifacts.kind {
        ArtifactStoreKind::Local => Ok(artifact_store::ArtifactStoreConfig::local(local_root)),
        ArtifactStoreKind::S3 => {
            let s3 = config
                .artifacts
                .s3
                .as_ref()
                .context("[artifacts] kind = s3 but S3 settings are missing (config bug)")?;
            Ok(artifact_store::ArtifactStoreConfig::s3(
                local_root,
                artifact_store::S3StoreConfig {
                    endpoint: s3.endpoint.clone(),
                    bucket: s3.bucket.clone(),
                    region: s3.region.clone(),
                    access_key_id: s3.access_key_id.clone(),
                    secret_access_key: s3.secret_access_key.clone(),
                },
            ))
        }
    }
}

fn build_openai_intent_resolver(config: &LlmConfig) -> anyhow::Result<OpenAiIntentResolver> {
    let api_key = config
        .openai_api_key
        .clone()
        .context("OpenAI API key is not configured")?;
    let provider_config = OpenAiIntentConfig::new(
        api_key,
        config.model.clone(),
        config.input_max_chars,
        std::time::Duration::from_secs(config.timeout_secs),
    );
    Ok(OpenAiIntentResolver::new(provider_config)?)
}

/// Compose and run the daemon from validated process configuration.
///
/// Process bootstrap remains in the binary; this entry point owns the
/// application graph so tests and production compile the same modules.
pub async fn run(config: DaemonConfig) -> anyhow::Result<()> {
    // The daemon is the sole DB client; the handler and CLI are API clients.
    // It connects as the owner, so it owns schema
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
    let gh = Arc::new(OctocrabClient::new(tokens.clone()));
    // The webhook processor runs concurrently with the fleet coordinator. Both
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
    // The job store. The job-creating handlers write through it; the fleet
    // coordinator prepares immutable assignments from it.
    let jobs_store = Arc::new(PostgresJobStore::new(pool.clone()));
    // Write-through inbox for the `/api` webhook-submit endpoint.
    let api_ingest = Arc::new(PostgresIngestStore::new(pool.clone()));
    let fleet_config =
        fleet::FleetConfig::load_from_env().context("loading worker fleet config")?;
    let postgres_fleet = sbgh_postgres::PostgresFleetStore::new(pool.clone());
    let block_validation_queue = fleet_config
        .github_block_validation
        .clone()
        .map(|trigger| {
            Arc::new(fleet::PostgresBlockValidationQueue::new(postgres_fleet.clone(), trigger))
                as Arc<dyn webhook_processor::BlockValidationQueue>
        });
    let issue_comment_handler = IssueCommentHandler::new(
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
    );
    let issue_comment_handler = match block_validation_queue {
        Some(queue) => issue_comment_handler.with_block_validation(queue),
        None => issue_comment_handler,
    };
    let classifier = BasicClassifier::builder()
        .with_handler(Arc::new(issue_comment_handler))
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
    // rather than per-job, then shared by job sourcing and execution/reporting.
    anyhow::ensure!(
        config.artifacts.kind == ArtifactStoreKind::S3,
        "worker fleet mode requires [artifacts] kind = \"s3\""
    );
    let artifact_store_config = execution_artifact_store_config(&config)?;
    let artifact_store = artifact_store::build_store(&artifact_store_config)
        .context("building the artifact store")?;
    let fleet_store: Arc<dyn sbgh_core::db::fleet::FleetStore> = Arc::new(postgres_fleet);
    let fleet_runtime = fleet::FleetRuntime::build(
        fleet_config,
        fleet_store.clone(),
        artifact_store.clone(),
        tokens,
    )
    .await
    .context("building worker fleet control plane")?;

    // Slack ad-hoc profiling, only when `[slack].enabled`. Resolve
    // the default repo → FK ids now (startup-fatal on misconfig, before serving)
    // and build the one Web API client shared by the reporter (terminal results)
    // and the socket connector (replies/reactions). `jobs_store` is cloned for
    // the connector before it moves into the `JobSource` below.
    let slack_runtime = if config.slack.enabled {
        let target = slack_target::resolve_target(
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
        let web_client: Arc<dyn sbgh_slack::SlackClient> =
            Arc::new(sbgh_slack::WebApiClient::new(bot_token));
        let intent_resolver: Option<Arc<dyn sbgh_intent::IntentResolver>> = if config.llm.enabled {
            Some(Arc::new(
                build_openai_intent_resolver(&config.llm)
                    .context("building OpenAI intent resolver")?,
            ))
        } else {
            None
        };
        tracing::info!(
            repo = %config.slack.default_repository,
            installation_id = target.installation_id,
            repo_id = target.repo_id,
            "slack: ad-hoc profiling enabled",
        );
        Some((
            sbgh_slack::SlackSocketConfig {
                app_token: config.slack.app_token.clone(),
                connector: sbgh_slack::SlackConnectorConfig::new(
                    config
                        .slack
                        .default_rev
                        .clone(),
                    config
                        .slack
                        .allowed_team_ids
                        .clone(),
                    config
                        .slack
                        .allowed_user_ids
                        .clone(),
                ),
            },
            target,
            Arc::new(slack_queue::SlackBenchmarkQueue::new(jobs_store.clone()))
                as Arc<dyn sbgh_slack::BenchmarkQueue>,
            web_client,
            intent_resolver,
            config
                .llm
                .per_user_rate_limit_per_minute,
            config
                .runner
                .max_clean_repetitions,
            config.runner.max_variants,
            config
                .runner
                .max_comparison_lifecycles,
            config
                .artifacts
                .binary_cache
                .enabled,
        ))
    } else {
        None
    };

    // The coordinator prepares immutable task payloads and projects durable
    // worker events. Workers are the sole execution owners.
    let runnable_jobs: Arc<dyn RunnableJobStore> = Arc::new(JobSource::new(
        jobs_store.clone(),
        repo_store.clone(),
        pull_request_store,
        artifact_store.clone(),
    ));
    let fleet_slack = slack_runtime
        .as_ref()
        .map(|(_, _, _, web_client, ..)| web_client.clone());
    let fleet_coordinator = fleet::FleetCoordinator::new(
        Arc::new(config.clone()),
        fleet::FleetCoordinatorDependencies {
            fleet: fleet_store,
            jobs: runnable_jobs.clone(),
            repeat_jobs: jobs_store.clone(),
            repos: repo_store,
            gh: gh.clone(),
            artifacts: artifact_store.clone(),
            slack: fleet_slack,
        },
    );

    // The operator `admin` API token is a cookie regenerated each boot; the
    // handler's `ingest` token comes from
    // config.
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

    // A `SIGINT` drains (stop claiming, finish in-flight), while a second
    // `SIGINT` or `SIGTERM` aborts. The
    // coordinator owns drain completion and fires `exit`, which stops the other
    // arms so the `try_join!` returns and the process exits cleanly.
    let shutdown = shutdown::Shutdown::new();

    tokio::try_join!(
        async {
            // Stop processing webhooks once shutdown is underway; any in-flight
            // claim is reclaimed by the processor's own sweep on the next boot.
            tokio::select! {
                r = processor.run() => r.map_err(anyhow::Error::from),
                _ = shutdown.exit.cancelled() => Ok(()),
            }
        },
        api::serve(&api_listen, api_router, shutdown.exit.clone()),
        fleet::run(fleet_runtime, shutdown.exit.clone()),
        fleet_coordinator.run(shutdown.clone()),
        shutdown::watch_signals(shutdown.clone()),
        async {
            // Slack socket-mode receive loop, only when enabled. It
            // stops accepting mentions at drain start; a startup failure (bad
            // app token, TLS) is logged but never crashes the daemon — Slack is
            // an optional surface. This arm then idles until full exit so it
            // never collapses the `try_join!` early.
            if let Some((
                cfg,
                target,
                jobs,
                web_client,
                intent_resolver,
                intent_rate_limit,
                max_clean_repetitions,
                max_variants,
                max_comparison_lifecycles,
                binary_cache_enabled,
            )) = slack_runtime
            {
                if let Err(e) = sbgh_slack::run(
                    cfg,
                    target,
                    jobs,
                    web_client,
                    intent_resolver,
                    sbgh_slack::SocketRunOptions {
                        intent_rate_limit_per_minute: intent_rate_limit,
                        max_clean_repetitions,
                        max_variants,
                        max_comparison_lifecycles,
                        binary_cache_enabled,
                    },
                    shutdown.draining.clone(),
                )
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

pub use artifact_store::{
    ArtifactStore, ArtifactStoreConfig, ArtifactUrlError, LocalFsStore, S3Store, S3StoreConfig,
    artifact_key, build_store,
};
pub use job_source::{
    BaselineRef, JobSource, ProgressTarget, RunnableJob, RunnableJobStore, metric_from_run,
};
pub use sbgh_slack::SlackJobTarget;
pub use slack_target::{ResolveTargetError, resolve_target};
pub use webhook_processor::{
    BasicClassifier, CreateHandler, InstallationHandler, InstallationRepositoriesHandler,
    IssueCommentHandler, ProcessorConfig, PullRequestHandler, PushHandler, WebhookProcessor,
};

#[cfg(test)]
mod composition_tests {
    use super::build_openai_intent_resolver;
    use sbgh_core::config::LlmConfig;

    #[test]
    fn openai_projection_requires_a_credential() {
        let error = build_openai_intent_resolver(&LlmConfig::default())
            .err()
            .expect("missing credential must fail");
        assert_eq!(error.to_string(), "OpenAI API key is not configured");
    }

    #[test]
    fn openai_projection_builds_from_narrow_configuration() {
        let config = LlmConfig {
            openai_api_key: Some("sk-test".into()),
            ..Default::default()
        };
        assert!(build_openai_intent_resolver(&config).is_ok());
    }
}
