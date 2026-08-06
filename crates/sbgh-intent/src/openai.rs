//! OpenAI Responses API adapter for task-creation intent extraction.

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::intent::{
    IntentOutcome, IntentProviderError, IntentResolutionJson, IntentResolver,
    intent_response_text_format, validate_intent_resolution,
};

/// The task-creation system prompt. Authored as Markdown so the resolution
/// rules stay reviewable next to the schema they constrain.
const INTENT_SYSTEM_PROMPT: &str = include_str!("../prompts/submission.md");

pub struct OpenAiIntentResolver {
    client: reqwest::Client,
    api_key: String,
    model: String,
    input_max_chars: usize,
    endpoint: String,
}

/// Provider configuration projected by the application composition root.
///
/// Deliberately does not implement `Debug`: it owns a live API credential.
pub struct OpenAiIntentConfig {
    api_key: String,
    model: String,
    input_max_chars: usize,
    timeout: std::time::Duration,
}

impl OpenAiIntentConfig {
    pub fn new(
        api_key: impl Into<String>,
        model: impl Into<String>,
        input_max_chars: usize,
        timeout: std::time::Duration,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            model: model.into(),
            input_max_chars,
            timeout,
        }
    }
}

impl OpenAiIntentResolver {
    pub fn new(cfg: OpenAiIntentConfig) -> Result<Self, IntentProviderError> {
        Self::with_endpoint(cfg, "https://api.openai.com/v1/responses")
    }

    fn with_endpoint(
        cfg: OpenAiIntentConfig,
        endpoint: impl Into<String>,
    ) -> Result<Self, IntentProviderError> {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(cfg.timeout)
            .build()
            .map_err(|e| {
                IntentProviderError::Message(format!("OpenAI client setup failed: {e}"))
            })?;
        Ok(Self {
            client,
            api_key: cfg.api_key,
            model: cfg.model,
            input_max_chars: cfg.input_max_chars,
            endpoint: endpoint.into(),
        })
    }

    #[cfg(test)]
    fn for_test(endpoint: impl Into<String>) -> Self {
        Self::with_endpoint(
            OpenAiIntentConfig::new(
                "sk-test",
                "gpt-test",
                1_000,
                std::time::Duration::from_secs(15),
            ),
            endpoint,
        )
        .expect("test client configuration is valid")
    }

    fn request_body(&self, text: &str) -> Value {
        openai_request_body(&self.model, text)
    }

    async fn send(&self, text: &str) -> Result<Value, IntentProviderError> {
        // Safe to log: the API key rides the bearer header, not the body. Prompt
        // is debug-only.
        let request_body = self.request_body(text);
        tracing::debug!(request_body = %request_body, "llm: openai request body");
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| IntentProviderError::Message(format!("OpenAI request failed: {e}")))?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|e| {
                IntentProviderError::Message(format!("OpenAI response was not JSON: {e}"))
            })?;
        tracing::debug!(status = %status, response_body = %body, "llm: openai response body");
        if !status.is_success() {
            let msg = body
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("OpenAI request failed");
            return Err(IntentProviderError::Message(format!("OpenAI returned {status}: {msg}")));
        }
        Ok(body)
    }

    /// The network + parse + validate path, split out so
    /// [`IntentResolver::resolve`] can time it and log the outcome.
    async fn resolve_request(&self, text: &str) -> Result<IntentOutcome, IntentProviderError> {
        let body = self.send(text).await?;
        let output = extract_openai_output_text(&body)?;
        let intent: IntentResolutionJson = serde_json::from_str(output).map_err(|e| {
            IntentProviderError::Message(format!("OpenAI returned malformed intent JSON: {e}"))
        })?;
        match validate_intent_resolution(intent) {
            Ok(outcome) => Ok(outcome),
            Err(e) => Ok(IntentOutcome::Invalid(e.to_string().into())),
        }
    }
}

#[async_trait]
impl IntentResolver for OpenAiIntentResolver {
    async fn resolve(&self, text: &str) -> Result<IntentOutcome, IntentProviderError> {
        let input_chars = text.chars().count();
        if input_chars > self.input_max_chars {
            tracing::info!(
                input_chars,
                max = self.input_max_chars,
                "llm: rejecting over-long request before any openai call"
            );
            return Ok(IntentOutcome::Invalid(
                format!("request is too long; keep it under {} characters", self.input_max_chars)
                    .into(),
            ));
        }
        tracing::info!(model = %self.model, input_chars, "llm: resolving task intent via openai");

        let started = std::time::Instant::now();
        let outcome = self
            .resolve_request(text)
            .await;
        let latency_ms = started.elapsed().as_millis();

        match &outcome {
            Ok(IntentOutcome::Resolved(_)) => {
                tracing::info!(latency_ms, outcome = "resolved", "llm: intent resolution complete")
            }
            Ok(IntentOutcome::Invalid(_)) => {
                tracing::info!(latency_ms, outcome = "invalid", "llm: intent resolution complete")
            }
            Err(e) => {
                tracing::warn!(latency_ms, outcome = "error", error = %e, "llm: intent resolution failed")
            }
        }
        outcome
    }
}

pub fn openai_request_body(model: &str, text: &str) -> Value {
    json!({
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": INTENT_SYSTEM_PROMPT
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": text
                    }
                ]
            }
        ],
        "text": {
            "format": intent_response_text_format()
        },
        "tools": []
    })
}

pub fn extract_openai_output_text(body: &Value) -> Result<&str, IntentProviderError> {
    if let Some(msg) = body
        .pointer("/error/message")
        .and_then(Value::as_str)
    {
        return Err(IntentProviderError::Message(format!("OpenAI error: {msg}")));
    }
    if body
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|s| matches!(s, "incomplete" | "failed" | "cancelled"))
    {
        return Err(IntentProviderError::Message(
            "OpenAI did not complete the intent resolution".into(),
        ));
    }
    let Some(output) = body
        .get("output")
        .and_then(Value::as_array)
    else {
        return Err(IntentProviderError::Message("OpenAI response had no output".into()));
    };
    for item in output {
        let Some(content) = item
            .get("content")
            .and_then(Value::as_array)
        else {
            continue;
        };
        for part in content {
            let part_type = part
                .get("type")
                .and_then(Value::as_str);
            if matches!(part_type, Some("output_text" | "text"))
                && let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
            {
                return Ok(text);
            }
        }
    }
    Err(IntentProviderError::Message("OpenAI response did not contain intent text".into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use sbgh_core::workload::{BenchmarkRequest, BlockSelector, WorkloadSpec, WorkloadTarget};

    use crate::intent::{
        EVAL_FIXTURES, EvalExpected, IntentEvalFixture, IntentResolutionJson, TaskCreationIntent,
        UserIntent, run_eval_fixtures,
    };

    #[test]
    fn openai_request_body_pins_structured_output_shape() {
        let body = openai_request_body("gpt-test", "bench block 1");
        assert_eq!(body["model"], "gpt-test");
        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["input"][0]["content"][0]["text"], INTENT_SYSTEM_PROMPT);
        assert_eq!(body["input"][1]["role"], "user");
        assert_eq!(body["input"][1]["content"][0]["text"], "bench block 1");
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        assert_eq!(body["text"]["format"]["strict"], true);
        assert_eq!(
            body["tools"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    /// The prompt and the strict schema constrain the same response, so a field
    /// added to one must be described by the other.
    #[test]
    fn prompt_documents_every_schema_field() {
        let format = intent_response_text_format();
        let properties = format["schema"]["properties"]
            .as_object()
            .expect("schema has properties");
        for field in properties.keys() {
            assert!(
                INTENT_SYSTEM_PROMPT.contains(field.as_str()),
                "system prompt never mentions the `{field}` schema field"
            );
        }
    }

    /// The request body is debug-logged, so it must never carry the API key
    /// (that travels only in the bearer header).
    #[test]
    fn request_body_never_carries_the_api_key() {
        let resolver = OpenAiIntentResolver::for_test("http://127.0.0.1:9");
        let body = resolver
            .request_body("bench block 1")
            .to_string();
        assert!(
            !body.contains("sk-test"),
            "the API key must never appear in the request body we debug-log: {body}"
        );
    }

    #[test]
    fn openai_response_text_is_extracted_from_response_output() {
        let body = json!({
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "{\"status\":\"invalid\",\"target_kind\":null,\"block\":null,\"block_range\":null,\"txids\":null,\"repetitions\":null,\"warmup\":null,\"rev\":null,\"reason\":\"missing target\",\"issues\":null}"
                        }
                    ]
                }
            ]
        });
        let text = extract_openai_output_text(&body).unwrap();
        let intent: IntentResolutionJson = serde_json::from_str(text).unwrap();
        assert_eq!(intent.reason.as_deref(), Some("missing target"));
    }

    #[test]
    fn openai_response_errors_are_safe_failures() {
        let errored = json!({"error": {"message": "bad request"}});
        assert!(extract_openai_output_text(&errored).is_err());
        let incomplete = json!({"status": "incomplete", "output": []});
        assert!(extract_openai_output_text(&incomplete).is_err());
        let malformed = json!({"status": "completed", "output": []});
        assert!(extract_openai_output_text(&malformed).is_err());
    }

    #[test]
    fn openai_resolver_rejects_overlong_input_before_request() {
        let resolver = OpenAiIntentResolver::for_test("http://127.0.0.1:9");
        let outcome = futures::executor::block_on(resolver.resolve(&"x".repeat(1_001))).unwrap();
        assert!(matches!(outcome, IntentOutcome::Invalid(_)));
    }

    #[test]
    fn live_eval_cases_cover_fixture_set() {
        assert!(EVAL_FIXTURES.len() >= 15);
        let fixture_names = EVAL_FIXTURES
            .iter()
            .map(|fixture| fixture.name)
            .collect::<BTreeSet<_>>();
        let test_names = OPENAI_LIVE_EVAL_NAMES
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(fixture_names.len(), EVAL_FIXTURES.len(), "fixture names must be unique");
        assert_eq!(
            test_names.len(),
            OPENAI_LIVE_EVAL_NAMES.len(),
            "live test names must be unique"
        );
        assert_eq!(test_names, fixture_names);
        assert!(
            EVAL_FIXTURES
                .iter()
                .any(|f| f.expected == EvalExpected::Invalid)
        );
        assert!(
            EVAL_FIXTURES
                .iter()
                .any(|f| f.expected == EvalExpected::Resolved)
        );
    }

    #[tokio::test]
    async fn eval_runner_counts_mismatches() {
        struct AlwaysInvalid;

        #[async_trait]
        impl IntentResolver for AlwaysInvalid {
            async fn resolve(&self, _text: &str) -> Result<IntentOutcome, IntentProviderError> {
                Ok(IntentOutcome::Invalid("nope".into()))
            }
        }

        let fixtures = [
            IntentEvalFixture::resolved("unit_resolved", "bench block 1"),
            IntentEvalFixture::invalid("unit_invalid", "bench something"),
        ];
        let report = run_eval_fixtures(&AlwaysInvalid, &fixtures)
            .await
            .unwrap();
        assert_eq!(report.total, 2);
        assert_eq!(report.passed, 1);
        assert_eq!(report.failures.len(), 1);
    }

    /// Status parity alone would pass a resolution that carries the wrong task
    /// fields — the exact shape of the original Slack misresolution.
    #[tokio::test]
    async fn eval_runner_counts_wrong_task_fields_as_failure() {
        struct AlwaysBenchmark;

        #[async_trait]
        impl IntentResolver for AlwaysBenchmark {
            async fn resolve(&self, _text: &str) -> Result<IntentOutcome, IntentProviderError> {
                Ok(IntentOutcome::Resolved(UserIntent::Create(TaskCreationIntent::Benchmark(
                    BenchmarkRequest::Single(WorkloadSpec {
                        target: WorkloadTarget::Blocks(vec![BlockSelector::Height(10)]),
                        clean_repetitions: 1,
                        warmup: Some(0),
                        rev: None,
                    }),
                ))))
            }
        }

        fn expect_validation(intent: &UserIntent) -> Result<(), String> {
            match intent {
                UserIntent::Create(TaskCreationIntent::BlockValidation(_)) => Ok(()),
                _ => Err("expected a block-validation intent".into()),
            }
        }

        let fixtures = [IntentEvalFixture::resolved_as(
            "unit_wrong_task_fields",
            "validate the latest 10 blocks",
            expect_validation,
        )];
        let report = run_eval_fixtures(&AlwaysBenchmark, &fixtures)
            .await
            .unwrap();
        assert_eq!(report.passed, 0);
        assert!(
            report.failures[0].contains("expected a block-validation intent"),
            "unexpected failure text: {}",
            report.failures[0]
        );
    }

    #[tokio::test]
    async fn configured_timeout_is_a_provider_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener
                .accept()
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        });
        let resolver = OpenAiIntentResolver::with_endpoint(
            OpenAiIntentConfig::new(
                "sk-test",
                "gpt-test",
                1_000,
                std::time::Duration::from_millis(25),
            ),
            format!("http://{address}"),
        )
        .unwrap();

        let error = resolver
            .resolve("bench block 1")
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("OpenAI request failed")
        );
        server.abort();
    }

    async fn run_openai_live_eval(fixture_name: &str) {
        // Keep semantic prompt evaluation independent from the production
        // request-latency policy and occasional provider latency spikes.
        const LIVE_EVAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

        let api_key = std::env::var("SBGH_OPENAI_API_KEY")
            .expect("set SBGH_OPENAI_API_KEY to run the real-model eval");
        let model = std::env::var("SBGH_LLM_MODEL").unwrap_or_else(|_| "gpt-5-mini".to_string());
        let resolver = OpenAiIntentResolver::new(OpenAiIntentConfig::new(
            api_key,
            model,
            1_000,
            LIVE_EVAL_TIMEOUT,
        ))
        .unwrap();
        let fixture = EVAL_FIXTURES
            .iter()
            .find(|fixture| fixture.name == fixture_name)
            .copied()
            .expect("live eval fixture name is covered by the corpus guard");
        let report = run_eval_fixtures(&resolver, std::slice::from_ref(&fixture))
            .await
            .unwrap();
        assert!(
            report.failures.is_empty(),
            "eval failures ({}/{} passed):\n{}",
            report.passed,
            report.total,
            report.failures.join("\n")
        );
    }

    macro_rules! openai_live_eval_cases {
        ($($name:ident),+ $(,)?) => {
            const OPENAI_LIVE_EVAL_NAMES: &[&str] = &[$(stringify!($name)),+];

            $(
                #[tokio::test]
                #[ignore = "requires SBGH_OPENAI_API_KEY and calls the real OpenAI Responses API"]
                async fn $name() {
                    run_openai_live_eval(stringify!($name)).await;
                }
            )+
        };
    }

    openai_live_eval_cases!(
        openai_live_eval_benchmark_single_block_defaults,
        openai_live_eval_benchmark_repetition_words,
        openai_live_eval_benchmark_block_range_single_ref,
        openai_live_eval_benchmark_compact_counts_and_warmup,
        openai_live_eval_benchmark_run_range,
        openai_live_eval_benchmark_txid,
        openai_live_eval_invalid_contextual_tx,
        openai_live_eval_invalid_bare_hash,
        openai_live_eval_invalid_contextual_branch,
        openai_live_eval_invalid_ambiguous_target,
        openai_live_eval_invalid_comparison_without_target,
        openai_live_eval_invalid_malformed_txid,
        openai_live_eval_invalid_reversed_range,
        openai_live_eval_invalid_zero_repetitions,
        openai_live_eval_benchmark_txid_single_ref,
        openai_live_eval_invalid_missing_benchmark_target,
        openai_live_eval_block_validation_default_selection,
        openai_live_eval_block_validation_recent_compact_count,
        openai_live_eval_block_validation_recent_exact_commit,
        openai_live_eval_block_validation_full,
        openai_live_eval_block_validation_range,
        openai_live_eval_invalid_contextual_validation,
        openai_live_eval_invalid_mixed_task_request,
    );
}
