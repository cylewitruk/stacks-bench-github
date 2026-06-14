//! OpenAI Responses API adapter for benchmark intent extraction.

use async_trait::async_trait;
use sbgh_core::config::LlmConfig;
use serde_json::{Value, json};

use crate::llm::intent::{
    IntentOutcome, IntentProviderError, IntentResolutionJson, IntentResolver,
    intent_response_text_format, validate_intent_resolution,
};

pub struct OpenAiIntentResolver {
    client: reqwest::Client,
    api_key: String,
    model: String,
    input_max_chars: usize,
    endpoint: String,
}

impl OpenAiIntentResolver {
    pub fn from_config(cfg: &LlmConfig) -> Result<Self, IntentProviderError> {
        let api_key = cfg
            .openai_api_key
            .clone()
            .ok_or_else(|| {
                IntentProviderError::Message("OpenAI API key is not configured".into())
            })?;
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(cfg.timeout_secs))
            .build()
            .map_err(|e| {
                IntentProviderError::Message(format!("OpenAI client setup failed: {e}"))
            })?;
        Ok(Self {
            client,
            api_key,
            model: cfg.model.clone(),
            input_max_chars: cfg.input_max_chars,
            endpoint: "https://api.openai.com/v1/responses".into(),
        })
    }

    #[cfg(test)]
    fn for_test(endpoint: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: "sk-test".into(),
            model: "gpt-test".into(),
            input_max_chars: 1_000,
            endpoint: endpoint.into(),
        }
    }

    fn request_body(&self, text: &str) -> Value {
        openai_request_body(&self.model, text)
    }

    async fn send(&self, text: &str) -> Result<Value, IntentProviderError> {
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&self.request_body(text))
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
        if !status.is_success() {
            let msg = body
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("OpenAI request failed");
            return Err(IntentProviderError::Message(format!("OpenAI returned {status}: {msg}")));
        }
        Ok(body)
    }
}

#[async_trait]
impl IntentResolver for OpenAiIntentResolver {
    async fn resolve(&self, text: &str) -> Result<IntentOutcome, IntentProviderError> {
        if text.chars().count() > self.input_max_chars {
            return Ok(IntentOutcome::Invalid(
                format!("request is too long; keep it under {} characters", self.input_max_chars)
                    .into(),
            ));
        }
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

pub fn openai_request_body(model: &str, text: &str) -> Value {
    json!({
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Resolve benchmark requests into the provided JSON schema. Return status=invalid when required benchmark inputs are missing or ambiguous, with a concise reason and field-level issues. Never emit CLI flags or extra text."
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
    use sbgh_core::config::LlmConfig;

    use super::*;
    use crate::llm::intent::{
        EVAL_FIXTURES, EvalExpected, IntentEvalFixture, IntentResolutionJson, run_eval_fixtures,
    };

    #[test]
    fn openai_request_body_pins_structured_output_shape() {
        let body = openai_request_body("gpt-test", "bench block 1");
        assert_eq!(body["model"], "gpt-test");
        assert_eq!(body["input"][0]["role"], "system");
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
    fn eval_fixture_has_minimum_review_set() {
        assert!(EVAL_FIXTURES.len() >= 15);
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
            IntentEvalFixture {
                prompt: "bench block 1",
                expected: EvalExpected::Resolved,
            },
            IntentEvalFixture {
                prompt: "bench something",
                expected: EvalExpected::Invalid,
            },
        ];
        let report = run_eval_fixtures(&AlwaysInvalid, &fixtures)
            .await
            .unwrap();
        assert_eq!(report.total, 2);
        assert_eq!(report.passed, 1);
        assert_eq!(report.failures.len(), 1);
    }

    #[tokio::test]
    #[ignore = "requires SBGH_OPENAI_API_KEY and calls the real OpenAI Responses API"]
    async fn openai_eval_fixture_set() {
        let api_key = std::env::var("SBGH_OPENAI_API_KEY")
            .expect("set SBGH_OPENAI_API_KEY to run the real-model eval");
        let mut cfg = LlmConfig {
            enabled: true,
            openai_api_key: Some(api_key),
            ..Default::default()
        };
        if let Ok(model) = std::env::var("SBGH_LLM_MODEL") {
            cfg.model = model;
        }
        let resolver = OpenAiIntentResolver::from_config(&cfg).unwrap();
        let report = run_eval_fixtures(&resolver, EVAL_FIXTURES)
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
}
