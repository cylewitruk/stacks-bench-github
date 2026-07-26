//! Provider-backed resolution of user requests into validated benchmark intent.

mod intent;
mod openai;

pub use intent::{
    IntentInvalid, IntentOutcome, IntentProviderError, IntentResolver, IntentValidationError,
};
pub use openai::{OpenAiIntentConfig, OpenAiIntentResolver};
