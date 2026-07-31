//! Provider-backed resolution of user requests into validated task-creation intent.

mod intent;
mod openai;

pub use intent::{
    BlockValidationIntent, IntentInvalid, IntentOutcome, IntentProviderError, IntentResolver,
    IntentValidationError, RequestedSource, TaskCreationIntent, UserIntent, ValidationSelection,
    ValidationSelectionKind,
};
pub use openai::{OpenAiIntentConfig, OpenAiIntentResolver};
