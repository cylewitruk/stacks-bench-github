pub mod health;
pub mod webhook;

use axum::Router;
use axum::routing::get;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health::health))
        .route("/webhook", axum::routing::post(webhook::handle))
}
