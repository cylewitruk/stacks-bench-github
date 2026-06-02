//! `ApiJson<T>` — a `Json<T>` request extractor whose extraction failures
//! (malformed body, missing required fields, unknown fields, type
//! mismatch) become an [`ApiErr`] (the `ApiError` envelope) instead of
//! axum's plain-text default `422`/`400`.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Request};

use crate::api::error::ApiErr;

pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    Json<T>: FromRequest<S, Rejection = JsonRejection>,
    S: Send + Sync,
{
    type Rejection = ApiErr;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(ApiJson(value)),
            Err(rejection) => Err(ApiErr::bad_request(rejection.body_text())),
        }
    }
}
