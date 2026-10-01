use axum::{extract::FromRequestParts, http::request::Parts, http::HeaderMap};
use sqlx::PgPool;

use super::ApiError;

pub const USER_HEADER: &str = "X-User-Id";
pub const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";

/// The authenticated caller. Demo-grade identity: taken from the `X-User-Id` header.
/// In production this extractor would validate a JWT / session instead; handlers would not change.
#[derive(Debug, Clone)]
pub struct CurrentUser(pub String);

impl<S: Send + Sync> FromRequestParts<S> for CurrentUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let value = parts
            .headers
            .get(USER_HEADER)
            .ok_or_else(|| ApiError::Unauthorized(format!("missing {USER_HEADER} header")))?
            .to_str()
            .map_err(|_| ApiError::Unauthorized(format!("{USER_HEADER} must be ASCII")))?
            .trim();
        if value.is_empty()
            || value.len() > 64
            || !value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '@'))
        {
            return Err(ApiError::Unauthorized(format!(
                "{USER_HEADER} must be 1-64 characters of letters, digits, '-', '_', '.', '@'"
            )));
        }
        Ok(CurrentUser(value.to_string()))
    }
}

/// Users are created on first write.
pub async fn ensure_user(pool: &PgPool, user_id: &str) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING").bind(user_id).execute(pool).await?;
    Ok(())
}

pub fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    match headers.get(IDEMPOTENCY_HEADER) {
        None => Ok(None),
        Some(v) => {
            let key = v.to_str().map_err(|_| ApiError::BadRequest("Idempotency-Key must be ASCII".into()))?.trim();
            if key.is_empty() || key.len() > 128 {
                return Err(ApiError::BadRequest("Idempotency-Key must be 1-128 characters".into()));
            }
            Ok(Some(key.to_string()))
        }
    }
}
