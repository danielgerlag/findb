use axum::{
    http::{Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    Json, Extension,
};
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::config::AuthConfig;

/// Authenticated caller identity, available to handlers via request extensions.
#[derive(Debug, Clone)]
pub struct CallerIdentity {
    pub name: String,
    pub role: String,
}

#[derive(Serialize)]
struct AuthError {
    success: bool,
    error: String,
}

pub async fn auth_middleware<B>(
    Extension(config): Extension<std::sync::Arc<AuthConfig>>,
    mut req: Request<B>,
    next: Next<B>,
) -> Response {
    if !config.enabled {
        req.extensions_mut().insert(CallerIdentity {
            name: "anonymous".to_string(),
            role: "admin".to_string(),
        });
        return next.run(req).await;
    }

    let api_key = req.headers()
        .get("X-API-Key")
        .or_else(|| req.headers().get(header::AUTHORIZATION))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s));

    match api_key {
        Some(key) => {
            match config.api_keys.iter().find(|entry| {
                    entry.key.as_bytes().ct_eq(key.as_bytes()).into()
                }) {
                Some(entry) => {
                    tracing::debug!(caller = %entry.name, role = %entry.role, "Authenticated request");
                    req.extensions_mut().insert(CallerIdentity {
                        name: entry.name.clone(),
                        role: entry.role.clone(),
                    });
                    next.run(req).await
                }
                None => {
                    tracing::warn!("Invalid API key presented");
                    (StatusCode::UNAUTHORIZED, Json(AuthError {
                        success: false,
                        error: "Invalid API key".to_string(),
                    })).into_response()
                }
            }
        }
        None => {
            (StatusCode::UNAUTHORIZED, Json(AuthError {
                success: false,
                error: "Missing API key. Provide X-API-Key header or Authorization: Bearer <key>".to_string(),
            })).into_response()
        }
    }
}
