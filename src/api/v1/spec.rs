use axum::{
    extract::Query,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

const FQL_REFERENCE_MD: &str = include_str!("../../../docs/ai/fql-reference.md");

#[derive(Deserialize)]
pub struct SpecQuery {
    #[serde(default)]
    pub format: Option<String>,
}

#[derive(Serialize)]
pub struct FqlSpecResponse {
    pub version: &'static str,
    pub language: &'static str,
    pub description: &'static str,
    pub reference: &'static str,
}

pub async fn fql_spec_handler(
    headers: HeaderMap,
    Query(params): Query<SpecQuery>,
) -> impl IntoResponse {
    let wants_markdown = params.format.as_deref() == Some("markdown")
        || headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/markdown") || v.contains("text/plain"))
            .unwrap_or(false);

    if wants_markdown {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/markdown; charset=utf-8")],
            FQL_REFERENCE_MD.to_string(),
        ).into_response()
    } else {
        let resp = FqlSpecResponse {
            version: "1.0",
            language: "FQL",
            description: "Financial Query Language for DblEntry — a Layer 2 database for double-entry bookkeeping",
            reference: FQL_REFERENCE_MD,
        };
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(&resp).unwrap(),
        ).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fql_reference_included() {
        assert!(!FQL_REFERENCE_MD.is_empty());
        assert!(FQL_REFERENCE_MD.contains("FQL"));
        assert!(FQL_REFERENCE_MD.contains("CREATE ACCOUNT"));
        assert!(FQL_REFERENCE_MD.contains("balance"));
    }

    #[test]
    fn test_fql_spec_response_serializes() {
        let resp = FqlSpecResponse {
            version: "1.0",
            language: "FQL",
            description: "test",
            reference: "test ref",
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"version\":\"1.0\""));
        assert!(json.contains("\"language\":\"FQL\""));
    }
}
