use serde::Serialize;

pub mod v1;

/// Text-format FQL response (backwards-compatible with unversioned `/fql` endpoint).
#[derive(Serialize)]
pub struct TextFqlResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub metadata: TextFqlMetadata,
}

#[derive(Serialize)]
pub struct TextFqlMetadata {
    pub statements_executed: usize,
    pub journals_created: usize,
}
