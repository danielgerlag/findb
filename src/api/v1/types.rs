use serde::Serialize;

#[derive(Serialize)]
pub struct FqlResponseV1 {
    pub success: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<ResultEntryDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub metadata: FqlMetadataDto,
}

#[derive(Serialize)]
pub struct ResultEntryDto {
    pub name: String,
    pub value: DataValueDto,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "value")]
pub enum DataValueDto {
    #[serde(rename = "null")]
    Null,
    #[serde(rename = "bool")]
    Bool(bool),
    #[serde(rename = "int")]
    Int(i64),
    #[serde(rename = "money")]
    Money(String),
    #[serde(rename = "percentage")]
    Percentage(String),
    #[serde(rename = "string")]
    String(String),
    #[serde(rename = "date")]
    Date(String),
    #[serde(rename = "list")]
    List(Vec<DataValueDto>),
    #[serde(rename = "map")]
    Map(Vec<MapEntryDto>),
    #[serde(rename = "account_id")]
    AccountId(String),
    #[serde(rename = "dimension")]
    Dimension(DimensionDto),
    #[serde(rename = "statement")]
    Statement(Vec<StatementTxnDto>),
    #[serde(rename = "trial_balance")]
    TrialBalance(Vec<TrialBalanceItemDto>),
    #[serde(rename = "lots")]
    Lots(Vec<LotItemDto>),
}

#[derive(Serialize)]
pub struct TrialBalanceItemDto {
    pub account_id: String,
    pub account_type: String,
    pub balance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credit: Option<String>,
}

#[derive(Serialize)]
pub struct StatementTxnDto {
    pub journal_id: String,
    pub date: String,
    pub description: String,
    pub amount: String,
    pub balance: String,
}

#[derive(Serialize)]
pub struct DimensionDto {
    pub key: String,
    pub value: Box<DataValueDto>,
}

#[derive(Serialize)]
pub struct MapEntryDto {
    pub key: String,
    pub value: DataValueDto,
}

#[derive(Serialize)]
pub struct LotItemDto {
    pub date: String,
    pub units: String,
    pub cost_per_unit: String,
    pub total_cost: String,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub dimensions: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
pub struct FqlMetadataDto {
    pub statements_executed: usize,
    pub journals_created: usize,
}
