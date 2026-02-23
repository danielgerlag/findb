use std::{collections::BTreeMap, sync::Arc};

use rust_decimal::Decimal;
use time::{Date, OffsetDateTime};

use super::DataValue;

pub struct JournalEntry {
    pub id: u128,
    pub sequence: u64,
    pub date: Date,
    pub description: Arc<str>,
    pub amount: Decimal,
    pub dimensions: BTreeMap<Arc<str>, Arc<DataValue>>,
    pub created_at: OffsetDateTime,
}
