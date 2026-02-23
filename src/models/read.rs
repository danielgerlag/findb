use std::{collections::BTreeMap, sync::Arc};

use rust_decimal::Decimal;
use time::Date;

use super::DataValue;

pub struct JournalEntry {
    pub date: Date,
    pub description: Arc<str>,
    pub amount: Decimal,
    pub dimensions: BTreeMap<Arc<str>, Arc<DataValue>>,
}
