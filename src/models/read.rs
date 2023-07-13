use std::{collections::BTreeMap, sync::Arc};

use time::Date;

use super::DataValue;

pub struct JournalEntry {
    pub date: Date,
    pub description: Arc<str>,
    pub amount: f64,
    pub dimensions: BTreeMap<Arc<str>, DataValue>,
}
