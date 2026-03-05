use dblentry_core::models::{AccountType, DataValue, StatementTxn, TrialBalanceItem};

use crate::statement_executor::ExecutionResult;

use super::types::*;

pub fn map_execution_results(results: &[ExecutionResult]) -> FqlResponseV1 {
    let mut entries = Vec::new();
    let mut total_journals = 0usize;

    for result in results {
        total_journals += result.journals_created;
        for (name, value) in &result.variables {
            entries.push(ResultEntryDto {
                name: name.to_string(),
                value: map_data_value(value),
            });
        }
    }

    FqlResponseV1 {
        success: true,
        results: entries,
        error: None,
        metadata: FqlMetadataDto {
            statements_executed: results.len(),
            journals_created: total_journals,
        },
    }
}

pub fn map_data_value(value: &DataValue) -> DataValueDto {
    match value {
        DataValue::Null => DataValueDto::Null,
        DataValue::Bool(b) => DataValueDto::Bool(*b),
        DataValue::Int(i) => DataValueDto::Int(*i),
        DataValue::Money(d) => DataValueDto::Money(d.to_string()),
        DataValue::Percentage(d) => DataValueDto::Percentage(d.to_string()),
        DataValue::String(s) => DataValueDto::String(s.to_string()),
        DataValue::Date(d) => DataValueDto::Date(d.to_string()),
        DataValue::AccountId(id) => DataValueDto::AccountId(id.to_string()),
        DataValue::Dimension((key, val)) => DataValueDto::Dimension(DimensionDto {
            key: key.to_string(),
            value: Box::new(map_data_value(val)),
        }),
        DataValue::List(items) => {
            DataValueDto::List(items.iter().map(map_data_value).collect())
        }
        DataValue::Map(map) => {
            DataValueDto::Map(
                map.iter()
                    .map(|(k, v)| MapEntryDto {
                        key: k.to_string(),
                        value: map_data_value(v),
                    })
                    .collect(),
            )
        }
        DataValue::Statement(txns) => {
            DataValueDto::Statement(txns.iter().map(map_statement_txn).collect())
        }
        DataValue::TrialBalance(items) => {
            DataValueDto::TrialBalance(items.iter().map(map_trial_balance_item).collect())
        }
        DataValue::Lots(lots) => {
            DataValueDto::Lots(lots.iter().map(map_lot_item).collect())
        }
    }
}

fn map_trial_balance_item(item: &TrialBalanceItem) -> TrialBalanceItemDto {
    let (debit, credit) = match item.account_type {
        AccountType::Asset | AccountType::Expense => {
            (Some(item.balance.to_string()), None)
        }
        AccountType::Liability | AccountType::Equity | AccountType::Income => {
            (None, Some(item.balance.to_string()))
        }
    };

    TrialBalanceItemDto {
        account_id: item.account_id.to_string(),
        account_type: serde_json::to_value(&item.account_type)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        balance: item.balance.to_string(),
        debit,
        credit,
    }
}

fn map_statement_txn(txn: &StatementTxn) -> StatementTxnDto {
    StatementTxnDto {
        journal_id: txn.journal_id.to_string(),
        date: txn.date.to_string(),
        description: txn.description.to_string(),
        amount: txn.amount.to_string(),
        balance: txn.balance.to_string(),
    }
}

fn map_lot_item(lot: &dblentry_core::models::LotItem) -> LotItemDto {
    LotItemDto {
        date: lot.date.to_string(),
        units: lot.units.to_string(),
        cost_per_unit: lot.cost_per_unit.to_string(),
        total_cost: lot.total_cost.to_string(),
    }
}

pub fn error_response(error: String) -> FqlResponseV1 {
    FqlResponseV1 {
        success: false,
        results: vec![],
        error: Some(error),
        metadata: FqlMetadataDto {
            statements_executed: 0,
            journals_created: 0,
        },
    }
}
