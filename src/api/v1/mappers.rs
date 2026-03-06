use dblentry_core::models::{AccountType, DataValue, StatementTxn, TrialBalanceItem};
use dblentry_core::storage::StorageError;

use crate::evaluator::EvaluationError;
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
        dimensions: lot.dimensions.iter()
            .map(|(k, v)| (k.to_string(), format!("{}", v)))
            .collect(),
    }
}

pub fn error_response(error: ApiErrorDto) -> FqlResponseV1 {
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

pub fn map_evaluation_error(e: &EvaluationError) -> ApiErrorDto {
    match e {
        EvaluationError::DivideByZero => ApiErrorDto {
            code: "DIVIDE_BY_ZERO".to_string(),
            message: e.to_string(),
            details: None,
        },
        EvaluationError::InvalidType => ApiErrorDto {
            code: "TYPE_ERROR".to_string(),
            message: e.to_string(),
            details: None,
        },
        EvaluationError::UnknownIdentifier(_) => ApiErrorDto {
            code: "UNKNOWN_IDENTIFIER".to_string(),
            message: e.to_string(),
            details: Some(ApiErrorDetails {
                line: None,
                column: None,
                suggestion: None,
            }),
        },
        EvaluationError::UnknownFunction(name) => {
            let known = vec![
                "balance", "statement", "trial_balance", "income_statement",
                "account_count", "convert", "fx_rate", "round", "abs", "min",
                "max", "units", "market_value", "unrealized_gain", "cost_basis", "lots",
            ];
            let suggestion = find_closest_match(name, &known);
            ApiErrorDto {
                code: "UNKNOWN_FUNCTION".to_string(),
                message: e.to_string(),
                details: Some(ApiErrorDetails {
                    line: None,
                    column: None,
                    suggestion,
                }),
            }
        }
        EvaluationError::InvalidArgument(_) => ApiErrorDto {
            code: "INVALID_ARGUMENT".to_string(),
            message: e.to_string(),
            details: None,
        },
        EvaluationError::InvalidArgumentCount(_) => ApiErrorDto {
            code: "INVALID_ARGUMENT_COUNT".to_string(),
            message: e.to_string(),
            details: None,
        },
        EvaluationError::StorageError(se) => map_storage_error(se),
        EvaluationError::NoRateFound => ApiErrorDto {
            code: "NO_RATE_FOUND".to_string(),
            message: e.to_string(),
            details: None,
        },
        EvaluationError::General(msg) => ApiErrorDto {
            code: "GENERAL_ERROR".to_string(),
            message: msg.clone(),
            details: None,
        },
    }
}

pub fn map_storage_error(e: &StorageError) -> ApiErrorDto {
    match e {
        StorageError::AccountNotFound(_) => ApiErrorDto {
            code: "ACCOUNT_NOT_FOUND".to_string(),
            message: e.to_string(),
            details: None,
        },
        StorageError::RateNotFound(_) => ApiErrorDto {
            code: "RATE_NOT_FOUND".to_string(),
            message: e.to_string(),
            details: None,
        },
        StorageError::EntityNotFound(_) => ApiErrorDto {
            code: "ENTITY_NOT_FOUND".to_string(),
            message: e.to_string(),
            details: None,
        },
        StorageError::EntityAlreadyExists(_) => ApiErrorDto {
            code: "ENTITY_ALREADY_EXISTS".to_string(),
            message: e.to_string(),
            details: None,
        },
        StorageError::NoActiveTransaction => ApiErrorDto {
            code: "TRANSACTION_ERROR".to_string(),
            message: e.to_string(),
            details: None,
        },
        StorageError::NoRateFound => ApiErrorDto {
            code: "NO_RATE_FOUND".to_string(),
            message: e.to_string(),
            details: None,
        },
        _ => ApiErrorDto {
            code: "STORAGE_ERROR".to_string(),
            message: e.to_string(),
            details: None,
        },
    }
}

pub fn map_parse_error(error_str: &str) -> ApiErrorDto {
    let mut line = None;
    let mut column = None;
    if let Some(pos) = error_str.find("at line ") {
        let rest = &error_str[pos + 8..];
        if let Some(comma) = rest.find(',') {
            line = rest[..comma].trim().parse().ok();
            if let Some(col_pos) = rest.find("column ") {
                let col_rest = &rest[col_pos + 7..];
                column = col_rest
                    .trim_end_matches(|c: char| !c.is_ascii_digit())
                    .parse()
                    .ok();
            }
        }
    }
    ApiErrorDto {
        code: "PARSE_ERROR".to_string(),
        message: format!("Parse error: {}", error_str),
        details: if line.is_some() || column.is_some() {
            Some(ApiErrorDetails {
                line,
                column,
                suggestion: None,
            })
        } else {
            None
        },
    }
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();
    let mut matrix = vec![vec![0usize; b_len + 1]; a_len + 1];
    for i in 0..=a_len {
        matrix[i][0] = i;
    }
    for j in 0..=b_len {
        matrix[0][j] = j;
    }
    for i in 1..=a_len {
        for j in 1..=b_len {
            let cost = if a.as_bytes()[i - 1] == b.as_bytes()[j - 1] {
                0
            } else {
                1
            };
            matrix[i][j] = (matrix[i - 1][j] + 1)
                .min(matrix[i][j - 1] + 1)
                .min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[a_len][b_len]
}

fn find_closest_match(input: &str, candidates: &[&str]) -> Option<String> {
    let input_lower = input.to_lowercase();
    candidates
        .iter()
        .map(|c| (c, levenshtein(&input_lower, &c.to_lowercase())))
        .filter(|(_, d)| *d <= 3)
        .min_by_key(|(_, d)| *d)
        .map(|(c, _)| format!("Did you mean '{}'?", c))
}
