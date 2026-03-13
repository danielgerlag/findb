use dblentry_core::{AccountType, DataValue, LotItem, StatementTxn, TrialBalanceItem};
use prettytable::{row, Table};

use crate::statement_executor::ExecutionResult;

pub fn format_data_value(value: &DataValue) -> String {
    match value {
        DataValue::Null => "null".to_string(),
        DataValue::Bool(b) => b.to_string(),
        DataValue::Int(i) => i.to_string(),
        DataValue::Money(m) => m.to_string(),
        DataValue::Percentage(p) => p.to_string(),
        DataValue::String(s) => s.to_string(),
        DataValue::Date(d) => d.to_string(),
        DataValue::List(items) => format!("{:?}", items),
        DataValue::Map(map) => format!("{:?}", map),
        DataValue::AccountId(id) => id.to_string(),
        DataValue::Dimension((name, inner)) => format!("{}={}", name, format_data_value(inner)),
        DataValue::Statement(txns) => format_statement(txns),
        DataValue::TrialBalance(items) => format_trial_balance(items),
        DataValue::Lots(lots) => format_lots(lots),
    }
}

pub fn format_execution_result(result: &ExecutionResult) -> String {
    let mut output = String::new();
    for (key, value) in &result.variables {
        output.push_str(key.as_ref());
        output.push_str(": ");
        output.push_str(&format_data_value(value));
        output.push('\n');
    }
    if result.journals_created > 0 {
        output.push_str("journals_created: ");
        output.push_str(&result.journals_created.to_string());
    }
    output
}

fn format_statement(txns: &[StatementTxn]) -> String {
    let mut table = Table::new();
    table.add_row(row!["Date", "Description", "Amount", "Balance"]);
    table.add_empty_row();

    for item in txns {
        table.add_row(row![item.date, item.description, item.amount, item.balance]);
    }

    format!("\n{}\n", table)
}

fn format_trial_balance(items: &[TrialBalanceItem]) -> String {
    let mut table = Table::new();
    table.add_row(row!["Account", "Debit", "Credit"]);
    table.add_empty_row();

    for item in items {
        match item.account_type {
            AccountType::Asset | AccountType::Expense => {
                table.add_row(row![item.account_id, item.balance, ""]);
            }
            AccountType::Liability | AccountType::Equity | AccountType::Income => {
                table.add_row(row![item.account_id, "", item.balance]);
            }
        }
    }

    format!("\n{}\n", table)
}

fn format_lots(lots: &[LotItem]) -> String {
    let mut table = Table::new();
    table.add_row(row!["Date", "Units", "Cost/Unit", "Total Cost"]);
    table.add_empty_row();

    for lot in lots {
        table.add_row(row![lot.date, lot.units, lot.cost_per_unit, lot.total_cost]);
    }

    format!("\n{}\n", table)
}
