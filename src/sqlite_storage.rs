use std::{
    collections::HashSet,
    ops::Bound,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use rust_decimal::Decimal;
use rusqlite::{params, Connection};
use time::{Date, Month, OffsetDateTime};
use uuid::Uuid;

use crate::{
    ast::{AccountExpression, AccountType},
    models::{
        write::{CreateJournalCommand, CreateRateCommand, LedgerEntryCommand, SetRateCommand},
        DataValue, StatementTxn,
    },
    storage::{StorageBackend, StorageError, TransactionId},
};

pub struct SqliteStorage {
    conn: Mutex<Connection>,
    tx_counter: AtomicU64,
    active_tx: Mutex<Option<TransactionId>>,
}

impl SqliteStorage {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()
        } else {
            Connection::open(path)
        }
        .map_err(|e| StorageError::Other(e.to_string()))?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let storage = Self {
            conn: Mutex::new(conn),
            tx_counter: AtomicU64::new(1),
            active_tx: Mutex::new(None),
        };
        storage.init_schema()?;
        Ok(storage)
    }

    fn init_schema(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                account_type TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rates (
                id TEXT NOT NULL,
                date TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (id, date)
            );

            CREATE TABLE IF NOT EXISTS journals (
                id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL,
                date TEXT NOT NULL,
                description TEXT NOT NULL,
                amount TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS journal_dimensions (
                journal_id TEXT NOT NULL,
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL,
                FOREIGN KEY (journal_id) REFERENCES journals(id)
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                journal_id TEXT NOT NULL,
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                amount TEXT NOT NULL,
                FOREIGN KEY (journal_id) REFERENCES journals(id),
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );

            CREATE TABLE IF NOT EXISTS ledger_entry_dimensions (
                ledger_entry_id INTEGER NOT NULL,
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL,
                FOREIGN KEY (ledger_entry_id) REFERENCES ledger_entries(id)
            );

            CREATE INDEX IF NOT EXISTS idx_ledger_account_date
                ON ledger_entries(account_id, date);

            CREATE INDEX IF NOT EXISTS idx_ledger_dim
                ON ledger_entry_dimensions(ledger_entry_id);

            CREATE INDEX IF NOT EXISTS idx_rates_lookup
                ON rates(id, date);

            CREATE TABLE IF NOT EXISTS sequence_counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                value INTEGER NOT NULL
            );

            INSERT OR IGNORE INTO sequence_counter (id, value) VALUES (1, 0);
            ",
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn next_sequence(conn: &Connection) -> Result<u64, StorageError> {
        conn.execute(
            "UPDATE sequence_counter SET value = value + 1 WHERE id = 1",
            [],
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;
        let seq: u64 = conn
            .query_row("SELECT value FROM sequence_counter WHERE id = 1", [], |r| {
                r.get(0)
            })
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(seq)
    }
}

fn date_to_str(d: Date) -> String {
    format!("{:04}-{:02}-{:02}", d.year(), d.month() as u8, d.day())
}

fn str_to_date(s: &str) -> Date {
    let parts: Vec<&str> = s.split('-').collect();
    let year = parts[0].parse::<i32>().unwrap();
    let month = parts[1].parse::<u8>().unwrap();
    let day = parts[2].parse::<u8>().unwrap();
    Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap()
}

fn account_type_to_str(at: &AccountType) -> &'static str {
    match at {
        AccountType::Asset => "ASSET",
        AccountType::Liability => "LIABILITY",
        AccountType::Equity => "EQUITY",
        AccountType::Income => "INCOME",
        AccountType::Expense => "EXPENSE",
    }
}

fn str_to_account_type(s: &str) -> AccountType {
    match s {
        "ASSET" => AccountType::Asset,
        "LIABILITY" => AccountType::Liability,
        "EQUITY" => AccountType::Equity,
        "INCOME" => AccountType::Income,
        "EXPENSE" => AccountType::Expense,
        _ => AccountType::Asset,
    }
}

fn data_value_to_str(dv: &DataValue) -> String {
    match dv {
        DataValue::String(s) => s.to_string(),
        DataValue::Int(i) => i.to_string(),
        DataValue::Money(m) => m.to_string(),
        DataValue::Bool(b) => b.to_string(),
        DataValue::Date(d) => date_to_str(*d),
        _ => format!("{}", dv),
    }
}

impl StorageBackend for SqliteStorage {
    fn create_account(&self, account: &AccountExpression) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO accounts (id, account_type) VALUES (?1, ?2)",
            params![account.id.as_ref(), account_type_to_str(&account.account_type)],
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn create_rate(&self, _rate: &CreateRateCommand) -> Result<(), StorageError> {
        // Rates table uses (id, date) as PK; creating a rate just means it's available
        // No row needed until set_rate is called — but we validate existence on get_rate
        // Insert a marker if needed (not strictly necessary with our schema)
        Ok(())
    }

    fn set_rate(&self, command: &SetRateCommand) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO rates (id, date, value) VALUES (?1, ?2, ?3)",
            params![
                command.id.as_ref(),
                date_to_str(command.date),
                command.rate.to_string()
            ],
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn get_rate(&self, id: &str, date: Date) -> Result<Decimal, StorageError> {
        let conn = self.conn.lock().unwrap();
        let result: Result<String, _> = conn.query_row(
            "SELECT value FROM rates WHERE id = ?1 AND date <= ?2 ORDER BY date DESC LIMIT 1",
            params![id, date_to_str(date)],
            |row| row.get(0),
        );
        match result {
            Ok(val) => Decimal::from_str(&val)
                .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(StorageError::NoRateFound),
            Err(e) => Err(StorageError::Other(e.to_string())),
        }
    }

    fn create_journal(&self, command: &CreateJournalCommand) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let jid = Uuid::new_v4().to_string();
        let seq = Self::next_sequence(&conn)?;
        let date_str = date_to_str(command.date);
        let now = OffsetDateTime::now_utc().to_string();

        conn.execute(
            "INSERT INTO journals (id, sequence, date, description, amount, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![jid, seq, date_str, command.description.as_ref(), command.amount.to_string(), now],
        ).map_err(|e| StorageError::Other(e.to_string()))?;

        // Insert journal dimensions
        for (k, v) in &command.dimensions {
            conn.execute(
                "INSERT INTO journal_dimensions (journal_id, dimension_key, dimension_value) VALUES (?1, ?2, ?3)",
                params![jid, k.as_ref(), data_value_to_str(v)],
            ).map_err(|e| StorageError::Other(e.to_string()))?;
        }

        // Look up account types for sign adjustment
        for entry in &command.ledger_entries {
            let (account_id, raw_amount) = match entry {
                LedgerEntryCommand::Debit { account_id, amount } => (account_id, *amount),
                LedgerEntryCommand::Credit { account_id, amount } => (account_id, -*amount),
            };

            // Get account type for sign convention
            let acct_type_str: String = conn
                .query_row(
                    "SELECT account_type FROM accounts WHERE id = ?1",
                    params![account_id.as_ref()],
                    |row| row.get(0),
                )
                .map_err(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => {
                        StorageError::AccountNotFound(account_id.to_string())
                    }
                    _ => StorageError::Other(e.to_string()),
                })?;

            let acct_type = str_to_account_type(&acct_type_str);
            let signed_amount = match acct_type {
                AccountType::Asset | AccountType::Expense => raw_amount,
                AccountType::Liability | AccountType::Equity | AccountType::Income => -raw_amount,
            };

            conn.execute(
                "INSERT INTO ledger_entries (journal_id, account_id, date, amount) VALUES (?1, ?2, ?3, ?4)",
                params![jid, account_id.as_ref(), date_str, signed_amount.to_string()],
            ).map_err(|e| StorageError::Other(e.to_string()))?;

            let le_id = conn.last_insert_rowid();

            // Copy dimensions to ledger entry
            for (k, v) in &command.dimensions {
                conn.execute(
                    "INSERT INTO ledger_entry_dimensions (ledger_entry_id, dimension_key, dimension_value) VALUES (?1, ?2, ?3)",
                    params![le_id, k.as_ref(), data_value_to_str(v)],
                ).map_err(|e| StorageError::Other(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn get_balance(
        &self,
        account_id: &str,
        date: Date,
        dimension: Option<&(Arc<str>, Arc<DataValue>)>,
    ) -> Result<Decimal, StorageError> {
        let conn = self.conn.lock().unwrap();

        // Verify account exists
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM accounts WHERE id = ?1",
                params![account_id],
                |row| row.get(0),
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        if !exists {
            return Err(StorageError::AccountNotFound(account_id.to_string()));
        }

        let date_str = date_to_str(date);

        let total: Decimal = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let mut stmt = conn.prepare(
                    "SELECT CAST(COALESCE(SUM(le.amount), 0) AS TEXT)
                     FROM ledger_entries le
                     JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                     WHERE le.account_id = ?1 AND le.date <= ?2
                       AND led.dimension_key = ?3 AND led.dimension_value = ?4"
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                let val: String = stmt.query_row(
                    params![account_id, date_str, dim_key.as_ref(), dim_val_str],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                Decimal::from_str(&val).unwrap_or(Decimal::ZERO)
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT CAST(COALESCE(SUM(le.amount), 0) AS TEXT)
                     FROM ledger_entries le
                     WHERE le.account_id = ?1 AND le.date <= ?2"
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                let val: String = stmt.query_row(
                    params![account_id, date_str],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                Decimal::from_str(&val).unwrap_or(Decimal::ZERO)
            }
        };

        Ok(total)
    }

    fn get_statement(
        &self,
        account_id: &str,
        from: Bound<Date>,
        to: Bound<Date>,
        dimension: Option<&(Arc<str>, Arc<DataValue>)>,
    ) -> Result<DataValue, StorageError> {
        let conn = self.conn.lock().unwrap();

        // Verify account exists
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM accounts WHERE id = ?1",
                params![account_id],
                |row| row.get(0),
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        if !exists {
            return Err(StorageError::AccountNotFound(account_id.to_string()));
        }

        // Get the opening balance
        let balance_date = match from {
            Bound::Included(d) => d.previous_day().unwrap(),
            Bound::Excluded(d) => d,
            Bound::Unbounded => Date::MIN,
        };

        // Build date range conditions
        let (from_op, from_str) = match from {
            Bound::Included(d) => (">=", date_to_str(d)),
            Bound::Excluded(d) => (">", date_to_str(d)),
            Bound::Unbounded => (">=", "0000-01-01".to_string()),
        };
        let (to_op, to_str) = match to {
            Bound::Included(d) => ("<=", date_to_str(d)),
            Bound::Excluded(d) => ("<", date_to_str(d)),
            Bound::Unbounded => ("<=", "9999-12-31".to_string()),
        };

        // Calculate opening balance (reuse get_balance logic but without the lock)
        let mut opening_balance = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let val: String = conn.query_row(
                    "SELECT CAST(COALESCE(SUM(le.amount), 0) AS TEXT)
                     FROM ledger_entries le
                     JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                     WHERE le.account_id = ?1 AND le.date <= ?2
                       AND led.dimension_key = ?3 AND led.dimension_value = ?4",
                    params![account_id, date_to_str(balance_date), dim_key.as_ref(), dim_val_str],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                Decimal::from_str(&val).unwrap_or(Decimal::ZERO)
            }
            None => {
                let val: String = conn.query_row(
                    "SELECT CAST(COALESCE(SUM(le.amount), 0) AS TEXT)
                     FROM ledger_entries le
                     WHERE le.account_id = ?1 AND le.date <= ?2",
                    params![account_id, date_to_str(balance_date)],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                Decimal::from_str(&val).unwrap_or(Decimal::ZERO)
            }
        };

        // Fetch entries in the date range
        let query = match dimension {
            Some(_) => format!(
                "SELECT le.journal_id, le.date, j.description, le.amount
                 FROM ledger_entries le
                 JOIN journals j ON j.id = le.journal_id
                 JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                 WHERE le.account_id = ?1 AND le.date {} ?2 AND le.date {} ?3
                   AND led.dimension_key = ?4 AND led.dimension_value = ?5
                 ORDER BY le.date, le.id",
                from_op, to_op
            ),
            None => format!(
                "SELECT le.journal_id, le.date, j.description, le.amount
                 FROM ledger_entries le
                 JOIN journals j ON j.id = le.journal_id
                 WHERE le.account_id = ?1 AND le.date {} ?2 AND le.date {} ?3
                 ORDER BY le.date, le.id",
                from_op, to_op
            ),
        };

        let mut stmt = conn.prepare(&query).map_err(|e| StorageError::Other(e.to_string()))?;

        let row_mapper = |row: &rusqlite::Row| -> rusqlite::Result<(String, String, String, String)> {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        };

        let rows: Vec<(String, String, String, String)> = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                stmt.query_map(
                    params![account_id, from_str, to_str, dim_key.as_ref(), dim_val_str],
                    row_mapper,
                )
                .map_err(|e| StorageError::Other(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::Other(e.to_string()))?
            }
            None => {
                stmt.query_map(
                    params![account_id, from_str, to_str],
                    row_mapper,
                )
                .map_err(|e| StorageError::Other(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::Other(e.to_string()))?
            }
        };

        let mut result = Vec::new();
        for (jid_str, date_str_row, desc, amt_str) in rows {
            let amount = Decimal::from_str(&amt_str).unwrap_or(Decimal::ZERO);
            opening_balance += amount;
            let journal_id = Uuid::parse_str(&jid_str)
                .map(|u| u.as_u128())
                .unwrap_or(0);
            result.push(StatementTxn {
                journal_id,
                date: str_to_date(&date_str_row),
                description: Arc::from(desc.as_str()),
                amount,
                balance: opening_balance,
            });
        }

        Ok(DataValue::Statement(result))
    }

    fn get_dimension_values(
        &self,
        account_id: &str,
        dimension_key: Arc<str>,
        from: Date,
        to: Date,
    ) -> Result<HashSet<Arc<DataValue>>, StorageError> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT DISTINCT led.dimension_value
             FROM ledger_entries le
             JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
             WHERE le.account_id = ?1 AND led.dimension_key = ?2
               AND le.date >= ?3 AND le.date <= ?4"
        ).map_err(|e| StorageError::Other(e.to_string()))?;

        let rows = stmt.query_map(
            params![account_id, dimension_key.as_ref(), date_to_str(from), date_to_str(to)],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut result = HashSet::new();
        for row in rows {
            let val = row.map_err(|e| StorageError::Other(e.to_string()))?;
            result.insert(Arc::new(DataValue::String(Arc::from(val.as_str()))));
        }
        Ok(result)
    }

    fn list_accounts(&self) -> Vec<(Arc<str>, AccountType)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, account_type FROM accounts ORDER BY id")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let at: String = row.get(1)?;
                Ok((id, at))
            })
            .unwrap();

        let mut result = Vec::new();
        for row in rows {
            if let Ok((id, at)) = row {
                result.push((Arc::from(id.as_str()), str_to_account_type(&at)));
            }
        }
        result
    }

    fn begin_transaction(&self) -> Result<TransactionId, StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("SAVEPOINT findb_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst);
        *self.active_tx.lock().unwrap() = Some(tx_id);
        tracing::debug!(tx_id, "SQLite transaction started");
        Ok(tx_id)
    }

    fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError> {
        let mut active = self.active_tx.lock().unwrap();
        if *active != Some(tx_id) {
            return Err(StorageError::NoActiveTransaction);
        }
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("RELEASE SAVEPOINT findb_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        *active = None;
        tracing::debug!(tx_id, "SQLite transaction committed");
        Ok(())
    }

    fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError> {
        let mut active = self.active_tx.lock().unwrap();
        if *active != Some(tx_id) {
            return Err(StorageError::NoActiveTransaction);
        }
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("ROLLBACK TO SAVEPOINT findb_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        *active = None;
        tracing::debug!(tx_id, "SQLite transaction rolled back");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_sqlite_basic_operations() {
        let storage = SqliteStorage::new(":memory:").unwrap();

        // Create accounts
        storage
            .create_account(&AccountExpression {
                id: Arc::from("bank"),
                account_type: AccountType::Asset,
            })
            .unwrap();
        storage
            .create_account(&AccountExpression {
                id: Arc::from("equity"),
                account_type: AccountType::Equity,
            })
            .unwrap();

        // Create journal
        let date = Date::from_calendar_date(2023, Month::January, 1).unwrap();
        let cmd = CreateJournalCommand {
            date,
            description: Arc::from("Investment"),
            amount: Decimal::from(1000),
            ledger_entries: vec![
                LedgerEntryCommand::Credit {
                    account_id: Arc::from("equity"),
                    amount: Decimal::from(1000),
                },
                LedgerEntryCommand::Debit {
                    account_id: Arc::from("bank"),
                    amount: Decimal::from(1000),
                },
            ],
            dimensions: BTreeMap::new(),
        };
        storage.create_journal(&cmd).unwrap();

        // Check balance
        let bal = storage
            .get_balance("bank", date, None)
            .unwrap();
        assert_eq!(bal, Decimal::from(1000));

        let eq_bal = storage
            .get_balance("equity", date, None)
            .unwrap();
        assert_eq!(eq_bal, Decimal::from(1000));
    }

    #[test]
    fn test_sqlite_transaction_rollback() {
        let storage = SqliteStorage::new(":memory:").unwrap();

        storage
            .create_account(&AccountExpression {
                id: Arc::from("bank"),
                account_type: AccountType::Asset,
            })
            .unwrap();
        storage
            .create_account(&AccountExpression {
                id: Arc::from("equity"),
                account_type: AccountType::Equity,
            })
            .unwrap();

        let date = Date::from_calendar_date(2023, Month::January, 1).unwrap();

        let tx_id = storage.begin_transaction().unwrap();
        storage
            .create_journal(&CreateJournalCommand {
                date,
                description: Arc::from("Test"),
                amount: Decimal::from(500),
                ledger_entries: vec![
                    LedgerEntryCommand::Credit {
                        account_id: Arc::from("equity"),
                        amount: Decimal::from(500),
                    },
                    LedgerEntryCommand::Debit {
                        account_id: Arc::from("bank"),
                        amount: Decimal::from(500),
                    },
                ],
                dimensions: BTreeMap::new(),
            })
            .unwrap();
        storage.rollback_transaction(tx_id).unwrap();

        let bal = storage.get_balance("bank", date, None).unwrap();
        assert_eq!(bal, Decimal::ZERO, "Balance should be 0 after rollback");
    }
}
