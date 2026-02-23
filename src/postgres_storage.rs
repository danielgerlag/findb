use std::{
    collections::HashSet,
    ops::Bound,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use postgres::{Client, NoTls};
use rust_decimal::Decimal;
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

pub struct PostgresStorage {
    client: Mutex<Client>,
    tx_counter: AtomicU64,
    active_tx: Mutex<Option<TransactionId>>,
}

impl PostgresStorage {
    pub fn new(connection_string: &str) -> Result<Self, StorageError> {
        let client = Client::connect(connection_string, NoTls)
            .map_err(|e| StorageError::Other(format!("PostgreSQL connection failed: {}", e)))?;

        let storage = Self {
            client: Mutex::new(client),
            tx_counter: AtomicU64::new(1),
            active_tx: Mutex::new(None),
        };
        storage.init_schema()?;
        Ok(storage)
    }

    fn init_schema(&self) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        client
            .batch_execute(
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
                sequence BIGINT NOT NULL,
                date TEXT NOT NULL,
                description TEXT NOT NULL,
                amount TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS journal_dimensions (
                journal_id TEXT NOT NULL REFERENCES journals(id),
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                id BIGSERIAL PRIMARY KEY,
                journal_id TEXT NOT NULL REFERENCES journals(id),
                account_id TEXT NOT NULL REFERENCES accounts(id),
                date TEXT NOT NULL,
                amount TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ledger_entry_dimensions (
                ledger_entry_id BIGINT NOT NULL REFERENCES ledger_entries(id),
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_pg_ledger_account_date
                ON ledger_entries(account_id, date);

            CREATE INDEX IF NOT EXISTS idx_pg_ledger_dim
                ON ledger_entry_dimensions(ledger_entry_id);

            CREATE INDEX IF NOT EXISTS idx_pg_rates_lookup
                ON rates(id, date);

            CREATE TABLE IF NOT EXISTS sequence_counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                value BIGINT NOT NULL
            );

            INSERT INTO sequence_counter (id, value) VALUES (1, 0)
                ON CONFLICT (id) DO NOTHING;
            ",
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn next_sequence(client: &mut Client) -> Result<u64, StorageError> {
        client
            .execute(
                "UPDATE sequence_counter SET value = value + 1 WHERE id = 1",
                &[],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let row = client
            .query_one("SELECT value FROM sequence_counter WHERE id = 1", &[])
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let seq: i64 = row.get(0);
        Ok(seq as u64)
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

impl StorageBackend for PostgresStorage {
    fn create_account(&self, account: &AccountExpression) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        client
            .execute(
                "INSERT INTO accounts (id, account_type) VALUES ($1, $2)
                 ON CONFLICT (id) DO UPDATE SET account_type = $2",
                &[&account.id.as_ref(), &account_type_to_str(&account.account_type)],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn create_rate(&self, _rate: &CreateRateCommand) -> Result<(), StorageError> {
        Ok(())
    }

    fn set_rate(&self, command: &SetRateCommand) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        let date_str = date_to_str(command.date);
        let val_str = command.rate.to_string();
        client
            .execute(
                "INSERT INTO rates (id, date, value) VALUES ($1, $2, $3)
                 ON CONFLICT (id, date) DO UPDATE SET value = $3",
                &[&command.id.as_ref(), &date_str, &val_str],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn get_rate(&self, id: &str, date: Date) -> Result<Decimal, StorageError> {
        let mut client = self.client.lock().unwrap();
        let date_str = date_to_str(date);
        let result = client.query_opt(
            "SELECT value FROM rates WHERE id = $1 AND date <= $2 ORDER BY date DESC LIMIT 1",
            &[&id, &date_str],
        );
        match result {
            Ok(Some(row)) => {
                let val: String = row.get(0);
                Decimal::from_str(&val)
                    .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))
            }
            Ok(None) => Err(StorageError::NoRateFound),
            Err(e) => Err(StorageError::Other(e.to_string())),
        }
    }

    fn create_journal(&self, command: &CreateJournalCommand) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        let jid = Uuid::new_v4().to_string();
        let seq = Self::next_sequence(&mut client)?;
        let seq_i64 = seq as i64;
        let date_str = date_to_str(command.date);
        let now = OffsetDateTime::now_utc().to_string();
        let amount_str = command.amount.to_string();

        client
            .execute(
                "INSERT INTO journals (id, sequence, date, description, amount, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &jid,
                    &seq_i64,
                    &date_str,
                    &command.description.as_ref(),
                    &amount_str,
                    &now,
                ],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;

        for (k, v) in &command.dimensions {
            let dim_val = data_value_to_str(v);
            client
                .execute(
                    "INSERT INTO journal_dimensions (journal_id, dimension_key, dimension_value)
                     VALUES ($1, $2, $3)",
                    &[&jid, &k.as_ref(), &dim_val],
                )
                .map_err(|e| StorageError::Other(e.to_string()))?;
        }

        for entry in &command.ledger_entries {
            let (account_id, raw_amount) = match entry {
                LedgerEntryCommand::Debit {
                    account_id,
                    amount,
                } => (account_id, *amount),
                LedgerEntryCommand::Credit {
                    account_id,
                    amount,
                } => (account_id, -*amount),
            };

            let row = client
                .query_opt(
                    "SELECT account_type FROM accounts WHERE id = $1",
                    &[&account_id.as_ref()],
                )
                .map_err(|e| StorageError::Other(e.to_string()))?
                .ok_or_else(|| StorageError::AccountNotFound(account_id.to_string()))?;

            let acct_type_str: String = row.get(0);
            let acct_type = str_to_account_type(&acct_type_str);
            let signed_amount = match acct_type {
                AccountType::Asset | AccountType::Expense => raw_amount,
                AccountType::Liability | AccountType::Equity | AccountType::Income => -raw_amount,
            };

            let amount_str = signed_amount.to_string();
            let le_row = client
                .query_one(
                    "INSERT INTO ledger_entries (journal_id, account_id, date, amount)
                     VALUES ($1, $2, $3, $4) RETURNING id",
                    &[&jid, &account_id.as_ref(), &date_str, &amount_str],
                )
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let le_id: i64 = le_row.get(0);

            for (k, v) in &command.dimensions {
                let dim_val = data_value_to_str(v);
                client
                    .execute(
                        "INSERT INTO ledger_entry_dimensions (ledger_entry_id, dimension_key, dimension_value)
                         VALUES ($1, $2, $3)",
                        &[&le_id, &k.as_ref(), &dim_val],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
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
        let mut client = self.client.lock().unwrap();

        // Verify account exists
        let exists = client
            .query_one(
                "SELECT COUNT(*) > 0 FROM accounts WHERE id = $1",
                &[&account_id],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let acct_exists: bool = exists.get(0);
        if !acct_exists {
            return Err(StorageError::AccountNotFound(account_id.to_string()));
        }

        let date_str = date_to_str(date);

        let total_str: String = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                         WHERE le.account_id = $1 AND le.date <= $2
                           AND led.dimension_key = $3 AND led.dimension_value = $4",
                        &[&account_id, &date_str, &dim_key.as_ref(), &dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
            None => {
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         WHERE le.account_id = $1 AND le.date <= $2",
                        &[&account_id, &date_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
        };

        Decimal::from_str(&total_str)
            .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))
    }

    fn get_statement(
        &self,
        account_id: &str,
        from: Bound<Date>,
        to: Bound<Date>,
        dimension: Option<&(Arc<str>, Arc<DataValue>)>,
    ) -> Result<DataValue, StorageError> {
        let mut client = self.client.lock().unwrap();

        // Verify account exists
        let exists = client
            .query_one(
                "SELECT COUNT(*) > 0 FROM accounts WHERE id = $1",
                &[&account_id],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let acct_exists: bool = exists.get(0);
        if !acct_exists {
            return Err(StorageError::AccountNotFound(account_id.to_string()));
        }

        let balance_date = match from {
            Bound::Included(d) => d.previous_day().unwrap(),
            Bound::Excluded(d) => d,
            Bound::Unbounded => Date::MIN,
        };

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

        // Opening balance
        let balance_date_str = date_to_str(balance_date);
        let opening_str: String = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                         WHERE le.account_id = $1 AND le.date <= $2
                           AND led.dimension_key = $3 AND led.dimension_value = $4",
                        &[&account_id, &balance_date_str, &dim_key.as_ref(), &dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
            None => {
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         WHERE le.account_id = $1 AND le.date <= $2",
                        &[&account_id, &balance_date_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
        };
        let mut opening_balance =
            Decimal::from_str(&opening_str).unwrap_or(Decimal::ZERO);

        // Fetch entries in range
        let query = match dimension {
            Some(_) => format!(
                "SELECT le.journal_id, le.date, j.description, le.amount
                 FROM ledger_entries le
                 JOIN journals j ON j.id = le.journal_id
                 JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                 WHERE le.account_id = $1 AND le.date {} $2 AND le.date {} $3
                   AND led.dimension_key = $4 AND led.dimension_value = $5
                 ORDER BY le.date, le.id",
                from_op, to_op
            ),
            None => format!(
                "SELECT le.journal_id, le.date, j.description, le.amount
                 FROM ledger_entries le
                 JOIN journals j ON j.id = le.journal_id
                 WHERE le.account_id = $1 AND le.date {} $2 AND le.date {} $3
                 ORDER BY le.date, le.id",
                from_op, to_op
            ),
        };

        let rows = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                client
                    .query(
                        &query,
                        &[&account_id, &from_str, &to_str, &dim_key.as_ref(), &dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?
            }
            None => client
                .query(&query, &[&account_id, &from_str, &to_str])
                .map_err(|e| StorageError::Other(e.to_string()))?,
        };

        let mut result = Vec::new();
        for row in rows {
            let jid_str: String = row.get(0);
            let date_str_row: String = row.get(1);
            let desc: String = row.get(2);
            let amt_str: String = row.get(3);

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
        let mut client = self.client.lock().unwrap();

        let rows = client
            .query(
                "SELECT DISTINCT led.dimension_value
                 FROM ledger_entries le
                 JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                 WHERE le.account_id = $1 AND led.dimension_key = $2
                   AND le.date >= $3 AND le.date <= $4",
                &[
                    &account_id,
                    &dimension_key.as_ref(),
                    &date_to_str(from),
                    &date_to_str(to),
                ],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;

        let mut result = HashSet::new();
        for row in rows {
            let val: String = row.get(0);
            result.insert(Arc::new(DataValue::String(Arc::from(val.as_str()))));
        }
        Ok(result)
    }

    fn list_accounts(&self) -> Vec<(Arc<str>, AccountType)> {
        let mut client = self.client.lock().unwrap();
        let rows = client
            .query("SELECT id, account_type FROM accounts ORDER BY id", &[])
            .unwrap_or_default();

        rows.iter()
            .map(|row| {
                let id: String = row.get(0);
                let at: String = row.get(1);
                (Arc::from(id.as_str()), str_to_account_type(&at))
            })
            .collect()
    }

    fn begin_transaction(&self) -> Result<TransactionId, StorageError> {
        let mut client = self.client.lock().unwrap();
        client
            .batch_execute("SAVEPOINT findb_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let tx_id = self.tx_counter.fetch_add(1, Ordering::SeqCst);
        *self.active_tx.lock().unwrap() = Some(tx_id);
        tracing::debug!(tx_id, "PostgreSQL transaction started");
        Ok(tx_id)
    }

    fn commit_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError> {
        let mut active = self.active_tx.lock().unwrap();
        if *active != Some(tx_id) {
            return Err(StorageError::NoActiveTransaction);
        }
        let mut client = self.client.lock().unwrap();
        client
            .batch_execute("RELEASE SAVEPOINT findb_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        *active = None;
        tracing::debug!(tx_id, "PostgreSQL transaction committed");
        Ok(())
    }

    fn rollback_transaction(&self, tx_id: TransactionId) -> Result<(), StorageError> {
        let mut active = self.active_tx.lock().unwrap();
        if *active != Some(tx_id) {
            return Err(StorageError::NoActiveTransaction);
        }
        let mut client = self.client.lock().unwrap();
        client
            .batch_execute("ROLLBACK TO SAVEPOINT findb_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        *active = None;
        tracing::debug!(tx_id, "PostgreSQL transaction rolled back");
        Ok(())
    }
}
