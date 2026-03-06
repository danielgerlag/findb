use std::{
    collections::{BTreeMap, HashSet},
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

use dblentry_core::{
    AccountExpression, AccountType, CostMethod, LotItem,
    CreateJournalCommand, CreateRateCommand, LedgerEntryCommand, SetRateCommand,
    DataValue, StatementTxn,
    StorageBackend, StorageError, TransactionId,
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

            CREATE TABLE IF NOT EXISTS lots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                units_remaining TEXT NOT NULL,
                cost_per_unit TEXT NOT NULL,
                journal_id TEXT NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );

            CREATE TABLE IF NOT EXISTS lot_dimensions (
                lot_id INTEGER NOT NULL,
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL,
                FOREIGN KEY (lot_id) REFERENCES lots(id)
            );

            CREATE INDEX IF NOT EXISTS idx_lot_account ON lots(account_id);
            CREATE INDEX IF NOT EXISTS idx_lot_dims ON lot_dimensions(lot_id, dimension_key, dimension_value);
            ",
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;

        // Safely add unit_rate_id column (ignore error if it already exists)
        let _ = conn.execute_batch("ALTER TABLE accounts ADD COLUMN unit_rate_id TEXT;");

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
    fn create_entity(&self, _entity_id: &str) -> Result<(), StorageError> {
        // TODO: Add entities table and full entity support
        Ok(())
    }

    fn list_entities(&self) -> Vec<Arc<str>> {
        vec![Arc::from("default")]
    }

    fn entity_exists(&self, _entity_id: &str) -> bool {
        true // SQLite currently only supports default entity
    }

    fn create_account(&self, _entity_id: &str, account: &AccountExpression) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let unit_rate_id = account.unit_rate_id.as_ref().map(|s| s.to_string());
        conn.execute(
            "INSERT OR REPLACE INTO accounts (id, account_type, unit_rate_id) VALUES (?1, ?2, ?3)",
            params![account.id.as_ref(), account_type_to_str(&account.account_type), unit_rate_id],
        )
        .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn create_rate(&self, _entity_id: &str, _rate: &CreateRateCommand) -> Result<(), StorageError> {
        // Rates table uses (id, date) as PK; creating a rate just means it's available
        // No row needed until set_rate is called — but we validate existence on get_rate
        // Insert a marker if needed (not strictly necessary with our schema)
        Ok(())
    }

    fn set_rate(&self, _entity_id: &str, command: &SetRateCommand) -> Result<(), StorageError> {
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

    fn get_rate(&self, _entity_id: &str, id: &str, date: Date) -> Result<Decimal, StorageError> {
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

    fn create_journal(&self, _entity_id: &str, command: &CreateJournalCommand) -> Result<(), StorageError> {
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
                LedgerEntryCommand::Debit { account_id, amount, .. } => (account_id, *amount),
                LedgerEntryCommand::Credit { account_id, amount, .. } => (account_id, -*amount),
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

            // Handle lot creation for debits with units
            if let LedgerEntryCommand::Debit { account_id, amount, units: Some(unit_count) } = entry {
                let unit_rate_id: Option<String> = conn.query_row(
                    "SELECT unit_rate_id FROM accounts WHERE id = ?1",
                    params![account_id.as_ref()],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?;

                if unit_rate_id.is_some() {
                    let cost_per_unit = if !unit_count.is_zero() {
                        *amount / *unit_count
                    } else {
                        Decimal::ZERO
                    };
                    conn.execute(
                        "INSERT INTO lots (account_id, date, units_remaining, cost_per_unit, journal_id) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![account_id.as_ref(), date_str, unit_count.to_string(), cost_per_unit.to_string(), jid],
                    ).map_err(|e| StorageError::Other(e.to_string()))?;

                    let lot_id = conn.last_insert_rowid();
                    for (k, v) in &command.dimensions {
                        conn.execute(
                            "INSERT INTO lot_dimensions (lot_id, dimension_key, dimension_value) VALUES (?1, ?2, ?3)",
                            params![lot_id, k.as_ref(), data_value_to_str(v)],
                        ).map_err(|e| StorageError::Other(e.to_string()))?;
                    }
                }
            }

            // Handle lot depletion for credits with units (FIFO)
            if let LedgerEntryCommand::Credit { account_id, units: Some(unit_count), .. } = entry {
                let unit_rate_id: Option<String> = conn.query_row(
                    "SELECT unit_rate_id FROM accounts WHERE id = ?1",
                    params![account_id.as_ref()],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?;

                if unit_rate_id.is_some() {
                    let mut remaining = *unit_count;
                    let mut lot_rows: Vec<(i64, String)> = Vec::new();
                    {
                        let mut stmt = conn.prepare(
                            "SELECT id, units_remaining FROM lots WHERE account_id = ?1 AND CAST(units_remaining AS REAL) > 0 ORDER BY date ASC"
                        ).map_err(|e| StorageError::Other(e.to_string()))?;
                        let rows = stmt.query_map(
                            params![account_id.as_ref()],
                            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                        ).map_err(|e| StorageError::Other(e.to_string()))?;
                        for row in rows {
                            lot_rows.push(row.map_err(|e| StorageError::Other(e.to_string()))?);
                        }
                    }

                    for (lot_id, units_str) in lot_rows {
                        if remaining.is_zero() {
                            break;
                        }
                        let lot_units = Decimal::from_str(&units_str)
                            .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                        if lot_units <= remaining {
                            remaining -= lot_units;
                            conn.execute(
                                "UPDATE lots SET units_remaining = '0' WHERE id = ?1",
                                params![lot_id],
                            ).map_err(|e| StorageError::Other(e.to_string()))?;
                        } else {
                            let new_remaining = lot_units - remaining;
                            remaining = Decimal::ZERO;
                            conn.execute(
                                "UPDATE lots SET units_remaining = ?1 WHERE id = ?2",
                                params![new_remaining.to_string(), lot_id],
                            ).map_err(|e| StorageError::Other(e.to_string()))?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_balance(
        &self,
        _entity_id: &str,
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
                       AND led.dimension_key = ?3 AND (led.dimension_value = ?4 OR led.dimension_value LIKE ?4 || '/%')"
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
        _entity_id: &str,
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
            Bound::Included(d) => d.previous_day().unwrap_or(d),
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
                       AND led.dimension_key = ?3 AND (led.dimension_value = ?4 OR led.dimension_value LIKE ?4 || '/%')",
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
                   AND led.dimension_key = ?4 AND (led.dimension_value = ?5 OR led.dimension_value LIKE ?5 || '/%')
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
        _entity_id: &str,
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

    fn list_accounts(&self, _entity_id: &str) -> Vec<(Arc<str>, AccountType)> {
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
        for (id, at) in rows.flatten() {
            result.push((Arc::from(id.as_str()), str_to_account_type(&at)));
        }
        result
    }

    fn list_rates(&self, _entity_id: &str) -> Vec<Arc<str>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT DISTINCT id FROM rates ORDER BY id")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                Ok(id)
            })
            .unwrap();

        rows.flatten().map(|id| Arc::from(id.as_str())).collect()
    }

    fn begin_transaction(&self) -> Result<TransactionId, StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("SAVEPOINT dblentry_tx")
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
        conn.execute_batch("RELEASE SAVEPOINT dblentry_tx")
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
        conn.execute_batch("ROLLBACK TO SAVEPOINT dblentry_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        *active = None;
        tracing::debug!(tx_id, "SQLite transaction rolled back");
        Ok(())
    }

    fn get_lots(&self, _entity_id: &str, account_id: &str, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Vec<LotItem>, StorageError> {
        let conn = self.conn.lock().unwrap();

        let lot_rows: Vec<(i64, String, String, String)> = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let mut stmt = conn.prepare(
                    "SELECT l.id, l.date, l.units_remaining, l.cost_per_unit FROM lots l
                     WHERE l.account_id = ?1 AND CAST(l.units_remaining AS REAL) > 0
                       AND EXISTS (
                         SELECT 1 FROM lot_dimensions ld
                         WHERE ld.lot_id = l.id AND ld.dimension_key = ?2
                           AND (ld.dimension_value = ?3 OR ld.dimension_value LIKE ?3 || '/%')
                       )
                     ORDER BY l.date ASC"
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                let rows = stmt.query_map(
                    params![account_id, dim_key.as_ref(), dim_val_str],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|e| StorageError::Other(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::Other(e.to_string()))?;
                rows
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT l.id, l.date, l.units_remaining, l.cost_per_unit FROM lots l
                     WHERE l.account_id = ?1 AND CAST(l.units_remaining AS REAL) > 0
                     ORDER BY l.date ASC"
                ).map_err(|e| StorageError::Other(e.to_string()))?;
                let rows = stmt.query_map(
                    params![account_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|e| StorageError::Other(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| StorageError::Other(e.to_string()))?;
                rows
            }
        };

        let mut result = Vec::new();
        for (lot_id, date_str, units_str, cpu_str) in lot_rows {
            let units = Decimal::from_str(&units_str)
                .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
            let cost_per_unit = Decimal::from_str(&cpu_str)
                .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;

            // Fetch dimensions for this lot
            let mut dims = BTreeMap::new();
            let mut dim_stmt = conn.prepare(
                "SELECT dimension_key, dimension_value FROM lot_dimensions WHERE lot_id = ?1"
            ).map_err(|e| StorageError::Other(e.to_string()))?;
            let dim_rows = dim_stmt.query_map(
                params![lot_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            ).map_err(|e| StorageError::Other(e.to_string()))?;
            for dim_row in dim_rows {
                let (k, v) = dim_row.map_err(|e| StorageError::Other(e.to_string()))?;
                dims.insert(Arc::from(k.as_str()), Arc::new(DataValue::String(Arc::from(v.as_str()))));
            }

            result.push(LotItem {
                date: str_to_date(&date_str),
                units,
                cost_per_unit,
                total_cost: units * cost_per_unit,
                dimensions: dims,
            });
        }

        Ok(result)
    }

    fn get_total_units(&self, _entity_id: &str, account_id: &str, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError> {
        let conn = self.conn.lock().unwrap();

        let total_str: String = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                conn.query_row(
                    "SELECT CAST(COALESCE(SUM(CAST(l.units_remaining AS REAL)), 0) AS TEXT) FROM lots l
                     WHERE l.account_id = ?1 AND CAST(l.units_remaining AS REAL) > 0
                       AND EXISTS (
                         SELECT 1 FROM lot_dimensions ld
                         WHERE ld.lot_id = l.id AND ld.dimension_key = ?2
                           AND (ld.dimension_value = ?3 OR ld.dimension_value LIKE ?3 || '/%')
                       )",
                    params![account_id, dim_key.as_ref(), dim_val_str],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?
            }
            None => {
                conn.query_row(
                    "SELECT CAST(COALESCE(SUM(CAST(l.units_remaining AS REAL)), 0) AS TEXT) FROM lots l
                     WHERE l.account_id = ?1 AND CAST(l.units_remaining AS REAL) > 0",
                    params![account_id],
                    |row| row.get(0),
                ).map_err(|e| StorageError::Other(e.to_string()))?
            }
        };

        Decimal::from_str(&total_str)
            .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))
    }

    fn deplete_lots(&self, _entity_id: &str, account_id: &str, units: Decimal, method: &CostMethod, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) -> Result<Decimal, StorageError> {
        let conn = self.conn.lock().unwrap();

        let order = match method {
            CostMethod::Fifo => "ASC",
            CostMethod::Lifo => "DESC",
            CostMethod::Average => "ASC",
        };

        // Build query with optional dimension filtering
        let lot_rows: Vec<(i64, String, String)> = if dimensions.is_empty() {
            let mut stmt = conn.prepare(&format!(
                "SELECT id, units_remaining, cost_per_unit FROM lots
                 WHERE account_id = ?1 AND CAST(units_remaining AS REAL) > 0
                 ORDER BY date {order}"
            )).map_err(|e| StorageError::Other(e.to_string()))?;
            let rows = stmt.query_map(
                params![account_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|e| StorageError::Other(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Other(e.to_string()))?;
            rows
        } else {
            // Build dimension filter: all dimension key/value pairs must match (prefix)
            let dim_conditions: Vec<String> = dimensions.iter().enumerate().map(|(i, (k, v))| {
                let _ = (k, v); // used below via parameter binding
                format!(
                    "EXISTS (SELECT 1 FROM lot_dimensions ld{i} WHERE ld{i}.lot_id = lots.id AND ld{i}.dimension_key = ?{p1} AND (ld{i}.dimension_value = ?{p2} OR ld{i}.dimension_value LIKE ?{p2} || '/%'))",
                    i = i, p1 = 2 + i * 2, p2 = 3 + i * 2
                )
            }).collect();

            let query = format!(
                "SELECT id, units_remaining, cost_per_unit FROM lots
                 WHERE account_id = ?1 AND CAST(units_remaining AS REAL) > 0
                   AND {}
                 ORDER BY date {order}",
                dim_conditions.join(" AND ")
            );

            let mut stmt = conn.prepare(&query).map_err(|e| StorageError::Other(e.to_string()))?;

            // Build params: account_id + pairs of (key, value)
            let mut param_values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            param_values.push(Box::new(account_id.to_string()));
            for (k, v) in dimensions {
                param_values.push(Box::new(k.to_string()));
                param_values.push(Box::new(data_value_to_str(v)));
            }
            let param_refs: Vec<&dyn rusqlite::types::ToSql> = param_values.iter().map(|p| p.as_ref()).collect();

            let rows = stmt.query_map(
                param_refs.as_slice(),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|e| StorageError::Other(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| StorageError::Other(e.to_string()))?;
            rows
        };

        if matches!(method, CostMethod::Average) {
            // Calculate weighted average cost across all matching lots
            let mut total_units = Decimal::ZERO;
            let mut total_cost = Decimal::ZERO;
            for (_, units_str, cpu_str) in &lot_rows {
                let lot_units = Decimal::from_str(units_str)
                    .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                let cpu = Decimal::from_str(cpu_str)
                    .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                total_units += lot_units;
                total_cost += lot_units * cpu;
            }

            if total_units.is_zero() {
                return Ok(Decimal::ZERO);
            }

            let avg_cost = total_cost / total_units;
            let mut remaining = units;
            let mut cost_basis = Decimal::ZERO;

            for (lot_id, units_str, _) in &lot_rows {
                if remaining.is_zero() {
                    break;
                }
                let lot_units = Decimal::from_str(units_str)
                    .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                if lot_units <= remaining {
                    remaining -= lot_units;
                    cost_basis += lot_units * avg_cost;
                    conn.execute(
                        "UPDATE lots SET units_remaining = '0' WHERE id = ?1",
                        params![lot_id],
                    ).map_err(|e| StorageError::Other(e.to_string()))?;
                } else {
                    cost_basis += remaining * avg_cost;
                    let new_remaining = lot_units - remaining;
                    remaining = Decimal::ZERO;
                    conn.execute(
                        "UPDATE lots SET units_remaining = ?1 WHERE id = ?2",
                        params![new_remaining.to_string(), lot_id],
                    ).map_err(|e| StorageError::Other(e.to_string()))?;
                }
            }

            Ok(cost_basis)
        } else {
            // FIFO / LIFO
            let mut remaining = units;
            let mut cost_basis = Decimal::ZERO;

            for (lot_id, units_str, cpu_str) in &lot_rows {
                if remaining.is_zero() {
                    break;
                }
                let lot_units = Decimal::from_str(units_str)
                    .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                let cpu = Decimal::from_str(cpu_str)
                    .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                if lot_units <= remaining {
                    remaining -= lot_units;
                    cost_basis += lot_units * cpu;
                    conn.execute(
                        "UPDATE lots SET units_remaining = '0' WHERE id = ?1",
                        params![lot_id],
                    ).map_err(|e| StorageError::Other(e.to_string()))?;
                } else {
                    cost_basis += remaining * cpu;
                    let new_remaining = lot_units - remaining;
                    remaining = Decimal::ZERO;
                    conn.execute(
                        "UPDATE lots SET units_remaining = ?1 WHERE id = ?2",
                        params![new_remaining.to_string(), lot_id],
                    ).map_err(|e| StorageError::Other(e.to_string()))?;
                }
            }

            Ok(cost_basis)
        }
    }

    fn split_lots(&self, _entity_id: &str, account_id: &str, new_per_old: Decimal, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();

        match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                conn.execute(
                    "UPDATE lots SET
                        units_remaining = CAST(CAST(units_remaining AS REAL) * CAST(?2 AS REAL) AS TEXT),
                        cost_per_unit = CAST(CAST(cost_per_unit AS REAL) / CAST(?2 AS REAL) AS TEXT)
                     WHERE account_id = ?1 AND CAST(units_remaining AS REAL) > 0
                       AND EXISTS (
                         SELECT 1 FROM lot_dimensions ld
                         WHERE ld.lot_id = lots.id AND ld.dimension_key = ?3
                           AND (ld.dimension_value = ?4 OR ld.dimension_value LIKE ?4 || '/%')
                       )",
                    params![account_id, new_per_old.to_string(), dim_key.as_ref(), dim_val_str],
                ).map_err(|e| StorageError::Other(e.to_string()))?;
            }
            None => {
                conn.execute(
                    "UPDATE lots SET
                        units_remaining = CAST(CAST(units_remaining AS REAL) * CAST(?2 AS REAL) AS TEXT),
                        cost_per_unit = CAST(CAST(cost_per_unit AS REAL) / CAST(?2 AS REAL) AS TEXT)
                     WHERE account_id = ?1 AND CAST(units_remaining AS REAL) > 0",
                    params![account_id, new_per_old.to_string()],
                ).map_err(|e| StorageError::Other(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn get_unit_rate_id(&self, _entity_id: &str, account_id: &str) -> Option<Arc<str>> {
        let conn = self.conn.lock().unwrap();
        let result: Result<Option<String>, _> = conn.query_row(
            "SELECT unit_rate_id FROM accounts WHERE id = ?1",
            params![account_id],
            |row| row.get(0),
        );
        match result {
            Ok(Some(id)) => Some(Arc::from(id.as_str())),
            _ => None,
        }
    }

    fn is_unit_account(&self, _entity_id: &str, account_id: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        let result: Result<bool, _> = conn.query_row(
            "SELECT unit_rate_id IS NOT NULL FROM accounts WHERE id = ?1",
            params![account_id],
            |row| row.get(0),
        );
        result.unwrap_or(false)
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
            .create_account("default", &AccountExpression {
                id: Arc::from("bank"),
                account_type: AccountType::Asset,
                unit_rate_id: None,
            })
            .unwrap();
        storage
            .create_account("default", &AccountExpression {
                id: Arc::from("equity"),
                account_type: AccountType::Equity,
                unit_rate_id: None,
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
                    units: None,
                },
                LedgerEntryCommand::Debit {
                    account_id: Arc::from("bank"),
                    amount: Decimal::from(1000),
                    units: None,
                },
            ],
            dimensions: BTreeMap::new(),
        };
        storage.create_journal("default", &cmd).unwrap();

        // Check balance
        let bal = storage
            .get_balance("default", "bank", date, None)
            .unwrap();
        assert_eq!(bal, Decimal::from(1000));

        let eq_bal = storage
            .get_balance("default", "equity", date, None)
            .unwrap();
        assert_eq!(eq_bal, Decimal::from(1000));
    }

    #[test]
    fn test_sqlite_transaction_rollback() {
        let storage = SqliteStorage::new(":memory:").unwrap();

        storage
            .create_account("default", &AccountExpression {
                id: Arc::from("bank"),
                account_type: AccountType::Asset,
                unit_rate_id: None,
            })
            .unwrap();
        storage
            .create_account("default", &AccountExpression {
                id: Arc::from("equity"),
                account_type: AccountType::Equity,
                unit_rate_id: None,
            })
            .unwrap();

        let date = Date::from_calendar_date(2023, Month::January, 1).unwrap();

        let tx_id = storage.begin_transaction().unwrap();
        storage
            .create_journal("default", &CreateJournalCommand {
                date,
                description: Arc::from("Test"),
                amount: Decimal::from(500),
                ledger_entries: vec![
                    LedgerEntryCommand::Credit {
                        account_id: Arc::from("equity"),
                        amount: Decimal::from(500),
                        units: None,
                    },
                    LedgerEntryCommand::Debit {
                        account_id: Arc::from("bank"),
                        amount: Decimal::from(500),
                        units: None,
                    },
                ],
                dimensions: BTreeMap::new(),
            })
            .unwrap();
        storage.rollback_transaction(tx_id).unwrap();

        let bal = storage.get_balance("default", "bank", date, None).unwrap();
        assert_eq!(bal, Decimal::ZERO, "Balance should be 0 after rollback");
    }
}

