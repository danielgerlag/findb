use std::{
    collections::{BTreeMap, HashSet},
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

use dblentry_core::{
    AccountExpression, AccountType, CostMethod, LotItem,
    CreateJournalCommand, CreateRateCommand, LedgerEntryCommand, SetRateCommand,
    DataValue, StatementTxn,
    StorageBackend, StorageError, TransactionId,
    escape_like,
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
            CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY
            );

            INSERT INTO entities (id) VALUES ('default')
                ON CONFLICT (id) DO NOTHING;

            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT NOT NULL,
                account_type TEXT NOT NULL,
                unit_rate_id TEXT,
                entity_id TEXT NOT NULL DEFAULT 'default',
                PRIMARY KEY (entity_id, id)
            );

            CREATE TABLE IF NOT EXISTS rates (
                id TEXT NOT NULL,
                date TEXT NOT NULL,
                value TEXT NOT NULL,
                entity_id TEXT NOT NULL DEFAULT 'default',
                PRIMARY KEY (entity_id, id, date)
            );

            CREATE TABLE IF NOT EXISTS journals (
                id TEXT NOT NULL,
                sequence BIGINT NOT NULL,
                date TEXT NOT NULL,
                description TEXT NOT NULL,
                amount TEXT NOT NULL,
                created_at TEXT NOT NULL,
                entity_id TEXT NOT NULL DEFAULT 'default',
                PRIMARY KEY (entity_id, id),
                UNIQUE (id)
            );

            CREATE TABLE IF NOT EXISTS journal_dimensions (
                journal_id TEXT NOT NULL REFERENCES journals(id),
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                id BIGSERIAL PRIMARY KEY,
                journal_id TEXT NOT NULL REFERENCES journals(id),
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                amount TEXT NOT NULL,
                entity_id TEXT NOT NULL DEFAULT 'default'
            );

            CREATE TABLE IF NOT EXISTS ledger_entry_dimensions (
                ledger_entry_id BIGINT NOT NULL REFERENCES ledger_entries(id),
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_pg_ledger_account_date
                ON ledger_entries(entity_id, account_id, date);

            CREATE INDEX IF NOT EXISTS idx_pg_ledger_dim
                ON ledger_entry_dimensions(ledger_entry_id);

            CREATE INDEX IF NOT EXISTS idx_pg_rates_lookup
                ON rates(entity_id, id, date);

            CREATE TABLE IF NOT EXISTS sequence_counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                value BIGINT NOT NULL
            );

            INSERT INTO sequence_counter (id, value) VALUES (1, 0)
                ON CONFLICT (id) DO NOTHING;

            -- Enable ltree extension (for hierarchical dimensions)
            CREATE EXTENSION IF NOT EXISTS ltree;

            CREATE TABLE IF NOT EXISTS lots (
                id BIGSERIAL PRIMARY KEY,
                account_id TEXT NOT NULL,
                date TEXT NOT NULL,
                units_remaining TEXT NOT NULL,
                cost_per_unit TEXT NOT NULL,
                journal_id TEXT NOT NULL,
                entity_id TEXT NOT NULL DEFAULT 'default'
            );

            CREATE TABLE IF NOT EXISTS lot_dimensions (
                lot_id BIGINT NOT NULL REFERENCES lots(id),
                dimension_key TEXT NOT NULL,
                dimension_value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_pg_lot_account ON lots(entity_id, account_id);
            CREATE INDEX IF NOT EXISTS idx_pg_lot_dims ON lot_dimensions(lot_id, dimension_key, dimension_value);
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
    fn create_entity(&self, entity_id: &str) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        client
            .execute(
                "INSERT INTO entities (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
                &[&entity_id],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn list_entities(&self) -> Vec<Arc<str>> {
        let mut client = self.client.lock().unwrap();
        let rows = client
            .query("SELECT id FROM entities ORDER BY id", &[])
            .unwrap_or_default();
        rows.iter()
            .map(|row| {
                let id: String = row.get(0);
                Arc::from(id.as_str())
            })
            .collect()
    }

    fn entity_exists(&self, entity_id: &str) -> bool {
        let mut client = self.client.lock().unwrap();
        let result = client.query_one(
            "SELECT COUNT(*) > 0 FROM entities WHERE id = $1",
            &[&entity_id],
        );
        match result {
            Ok(row) => row.get(0),
            Err(_) => false,
        }
    }

    fn create_account(&self, entity_id: &str, account: &AccountExpression) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        let unit_rate_id_opt = account.unit_rate_id.as_ref().map(|r| r.as_ref());
        let rows = client
            .execute(
                "INSERT INTO accounts (id, account_type, unit_rate_id, entity_id) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (entity_id, id) DO NOTHING",
                &[&account.id.as_ref(), &account_type_to_str(&account.account_type), &unit_rate_id_opt, &entity_id],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        if rows == 0 {
            return Err(StorageError::DuplicateAccount(account.id.to_string()));
        }
        Ok(())
    }

    fn create_rate(&self, _entity_id: &str, _rate: &CreateRateCommand) -> Result<(), StorageError> {
        // Rate creation is a no-op; rates are stored via set_rate
        Ok(())
    }

    fn set_rate(&self, entity_id: &str, command: &SetRateCommand) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        let date_str = date_to_str(command.date);
        let val_str = command.rate.to_string();
        client
            .execute(
                "INSERT INTO rates (id, date, value, entity_id) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (entity_id, id, date) DO UPDATE SET value = $3",
                &[&command.id.as_ref(), &date_str, &val_str, &entity_id],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        Ok(())
    }

    fn get_rate(&self, entity_id: &str, id: &str, date: Date) -> Result<Decimal, StorageError> {
        let mut client = self.client.lock().unwrap();
        let date_str = date_to_str(date);
        let result = client.query_opt(
            "SELECT value FROM rates WHERE entity_id = $1 AND id = $2 AND date <= $3 ORDER BY date DESC LIMIT 1",
            &[&entity_id, &id, &date_str],
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

    fn create_journal(&self, entity_id: &str, command: &CreateJournalCommand) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        let jid = Uuid::new_v4().to_string();
        let seq = Self::next_sequence(&mut client)?;
        let seq_i64 = seq as i64;
        let date_str = date_to_str(command.date);
        let now = OffsetDateTime::now_utc().to_string();
        let amount_str = command.amount.to_string();

        client
            .execute(
                "INSERT INTO journals (id, sequence, date, description, amount, created_at, entity_id)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[
                    &jid,
                    &seq_i64,
                    &date_str,
                    &command.description.as_ref(),
                    &amount_str,
                    &now,
                    &entity_id,
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
                    ..
                } => (account_id, *amount),
                LedgerEntryCommand::Credit {
                    account_id,
                    amount,
                    ..
                } => (account_id, -*amount),
            };

            let row = client
                .query_opt(
                    "SELECT account_type FROM accounts WHERE entity_id = $1 AND id = $2",
                    &[&entity_id, &account_id.as_ref()],
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
                    "INSERT INTO ledger_entries (journal_id, account_id, date, amount, entity_id)
                     VALUES ($1, $2, $3, $4, $5) RETURNING id",
                    &[&jid, &account_id.as_ref(), &date_str, &amount_str, &entity_id],
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

            // Handle lot creation for debits with units
            if let LedgerEntryCommand::Debit { account_id, amount, units: Some(unit_count) } = entry {
                let unit_rate_row = client
                    .query_opt(
                        "SELECT unit_rate_id FROM accounts WHERE entity_id = $1 AND id = $2",
                        &[&entity_id, &account_id.as_ref()],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;

                let unit_rate_id: Option<String> = unit_rate_row.and_then(|row| row.get(0));

                if unit_rate_id.is_some() {
                    let cost_per_unit = if !unit_count.is_zero() {
                        *amount / *unit_count
                    } else {
                        Decimal::ZERO
                    };
                    let units_str = unit_count.to_string();
                    let cpu_str = cost_per_unit.to_string();
                    let lot_row = client
                        .query_one(
                            "INSERT INTO lots (account_id, date, units_remaining, cost_per_unit, journal_id, entity_id)
                             VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                            &[&account_id.as_ref(), &date_str, &units_str, &cpu_str, &jid, &entity_id],
                        )
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let lot_id: i64 = lot_row.get(0);
                    for (k, v) in &command.dimensions {
                        let dim_val = data_value_to_str(v);
                        client
                            .execute(
                                "INSERT INTO lot_dimensions (lot_id, dimension_key, dimension_value)
                                 VALUES ($1, $2, $3)",
                                &[&lot_id, &k.as_ref(), &dim_val],
                            )
                            .map_err(|e| StorageError::Other(e.to_string()))?;
                    }
                }
            }

            // Handle lot depletion for credits with units (FIFO)
            if let LedgerEntryCommand::Credit { account_id, units: Some(unit_count), .. } = entry {
                let unit_rate_row = client
                    .query_opt(
                        "SELECT unit_rate_id FROM accounts WHERE entity_id = $1 AND id = $2",
                        &[&entity_id, &account_id.as_ref()],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;

                let unit_rate_id: Option<String> = unit_rate_row.and_then(|row| row.get(0));

                if unit_rate_id.is_some() {
                    let mut remaining = *unit_count;
                    let lot_rows = client
                        .query(
                            "SELECT id, units_remaining FROM lots
                             WHERE entity_id = $1 AND account_id = $2 AND units_remaining::NUMERIC > 0
                             ORDER BY date ASC, id ASC",
                            &[&entity_id, &account_id.as_ref()],
                        )
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    for lot_row in &lot_rows {
                        if remaining.is_zero() {
                            break;
                        }
                        let lot_id: i64 = lot_row.get(0);
                        let units_str: String = lot_row.get(1);
                        let lot_units = Decimal::from_str(&units_str)
                            .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))?;
                        if lot_units <= remaining {
                            remaining -= lot_units;
                            client
                                .execute(
                                    "UPDATE lots SET units_remaining = '0' WHERE id = $1",
                                    &[&lot_id],
                                )
                                .map_err(|e| StorageError::Other(e.to_string()))?;
                        } else {
                            let new_remaining = (lot_units - remaining).to_string();
                            remaining = Decimal::ZERO;
                            client
                                .execute(
                                    "UPDATE lots SET units_remaining = $1 WHERE id = $2",
                                    &[&new_remaining, &lot_id],
                                )
                                .map_err(|e| StorageError::Other(e.to_string()))?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_balance(
        &self,
        entity_id: &str,
        account_id: &str,
        date: Date,
        dimension: Option<&(Arc<str>, Arc<DataValue>)>,
    ) -> Result<Decimal, StorageError> {
        let mut client = self.client.lock().unwrap();

        // Verify account exists
        let exists = client
            .query_one(
                "SELECT COUNT(*) > 0 FROM accounts WHERE entity_id = $1 AND id = $2",
                &[&entity_id, &account_id],
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
                let escaped_dim_val_str = escape_like(&dim_val_str);
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                         WHERE le.entity_id = $1 AND le.account_id = $2 AND le.date <= $3
                           AND led.dimension_key = $4
                           AND (led.dimension_value = $5 OR led.dimension_value LIKE $6 || '/%' ESCAPE '\\')",
                        &[&entity_id, &account_id, &date_str, &dim_key.as_ref(), &dim_val_str, &escaped_dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
            None => {
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         WHERE le.entity_id = $1 AND le.account_id = $2 AND le.date <= $3",
                        &[&entity_id, &account_id, &date_str],
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
        entity_id: &str,
        account_id: &str,
        from: Bound<Date>,
        to: Bound<Date>,
        dimension: Option<&(Arc<str>, Arc<DataValue>)>,
    ) -> Result<DataValue, StorageError> {
        let mut client = self.client.lock().unwrap();

        // Verify account exists
        let exists = client
            .query_one(
                "SELECT COUNT(*) > 0 FROM accounts WHERE entity_id = $1 AND id = $2",
                &[&entity_id, &account_id],
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;
        let acct_exists: bool = exists.get(0);
        if !acct_exists {
            return Err(StorageError::AccountNotFound(account_id.to_string()));
        }

        let balance_date = match from {
            Bound::Included(d) => d.previous_day().unwrap_or(d),
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
                let escaped_dim_val_str = escape_like(&dim_val_str);
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         JOIN ledger_entry_dimensions led ON led.ledger_entry_id = le.id
                         WHERE le.entity_id = $1 AND le.account_id = $2 AND le.date <= $3
                           AND led.dimension_key = $4
                           AND (led.dimension_value = $5 OR led.dimension_value LIKE $6 || '/%' ESCAPE '\\')",
                        &[&entity_id, &account_id, &balance_date_str, &dim_key.as_ref(), &dim_val_str, &escaped_dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
            None => {
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(le.amount::NUMERIC), 0)::TEXT
                         FROM ledger_entries le
                         WHERE le.entity_id = $1 AND le.account_id = $2 AND le.date <= $3",
                        &[&entity_id, &account_id, &balance_date_str],
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
                 WHERE le.entity_id = $1 AND le.account_id = $2 AND le.date {} $3 AND le.date {} $4
                   AND led.dimension_key = $5
                   AND (led.dimension_value = $6 OR led.dimension_value LIKE $7 || '/%' ESCAPE '\\')
                 ORDER BY le.date, le.id",
                from_op, to_op
            ),
            None => format!(
                "SELECT le.journal_id, le.date, j.description, le.amount
                 FROM ledger_entries le
                 JOIN journals j ON j.id = le.journal_id
                 WHERE le.entity_id = $1 AND le.account_id = $2 AND le.date {} $3 AND le.date {} $4
                 ORDER BY le.date, le.id",
                from_op, to_op
            ),
        };

        let rows = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let escaped_dim_val_str = escape_like(&dim_val_str);
                client
                    .query(
                        &query,
                        &[&entity_id, &account_id, &from_str, &to_str, &dim_key.as_ref(), &dim_val_str, &escaped_dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?
            }
            None => client
                .query(&query, &[&entity_id, &account_id, &from_str, &to_str])
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
        entity_id: &str,
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
                 WHERE le.entity_id = $1 AND le.account_id = $2 AND led.dimension_key = $3
                   AND le.date >= $4 AND le.date <= $5",
                &[
                    &entity_id,
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

    fn list_accounts(&self, entity_id: &str) -> Vec<(Arc<str>, AccountType)> {
        let mut client = self.client.lock().unwrap();
        let rows = client
            .query("SELECT id, account_type FROM accounts WHERE entity_id = $1 ORDER BY id", &[&entity_id])
            .unwrap_or_default();

        rows.iter()
            .map(|row| {
                let id: String = row.get(0);
                let at: String = row.get(1);
                (Arc::from(id.as_str()), str_to_account_type(&at))
            })
            .collect()
    }

    fn list_rates(&self, entity_id: &str) -> Vec<Arc<str>> {
        let mut client = self.client.lock().unwrap();
        let rows = client
            .query("SELECT DISTINCT id FROM rates WHERE entity_id = $1 ORDER BY id", &[&entity_id])
            .unwrap_or_default();

        rows.iter()
            .map(|row| {
                let id: String = row.get(0);
                Arc::from(id.as_str())
            })
            .collect()
    }

    fn begin_transaction(&self) -> Result<TransactionId, StorageError> {
        let mut client = self.client.lock().unwrap();
        client
            .batch_execute("SAVEPOINT dblentry_tx")
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
            .batch_execute("RELEASE SAVEPOINT dblentry_tx")
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
            .batch_execute("ROLLBACK TO SAVEPOINT dblentry_tx")
            .map_err(|e| StorageError::Other(e.to_string()))?;
        *active = None;
        tracing::debug!(tx_id, "PostgreSQL transaction rolled back");
        Ok(())
    }

    fn get_lots(&self, entity_id: &str, account_id: &str, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Vec<LotItem>, StorageError> {
        let mut client = self.client.lock().unwrap();

        let lot_rows: Vec<(i64, String, String, String)> = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let escaped_dim_val_str = escape_like(&dim_val_str);
                let rows = client
                    .query(
                        "SELECT l.id, l.date, l.units_remaining, l.cost_per_unit FROM lots l
                         WHERE l.entity_id = $1 AND l.account_id = $2 AND l.units_remaining::NUMERIC > 0
                           AND EXISTS (
                             SELECT 1 FROM lot_dimensions ld
                             WHERE ld.lot_id = l.id AND ld.dimension_key = $3
                               AND (ld.dimension_value = $4 OR ld.dimension_value LIKE $5 || '/%' ESCAPE '\\')
                           )
                         ORDER BY l.date ASC",
                        &[&entity_id, &account_id, &dim_key.as_ref(), &dim_val_str, &escaped_dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                rows.iter()
                    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                    .collect()
            }
            None => {
                let rows = client
                    .query(
                        "SELECT l.id, l.date, l.units_remaining, l.cost_per_unit FROM lots l
                         WHERE l.entity_id = $1 AND l.account_id = $2 AND l.units_remaining::NUMERIC > 0
                         ORDER BY l.date ASC",
                        &[&entity_id, &account_id],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                rows.iter()
                    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
                    .collect()
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
            let dim_rows = client
                .query(
                    "SELECT dimension_key, dimension_value FROM lot_dimensions WHERE lot_id = $1",
                    &[&lot_id],
                )
                .map_err(|e| StorageError::Other(e.to_string()))?;
            for dim_row in &dim_rows {
                let k: String = dim_row.get(0);
                let v: String = dim_row.get(1);
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

    fn get_total_units(&self, entity_id: &str, account_id: &str, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<Decimal, StorageError> {
        let mut client = self.client.lock().unwrap();

        let total_str: String = match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let escaped_dim_val_str = escape_like(&dim_val_str);
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(l.units_remaining::NUMERIC), 0)::TEXT FROM lots l
                         WHERE l.entity_id = $1 AND l.account_id = $2 AND l.units_remaining::NUMERIC > 0
                           AND EXISTS (
                             SELECT 1 FROM lot_dimensions ld
                             WHERE ld.lot_id = l.id AND ld.dimension_key = $3
                               AND (ld.dimension_value = $4 OR ld.dimension_value LIKE $5 || '/%' ESCAPE '\\')
                           )",
                        &[&entity_id, &account_id, &dim_key.as_ref(), &dim_val_str, &escaped_dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
            None => {
                let row = client
                    .query_one(
                        "SELECT COALESCE(SUM(l.units_remaining::NUMERIC), 0)::TEXT FROM lots l
                         WHERE l.entity_id = $1 AND l.account_id = $2 AND l.units_remaining::NUMERIC > 0",
                        &[&entity_id, &account_id],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                row.get(0)
            }
        };

        Decimal::from_str(&total_str)
            .map_err(|e| StorageError::Other(format!("Invalid decimal: {}", e)))
    }

    fn deplete_lots(&self, entity_id: &str, account_id: &str, units: Decimal, method: &CostMethod, dimensions: &BTreeMap<Arc<str>, Arc<DataValue>>) -> Result<Decimal, StorageError> {
        let mut client = self.client.lock().unwrap();

        let order = match method {
            CostMethod::Fifo => "ASC",
            CostMethod::Lifo => "DESC",
            CostMethod::Average => "ASC",
        };

        // Build query with optional dimension filtering
        let lot_rows: Vec<(i64, String, String)> = if dimensions.is_empty() {
            let query = format!(
                "SELECT id, units_remaining, cost_per_unit FROM lots
                 WHERE entity_id = $1 AND account_id = $2 AND units_remaining::NUMERIC > 0
                 ORDER BY date {order}, id {order}"
            );
            let rows = client
                .query(&query, &[&entity_id, &account_id])
                .map_err(|e| StorageError::Other(e.to_string()))?;
            rows.iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect()
        } else {
            // Build dimension filter: all dimension key/value pairs must match (prefix)
            let dim_conditions: Vec<String> = dimensions.iter().enumerate().map(|(i, _)| {
                let p1 = 3 + i * 3;
                let p2 = 4 + i * 3;
                let p3 = 5 + i * 3;
                format!(
                    "EXISTS (SELECT 1 FROM lot_dimensions ld{i} WHERE ld{i}.lot_id = lots.id AND ld{i}.dimension_key = ${p1} AND (ld{i}.dimension_value = ${p2} OR ld{i}.dimension_value LIKE ${p3} || '/%' ESCAPE '\\'))"
                )
            }).collect();

            let query = format!(
                "SELECT id, units_remaining, cost_per_unit FROM lots
                 WHERE entity_id = $1 AND account_id = $2 AND units_remaining::NUMERIC > 0
                   AND {}
                 ORDER BY date {order}, id {order}",
                dim_conditions.join(" AND ")
            );

            // Build params: entity_id + account_id + triples of (key, value, escaped_value)
            let mut param_values: Vec<String> = Vec::new();
            param_values.push(entity_id.to_string());
            param_values.push(account_id.to_string());
            for (k, v) in dimensions {
                param_values.push(k.to_string());
                let val = data_value_to_str(v);
                param_values.push(val.clone());
                param_values.push(escape_like(&val));
            }
            let param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
                param_values.iter().map(|s| s as &(dyn postgres::types::ToSql + Sync)).collect();

            let rows = client
                .query(&query, &param_refs)
                .map_err(|e| StorageError::Other(e.to_string()))?;
            rows.iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect()
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
                    client
                        .execute(
                            "UPDATE lots SET units_remaining = '0' WHERE id = $1",
                            &[lot_id],
                        )
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                } else {
                    cost_basis += remaining * avg_cost;
                    let new_remaining = (lot_units - remaining).to_string();
                    remaining = Decimal::ZERO;
                    client
                        .execute(
                            "UPDATE lots SET units_remaining = $1 WHERE id = $2",
                            &[&new_remaining, lot_id],
                        )
                        .map_err(|e| StorageError::Other(e.to_string()))?;
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
                    client
                        .execute(
                            "UPDATE lots SET units_remaining = '0' WHERE id = $1",
                            &[lot_id],
                        )
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                } else {
                    cost_basis += remaining * cpu;
                    let new_remaining = (lot_units - remaining).to_string();
                    remaining = Decimal::ZERO;
                    client
                        .execute(
                            "UPDATE lots SET units_remaining = $1 WHERE id = $2",
                            &[&new_remaining, lot_id],
                        )
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                }
            }

            Ok(cost_basis)
        }
    }

    fn split_lots(&self, entity_id: &str, account_id: &str, new_per_old: Decimal, dimension: Option<&(Arc<str>, Arc<DataValue>)>) -> Result<(), StorageError> {
        let mut client = self.client.lock().unwrap();
        let ratio_str = new_per_old.to_string();

        match dimension {
            Some((dim_key, dim_val)) => {
                let dim_val_str = data_value_to_str(dim_val);
                let escaped_dim_val_str = escape_like(&dim_val_str);
                client
                    .execute(
                        "UPDATE lots SET
                            units_remaining = (units_remaining::NUMERIC * $3::NUMERIC)::TEXT,
                            cost_per_unit = (cost_per_unit::NUMERIC / $3::NUMERIC)::TEXT
                         WHERE entity_id = $1 AND account_id = $2 AND units_remaining::NUMERIC > 0
                           AND EXISTS (
                             SELECT 1 FROM lot_dimensions ld
                             WHERE ld.lot_id = lots.id AND ld.dimension_key = $4
                               AND (ld.dimension_value = $5 OR ld.dimension_value LIKE $6 || '/%' ESCAPE '\\')
                           )",
                        &[&entity_id, &account_id, &ratio_str, &dim_key.as_ref(), &dim_val_str, &escaped_dim_val_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
            }
            None => {
                client
                    .execute(
                        "UPDATE lots SET
                            units_remaining = (units_remaining::NUMERIC * $3::NUMERIC)::TEXT,
                            cost_per_unit = (cost_per_unit::NUMERIC / $3::NUMERIC)::TEXT
                         WHERE entity_id = $1 AND account_id = $2 AND units_remaining::NUMERIC > 0",
                        &[&entity_id, &account_id, &ratio_str],
                    )
                    .map_err(|e| StorageError::Other(e.to_string()))?;
            }
        }

        Ok(())
    }

    fn get_unit_rate_id(&self, entity_id: &str, account_id: &str) -> Option<Arc<str>> {
        let mut client = self.client.lock().unwrap();
        let result = client.query_opt(
            "SELECT unit_rate_id FROM accounts WHERE entity_id = $1 AND id = $2",
            &[&entity_id, &account_id],
        );
        match result {
            Ok(Some(row)) => {
                let val: Option<String> = row.get(0);
                val.map(|s| Arc::from(s.as_str()))
            }
            _ => None,
        }
    }

    fn is_unit_account(&self, entity_id: &str, account_id: &str) -> bool {
        let mut client = self.client.lock().unwrap();
        let result = client.query_opt(
            "SELECT unit_rate_id IS NOT NULL FROM accounts WHERE entity_id = $1 AND id = $2",
            &[&entity_id, &account_id],
        );
        match result {
            Ok(Some(row)) => row.get::<_, bool>(0),
            _ => false,
        }
    }
}

