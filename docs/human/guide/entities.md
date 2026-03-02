# Entities

Entities provide **multi-tenancy** — each entity is a completely isolated set of books with its own accounts, journals, and rates.

## Creating Entities

```sql
CREATE ENTITY 'Acme Corp';
CREATE ENTITY 'Globex Inc';
```

Entity names are single-quoted strings. They can contain spaces, numbers, and special characters.

## Switching Entities

Use `USE ENTITY` to switch the active entity. All subsequent statements operate on that entity:

```sql
USE ENTITY 'Acme Corp';
CREATE ACCOUNT @bank ASSET;
CREATE JOURNAL 2024-01-01, 10000, 'Initial capital'
  DEBIT @bank,
  CREDIT @equity;

USE ENTITY 'Globex Inc';
CREATE ACCOUNT @bank ASSET;  -- Different @bank, separate entity
```

## Default Entity

If you never use `CREATE ENTITY` or `USE ENTITY`, all operations happen in the `default` entity. This ensures backward compatibility — existing scripts work unchanged.

## Isolation Guarantees

- Accounts in one entity are **invisible** to other entities
- Journals in one entity **cannot reference** accounts in another entity
- Rates are **scoped per entity**
- Trial balances and statements only show data from the active entity
- Transactions are **scoped per entity**

## Use Cases

| Pattern | Example |
|---------|---------|
| Multi-company | Separate books for each legal entity |
| Departmental | Isolated P&L per business unit |
| Sandbox / Testing | Create a throwaway entity for experimentation |
| Multi-fund | Each investment fund as its own entity |
| Guided Tours | Each tour walkthrough in an isolated entity |
