# Grammar

The formal grammar for FQL, expressed in EBNF notation. This is derived from the PEG parser in `src/lexer.rs`.

## Top Level

```ebnf
script        = statement (";" statement)* ";"?

statement     = create_command
              | get_expression
              | set_command
              | accrue_command
              | use_entity
              | "BEGIN"
              | "COMMIT"
              | "ROLLBACK"
```

## Commands

```ebnf
create_command = "CREATE" ( entity | account | journal | rate )

entity         = "ENTITY" text
account        = "ACCOUNT" account_id account_type
journal        = "JOURNAL" expression "," expression "," expression
                 ["FOR" dimension ("," dimension)*]
                 ledger_op ("," ledger_op)*
rate           = "RATE" identifier

set_command    = "SET" "RATE" identifier expression expression

get_expression = "GET" alias_expr ("," alias_expr)*
alias_expr     = expression "AS" identifier

use_entity     = "USE" "ENTITY" text

accrue_command = "ACCRUE" account_id
                 "FROM" expression "TO" expression
                 "WITH" "RATE" identifier
                 [compound_method]
                 "BY" identifier
                 "INTO" "JOURNAL" expression "," expression
                 ledger_op ("," ledger_op)*

compound_method = "COMPOUND" ("DAILY" | "CONTINUOUS")
```

## Expressions

```ebnf
expression     = or_expr

or_expr        = and_expr ("OR" and_expr)*
and_expr       = not_expr ("AND" not_expr)*
not_expr       = "NOT" not_expr | comparison
comparison     = addition (comp_op addition)?
               | addition "IS" "NOT"? "NULL"
               | addition "IN" list
addition       = multiplication (("+" | "-") multiplication)*
multiplication = unary (("*" | "/") unary)*
unary          = modexp ("%" modexp)* | modexp ("^" modexp)*
modexp         = atom

atom           = "(" expression ")"
               | function_call
               | case_expr
               | list
               | literal
               | account_id
               | parameter
               | identifier ["." identifier]

function_call  = identifier "(" [expression ("," expression)*] ")"

case_expr      = "CASE" [expression]
                 ("WHEN" expression "THEN" expression)+
                 ["ELSE" expression]
                 "END"

list           = "[" expression ("," expression)* "]"
```

## Lexical Elements

```ebnf
ledger_op      = ("DEBIT" | "CREDIT") account_id [expression]

dimension      = identifier "=" expression

account_type   = "ASSET" | "LIABILITY" | "INCOME" | "EXPENSE" | "EQUITY"

comp_op        = "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="

literal        = integer | real | percentage | text | date | boolean | "NULL"

integer        = "-"? [0-9]+
real           = "-"? [0-9]+ "." [0-9]+
percentage     = [0-9]+ ("." [0-9]+)? "%"
text           = "'" ([^'\n] | "''")* "'"
date           = [0-9]{4} "-" [0-9]{2} "-" [0-9]{2}
boolean        = "TRUE" | "FALSE"

account_id     = "@" identifier
parameter      = "$" identifier
identifier     = [a-zA-Z_] [a-zA-Z0-9_]*
```

## Keywords

All keywords are case-insensitive.

```
ACCOUNT  ACCRUE   AND      AS       ASSET    BEGIN
BY       CASE     COMMIT   COMPOUND CONTINUOUS CREATE
CREDIT   DAILY    DEBIT    DELETE   ELSE     END
ENTITY   EQUITY   EXISTS   EXPENSE  FALSE    FOR
FROM     GET      ID       IN       INCOME   INTO
IS       JOURNAL  LABEL    LIABILITY NOT      NULL
OR       RATE     RETURN   ROLLBACK SELECT   SET
THEN     TO       TRUE     USE      WHEN     WHERE
WITH
```
