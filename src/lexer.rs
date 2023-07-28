#![allow(clippy::redundant_closure_call)]

use std::time::UNIX_EPOCH;

use super::ast::*;
use peg::{error::ParseError, str::LineCol};
use time::{Date, Month};
use std::collections::BTreeMap;


peg::parser! {
    grammar fql() for str {
        use std::sync::Arc;
        //use ast::

        rule kw_select()    = ("SELECT" / "select")
        rule kw_get()       = ("GET" / "get")
        rule kw_set()       = ("SET" / "set")
        rule kw_create()    = ("CREATE" / "create")
        rule kw_journal()   = ("JOURNAL" / "journal")
        rule kw_account()   = ("ACCOUNT" / "account")
        rule kw_balance()   = ("BALANCE" / "balance")
        rule kw_rate()      = ("RATE" / "rate")

        rule kw_debit()     = ("DEBIT" / "debit")
        rule kw_credit()    = ("CREDIT" / "credit")

        rule kw_asset()     = ("ASSET" / "asset")
        rule kw_liability() = ("LIABILITY" / "liability")
        rule kw_income()    = ("INCOME" / "income")
        rule kw_expense()   = ("EXPENSE" / "expense")
        rule kw_equity()    = ("EQUITY" / "equity")

        rule kw_for()       = ("FOR" / "for")
        
        rule kw_delete()    = ("DELETE" / "delete")
        rule kw_where()     = ("WHERE" / "where")
        rule kw_return()    = ("RETURN" / "return")
        rule kw_true()      = ("TRUE" / "true")
        rule kw_false()     = ("FALSE" / "false")
        rule kw_null()      = ("NULL" / "null")
        rule kw_and()       = ("AND" / "and")
        rule kw_or()        = ("OR" / "or")
        rule kw_not()       = ("NOT" / "not")
        rule kw_is()        = ("IS" / "is")
        rule kw_id()        = ("ID" / "id")
        rule kw_label()     = ("LABEL" / "label")
        rule kw_as()        = ("AS" / "as")
        rule kw_case()      = ("CASE" / "case")
        rule kw_when()      = ("WHEN" / "when")
        rule kw_then()      = ("THEN" / "then")
        rule kw_else()      = ("ELSE" / "else")
        rule kw_end()       = ("END" / "end")
        rule kw_with()      = ("WITH" / "with")
        rule kw_in()        = ("IN" / "in")
        rule kw_exists()    = ("EXISTS" / "exists")

        rule _()
            = [' ']

        rule __()
            = [' ' | '\n' | '\t']

        rule alpha()
            = ['a'..='z' | 'A'..='Z']

        rule num()
            = ['0'..='9']

        rule alpha_num()
            = ['a'..='z' | 'A'..='Z' | '0'..='9' | '_']


        // e.g. '42', '-1'
        rule integer() -> i64
            = integer:$("-"?num()+) {? integer.parse().or(Err("invalid integer")) }

        // e.g. '-0.53', '34346.245', '236.0'
        rule real() -> f64
            = real:$("-"? num()+ "." num()+) {? real.parse().or(Err("invalid real"))}

        // e.g. 'TRUE', 'FALSE'
        rule boolean() -> bool
            = kw_true() { true } / kw_false() { false }

        // e.g. 'hello world'
        rule text() -> Arc<str>
            = "'" text:$([^ '\'' | '\n' | '\r']*) "'" { Arc::from(text) }

        rule date() -> Date
            = year:$(num()*<4,4>) "-" month:$(num()*<2,2>) "-" day:$(num()*<2,2>) {? 
                
                let year = year.parse::<i32>().or(Err("invalid year")).unwrap();
                let month = month.parse::<u8>().or(Err("invalid month")).unwrap();
                let day = day.parse::<u8>().or(Err("invalid day")).unwrap();

                if month > 12 || day > 31 {
                    return Err("invalid date");
                }
                let month = Month::try_from(month).or(Err("invalid month")).unwrap(); 
                let result = Date::from_calendar_date(year, month, day).unwrap();
                
                Ok(result)
            }

        // e.g. 'TRUE', '42', 'hello world'
        rule literal() -> Literal
            = r:real() { Literal::Real(r) }
            / d:date() { Literal::Date(d) }
            / i:integer() { Literal::Integer(i) }
            / b:boolean() { Literal::Boolean(b) }
            / t:text() { Literal::Text(t) }
            / a:account_id() { Literal::Account(a) }            
            / pr:real() "%" { Literal::Percentage(pr) }            
            / pi:integer() "%" { Literal::Percentage(pi as f64) }            
            / kw_null() { Literal::Null }


        rule ledger_operation() -> LedgerOperation
            = kw_debit() __+ account:account_id() __* amount:expression()? { LedgerOperation::Debit(LedgerOperationData { account, amount }) }
            / kw_credit() __+ account:account_id() __* amount:expression()? { LedgerOperation::Credit(LedgerOperationData { account, amount }) }

        rule ledger_operations() -> Vec<LedgerOperation>
            = ledger_operations:(ledger_operation() ** (__* "|" __*)) { ledger_operations }
            
        rule projection_expression() -> Expression
            = z:expression() _* kw_as() _* a:ident() { UnaryExpression::alias(z, a) }
            / expression()

        rule when_expression() -> (Expression, Expression)
            = kw_when() __+ when:expression() __+ kw_then() __+ then:expression() __+ { (when, then) }
        
        rule else_expression() -> Expression
            = kw_else() __+ else_:expression() __+ { else_ }
        
            #[cache_left_rec]
        pub rule expression() -> Expression
            = precedence!{
                a:(@) __* kw_and() __* b:@ { BinaryExpression::and(a, b) }
                a:(@) __* kw_or() __* b:@ { BinaryExpression::or(a, b) }
                --
                kw_not() _* c:(@) { UnaryExpression::not(c) }
                --
                a:(@) __* "="  __* b:@ { BinaryExpression::eq(a, b) }
                a:(@) __* ("<>" / "!=") __* b:@ { BinaryExpression::ne(a, b) }
                a:(@) __* "<"  __* b:@ { BinaryExpression::lt(a, b) }
                a:(@) __* "<=" __* b:@ { BinaryExpression::le(a, b) }
                a:(@) __* ">"  __* b:@ { BinaryExpression::gt(a, b) }
                a:(@) __* ">=" __* b:@ { BinaryExpression::ge(a, b) }
                a:(@) __* kw_in() __* b:@ { BinaryExpression::in_(a, b) }
                --
                a:(@) __* "+" __* b:@ { BinaryExpression::add(a, b) }
                a:(@) __* "-" __* b:@ { BinaryExpression::subtract(a, b) }
                --
                a:(@) __* "*" __* b:@ { BinaryExpression::multiply(a, b) }
                a:(@) __* "/" __* b:@ { BinaryExpression::divide(a, b) }
                --
                a:(@) __* "%" __* b:@ { BinaryExpression::modulo(a, b) }
                a:(@) __* "^" __* b:@ { BinaryExpression::exponent(a, b) }
                --
                e:(@) __+ kw_is() _+ kw_null() { UnaryExpression::is_null(e) }
                e:(@) __+ kw_is() _+ kw_not() _+ kw_null() { UnaryExpression::is_not_null(e) }
                kw_with() __+ kw_rate() __+ r:ident() { UnaryExpression::rate(r) }
                kw_case() __* mtch:expression()? __* when:when_expression()+ __* else_:else_expression()? __* kw_end() { CaseExpression::case(mtch, when, else_) }
                kw_case() __* when:when_expression()+ __* else_:else_expression()? __* kw_end() { CaseExpression::case(None, when, else_) }
                "$" name:ident() { UnaryExpression::parameter(name) }
                l:literal() { UnaryExpression::literal(l) }
                p:property() { UnaryExpression::property(p.0, p.1) }
                pos: position!() func:ident() _* "(" __* params:expression() ** (_* "," _*) __* ")" { FunctionExpression::function(func, params, pos ) }
                dim:dimension() { UnaryExpression::dimension(dim.0, dim.1) }
                i:ident() { UnaryExpression::ident(i) }                
                --
                
                "(" __* c:expression() __* ")" { c }
                "[" __* c:expression() ** (_* "," _*) __* "]" { ListExpression::list(c) }
            }

        rule ident() -> Arc<str>
            = ident:$(alpha()alpha_num()*) { Arc::from(ident) }

        rule account_id() -> Arc<str>
            = "@" ident:$(alpha()alpha_num()*) { Arc::from(ident) }

        rule property() -> (Arc<str>, Arc<str>)
            = name:ident() "." key:ident() { (name, key) }

        rule dimensions() -> BTreeMap<Arc<str>, Expression>
            = x:dimension() ** (__* "," __*) { x.into_iter().collect() }
        
        rule dimension() -> (Arc<str>, Expression)
            = x:(name:ident() __* "=" __* value:expression() __* { (name, value) })

        rule journal() -> JournalExpression
            = kw_journal() __* date:expression() __* "," __* amount:expression() __* "," __* description:expression() __* dims:(kw_for() __+ dims:dimensions() {dims})? __* ops:ledger_operations() { JournalExpression {
                    date,
                    amount,
                    description,
                    operations: ops,
                    dimensions: dims.unwrap_or_default(),
                } 
            }

        rule account_type() -> AccountType
            = kw_asset() { AccountType::Asset }
            / kw_liability() { AccountType::Liability }
            / kw_income() { AccountType::Income }
            / kw_expense() { AccountType::Expense }
            / kw_equity() { AccountType::Equity }
        
        rule account() -> AccountExpression
            = kw_account() __* id:account_id() __+ account_type:account_type()  { 
                AccountExpression { 
                    id, 
                    account_type,
                } 
            }

        rule rate() -> CreateRateExpression
            = kw_rate() __* id:ident() { 
                CreateRateExpression { 
                    id,
                } 
            }

        rule set_command() -> SetCommand
            = kw_set() __+ kw_rate() __+ id:ident() __+ rate:expression() __+ date:expression() { SetCommand::Rate(SetRateExpression { 
                id, 
                date, 
                rate
            })}

        rule create_command() -> CreateCommand
            = kw_create() __* journal:journal()  { CreateCommand::Journal(journal) }
            / kw_create() __* account:account()  { CreateCommand::Account(account) }
            / kw_create() __* rate:rate()  { CreateCommand::Rate(rate) }
        
        pub rule statement() -> Statement
            = c:create_command() { Statement::Create(c) }
            / kw_get() __+ e:projection_expression() ** (__* "," __*) { Statement::Get(GetExpression::get(e)) }
            / s:set_command() { Statement::Set(s) }

        pub rule statements() -> Vec<Statement>
            = s:statement() ** (__* ";" __*) __* ";"? { s }

    }
}

pub fn parse(input: &str) -> Result<Vec<Statement>, ParseError<LineCol>> {
    fql::statements(input)
}

