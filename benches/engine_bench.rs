use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dblentry::evaluator::{ExpressionEvaluator, QueryVariables};
use dblentry::function_registry::{Function, FunctionRegistry};
use dblentry::functions::{Balance, Statement, TrialBalance};
use dblentry::lexer;
use dblentry::statement_executor::{ExecutionContext, StatementExecutor};
use dblentry::storage::{InMemoryStorage, StorageBackend};

fn setup() -> (Arc<dyn StorageBackend>, StatementExecutor) {
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let registry = FunctionRegistry::new();
    registry.register_function(
        "balance",
        Function::Scalar(Arc::new(Balance::new(storage.clone()))),
    );
    registry.register_function(
        "statement",
        Function::Scalar(Arc::new(Statement::new(storage.clone()))),
    );
    registry.register_function(
        "trial_balance",
        Function::Scalar(Arc::new(TrialBalance::new(storage.clone()))),
    );
    let evaluator = Arc::new(ExpressionEvaluator::new(
        Arc::new(registry),
        storage.clone(),
    ));
    let exec = StatementExecutor::new(evaluator, storage.clone());
    (storage, exec)
}

fn seed_data(exec: &StatementExecutor) {
    let stmts = lexer::parse(
        "
        CREATE ACCOUNT @bank ASSET;
        CREATE ACCOUNT @loans ASSET;
        CREATE ACCOUNT @equity EQUITY;
        CREATE ACCOUNT @interest INCOME;

        CREATE RATE prime;
        SET RATE prime 0.05 2023-01-01;

        CREATE JOURNAL 2023-01-01, 100000, 'Seed' CREDIT @equity, DEBIT @bank;
    ",
    )
    .unwrap();

    let eff = time::Date::from_calendar_date(2023, time::Month::January, 1).unwrap();
    let mut ctx = ExecutionContext::new(eff, QueryVariables::new());
    for s in &stmts {
        exec.execute(&mut ctx, s).unwrap();
    }

    // Create 100 loan journals with dimensions
    for i in 0..100 {
        let fql = format!(
            "CREATE JOURNAL 2023-02-01, 1000, 'Loan {}' FOR Customer='C{}' DEBIT @loans, CREDIT @bank",
            i, i
        );
        let stmts = lexer::parse(&fql).unwrap();
        exec.execute(&mut ctx, &stmts[0]).unwrap();
    }
}

fn bench_parse(c: &mut Criterion) {
    let script = "GET balance(@bank, 2023-12-31) AS result";
    c.bench_function("parse_simple_get", |b| {
        b.iter(|| lexer::parse(black_box(script)).unwrap())
    });

    let script = "
        CREATE JOURNAL 2023-01-01, 1000, 'Test'
        FOR Customer='John', Region='US'
        CREDIT @equity, DEBIT @bank;
        GET balance(@bank, 2023-12-31) AS result,
            trial_balance(2023-12-31) AS tb
    ";
    c.bench_function("parse_multi_statement", |b| {
        b.iter(|| lexer::parse(black_box(script)).unwrap())
    });
}

fn bench_balance_query(c: &mut Criterion) {
    let (_storage, exec) = setup();
    seed_data(&exec);

    let stmts = lexer::parse("GET balance(@bank, 2023-12-31) AS result").unwrap();
    let eff = time::Date::from_calendar_date(2023, time::Month::December, 31).unwrap();

    c.bench_function("balance_query", |b| {
        b.iter(|| {
            let mut ctx = ExecutionContext::new(eff, QueryVariables::new());
            exec.execute(&mut ctx, black_box(&stmts[0])).unwrap()
        })
    });
}

fn bench_trial_balance(c: &mut Criterion) {
    let (_storage, exec) = setup();
    seed_data(&exec);

    let stmts = lexer::parse("GET trial_balance(2023-12-31) AS result").unwrap();
    let eff = time::Date::from_calendar_date(2023, time::Month::December, 31).unwrap();

    c.bench_function("trial_balance", |b| {
        b.iter(|| {
            let mut ctx = ExecutionContext::new(eff, QueryVariables::new());
            exec.execute(&mut ctx, black_box(&stmts[0])).unwrap()
        })
    });
}

fn bench_journal_creation(c: &mut Criterion) {
    let (_storage, exec) = setup();

    // Setup accounts first
    let setup_stmts = lexer::parse(
        "CREATE ACCOUNT @bank ASSET; CREATE ACCOUNT @equity EQUITY",
    )
    .unwrap();
    let eff = time::Date::from_calendar_date(2023, time::Month::January, 1).unwrap();
    let mut ctx = ExecutionContext::new(eff, QueryVariables::new());
    for s in &setup_stmts {
        exec.execute(&mut ctx, s).unwrap();
    }

    let stmts = lexer::parse(
        "CREATE JOURNAL 2023-01-01, 1000, 'Bench' CREDIT @equity, DEBIT @bank",
    )
    .unwrap();

    c.bench_function("journal_creation", |b| {
        b.iter(|| {
            let mut ctx = ExecutionContext::new(eff, QueryVariables::new());
            exec.execute(&mut ctx, black_box(&stmts[0])).unwrap()
        })
    });
}

criterion_group!(
    benches,
    bench_parse,
    bench_balance_query,
    bench_trial_balance,
    bench_journal_creation
);
criterion_main!(benches);
