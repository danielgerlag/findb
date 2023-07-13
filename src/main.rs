pub mod ast;
pub mod lexer;
pub mod evaluator;
pub mod statement_executor;
pub mod models;
pub mod storage;

fn main() {
    let query = 
    "CREATE JOURNAL 
        2020-01-01, 100, 'Test'
    FOR
        Customer='John Doe',
        Region='US'
    DEBIT bank 100
    CREDIT cash 100
    ";
    
    let statements = lexer::parse(query).unwrap();

    println!("{:#?}", statements);
}
