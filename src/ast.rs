use std::{sync::Arc, collections::BTreeMap};

use time::Date;




#[derive(Debug, Clone, PartialEq)]
pub enum Statement{
    Create(CreateCommand),
    Select,
    Accrue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateCommand {
    Account,
    Journal(JournalExpression),
    Rate,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LedgerOperation {
    Debit(LedgerOperationData),
    Credit(LedgerOperationData),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LedgerOperationData {
    pub account: Arc<str>,
    pub amount: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JournalExpression {
    pub date: Expression,
    pub description: Expression,    
    pub amount: Expression,
    pub operations: Vec<LedgerOperation>,
    pub dimensions: BTreeMap<Arc<str>, Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    UnaryExpression(UnaryExpression),
    BinaryExpression(BinaryExpression),
    VariadicExpression(VariadicExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryExpression {
    Not(Box<Expression>),
    Exists(Box<Expression>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    Literal(Literal),
    Property { name: Arc<str>, key: Arc<str> },
    Parameter(Arc<str>),
    Identifier(Arc<str>),
    Alias { source: Box<Expression>, alias: Arc<str> },
}

impl UnaryExpression {
    pub fn literal(value: Literal) -> Expression {
        Expression::UnaryExpression(UnaryExpression::Literal(value))
    }
    
    pub fn parameter(name: Arc<str>) -> Expression {
        Expression::UnaryExpression(UnaryExpression::Parameter(name))
    }
    
    pub fn property(name: Arc<str>, key: Arc<str>) -> Expression {
        Expression::UnaryExpression(UnaryExpression::Property { name, key })
    }

    pub fn alias(source: Expression, alias: Arc<str>) -> Expression {
        Expression::UnaryExpression(Self::Alias { source: Box::new(source), alias })
    }

    pub fn not(cond: Expression) -> Expression {
        Expression::UnaryExpression(Self::Not(Box::new(cond)))
    }

    pub fn ident(ident: Arc<str>) -> Expression {
        Expression::UnaryExpression(Self::Identifier(ident))
    }

    pub fn is_null(expr: Expression) -> Expression {
        Expression::UnaryExpression(Self::IsNull(Box::new(expr)))
    }

    pub fn is_not_null(expr: Expression) -> Expression {
        Expression::UnaryExpression(Self::IsNotNull(Box::new(expr)))
    }
}



#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Date(Date),
    Integer(i64),
    Real(f64),
    Boolean(bool),
    Text(Arc<str>),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryExpression {
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    
    Eq(Box<Expression>, Box<Expression>),
    Ne(Box<Expression>, Box<Expression>),
    Lt(Box<Expression>, Box<Expression>),
    Le(Box<Expression>, Box<Expression>),
    Gt(Box<Expression>, Box<Expression>),
    Ge(Box<Expression>, Box<Expression>),
    In(Box<Expression>, Box<Expression>),
    
    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Exponent(Box<Expression>, Box<Expression>),

}

impl BinaryExpression {
    pub fn and(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::And(Box::new(a), Box::new(b)))
    }

    pub fn or(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Or(Box::new(a), Box::new(b)))
    }

    pub fn eq(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Eq(Box::new(a), Box::new(b)))
    }

    pub fn ne(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Ne(Box::new(a), Box::new(b)))
    }

    pub fn lt(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Lt(Box::new(a), Box::new(b)))
    }

    pub fn le(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Le(Box::new(a), Box::new(b)))
    }

    pub fn gt(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Gt(Box::new(a), Box::new(b)))
    }

    pub fn in_(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::In(Box::new(a), Box::new(b)))
    }

    pub fn ge(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Ge(Box::new(a), Box::new(b)))
    }

    pub fn add(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Add(Box::new(a), Box::new(b)))
    }

    pub fn subtract(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Subtract(Box::new(a), Box::new(b)))
    }

    pub fn multiply(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Multiply(Box::new(a), Box::new(b)))
    }

    pub fn divide(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Divide(Box::new(a), Box::new(b)))
    }

    pub fn modulo(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Modulo(Box::new(a), Box::new(b)))
    }

    pub fn exponent(a: Expression, b: Expression) -> Expression {
        Expression::BinaryExpression(Self::Exponent(Box::new(a), Box::new(b)))
    }

}

#[derive(Debug, Clone, PartialEq)]
pub enum VariadicExpression {
    FunctionExpression(FunctionExpression),
    CaseExpression(CaseExpression),
    ListExpression(ListExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionExpression {
    pub name: Arc<str>, 
    pub args: Vec<Expression>,
    pub position_in_query: usize
}

impl FunctionExpression {
  pub fn function(name: Arc<str>, args: Vec<Expression>, position_in_query: usize) -> Expression {
    Expression::VariadicExpression(VariadicExpression::FunctionExpression(FunctionExpression{ name, args, position_in_query }))
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpression {
    pub match_: Option<Box<Expression>>,
    pub when : Vec<(Expression, Expression)>,
    pub else_: Option<Box<Expression>>
}

impl CaseExpression {
  pub fn case(match_: Option<Expression>, when: Vec<(Expression, Expression)>, else_: Option<Expression>) -> Expression {
    Expression::VariadicExpression(VariadicExpression::CaseExpression(CaseExpression{ 
      match_: match match_ {
        Some(m) => Some(Box::new(m)),
        None => None
      },
      when, 
      else_ : match else_ {
        Some(e) => Some(Box::new(e)),
        None => None
      }
    }))
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListExpression {
    pub elements : Vec<Expression>,
}

impl ListExpression {
  pub fn list(elements: Vec<Expression>) -> Expression {
    Expression::VariadicExpression(VariadicExpression::ListExpression(ListExpression{ elements }))
  }
}
