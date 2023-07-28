use std::{collections::HashMap, sync::{Arc, RwLock}};

use serde_json::Value;

use crate::{evaluator::{ExpressionEvaluationContext, EvaluationError}, models::DataValue};


pub enum Function {
  Scalar(Arc<dyn ScalarFunction>),
}

pub trait ScalarFunction: Send + Sync {
  fn call(&self, context: &ExpressionEvaluationContext, args: Vec<DataValue>) -> Result<DataValue, EvaluationError>;
}

pub struct FunctionRegistry {
  functions: Arc<RwLock<HashMap<String, Arc<Function>>>>,
}

impl FunctionRegistry {
  pub fn new() -> FunctionRegistry {
    let result = FunctionRegistry {
      functions: Arc::new(RwLock::new(HashMap::new())),
    };


    result
  }

  pub fn register_function(&self, name: &str, function: Function) {
    let mut lock = self.functions.write().unwrap();
    lock.insert(name.to_string(), Arc::new(function));
  }

  pub fn get_function(&self, name: &str) -> Option<Arc<Function>> {
    let lock = self.functions.read().unwrap();
    match lock.get(name) {
      Some(f) => Some(f.clone()),
      None => None,
    }
  }
}
