use std::{collections::HashMap, sync::{Arc, RwLock}};

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

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry {
  pub fn new() -> FunctionRegistry {
    


    FunctionRegistry {
      functions: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  pub fn register_function(&self, name: &str, function: Function) {
    let mut lock = self.functions.write().unwrap();
    lock.insert(name.to_string(), Arc::new(function));
  }

  pub fn get_function(&self, name: &str) -> Option<Arc<Function>> {
    let lock = self.functions.read().unwrap();
    lock.get(name).cloned()
  }
}
