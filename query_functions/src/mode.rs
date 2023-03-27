use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::physical_expr::{AggregateExpr, PhysicalExpr};
use datafusion::physical_plan::Accumulator;
use std::collections::HashMap;

/// Expression for a MODE aggregation.
#[derive(Debug)]
pub struct Mode {
    /// Column name
    name: String,
    /// The DataType used to hold the state for each input
    state_data_type: DataType,
    /// The input arguments
    expr: Arc<dyn PhysicalExpr>,
}

impl Mode {
    /// Create a new MODE aggregate function.
    pub fn new(input_data_type: DataType, expr: Arc<dyn PhysicalExpr>, name: String) -> Self {
        Self {
            name,
            state_data_type: input_data_type,
            expr,
        }
    }
}

impl AggregateExpr for Mode {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::UInt64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ModeAccumulator {
            frequency: HashMap::default(),
            state_data_type: self.state_data_type.clone(),
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "mode"),
            DataType::List(Box::new(Field::new(
                "item",
                self.state_data_type.clone(),
                true,
            ))),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    frequency: HashMap<ScalarValue, u64>,
    state_data_type: DataType,
}

impl Accumulator for ModeAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let (vals, counts): (Vec<_>, Vec<_>) = self
            .frequency
            .iter()
            .map(|(v, c)| (v.clone(), ScalarValue::from(*c)))
            .unzip();
        Ok(vec![
            ScalarValue::new_list(Some(vals), self.state_data_type.clone()),
            ScalarValue::new_list(Some(counts), DataType::UInt64),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        todo!()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        todo!()
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self
            .frequency
            .iter()
            .max_by(|(_, x), (_, y)| x.cmp(y))
            .map(|(k, _)| k.clone())
        {
            Some(v) => Ok(v),
            None => ScalarValue::try_from(&self.state_data_type),
        }
    }

    fn size(&self) -> usize {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
