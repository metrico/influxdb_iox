use std::fmt::{Debug, Display};

use async_trait::async_trait;

#[async_trait]
pub trait Scheduler: Send + Sync + Debug + Display {}
