use std::fmt::Debug;

use async_trait::async_trait;

#[async_trait]
pub trait Scheduler: Send + Sync + Debug {}
