use std::fmt::{Debug, Display};

use async_trait::async_trait;

use crate::scheduler::Scheduler;

#[derive(Debug, Default)]
pub struct LocalScheduler;

#[async_trait]
impl Scheduler for LocalScheduler {}

impl Display for LocalScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "local_compaction_scheduler")
    }
}
