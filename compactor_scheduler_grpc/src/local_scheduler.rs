use std::fmt::Debug;

use async_trait::async_trait;

use crate::scheduler::Scheduler;

#[derive(Debug, Default)]
pub struct LocalScheduler;

#[async_trait]
impl Scheduler for LocalScheduler {}
