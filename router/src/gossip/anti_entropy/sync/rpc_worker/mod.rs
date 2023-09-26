//! Anti-entropy sync worker types and tasks.

pub mod grpc_connector;
mod worker_task;
pub(crate) use worker_task::*;
pub(crate) mod task_set;
