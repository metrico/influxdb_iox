//! gRPC service for scheduling compactor tasks.

mod local_scheduler;
pub use local_scheduler::LocalScheduler;
mod scheduler;
pub use scheduler::Scheduler;
mod service;
pub use service::*;
