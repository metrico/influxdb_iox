use std::{fmt::Debug, sync::Arc};

use parking_lot::{Mutex, MutexGuard};

use super::super::partition::PartitionData;

pub(crate) trait PostWriteObserver: Send + Sync + Debug {
    fn observe(&self, partition: Arc<Mutex<PartitionData>>, guard: MutexGuard<'_, PartitionData>);
}
