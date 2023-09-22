use async_trait::async_trait;
use merkle_search_tree::digest::RootHash;
use parking_lot::Mutex;

use super::consistency_prober::ConsistencyProber;

#[derive(Debug, Default)]
pub struct MockConsistencyProber {
    calls: Mutex<Vec<(RootHash, u16)>>,
}

impl MockConsistencyProber {
    pub fn calls(&self) -> Vec<(RootHash, u16)> {
        self.calls.lock().clone()
    }
}

#[async_trait]
impl ConsistencyProber for MockConsistencyProber {
    async fn probe(&self, local_root: RootHash, rpc_bind: u16) {
        self.calls.lock().push((local_root, rpc_bind));
    }
}
