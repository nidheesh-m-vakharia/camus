//! The broker's current WAL shard. `truncate-to-sealed` swaps in a new one.

use std::sync::{Arc, RwLock};

use camus_wal::Shard;

/// Holds the shard every Kafka request and Raft RPC goes to.
pub struct WalSlot {
    inner: RwLock<Arc<Shard>>,
}

impl WalSlot {
    pub fn new(shard: Shard) -> Self {
        Self {
            inner: RwLock::new(Arc::new(shard)),
        }
    }

    /// The shard serving requests now.
    pub fn get(&self) -> Arc<Shard> {
        self.inner.read().expect("wal slot").clone()
    }

    /// Serve requests from `shard` from now on. Returns the old one.
    pub fn replace(&self, shard: Shard) -> Arc<Shard> {
        std::mem::replace(&mut *self.inner.write().expect("wal slot"), Arc::new(shard))
    }
}
