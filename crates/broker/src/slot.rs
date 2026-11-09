//! The broker's WAL shards, by id (ADR 0014). `truncate-to-sealed` swaps in a
//! new shard for one id.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use camus_wal::Shard;

/// Holds the shards Kafka requests and Raft RPC go to.
pub struct WalSlot {
    inner: RwLock<BTreeMap<u32, Arc<Shard>>>,
}

impl WalSlot {
    /// A set holding shard 0, the only shard of a single-shard cluster.
    pub fn new(shard: Shard) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::from([(0, Arc::new(shard))])),
        }
    }

    /// Shard 0. Admin endpoints that predate shard ids report on it.
    pub fn get(&self) -> Arc<Shard> {
        self.shard(0).expect("shard 0 is always open")
    }

    /// Shard `id`, when this broker has it open.
    pub fn shard(&self, id: u32) -> Option<Arc<Shard>> {
        self.inner.read().expect("wal slot").get(&id).cloned()
    }

    /// Every open shard, by id.
    pub fn all(&self) -> Vec<(u32, Arc<Shard>)> {
        self.inner
            .read()
            .expect("wal slot")
            .iter()
            .map(|(id, shard)| (*id, shard.clone()))
            .collect()
    }

    /// Serve shard `id` from `shard` from now on. Returns the old one.
    pub fn replace(&self, id: u32, shard: Shard) -> Option<Arc<Shard>> {
        self.inner
            .write()
            .expect("wal slot")
            .insert(id, Arc::new(shard))
    }
}
