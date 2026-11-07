//! Transactions. The metadata leader is the transaction coordinator.
//!
//! A transactional batch stays invisible to `read_committed` until `EndTxn`
//! records its offset range as committed. Abort and fencing record the range
//! as aborted. There is no control-batch marker; the ranges are the record.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalog::{self, Outcome, Partition, Span};
use crate::groups::{self, OffsetCommit};
use crate::producers::{self, Issued};

const FENCED: i16 = 90;
const INVALID_TXN: i16 = 48;

/// One produced range that belongs to a transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxnSpan {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: i32,
    /// Inclusive.
    pub start: i64,
    /// Exclusive.
    pub end: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Txn {
    producer_id: i64,
    epoch: i16,
    /// `empty`, `ongoing`, `committed`, or `aborted`.
    state: String,
    partitions: Vec<(String, i32)>,
    spans: Vec<TxnSpan>,
    group: Option<String>,
    offsets: Vec<OffsetCommit>,
    /// From `InitProducerId`. `0` means the default of 60 seconds.
    #[serde(default = "default_timeout")]
    timeout_ms: i32,
    /// Wall time the transaction became `ongoing`, or `-1` when it is not.
    #[serde(default = "default_start")]
    started_ms: i64,
}

fn default_timeout() -> i32 {
    60_000
}

fn default_start() -> i64 {
    -1
}

/// A stored transaction, in the names `DescribeTransactions` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnInfo {
    /// Transactional id.
    pub transactional_id: String,
    /// `Empty`, `Ongoing`, `CompleteCommit`, or `CompleteAbort`.
    pub state: String,
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub epoch: i16,
    /// Timeout from `InitProducerId`.
    pub timeout_ms: i32,
    /// Start time while ongoing, otherwise `-1`.
    pub started_ms: i64,
    /// Partitions in the current transaction. Empty unless it is ongoing.
    pub partitions: Vec<(String, i32)>,
}

type Get<'a> = &'a dyn Fn(&str) -> Result<Option<String>, String>;

fn key(id: &str) -> String {
    format!("txn/{id}")
}

fn load(get: Get<'_>, id: &str) -> Result<Option<Txn>, String> {
    match get(&key(id))? {
        Some(raw) => Ok(Some(
            serde_json::from_str(&raw).map_err(|err| err.to_string())?,
        )),
        None => Ok(None),
    }
}

fn store_txn(put: &mut Vec<(String, String)>, id: &str, txn: &Txn) -> Result<(), String> {
    put.push((
        key(id),
        serde_json::to_string(txn).map_err(|err| err.to_string())?,
    ));
    put.push((
        format!("txn-by-producer/{}", txn.producer_id),
        id.to_string(),
    ));
    Ok(())
}

fn topic_of(get: Get<'_>, name: &str) -> Result<Option<catalog::Topic>, String> {
    match get(&catalog::topic_storage_key(name))? {
        Some(raw) => Ok(Some(catalog::parse_topic(&raw)?)),
        None => Ok(None),
    }
}

fn partition_row(
    get: Get<'_>,
    topic: &str,
    partition: i32,
) -> Result<Option<(String, Partition)>, String> {
    let Some(topic) = topic_of(get, topic)? else {
        return Ok(None);
    };
    let key = catalog::partition_storage_key(&topic.id, partition);
    let Some(raw) = get(&key)? else {
        return Ok(None);
    };
    Ok(Some((key, catalog::parse_partition(&raw)?)))
}

fn mark(
    get: Get<'_>,
    put: &mut Vec<(String, String)>,
    spans: &[TxnSpan],
    committed: bool,
) -> Result<(), String> {
    let mut dirty: BTreeMap<String, Partition> = BTreeMap::new();
    for span in spans {
        let Some((key, loaded)) = partition_row(get, &span.topic, span.partition)? else {
            continue;
        };
        let partition = dirty.entry(key).or_insert(loaded);
        let range = Span {
            start: span.start,
            end: span.end,
        };
        if committed {
            partition.txn_committed.push(range);
        } else {
            partition.txn_aborted.push(range);
        }
    }
    for (key, partition) in dirty {
        put.push((
            key,
            serde_json::to_string(&partition).map_err(|err| err.to_string())?,
        ));
    }
    Ok(())
}

fn applied(put: Vec<(String, String)>, value: String) -> Issued {
    Issued::Applied { put, value }
}

/// Allocate or bump the epoch for a transactional id. An ongoing transaction
/// is aborted, which fences the previous producer.
pub fn init(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    timeout_ms: i32,
) -> Result<Issued, String> {
    if let Some(mut txn) = load(get, transactional_id)? {
        if producer_id >= 0 && (producer_id != txn.producer_id || producer_epoch != txn.epoch) {
            return Ok(Issued::Reject(FENCED));
        }
        let Issued::Applied { mut put, value } =
            producers::init_producer(get, false, txn.producer_id, txn.epoch)?
        else {
            return Ok(Issued::Reject(FENCED));
        };
        if txn.state == "ongoing" {
            mark(get, &mut put, &txn.spans, false)?;
        }
        let Some((_, epoch)) = value.split_once(',') else {
            return Ok(Issued::Reject(INVALID_TXN));
        };
        txn.epoch = epoch
            .parse()
            .map_err(|err: std::num::ParseIntError| err.to_string())?;
        txn.state = "empty".into();
        txn.partitions.clear();
        txn.spans.clear();
        txn.offsets.clear();
        txn.group = None;
        txn.started_ms = -1;
        txn.timeout_ms = timeout_ms;
        store_txn(&mut put, transactional_id, &txn)?;
        return Ok(applied(put, value));
    }
    let Issued::Applied { mut put, value } = producers::init_producer(get, false, -1, -1)? else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    let Some((id, epoch)) = value.split_once(',') else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    let txn = Txn {
        producer_id: id
            .parse()
            .map_err(|err: std::num::ParseIntError| err.to_string())?,
        epoch: epoch
            .parse()
            .map_err(|err: std::num::ParseIntError| err.to_string())?,
        state: "empty".into(),
        partitions: Vec::new(),
        spans: Vec::new(),
        group: None,
        offsets: Vec::new(),
        timeout_ms,
        started_ms: -1,
    };
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, value))
}

/// Add partitions. A new transaction starts when the previous one has ended.
pub fn add_partitions(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    partitions: &[(String, i32)],
    started_ms: i64,
) -> Result<Issued, String> {
    let Some(mut txn) = load(get, transactional_id)? else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    if txn.producer_id != producer_id || txn.epoch != epoch {
        return Ok(Issued::Reject(FENCED));
    }
    if txn.state != "ongoing" {
        txn.state = "ongoing".into();
        txn.partitions.clear();
        txn.spans.clear();
        txn.offsets.clear();
        txn.group = None;
        txn.started_ms = started_ms;
    }
    for partition in partitions {
        if partition_row(get, &partition.0, partition.1)?.is_none() {
            return Ok(Issued::Reject(3));
        }
        if !txn.partitions.contains(partition) {
            txn.partitions.push(partition.clone());
        }
    }
    let mut put = Vec::new();
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, String::new()))
}

/// Remember a produced range. Called before the produce is acknowledged.
pub fn note(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    span: TxnSpan,
) -> Result<Issued, String> {
    let Some(mut txn) = load(get, transactional_id)? else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    if txn.producer_id != producer_id || txn.epoch != epoch || txn.state != "ongoing" {
        // The batch is already in the log. Abort it so it cannot stay open and
        // hold the last stable offset, and tell the producer the write is not
        // part of a transaction. The value is the Kafka error; the puts still
        // apply.
        let mut put = Vec::new();
        mark(get, &mut put, &[span], false)?;
        return Ok(applied(put, INVALID_TXN.to_string()));
    }
    if !txn
        .partitions
        .iter()
        .any(|item| item.0 == span.topic && item.1 == span.partition)
    {
        return Ok(Issued::Reject(INVALID_TXN));
    }
    if txn.spans.iter().any(|item| {
        item.topic == span.topic && item.partition == span.partition && item.start == span.start
    }) {
        return Ok(applied(Vec::new(), String::new()));
    }
    txn.spans.push(span);
    let mut put = Vec::new();
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, String::new()))
}

/// Stage group offsets. They are written only if the transaction commits.
pub fn stage_offsets(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    group: &str,
    offsets: Vec<OffsetCommit>,
) -> Result<Issued, String> {
    let Some(mut txn) = load(get, transactional_id)? else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    if txn.producer_id != producer_id || txn.epoch != epoch {
        return Ok(Issued::Reject(FENCED));
    }
    if txn.state != "ongoing" {
        return Ok(Issued::Reject(INVALID_TXN));
    }
    txn.group = Some(group.to_string());
    txn.offsets = offsets;
    let mut put = Vec::new();
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, String::new()))
}

/// `Ok(())` when this producer has an ongoing transaction.
pub fn ensure(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
) -> Result<(), i16> {
    let Ok(Some(txn)) = load(get, transactional_id) else {
        return Err(INVALID_TXN);
    };
    if txn.producer_id != producer_id || txn.epoch != epoch {
        return Err(FENCED);
    }
    if txn.state != "ongoing" {
        return Err(INVALID_TXN);
    }
    Ok(())
}

/// `Ok(())` when this producer may write the partition transactionally.
pub fn check(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
) -> Result<(), i16> {
    let Ok(Some(txn)) = load(get, transactional_id) else {
        return Err(INVALID_TXN);
    };
    if txn.producer_id != producer_id || txn.epoch != epoch {
        return Err(FENCED);
    }
    if txn.state != "ongoing"
        || !txn
            .partitions
            .iter()
            .any(|item| item.0 == topic && item.1 == partition)
    {
        return Err(INVALID_TXN);
    }
    Ok(())
}

/// Commit or abort. Commit writes the staged offsets and publishes the ranges.
pub fn end(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    committed: bool,
) -> Result<Issued, String> {
    let Some(mut txn) = load(get, transactional_id)? else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    if txn.producer_id != producer_id || txn.epoch != epoch {
        return Ok(Issued::Reject(FENCED));
    }
    if txn.state != "ongoing" {
        return Ok(Issued::Reject(INVALID_TXN));
    }
    let mut put = Vec::new();
    if committed {
        mark(get, &mut put, &txn.spans, true)?;
        if let Some(group) = txn.group.clone() {
            for offset in &txn.offsets {
                put.push((
                    groups::offset_key(&group, &offset.topic_id, offset.partition),
                    serde_json::to_string(&offset.record).map_err(|err| err.to_string())?,
                ));
            }
        }
        txn.state = "committed".into();
    } else {
        mark(get, &mut put, &txn.spans, false)?;
        txn.state = "aborted".into();
    }
    txn.spans.clear();
    txn.offsets.clear();
    txn.group = None;
    txn.started_ms = -1;
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, String::new()))
}

/// Read one stored transaction. `raw` is the JSON at `txn/{id}`.
pub fn inspect(transactional_id: &str, raw: &str) -> Result<TxnInfo, String> {
    let txn: Txn = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    let ongoing = txn.state == "ongoing";
    Ok(TxnInfo {
        transactional_id: transactional_id.to_string(),
        state: kafka_state(&txn.state).to_string(),
        producer_id: txn.producer_id,
        epoch: txn.epoch,
        timeout_ms: if txn.timeout_ms > 0 {
            txn.timeout_ms
        } else {
            default_timeout()
        },
        started_ms: if ongoing { txn.started_ms } else { -1 },
        partitions: if ongoing { txn.partitions } else { Vec::new() },
    })
}

/// Kafka's name for a stored state. Unknown stored values stay empty.
pub fn kafka_state(state: &str) -> &'static str {
    match state {
        "empty" => "Empty",
        "ongoing" => "Ongoing",
        "committed" => "CompleteCommit",
        "aborted" => "CompleteAbort",
        _ => "",
    }
}

/// Names `ListTransactions` accepts as a state filter.
pub fn known_state(state: &str) -> bool {
    matches!(
        state,
        "Empty"
            | "Ongoing"
            | "PrepareCommit"
            | "PrepareAbort"
            | "CompleteCommit"
            | "CompleteAbort"
            | "Dead"
            | "PrepareEpochFence"
    )
}

/// Map an [`Issued`] result onto a catalog outcome.
pub fn outcome(issued: Result<Issued, String>) -> Result<Outcome, String> {
    match issued? {
        Issued::Applied { put, value } => Ok(Outcome::Change {
            put,
            del: Vec::new(),
            value: Some(value),
        }),
        Issued::Reject(code) => Ok(Outcome::Reject(code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groups::OffsetRecord;
    use std::collections::HashMap;

    fn memory() -> HashMap<String, String> {
        let mut store = HashMap::new();
        let put = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let Outcome::Change { put, .. } =
                catalog::create_topic(&get, "orders", "topic-1", 1, "wal").unwrap()
            else {
                panic!("topic");
            };
            put
        };
        for (key, value) in put {
            store.insert(key, value);
        }
        store
    }

    fn apply(store: &mut HashMap<String, String>, issued: Issued) {
        let Issued::Applied { put, .. } = issued else {
            panic!("rejected");
        };
        for (key, value) in put {
            store.insert(key, value);
        }
    }

    fn call(
        store: &HashMap<String, String>,
        body: impl FnOnce(&dyn Fn(&str) -> Result<Option<String>, String>) -> Result<Issued, String>,
    ) -> Issued {
        let get = |key: &str| Ok(store.get(key).cloned());
        body(&get).expect("txn command")
    }

    #[test]
    fn commit_publishes_the_range_and_the_offset() {
        let mut store = memory();
        let (issued, id, epoch) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", -1, -1, 60_000).unwrap();
            let text = match &issued {
                Issued::Applied { value, .. } => value.clone(),
                Issued::Reject(code) => panic!("init {code}"),
            };
            let (id, epoch) = text.split_once(',').unwrap();
            (
                issued,
                id.parse::<i64>().unwrap(),
                epoch.parse::<i16>().unwrap(),
            )
        };
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            add_partitions(get, "checkout", id, epoch, &[("orders".into(), 0)], 1_000)
        });
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            note(
                get,
                "checkout",
                id,
                epoch,
                TxnSpan {
                    topic: "orders".into(),
                    partition: 0,
                    start: 0,
                    end: 2,
                },
            )
        });
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            stage_offsets(
                get,
                "checkout",
                id,
                epoch,
                "readers",
                vec![OffsetCommit {
                    topic_id: "topic-1".into(),
                    partition: 0,
                    record: OffsetRecord {
                        offset: 2,
                        leader_epoch: 0,
                        metadata: None,
                        commit_timestamp: 0,
                    },
                }],
            )
        });
        apply(&mut store, issued);
        let issued = call(&store, |get| end(get, "checkout", id, epoch, true));
        apply(&mut store, issued);
        let get = |key: &str| Ok(store.get(key).cloned());
        let (_, partition) = partition_row(&get, "orders", 0).unwrap().unwrap();
        assert_eq!(partition.txn_committed, vec![Span { start: 0, end: 2 }]);
        assert!(store.contains_key(&groups::offset_key("readers", "topic-1", 0)));
    }

    #[test]
    fn a_second_init_aborts_the_open_range() {
        let mut store = memory();
        let (issued, id, epoch) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", -1, -1, 60_000).unwrap();
            let text = match &issued {
                Issued::Applied { value, .. } => value.clone(),
                Issued::Reject(code) => panic!("init {code}"),
            };
            let (id, epoch) = text.split_once(',').unwrap();
            (
                issued,
                id.parse::<i64>().unwrap(),
                epoch.parse::<i16>().unwrap(),
            )
        };
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            add_partitions(get, "checkout", id, epoch, &[("orders".into(), 0)], 1_000)
        });
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            note(
                get,
                "checkout",
                id,
                epoch,
                TxnSpan {
                    topic: "orders".into(),
                    partition: 0,
                    start: 0,
                    end: 1,
                },
            )
        });
        apply(&mut store, issued);
        let (issued, bumped) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", id, epoch, 60_000).unwrap();
            let bumped = match &issued {
                Issued::Applied { value, .. } => value.clone(),
                Issued::Reject(code) => panic!("fence {code}"),
            };
            (issued, bumped)
        };
        assert_ne!(bumped, format!("{id},{epoch}"));
        apply(&mut store, issued);
        let get = |key: &str| Ok(store.get(key).cloned());
        let (_, partition) = partition_row(&get, "orders", 0).unwrap().unwrap();
        assert_eq!(partition.txn_aborted, vec![Span { start: 0, end: 1 }]);
    }

    #[test]
    fn a_note_after_commit_aborts_the_batch_and_rejects_the_produce() {
        let mut store = memory();
        let (issued, id, epoch) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", -1, -1, 60_000).unwrap();
            let text = match &issued {
                Issued::Applied { value, .. } => value.clone(),
                Issued::Reject(code) => panic!("init {code}"),
            };
            let (id, epoch) = text.split_once(',').unwrap();
            (
                issued,
                id.parse::<i64>().unwrap(),
                epoch.parse::<i16>().unwrap(),
            )
        };
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            add_partitions(get, "checkout", id, epoch, &[("orders".into(), 0)], 1_000)
        });
        apply(&mut store, issued);
        let issued = call(&store, |get| end(get, "checkout", id, epoch, true));
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            note(
                get,
                "checkout",
                id,
                epoch,
                TxnSpan {
                    topic: "orders".into(),
                    partition: 0,
                    start: 4,
                    end: 5,
                },
            )
        });
        match &issued {
            Issued::Applied { value, .. } => assert_eq!(value, "48"),
            Issued::Reject(code) => panic!("note dropped the abort: {code}"),
        }
        apply(&mut store, issued);
        let get = |key: &str| Ok(store.get(key).cloned());
        let (_, partition) = partition_row(&get, "orders", 0).unwrap().unwrap();
        assert_eq!(partition.txn_aborted, vec![Span { start: 4, end: 5 }]);
    }
}
