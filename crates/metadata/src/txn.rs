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
    /// Producer that wrote the batches. Absent on spans stored before ADR 0015.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_id: Option<i64>,
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
    /// Wall time of the last command that changed this id, or `-1` for a
    /// row written before Camus recorded it.
    #[serde(default = "default_start")]
    updated_ms: i64,
    /// The epoch before a timeout abort bumped it, or `-1`. A producer still
    /// on that epoch may call `InitProducerId` to recover (KIP-360).
    #[serde(default = "no_epoch")]
    last_epoch: i16,
}

fn default_timeout() -> i32 {
    60_000
}

fn default_start() -> i64 {
    -1
}

fn no_epoch() -> i16 {
    -1
}

fn touch(txn: &mut Txn, now_ms: i64) {
    if now_ms > 0 {
        txn.updated_ms = now_ms;
    }
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
            producer_id: span.producer_id,
        };
        if committed {
            partition.txn_committed.push(range);
        } else {
            partition.txn_aborted.push(range);
        }
    }
    for (key, mut partition) in dirty {
        crate::catalog::prune_spans(&mut partition);
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
/// is aborted, which fences the previous producer. `now_ms` is the
/// proposer's clock, or `0` when the command did not carry one.
pub fn init(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    timeout_ms: i32,
    now_ms: i64,
) -> Result<Issued, String> {
    if let Some(mut txn) = load(get, transactional_id)? {
        let current = producer_epoch == txn.epoch
            || (txn.last_epoch >= 0 && producer_epoch == txn.last_epoch);
        if producer_id >= 0 && (producer_id != txn.producer_id || !current) {
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
        txn.last_epoch = -1;
        touch(&mut txn, now_ms);
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
        updated_ms: if now_ms > 0 { now_ms } else { -1 },
        last_epoch: -1,
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
    touch(&mut txn, started_ms);
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
    if txn
        .spans
        .iter()
        .any(|item| item.topic == span.topic && item.partition == span.partition)
    {
        // The first batch on this partition already opened the span. EndTxn
        // writes the end (ADR 0015).
        return Ok(applied(Vec::new(), String::new()));
    }
    let mut span = span;
    if span.producer_id.is_none() {
        span.producer_id = Some(producer_id);
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
    now_ms: i64,
    ends: &[TxnSpan],
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
    // Spans from the WAL (ADR 0019) are the ranges to publish. An empty
    // list is a command from before that, which already stored them.
    let mut ranges = if ends.is_empty() {
        txn.spans.clone()
    } else {
        ends.to_vec()
    };
    for span in &mut ranges {
        if span.producer_id.is_none() {
            span.producer_id = Some(producer_id);
        }
    }
    let mut put = Vec::new();
    if committed {
        mark(get, &mut put, &ranges, true)?;
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
        mark(get, &mut put, &ranges, false)?;
        txn.state = "aborted".into();
    }
    txn.spans.clear();
    txn.offsets.clear();
    txn.group = None;
    txn.started_ms = -1;
    touch(&mut txn, now_ms);
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, String::new()))
}

/// Abort a transaction that has been ongoing for longer than its timeout,
/// as Kafka's coordinator does, and bump the epoch so the producer that
/// started it is fenced. The command names the producer and epoch the
/// proposer saw, so a transaction that ended or restarted since is left
/// alone. `value` is `"1"` when it aborted, empty otherwise.
pub fn abort_timed_out(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    now_ms: i64,
) -> Result<Issued, String> {
    let Some(mut txn) = load(get, transactional_id)? else {
        return Ok(applied(Vec::new(), String::new()));
    };
    let timeout = i64::from(if txn.timeout_ms > 0 {
        txn.timeout_ms
    } else {
        default_timeout()
    });
    if txn.producer_id != producer_id
        || txn.epoch != epoch
        || txn.state != "ongoing"
        || txn.started_ms < 0
        || now_ms.saturating_sub(txn.started_ms) <= timeout
    {
        return Ok(applied(Vec::new(), String::new()));
    }
    let Issued::Applied { mut put, value } =
        producers::init_producer(get, false, txn.producer_id, txn.epoch)?
    else {
        return Ok(applied(Vec::new(), String::new()));
    };
    let Some((_, bumped)) = value.split_once(',') else {
        return Ok(Issued::Reject(INVALID_TXN));
    };
    mark(get, &mut put, &txn.spans, false)?;
    txn.last_epoch = txn.epoch;
    txn.epoch = bumped
        .parse()
        .map_err(|err: std::num::ParseIntError| err.to_string())?;
    txn.state = "aborted".into();
    txn.partitions.clear();
    txn.spans.clear();
    txn.offsets.clear();
    txn.group = None;
    txn.started_ms = -1;
    touch(&mut txn, now_ms);
    store_txn(&mut put, transactional_id, &txn)?;
    Ok(applied(put, "1".into()))
}

/// Remove a transactional id that has not changed for `expiration_ms`, as
/// Kafka's `transactional.id.expiration.ms` does. An ongoing transaction is
/// never removed. A row without an update time gets one now, so it expires
/// one period later.
pub fn expire_id(
    get: Get<'_>,
    transactional_id: &str,
    producer_id: i64,
    epoch: i16,
    now_ms: i64,
    expiration_ms: i64,
) -> Result<Outcome, String> {
    let unchanged = Outcome::Change {
        put: Vec::new(),
        del: Vec::new(),
        value: Some(String::new()),
    };
    let Some(mut txn) = load(get, transactional_id)? else {
        return Ok(unchanged);
    };
    if txn.producer_id != producer_id || txn.epoch != epoch || txn.state == "ongoing" {
        return Ok(unchanged);
    }
    if txn.updated_ms < 0 {
        touch(&mut txn, now_ms);
        let mut put = Vec::new();
        store_txn(&mut put, transactional_id, &txn)?;
        return Ok(Outcome::Change {
            put,
            del: Vec::new(),
            value: Some(String::new()),
        });
    }
    if now_ms.saturating_sub(txn.updated_ms) < expiration_ms {
        return Ok(unchanged);
    }
    Ok(Outcome::Change {
        put: Vec::new(),
        del: vec![
            key(transactional_id),
            format!("txn-by-producer/{}", txn.producer_id),
            format!("producer/{}", txn.producer_id),
        ],
        value: Some("1".into()),
    })
}

/// What [`expire_id`] and [`abort_timed_out`] need to know about a stored
/// transaction, read on the leader to decide which commands to propose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnClock {
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub epoch: i16,
    /// The transaction is ongoing.
    pub ongoing: bool,
    /// When it became ongoing, or `-1`.
    pub started_ms: i64,
    /// Its timeout.
    pub timeout_ms: i32,
    /// Last change, or `-1`.
    pub updated_ms: i64,
}

/// Read the clock fields of the JSON at `txn/{id}`.
pub fn clock(raw: &str) -> Result<TxnClock, String> {
    let txn: Txn = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    Ok(TxnClock {
        producer_id: txn.producer_id,
        epoch: txn.epoch,
        ongoing: txn.state == "ongoing",
        started_ms: txn.started_ms,
        timeout_ms: if txn.timeout_ms > 0 {
            txn.timeout_ms
        } else {
            default_timeout()
        },
        updated_ms: txn.updated_ms,
    })
}

/// An ongoing transaction's producer and the partitions it added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenTxn {
    /// Producer id.
    pub producer_id: i64,
    /// Producer epoch.
    pub epoch: i16,
    /// Partitions added to this transaction.
    pub partitions: Vec<(String, i32)>,
    /// Spans already stored, from a `NoteTxn` or a seal.
    pub spans: Vec<TxnSpan>,
}

/// The ongoing transaction at `txn/{id}`, or `None` when it is not ongoing.
pub fn ongoing(raw: &str) -> Result<Option<OpenTxn>, String> {
    let txn: Txn = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    if txn.state != "ongoing" {
        return Ok(None);
    }
    Ok(Some(OpenTxn {
        producer_id: txn.producer_id,
        epoch: txn.epoch,
        partitions: txn.partitions,
        spans: txn.spans,
    }))
}

/// `Some(true)` when this producer already committed, `Some(false)` when it
/// aborted. `None` when the id is still ongoing or belongs to someone else.
pub fn finished(raw: &str, producer_id: i64, epoch: i16) -> Result<Option<bool>, String> {
    let txn: Txn = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    if txn.producer_id != producer_id || txn.epoch != epoch {
        return Ok(None);
    }
    Ok(match txn.state.as_str() {
        "committed" => Some(true),
        "aborted" => Some(false),
        _ => None,
    })
}

/// Partitions added to the transaction at `txn/{id}`, including after it ends.
pub fn partitions(raw: &str) -> Result<Vec<(String, i32)>, String> {
    let txn: Txn = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    Ok(txn.partitions)
}

/// The ranges of the ongoing transaction at `txn/{id}`, or none when it is
/// not ongoing.
pub fn spans(raw: &str) -> Result<Vec<TxnSpan>, String> {
    let txn: Txn = serde_json::from_str(raw).map_err(|err| err.to_string())?;
    Ok(if txn.state == "ongoing" {
        txn.spans
    } else {
        Vec::new()
    })
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
                catalog::create_topic(&get, "orders", "topic-1", 1, "wal", &[]).unwrap()
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
            let issued = init(&get, "checkout", -1, -1, 60_000, 0).unwrap();
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
                    producer_id: None,
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
        let issued = call(&store, |get| end(get, "checkout", id, epoch, true, 0, &[]));
        apply(&mut store, issued);
        let get = |key: &str| Ok(store.get(key).cloned());
        let (_, partition) = partition_row(&get, "orders", 0).unwrap().unwrap();
        assert_eq!(
            partition.txn_committed,
            vec![Span {
                start: 0,
                end: 2,
                producer_id: Some(id),
            }]
        );
        assert!(store.contains_key(&groups::offset_key("readers", "topic-1", 0)));
    }

    #[test]
    fn a_second_init_aborts_the_open_range() {
        let mut store = memory();
        let (issued, id, epoch) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", -1, -1, 60_000, 0).unwrap();
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
                    producer_id: None,
                },
            )
        });
        apply(&mut store, issued);
        let (issued, bumped) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", id, epoch, 60_000, 0).unwrap();
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
        assert_eq!(
            partition.txn_aborted,
            vec![Span {
                start: 0,
                end: 1,
                producer_id: Some(id),
            }]
        );
    }

    #[test]
    fn a_note_after_commit_aborts_the_batch_and_rejects_the_produce() {
        let mut store = memory();
        let (issued, id, epoch) = {
            let get = |key: &str| Ok(store.get(key).cloned());
            let issued = init(&get, "checkout", -1, -1, 60_000, 0).unwrap();
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
        let issued = call(&store, |get| end(get, "checkout", id, epoch, true, 0, &[]));
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
                    producer_id: None,
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
        assert_eq!(
            partition.txn_aborted,
            vec![Span {
                start: 4,
                end: 5,
                producer_id: None
            }]
        );
    }

    fn started(store: &mut HashMap<String, String>, at: i64) -> (i64, i16) {
        let issued = call(store, |get| init(get, "checkout", -1, -1, 1_000, at));
        let Issued::Applied { value, .. } = &issued else {
            panic!("init");
        };
        let (id, epoch) = value.split_once(',').unwrap();
        let (id, epoch) = (id.parse::<i64>().unwrap(), epoch.parse::<i16>().unwrap());
        apply(store, issued);
        let issued = call(store, |get| {
            add_partitions(get, "checkout", id, epoch, &[("orders".into(), 0)], at)
        });
        apply(store, issued);
        let issued = call(store, |get| {
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
                    producer_id: None,
                },
            )
        });
        apply(store, issued);
        (id, epoch)
    }

    fn aborted_spans(store: &HashMap<String, String>) -> usize {
        let topic = catalog::parse_topic(&store["topic/orders"]).unwrap();
        let part = catalog::parse_partition(&store[&catalog::partition_storage_key(&topic.id, 0)])
            .unwrap();
        part.txn_aborted.len()
    }

    #[test]
    fn end_txn_marks_a_span_the_wal_held() -> Result<(), String> {
        let mut store = memory();
        let issued = call(&store, |get| init(get, "checkout", -1, -1, 1_000, 1_000));
        let Issued::Applied { value, .. } = &issued else {
            return Err("init".into());
        };
        let mut parts = value.split(',');
        let id: i64 = parts
            .next()
            .ok_or("id")?
            .parse()
            .map_err(|err: std::num::ParseIntError| err.to_string())?;
        let epoch: i16 = parts
            .next()
            .ok_or("epoch")?
            .parse()
            .map_err(|err: std::num::ParseIntError| err.to_string())?;
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            add_partitions(get, "checkout", id, epoch, &[("orders".into(), 0)], 1_000)
        });
        apply(&mut store, issued);
        let issued = call(&store, |get| {
            end(
                get,
                "checkout",
                id,
                epoch,
                true,
                2_000,
                &[TxnSpan {
                    topic: "orders".into(),
                    partition: 0,
                    start: 0,
                    end: 4,
                    producer_id: None,
                }],
            )
        });
        apply(&mut store, issued);
        let topic = catalog::parse_topic(store.get("topic/orders").ok_or("topic")?)?;
        let key = catalog::partition_storage_key(&topic.id, 0);
        let part = catalog::parse_partition(store.get(&key).ok_or("partition")?)?;
        let span = part.txn_committed.first().ok_or("no committed span")?;
        if span.start != 0 || span.end != 4 {
            return Err(format!("{}..{}", span.start, span.end));
        }
        Ok(())
    }

    #[test]
    fn a_transaction_past_its_timeout_is_aborted_and_fenced() {
        let mut store = memory();
        let (id, epoch) = started(&mut store, 10_000);
        let early = call(&store, |get| {
            abort_timed_out(get, "checkout", id, epoch, 10_500)
        });
        let Issued::Applied { value, .. } = &early else {
            panic!("abort check");
        };
        assert!(value.is_empty(), "within the timeout");
        let late = call(&store, |get| {
            abort_timed_out(get, "checkout", id, epoch, 11_001)
        });
        let Issued::Applied { value, .. } = &late else {
            panic!("abort");
        };
        assert_eq!(value, "1");
        apply(&mut store, late);
        assert_eq!(aborted_spans(&store), 1);
        let get = |key: &str| Ok(store.get(key).cloned());
        assert_eq!(ensure(&get, "checkout", id, epoch), Err(FENCED));
        let info = inspect("checkout", &store["txn/checkout"]).unwrap();
        assert_eq!(info.state, "CompleteAbort");
        assert_eq!(info.epoch, epoch + 1);

        // KIP-360: the fenced producer may recover from its old epoch.
        let recovered = call(&store, |get| {
            init(get, "checkout", id, epoch, 1_000, 12_000)
        });
        let Issued::Applied { value, .. } = recovered else {
            panic!("recovery was rejected");
        };
        assert_eq!(value, format!("{id},{}", epoch + 2));
    }

    #[test]
    fn a_stale_abort_command_changes_nothing() {
        let mut store = memory();
        let (id, epoch) = started(&mut store, 10_000);
        let issued = call(&store, |get| {
            end(get, "checkout", id, epoch, true, 10_100, &[])
        });
        apply(&mut store, issued);
        let stale = call(&store, |get| {
            abort_timed_out(get, "checkout", id, epoch, 99_000)
        });
        let Issued::Applied { put, value } = stale else {
            panic!("stale");
        };
        assert!(put.is_empty() && value.is_empty());
        assert_eq!(aborted_spans(&store), 0);
    }

    #[test]
    fn an_idle_transactional_id_expires_and_an_ongoing_one_does_not() {
        let mut store = memory();
        let (id, epoch) = started(&mut store, 10_000);
        let get = |key: &str| Ok(store.get(key).cloned());
        let Outcome::Change { del, .. } =
            expire_id(&get, "checkout", id, epoch, 10_000_000, 1_000).unwrap()
        else {
            panic!("expire");
        };
        assert!(del.is_empty(), "ongoing transactions never expire");
        let issued = call(&store, |get| {
            end(get, "checkout", id, epoch, false, 20_000, &[])
        });
        apply(&mut store, issued);
        let get = |key: &str| Ok(store.get(key).cloned());
        let Outcome::Change { del, .. } =
            expire_id(&get, "checkout", id, epoch, 20_500, 1_000).unwrap()
        else {
            panic!("expire");
        };
        assert!(del.is_empty(), "not idle long enough");
        let Outcome::Change { del, .. } =
            expire_id(&get, "checkout", id, epoch, 21_000, 1_000).unwrap()
        else {
            panic!("expire");
        };
        assert!(del.contains(&"txn/checkout".to_string()), "{del:?}");
    }

    #[test]
    fn a_second_batch_keeps_one_span_and_end_txn_sets_the_end() -> Result<(), String> {
        let mut store = memory();
        let (id, epoch) = started(&mut store, 1_000);
        let issued = call(&store, |get| {
            note(
                get,
                "checkout",
                id,
                epoch,
                TxnSpan {
                    topic: "orders".into(),
                    partition: 0,
                    start: 1,
                    end: 2,
                    producer_id: None,
                },
            )
        });
        let Issued::Applied { put, .. } = &issued else {
            return Err("second note rejected".into());
        };
        if !put.is_empty() {
            return Err("second note wrote metadata".into());
        }
        let issued = call(&store, |get| {
            end(
                get,
                "checkout",
                id,
                epoch,
                true,
                2_000,
                &[TxnSpan {
                    topic: "orders".into(),
                    partition: 0,
                    start: 0,
                    end: 4,
                    producer_id: Some(id),
                }],
            )
        });
        apply(&mut store, issued);
        let get = |key: &str| Ok(store.get(key).cloned());
        let (_, partition) = partition_row(&get, "orders", 0)?.ok_or("missing partition")?;
        let span = partition.txn_committed.first().ok_or("missing span")?;
        if span.start != 0 || span.end != 4 || span.producer_id != Some(id) {
            return Err(format!("span {span:?}"));
        }
        if !span.covers(3, id) || span.covers(3, id.saturating_add(1)) {
            return Err("producer match".into());
        }
        Ok(())
    }
}
