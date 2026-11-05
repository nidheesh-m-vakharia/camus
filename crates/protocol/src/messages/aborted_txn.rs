//! AbortedTxn
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/AbortedTxn.json).
// WARNING: the items of this module are generated and should not be edited directly
#![allow(unused)]

use std::borrow::Borrow;
use std::collections::BTreeMap;

use anyhow::{bail, Result};
use bytes::Bytes;
use uuid::Uuid;

use crate::protocol::{
    buf::{ByteBuf, ByteBufMut},
    compute_unknown_tagged_fields_size, types, write_unknown_tagged_fields, Decodable, Decoder,
    Encodable, Encoder, HeaderVersion, Message, StrBytes, VersionRange,
};

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct AbortedTxn {
    /// The producer id associated with the aborted transaction
    ///
    /// Supported API versions: 0
    pub producer_id: i64,

    /// The first offset in the aborted transaction
    ///
    /// Supported API versions: 0
    pub first_offset: i64,

    /// The last offset in the aborted transaction
    ///
    /// Supported API versions: 0
    pub last_offset: i64,

    /// The last stable offset at the time the transaction was aborted
    ///
    /// Supported API versions: 0
    pub last_stable_offset: i64,
}

impl AbortedTxn {
    /// Sets `producer_id` to the passed value.
    ///
    /// The producer id associated with the aborted transaction
    ///
    /// Supported API versions: 0
    pub fn with_producer_id(mut self, value: i64) -> Self {
        self.producer_id = value;
        self
    }
    /// Sets `first_offset` to the passed value.
    ///
    /// The first offset in the aborted transaction
    ///
    /// Supported API versions: 0
    pub fn with_first_offset(mut self, value: i64) -> Self {
        self.first_offset = value;
        self
    }
    /// Sets `last_offset` to the passed value.
    ///
    /// The last offset in the aborted transaction
    ///
    /// Supported API versions: 0
    pub fn with_last_offset(mut self, value: i64) -> Self {
        self.last_offset = value;
        self
    }
    /// Sets `last_stable_offset` to the passed value.
    ///
    /// The last stable offset at the time the transaction was aborted
    ///
    /// Supported API versions: 0
    pub fn with_last_stable_offset(mut self, value: i64) -> Self {
        self.last_stable_offset = value;
        self
    }
}

impl Encodable for AbortedTxn {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int64.encode(buf, &self.producer_id)?;
        types::Int64.encode(buf, &self.first_offset)?;
        types::Int64.encode(buf, &self.last_offset)?;
        types::Int64.encode(buf, &self.last_stable_offset)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int64.compute_size(&self.producer_id)?;
        total_size += types::Int64.compute_size(&self.first_offset)?;
        total_size += types::Int64.compute_size(&self.last_offset)?;
        total_size += types::Int64.compute_size(&self.last_stable_offset)?;

        Ok(total_size)
    }
}

impl Decodable for AbortedTxn {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let producer_id = types::Int64.decode(buf)?;
        let first_offset = types::Int64.decode(buf)?;
        let last_offset = types::Int64.decode(buf)?;
        let last_stable_offset = types::Int64.decode(buf)?;
        Ok(Self {
            producer_id,
            first_offset,
            last_offset,
            last_stable_offset,
        })
    }
}

impl Default for AbortedTxn {
    fn default() -> Self {
        Self {
            producer_id: 0,
            first_offset: 0,
            last_offset: 0,
            last_stable_offset: 0,
        }
    }
}

impl Message for AbortedTxn {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
