//! StreamsGroupTopologyDescriptionUpdateRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/StreamsGroupTopologyDescriptionUpdateRequest.json).
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
pub struct StreamsGroupTopologyDescriptionUpdateRequest {
    /// The streams group identifier.
    ///
    /// Supported API versions: 0
    pub group_id: super::GroupId,

    /// The ID of the streams group member sending the push. The broker validates that this member is still in the group; mismatches (including when the group itself has been deleted) are rejected with UNKNOWN_MEMBER_ID so the client treats itself as fenced and rejoins.
    ///
    /// Supported API versions: 0
    pub member_id: StrBytes,

    /// The epoch of the topology being described.
    ///
    /// Supported API versions: 0
    pub topology_epoch: i32,

    /// The topology description.
    ///
    /// Supported API versions: 0
    pub topology_description: TopologyDescription,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl StreamsGroupTopologyDescriptionUpdateRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The streams group identifier.
    ///
    /// Supported API versions: 0
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The ID of the streams group member sending the push. The broker validates that this member is still in the group; mismatches (including when the group itself has been deleted) are rejected with UNKNOWN_MEMBER_ID so the client treats itself as fenced and rejoins.
    ///
    /// Supported API versions: 0
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `topology_epoch` to the passed value.
    ///
    /// The epoch of the topology being described.
    ///
    /// Supported API versions: 0
    pub fn with_topology_epoch(mut self, value: i32) -> Self {
        self.topology_epoch = value;
        self
    }
    /// Sets `topology_description` to the passed value.
    ///
    /// The topology description.
    ///
    /// Supported API versions: 0
    pub fn with_topology_description(mut self, value: TopologyDescription) -> Self {
        self.topology_description = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for StreamsGroupTopologyDescriptionUpdateRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.group_id)?;
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.topology_epoch)?;
        types::Struct { version }.encode(buf, &self.topology_description)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.group_id)?;
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::Int32.compute_size(&self.topology_epoch)?;
        total_size += types::Struct { version }.compute_size(&self.topology_description)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for StreamsGroupTopologyDescriptionUpdateRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let group_id = types::CompactString.decode(buf)?;
        let member_id = types::CompactString.decode(buf)?;
        let topology_epoch = types::Int32.decode(buf)?;
        let topology_description = types::Struct { version }.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            group_id,
            member_id,
            topology_epoch,
            topology_description,
            unknown_tagged_fields,
        })
    }
}

impl Default for StreamsGroupTopologyDescriptionUpdateRequest {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            member_id: Default::default(),
            topology_epoch: 0,
            topology_description: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for StreamsGroupTopologyDescriptionUpdateRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyDescription {
    /// The subtopologies that make up this topology.
    ///
    /// Supported API versions: 0
    pub subtopologies: Vec<TopologyDescriptionSubtopology>,

    /// Global state stores used by this topology.
    ///
    /// Supported API versions: 0
    pub global_stores: Vec<TopologyDescriptionGlobalStore>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopologyDescription {
    /// Sets `subtopologies` to the passed value.
    ///
    /// The subtopologies that make up this topology.
    ///
    /// Supported API versions: 0
    pub fn with_subtopologies(mut self, value: Vec<TopologyDescriptionSubtopology>) -> Self {
        self.subtopologies = value;
        self
    }
    /// Sets `global_stores` to the passed value.
    ///
    /// Global state stores used by this topology.
    ///
    /// Supported API versions: 0
    pub fn with_global_stores(mut self, value: Vec<TopologyDescriptionGlobalStore>) -> Self {
        self.global_stores = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for TopologyDescription {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Struct { version }).encode(buf, &self.subtopologies)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.global_stores)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.subtopologies)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.global_stores)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for TopologyDescription {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let subtopologies = types::CompactArray(types::Struct { version }).decode(buf)?;
        let global_stores = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            subtopologies,
            global_stores,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopologyDescription {
    fn default() -> Self {
        Self {
            subtopologies: Default::default(),
            global_stores: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopologyDescription {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyDescriptionGlobalStore {
    /// The source node providing data to the global store.
    ///
    /// Supported API versions: 0
    pub source: TopologyDescriptionNode,

    /// The processor node that populates the global store.
    ///
    /// Supported API versions: 0
    pub processor: TopologyDescriptionNode,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopologyDescriptionGlobalStore {
    /// Sets `source` to the passed value.
    ///
    /// The source node providing data to the global store.
    ///
    /// Supported API versions: 0
    pub fn with_source(mut self, value: TopologyDescriptionNode) -> Self {
        self.source = value;
        self
    }
    /// Sets `processor` to the passed value.
    ///
    /// The processor node that populates the global store.
    ///
    /// Supported API versions: 0
    pub fn with_processor(mut self, value: TopologyDescriptionNode) -> Self {
        self.processor = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for TopologyDescriptionGlobalStore {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Struct { version }.encode(buf, &self.source)?;
        types::Struct { version }.encode(buf, &self.processor)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Struct { version }.compute_size(&self.source)?;
        total_size += types::Struct { version }.compute_size(&self.processor)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for TopologyDescriptionGlobalStore {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let source = types::Struct { version }.decode(buf)?;
        let processor = types::Struct { version }.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            source,
            processor,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopologyDescriptionGlobalStore {
    fn default() -> Self {
        Self {
            source: Default::default(),
            processor: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopologyDescriptionGlobalStore {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyDescriptionNode {
    /// The name of this node (e.g., KSTREAM-SOURCE-0000000000).
    ///
    /// Supported API versions: 0
    pub name: StrBytes,

    /// The type of this node: 1=SOURCE, 2=PROCESSOR, 3=SINK.
    ///
    /// Supported API versions: 0
    pub node_type: i8,

    /// The source topics this node reads from. Defined only for source nodes, may be empty if source topics are dynamically determined.
    ///
    /// Supported API versions: 0
    pub source_topics: Vec<super::TopicName>,

    /// The topic this node writes to. Defined only for sink nodes, may be null if sink topic is dynamically determined.
    ///
    /// Supported API versions: 0
    pub sink_topic: Option<super::TopicName>,

    /// The state store names accessed by this node. Defined only for processor nodes.
    ///
    /// Supported API versions: 0
    pub stores: Vec<StrBytes>,

    /// The names of successor nodes in the processing graph. Predecessor relationships are reconstructed from this field.
    ///
    /// Supported API versions: 0
    pub successors: Vec<StrBytes>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopologyDescriptionNode {
    /// Sets `name` to the passed value.
    ///
    /// The name of this node (e.g., KSTREAM-SOURCE-0000000000).
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: StrBytes) -> Self {
        self.name = value;
        self
    }
    /// Sets `node_type` to the passed value.
    ///
    /// The type of this node: 1=SOURCE, 2=PROCESSOR, 3=SINK.
    ///
    /// Supported API versions: 0
    pub fn with_node_type(mut self, value: i8) -> Self {
        self.node_type = value;
        self
    }
    /// Sets `source_topics` to the passed value.
    ///
    /// The source topics this node reads from. Defined only for source nodes, may be empty if source topics are dynamically determined.
    ///
    /// Supported API versions: 0
    pub fn with_source_topics(mut self, value: Vec<super::TopicName>) -> Self {
        self.source_topics = value;
        self
    }
    /// Sets `sink_topic` to the passed value.
    ///
    /// The topic this node writes to. Defined only for sink nodes, may be null if sink topic is dynamically determined.
    ///
    /// Supported API versions: 0
    pub fn with_sink_topic(mut self, value: Option<super::TopicName>) -> Self {
        self.sink_topic = value;
        self
    }
    /// Sets `stores` to the passed value.
    ///
    /// The state store names accessed by this node. Defined only for processor nodes.
    ///
    /// Supported API versions: 0
    pub fn with_stores(mut self, value: Vec<StrBytes>) -> Self {
        self.stores = value;
        self
    }
    /// Sets `successors` to the passed value.
    ///
    /// The names of successor nodes in the processing graph. Predecessor relationships are reconstructed from this field.
    ///
    /// Supported API versions: 0
    pub fn with_successors(mut self, value: Vec<StrBytes>) -> Self {
        self.successors = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for TopologyDescriptionNode {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::Int8.encode(buf, &self.node_type)?;
        types::CompactArray(types::CompactString).encode(buf, &self.source_topics)?;
        types::CompactString.encode(buf, &self.sink_topic)?;
        types::CompactArray(types::CompactString).encode(buf, &self.stores)?;
        types::CompactArray(types::CompactString).encode(buf, &self.successors)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size += types::Int8.compute_size(&self.node_type)?;
        total_size +=
            types::CompactArray(types::CompactString).compute_size(&self.source_topics)?;
        total_size += types::CompactString.compute_size(&self.sink_topic)?;
        total_size += types::CompactArray(types::CompactString).compute_size(&self.stores)?;
        total_size += types::CompactArray(types::CompactString).compute_size(&self.successors)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for TopologyDescriptionNode {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let node_type = types::Int8.decode(buf)?;
        let source_topics = types::CompactArray(types::CompactString).decode(buf)?;
        let sink_topic = types::CompactString.decode(buf)?;
        let stores = types::CompactArray(types::CompactString).decode(buf)?;
        let successors = types::CompactArray(types::CompactString).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            name,
            node_type,
            source_topics,
            sink_topic,
            stores,
            successors,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopologyDescriptionNode {
    fn default() -> Self {
        Self {
            name: Default::default(),
            node_type: 0,
            source_topics: Default::default(),
            sink_topic: None,
            stores: Default::default(),
            successors: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopologyDescriptionNode {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyDescriptionSubtopology {
    /// The subtopology identifier, unique within the topology.
    ///
    /// Supported API versions: 0
    pub subtopology_id: StrBytes,

    /// The processing nodes in this subtopology.
    ///
    /// Supported API versions: 0
    pub nodes: Vec<TopologyDescriptionNode>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopologyDescriptionSubtopology {
    /// Sets `subtopology_id` to the passed value.
    ///
    /// The subtopology identifier, unique within the topology.
    ///
    /// Supported API versions: 0
    pub fn with_subtopology_id(mut self, value: StrBytes) -> Self {
        self.subtopology_id = value;
        self
    }
    /// Sets `nodes` to the passed value.
    ///
    /// The processing nodes in this subtopology.
    ///
    /// Supported API versions: 0
    pub fn with_nodes(mut self, value: Vec<TopologyDescriptionNode>) -> Self {
        self.nodes = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "client")]
impl Encodable for TopologyDescriptionSubtopology {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.subtopology_id)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.nodes)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.subtopology_id)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.nodes)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for TopologyDescriptionSubtopology {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let subtopology_id = types::CompactString.decode(buf)?;
        let nodes = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            subtopology_id,
            nodes,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopologyDescriptionSubtopology {
    fn default() -> Self {
        Self {
            subtopology_id: Default::default(),
            nodes: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopologyDescriptionSubtopology {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for StreamsGroupTopologyDescriptionUpdateRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
