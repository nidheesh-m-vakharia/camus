//! ApiVersionsRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsRequest.json).
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

/// Valid versions: 0-5
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ApiVersionsRequest {
    /// The name of the client.
    ///
    /// Supported API versions: 3-5
    pub client_software_name: StrBytes,

    /// The version of the client.
    ///
    /// Supported API versions: 3-5
    pub client_software_version: StrBytes,

    /// The cluster ID the client intends to connect to. Provide both ClusterId and NodeId, if known.
    ///
    /// Supported API versions: 5
    pub cluster_id: Option<StrBytes>,

    /// The node ID the client intends to connect to. Provide both ClusterId and NodeId, if known.
    ///
    /// Supported API versions: 5
    pub node_id: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl ApiVersionsRequest {
    /// Sets `client_software_name` to the passed value.
    ///
    /// The name of the client.
    ///
    /// Supported API versions: 3-5
    pub fn with_client_software_name(mut self, value: StrBytes) -> Self {
        self.client_software_name = value;
        self
    }
    /// Sets `client_software_version` to the passed value.
    ///
    /// The version of the client.
    ///
    /// Supported API versions: 3-5
    pub fn with_client_software_version(mut self, value: StrBytes) -> Self {
        self.client_software_version = value;
        self
    }
    /// Sets `cluster_id` to the passed value.
    ///
    /// The cluster ID the client intends to connect to. Provide both ClusterId and NodeId, if known.
    ///
    /// Supported API versions: 5
    pub fn with_cluster_id(mut self, value: Option<StrBytes>) -> Self {
        self.cluster_id = value;
        self
    }
    /// Sets `node_id` to the passed value.
    ///
    /// The node ID the client intends to connect to. Provide both ClusterId and NodeId, if known.
    ///
    /// Supported API versions: 5
    pub fn with_node_id(mut self, value: i32) -> Self {
        self.node_id = value;
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
impl Encodable for ApiVersionsRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.client_software_name)?;
        }
        if version >= 3 {
            types::CompactString.encode(buf, &self.client_software_version)?;
        }
        if version >= 5 {
            types::CompactString.encode(buf, &self.cluster_id)?;
        }
        if version >= 5 {
            types::Int32.encode(buf, &self.node_id)?;
        }
        if version >= 3 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

            write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        }
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        if version >= 3 {
            total_size += types::CompactString.compute_size(&self.client_software_name)?;
        }
        if version >= 3 {
            total_size += types::CompactString.compute_size(&self.client_software_version)?;
        }
        if version >= 5 {
            total_size += types::CompactString.compute_size(&self.cluster_id)?;
        }
        if version >= 5 {
            total_size += types::Int32.compute_size(&self.node_id)?;
        }
        if version >= 3 {
            let num_tagged_fields = self.unknown_tagged_fields.len();
            if num_tagged_fields > std::u32::MAX as usize {
                bail!(
                    "Too many tagged fields to encode ({} fields)",
                    num_tagged_fields
                );
            }
            total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

            total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        }
        Ok(total_size)
    }
}

#[cfg(feature = "broker")]
impl Decodable for ApiVersionsRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version < 0 || version > 5 {
            bail!("specified version not supported by this message type");
        }
        let client_software_name = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let client_software_version = if version >= 3 {
            types::CompactString.decode(buf)?
        } else {
            Default::default()
        };
        let cluster_id = if version >= 5 {
            types::CompactString.decode(buf)?
        } else {
            None
        };
        let node_id = if version >= 5 {
            types::Int32.decode(buf)?
        } else {
            -1
        };
        let mut unknown_tagged_fields = BTreeMap::new();
        if version >= 3 {
            let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
            for _ in 0..num_tagged_fields {
                let tag: u32 = types::UnsignedVarInt.decode(buf)?;
                let size: u32 = types::UnsignedVarInt.decode(buf)?;
                let unknown_value = buf.try_get_bytes(size as usize)?;
                unknown_tagged_fields.insert(tag as i32, unknown_value);
            }
        }
        Ok(Self {
            client_software_name,
            client_software_version,
            cluster_id,
            node_id,
            unknown_tagged_fields,
        })
    }
}

impl Default for ApiVersionsRequest {
    fn default() -> Self {
        Self {
            client_software_name: Default::default(),
            client_software_version: Default::default(),
            cluster_id: None,
            node_id: -1,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for ApiVersionsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 5 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for ApiVersionsRequest {
    fn header_version(version: i16) -> i16 {
        if version >= 3 {
            2
        } else {
            1
        }
    }
}
