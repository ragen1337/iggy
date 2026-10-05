// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_u64_le, read_u128_le};

/// Offsets and retry results belong to this exact history, even when numeric IDs are reused.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionHistory {
    /// Metadata creation revision; changes when the numeric partition ID is reused.
    pub incarnation: u64,
    /// Number of completed destructive history resets within this incarnation.
    pub purge_generation: u64,
}

impl WireEncode for PartitionHistory {
    fn encoded_size(&self) -> usize {
        2 * size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.incarnation);
        buf.put_u64_le(self.purge_generation);
    }
}

impl WireDecode for PartitionHistory {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        Ok((
            Self {
                incarnation: read_u64_le(buf, 0)?,
                purge_generation: read_u64_le(buf, size_of::<u64>())?,
            },
            2 * size_of::<u64>(),
        ))
    }
}

/// Captured before an operation is sent. Retries and delayed offset writes retain
/// this value; discovering a newer history or owner does not authorize old work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionContext {
    /// Exact message history to which this operation belongs.
    pub history: PartitionHistory,
    /// Installed consumer-group owner generation; zero for operations without a group.
    pub owner_generation: u64,
    /// Applied metadata frontier, sent as the request's `minimum_metadata_op`.
    pub metadata_op: u64,
}

impl PartitionContext {
    pub const ENCODED_SIZE: usize = 4 * size_of::<u64>();

    #[must_use]
    pub fn to_le_bytes(self) -> [u8; Self::ENCODED_SIZE] {
        let mut bytes = [0; Self::ENCODED_SIZE];
        bytes[..8].copy_from_slice(&self.history.incarnation.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.history.purge_generation.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.owner_generation.to_le_bytes());
        bytes[24..].copy_from_slice(&self.metadata_op.to_le_bytes());
        bytes
    }

    pub const fn stamp(self, header: &mut crate::RequestHeader) {
        header.partition_incarnation = self.history.incarnation;
        header.purge_generation = self.history.purge_generation;
        header.owner_generation = self.owner_generation;
        header.minimum_metadata_op = self.metadata_op;
    }
}

impl WireEncode for PartitionContext {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.to_le_bytes());
    }
}

impl WireDecode for PartitionContext {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (history, consumed) = PartitionHistory::decode(buf)?;
        Ok((
            Self {
                history,
                owner_generation: read_u64_le(buf, consumed)?,
                metadata_op: read_u64_le(buf, consumed + size_of::<u64>())?,
            },
            Self::ENCODED_SIZE,
        ))
    }
}

/// Exact offset authority installed by a partition log.
///
/// `generation` changes only when this partition's assignment changes.
/// Zero client and session together install an unowned fence without discarding its generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerGroupOwner {
    pub client_id: u128,
    pub session: u64,
    pub generation: u64,
}

impl ConsumerGroupOwner {
    #[must_use]
    pub const fn is_unassigned(self) -> bool {
        self.client_id == 0 && self.session == 0
    }
}

impl WireEncode for ConsumerGroupOwner {
    fn encoded_size(&self) -> usize {
        size_of::<u128>() + 2 * size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
        buf.put_u64_le(self.generation);
    }
}

impl WireDecode for ConsumerGroupOwner {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let owner = Self {
            client_id: read_u128_le(buf, 0)?,
            session: read_u64_le(buf, size_of::<u128>())?,
            generation: read_u64_le(buf, size_of::<u128>() + size_of::<u64>())?,
        };
        if (owner.client_id == 0) != (owner.session == 0) {
            return Err(WireError::Validation(
                "incomplete consumer owner identity".into(),
            ));
        }
        Ok((owner, owner.encoded_size()))
    }
}
