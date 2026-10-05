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
use crate::codec::{WireDecode, WireEncode, read_u64_le};
use crate::primitives::partition_history::PartitionHistory;

/// The target history of an ordered reset. Its committed partition op is the cutoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionPartitionHistoryRequest {
    pub history: PartitionHistory,
    pub metadata_op: u64,
    pub deleted: bool,
}

impl TransitionPartitionHistoryRequest {
    pub const ENCODED_SIZE: usize = 4 * size_of::<u64>();
}

impl WireEncode for TransitionPartitionHistoryRequest {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.history.encode(buf);
        buf.put_u64_le(self.metadata_op);
        buf.put_u64_le(u64::from(self.deleted));
    }
}

impl WireDecode for TransitionPartitionHistoryRequest {
    fn decode_from(buf: &[u8]) -> Result<Self, WireError> {
        let (request, consumed) = Self::decode(buf)?;
        if consumed != buf.len() {
            return Err(WireError::Validation(
                "trailing history transition bytes".into(),
            ));
        }
        Ok(request)
    }

    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let (history, pos) = PartitionHistory::decode(buf)?;
        let metadata_op = read_u64_le(buf, pos)?;
        let flags = read_u64_le(buf, pos + size_of::<u64>())?;
        if history.purge_generation == 0 || metadata_op == 0 || flags > 1 {
            return Err(WireError::Validation(
                "invalid history transition identity or flags".into(),
            ));
        }
        Ok((
            Self {
                history,
                metadata_op,
                deleted: flags == 1,
            },
            Self::ENCODED_SIZE,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_fence_round_trip_and_malformed_frames() {
        for deleted in [false, true] {
            let request = TransitionPartitionHistoryRequest {
                history: PartitionHistory {
                    incarnation: 17,
                    purge_generation: 4,
                },
                metadata_op: 52,
                deleted,
            };
            let encoded = request.to_bytes();
            assert_eq!(
                encoded.len(),
                TransitionPartitionHistoryRequest::ENCODED_SIZE
            );
            assert_eq!(
                TransitionPartitionHistoryRequest::decode_from(&encoded).unwrap(),
                request
            );
            for length in 0..encoded.len() {
                assert!(
                    TransitionPartitionHistoryRequest::decode_from(&encoded[..length]).is_err()
                );
            }
            let mut trailing = encoded.to_vec();
            trailing.push(0);
            assert!(TransitionPartitionHistoryRequest::decode_from(&trailing).is_err());
            for offset in [8, 16] {
                let mut invalid = encoded.to_vec();
                invalid[offset..offset + size_of::<u64>()].fill(0);
                assert!(TransitionPartitionHistoryRequest::decode_from(&invalid).is_err());
            }
            let mut invalid = encoded.to_vec();
            invalid[24] = 2;
            assert!(TransitionPartitionHistoryRequest::decode_from(&invalid).is_err());
        }
    }
}
