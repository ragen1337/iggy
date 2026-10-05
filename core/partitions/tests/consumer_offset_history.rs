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

use consensus::{LocalPipeline, Sequencer, VsrConsensus, oneshot_channel};
use futures::FutureExt;
use iggy_binary_protocol::batch::{BatchIntegrity, decode_batch_slice_with};
use iggy_binary_protocol::primitives::partition_history::{ConsumerGroupOwner, PartitionHistory};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::partitions::{
    InstallConsumerGroupOwnerRequest, TransitionPartitionHistoryRequest,
};
use iggy_binary_protocol::{
    AckLevel, Command, Operation, PrepareOkHeader, ReplyHeader, RoutedRequestHeader, WireConsumer,
    WireEncode, WireIdentifier,
};
use iggy_common::{ConsumerKind, IggyByteSize, IggyError, PartitionStats, PollingStrategy};
use message_bus::{AUTO_COMMIT_CLIENT_ID, IggyMessageBus};
use partitions::{
    IggyPartition, IggyPartitions, Partition, PartitionIoStep, PartitionPathLayout,
    PartitionsConfig, PollingArgs, PollingConsumer, PurgeError, largest_legal_job_charge,
};
use server_common::Message;
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::{IggyNamespace, ShardId};
use std::rc::Rc;
use std::sync::Arc;

const CONSUMER_ID: u32 = 7;
const METADATA_WATERMARK: u64 = 100;
const REPLACEMENT: &[u8] = b"replacement record";
const CLUSTER_REPLICAS: u8 = 3;
// Only a single replica applies a `NoAck` offset mutation without replication.
const SINGLE_REPLICA: u8 = 1;
const IO_STEPS_MAX: usize = 64;
type TestPartition = IggyPartition<Rc<IggyMessageBus>>;

#[compio::test]
async fn given_admitted_offset_store_when_purging_should_drain_before_the_history_fence() {
    for client_id in [42, AUTO_COMMIT_CLIENT_ID] {
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let (mut partition, config, _directory) =
                Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
            let consumer = polling_consumer(kind);
            let (returned, delivered) = poll(
                partition,
                &config,
                consumer,
                &PollingArgs::new(PollingStrategy::first(), 1, false),
            );
            partition = returned;
            assert_eq!(delivered, vec![b"old record".to_vec()]);
            partition
                .on_request(append(&partition, 2, b"admitted record"), None)
                .await;
            let delayed = store(&partition, client_id, 2, kind, 0);
            let (sender, receiver) = oneshot_channel();
            partition
                .on_request(store(&partition, client_id, 1, kind, 0), Some(sender))
                .await;
            assert_eq!(partition.consensus().request_queue_len(), 1);

            assert_transition_waits(&mut partition).await;
            commit_pending(&mut partition, &config).await;
            assert_success(receiver);
            assert_eq!(partition.get_consumer_offset(consumer), Some(0));
            transition_history(&mut partition, &config, false).await;
            install_owner(&mut partition, &config).await;
            partition
                .on_request(append(&partition, 3, REPLACEMENT), None)
                .await;
            commit_pending(&mut partition, &config).await;
            assert_eq!(partition.get_consumer_offset(consumer), None);

            let before = partition.consensus().sequencer().current_sequence();
            let (sender, receiver) = oneshot_channel();
            partition.on_request(delayed, Some(sender)).await;
            assert_status(receiver, IggyError::HistoryUnavailable.as_code());
            assert_eq!(partition.consensus().sequencer().current_sequence(), before);
            let (_, next) = poll(
                partition,
                &config,
                consumer,
                &PollingArgs::new(PollingStrategy::next(), 10, false),
            );
            assert_eq!(next, vec![REPLACEMENT.to_vec()]);
        }
    }
}

#[compio::test]
async fn given_queued_automatic_commit_when_purging_should_keep_replacement_records_unread() {
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
    partition
        .on_request(append(&partition, 2, b"admitted record"), None)
        .await;
    let consumer = polling_consumer(ConsumerKind::Consumer);
    let (mut partition, delivered) = poll(
        partition,
        &config,
        consumer,
        &PollingArgs::new(PollingStrategy::first(), 1, true),
    );
    assert_eq!(delivered, vec![b"old record".to_vec()]);
    assert_eq!(partition.consensus().request_queue_len(), 1);
    assert_transition_waits(&mut partition).await;
    commit_pending(&mut partition, &config).await;
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    transition_history(&mut partition, &config, false).await;
    partition
        .on_request(append(&partition, 3, REPLACEMENT), None)
        .await;
    commit_pending(&mut partition, &config).await;
    assert_eq!(partition.get_consumer_offset(consumer), None);
    let (_, next) = poll(
        partition,
        &config,
        consumer,
        &PollingArgs::new(PollingStrategy::next(), 10, false),
    );
    assert_eq!(next, vec![REPLACEMENT.to_vec()]);
}

#[compio::test]
async fn given_queued_explicit_store_when_history_is_unchanged_should_allow_rewind() {
    for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
        let (mut partition, config, _directory) =
            Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
        partition
            .on_request(append(&partition, 2, b"second record"), None)
            .await;
        commit_pending(&mut partition, &config).await;
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(store(&partition, 42, 1, kind, 1), Some(sender))
            .await;
        commit_pending(&mut partition, &config).await;
        assert_success(receiver);
        assert_eq!(
            partition.get_consumer_offset(polling_consumer(kind)),
            Some(1)
        );

        partition
            .on_request(append(&partition, 3, b"third record"), None)
            .await;
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(store(&partition, 42, 2, kind, 0), Some(sender))
            .await;
        assert_eq!(partition.consensus().request_queue_len(), 1);
        commit_pending(&mut partition, &config).await;
        assert_success(receiver);
        assert_eq!(
            partition.get_consumer_offset(polling_consumer(kind)),
            Some(0),
            "explicit stores may rewind within the same history"
        );
    }
}

#[compio::test]
async fn given_delayed_offset_delete_when_history_changes_should_reject_without_replay() {
    for purge in [false, true] {
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let (mut partition, config, _directory) =
                Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
            let (sender, receiver) = oneshot_channel();
            partition
                .on_request(store(&partition, 42, 1, kind, 0), Some(sender))
                .await;
            commit_pending(&mut partition, &config).await;
            assert_success(receiver);
            let delayed = delete(&partition, kind, 2);
            if purge {
                transition_history(&mut partition, &config, false).await;
                install_owner(&mut partition, &config).await;
            }
            let before = partition.consensus().sequencer().current_sequence();
            let (sender, receiver) = oneshot_channel();
            partition.on_request(delayed, Some(sender)).await;
            commit_pending(&mut partition, &config).await;
            assert_status(
                receiver,
                if purge {
                    IggyError::HistoryUnavailable.as_code()
                } else {
                    0
                },
            );
            assert_eq!(
                partition.consensus().sequencer().current_sequence(),
                before + u64::from(!purge)
            );
            assert_eq!(partition.get_consumer_offset(polling_consumer(kind)), None);
        }
    }
}

#[compio::test]
async fn given_full_offset_queue_when_purging_should_drain_every_admitted_request_before_cutoff() {
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
    partition
        .on_request(append(&partition, 2, b"admitted record"), None)
        .await;
    let mut receivers = Vec::new();
    for client_id in 43..=48 {
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(
                store(&partition, client_id, 1, ConsumerKind::Consumer, 0),
                Some(sender),
            )
            .await;
        receivers.push(receiver);
    }
    assert_eq!(partition.consensus().request_queue_len(), 6);
    assert_transition_waits(&mut partition).await;
    let (sender, refused) = oneshot_channel();
    partition
        .on_request(append(&partition, 3, b"arrived during drain"), Some(sender))
        .await;
    assert_status(refused, IggyError::TransientNotAccepted.as_code());
    commit_pending(&mut partition, &config).await;
    for receiver in receivers {
        assert_success(receiver);
    }
    assert_eq!(partition.consensus().request_queue_len(), 0);
    transition_history(&mut partition, &config, false).await;
    partition
        .on_request(append(&partition, 3, REPLACEMENT), None)
        .await;
    commit_pending(&mut partition, &config).await;
    let (_, next) = poll(
        partition,
        &config,
        polling_consumer(ConsumerKind::Consumer),
        &PollingArgs::new(PollingStrategy::next(), 10, false),
    );
    assert_eq!(next, vec![REPLACEMENT.to_vec()]);
}

#[compio::test]
async fn given_delayed_delete_when_replacement_checkpoint_commits_should_preserve_new_progress() {
    let kind = ConsumerKind::Consumer;
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(store(&partition, 42, 1, kind, 0), Some(sender))
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    let delayed = delete(&partition, kind, 2);
    transition_history(&mut partition, &config, false).await;
    partition
        .on_request(append(&partition, 2, REPLACEMENT), None)
        .await;
    commit_pending(&mut partition, &config).await;
    let (sender, stored) = oneshot_channel();
    partition
        .on_request(store(&partition, 43, 1, kind, 0), Some(sender))
        .await;
    partition.resume_queued_requests().await;
    commit_pending(&mut partition, &config).await;
    assert_success(stored);
    let before = partition.consensus().sequencer().current_sequence();
    let (sender, deleted) = oneshot_channel();
    partition.on_request(delayed, Some(sender)).await;
    assert_status(deleted, IggyError::HistoryUnavailable.as_code());
    assert_eq!(
        partition.get_consumer_offset(polling_consumer(kind)),
        Some(0)
    );
    assert_eq!(partition.consensus().sequencer().current_sequence(), before);
}

#[compio::test]
async fn given_committed_delete_fence_when_requests_arrive_should_keep_the_incarnation_closed() {
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
    let delayed = append(&partition, 2, b"delayed old record");
    transition_history(&mut partition, &config, true).await;
    let before = partition.consensus().sequencer().current_sequence();
    for request in [delayed, append(&partition, 3, b"restamped record")] {
        let (sender, receiver) = oneshot_channel();
        partition.on_request(request, Some(sender)).await;
        assert_status(receiver, IggyError::HistoryUnavailable.as_code());
    }
    assert_eq!(partition.consensus().sequencer().current_sequence(), before);
}

#[allow(clippy::future_not_send)]
async fn install_owner(partition: &mut TestPartition, config: &PartitionsConfig) {
    let generation = partition.history().purge_generation + 1;
    let installation = InstallConsumerGroupOwnerRequest {
        history: partition.history(),
        group_id: u64::from(CONSUMER_ID),
        owner: ConsumerGroupOwner {
            client_id: 42,
            session: 1,
            generation,
        },
        metadata_op: 2 * generation - 1,
    };
    install_owner_record(partition, config, installation).await;
}

#[allow(clippy::future_not_send)]
async fn install_owner_record(
    partition: &mut TestPartition,
    config: &PartitionsConfig,
    installation: InstallConsumerGroupOwnerRequest,
) {
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            offset_request(
                partition,
                Operation::InstallConsumerGroupOwner,
                AUTO_COMMIT_CLIENT_ID,
                installation.metadata_op,
                &installation.to_bytes(),
                0,
            ),
            Some(sender),
        )
        .await;
    commit_pending(partition, config).await;
    assert_success(receiver);
    assert!(
        partition
            .installed_consumer_group_owner(&installation)
            .is_some()
    );
}

#[compio::test]
async fn given_unassigned_owner_when_automatic_offsets_arrive_should_only_allow_matching_cleanup() {
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
    let consumer = polling_consumer(ConsumerKind::ConsumerGroup);
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            store(&partition, 42, 1, ConsumerKind::ConsumerGroup, 0),
            Some(sender),
        )
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    let stale_cleanup = delete(&partition, ConsumerKind::ConsumerGroup, 2).transmute_header(
        |old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.client = AUTO_COMMIT_CLIENT_ID;
        },
    );
    let installation = InstallConsumerGroupOwnerRequest {
        history: partition.history(),
        group_id: u64::from(CONSUMER_ID),
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
    };
    install_owner_record(&mut partition, &config, installation).await;
    let automatic_store = store(
        &partition,
        AUTO_COMMIT_CLIENT_ID,
        3,
        ConsumerKind::ConsumerGroup,
        0,
    );
    let before = partition.consensus().sequencer().current_sequence();
    for request in [automatic_store, stale_cleanup] {
        let (sender, receiver) = oneshot_channel();
        partition.on_request(request, Some(sender)).await;
        assert_status(
            receiver,
            IggyError::ConsumerGroupPartitionNotOwned(CONSUMER_ID, 0).as_code(),
        );
        assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    }
    assert_eq!(partition.consensus().sequencer().current_sequence(), before);

    let cleanup = delete(&partition, ConsumerKind::ConsumerGroup, 4).transmute_header(
        |old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.client = AUTO_COMMIT_CLIENT_ID;
        },
    );
    let delayed_cleanup = cleanup.deep_copy();
    let (sender, receiver) = oneshot_channel();
    partition.on_request(cleanup, Some(sender)).await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    assert_eq!(partition.get_consumer_offset(consumer), None);

    install_owner_record(
        &mut partition,
        &config,
        InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: 77,
                session: 1,
                generation: 3,
            },
            metadata_op: 3,
            ..installation
        },
    )
    .await;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            store(&partition, 77, 1, ConsumerKind::ConsumerGroup, 0),
            Some(sender),
        )
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    let (sender, receiver) = oneshot_channel();
    partition.on_request(delayed_cleanup, Some(sender)).await;
    assert_status(
        receiver,
        IggyError::ConsumerGroupPartitionNotOwned(CONSUMER_ID, 0).as_code(),
    );
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
}

fn history_fence(partition: &TestPartition, deleted: bool) -> Message<RoutedRequestHeader> {
    let generation = partition.history().purge_generation + 1;
    let transition = TransitionPartitionHistoryRequest {
        history: PartitionHistory {
            purge_generation: generation,
            ..partition.history()
        },
        metadata_op: 2 * generation,
        deleted,
    };
    offset_request(
        partition,
        Operation::TransitionPartitionHistory,
        AUTO_COMMIT_CLIENT_ID,
        transition.metadata_op,
        &transition.to_bytes(),
        0,
    )
}

#[allow(clippy::future_not_send)]
async fn assert_transition_waits(partition: &mut TestPartition) {
    let before = partition.consensus().sequencer().current_sequence();
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(history_fence(partition, false), Some(sender))
        .await;
    assert_status(receiver, IggyError::TransientNotAccepted.as_code());
    assert_eq!(partition.consensus().sequencer().current_sequence(), before);
}

#[allow(clippy::future_not_send)]
async fn transition_history(
    partition: &mut TestPartition,
    config: &PartitionsConfig,
    deleted: bool,
) {
    assert_eq!(partition.consensus().request_queue_len(), 0);
    let cutoff = partition.consensus().commit_min() + 1;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(history_fence(partition, deleted), Some(sender))
        .await;
    commit_pending(partition, config).await;
    assert_success(receiver);
    assert_eq!(
        partition.purge_floor_op(),
        cutoff,
        "the committed fence is the only cleanup cutoff"
    );
    assert_eq!(partition.consensus().commit_min(), cutoff);
}

#[compio::test]
async fn given_waiting_no_ack_offset_mutation_when_purged_should_reject_without_replay() {
    for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
        for delete_offset in [false, true] {
            let (mut partition, config, _directory) =
                Box::pin(partition_with_old_record(SINGLE_REPLICA)).await;
            let (mutation, refusal) = if delete_offset {
                let (sender, stored) = oneshot_channel();
                partition
                    .on_request(
                        store_with_ack(&partition, 42, 1, kind, 0, AckLevel::NoAck),
                        Some(sender),
                    )
                    .await;
                assert_success(stored);
                (
                    delete_with_ack(&partition, kind, 2, AckLevel::NoAck),
                    IggyError::ConsumerOffsetNotFound(CONSUMER_ID as usize),
                )
            } else {
                (
                    store_with_ack(&partition, 42, 2, kind, 0, AckLevel::NoAck),
                    IggyError::InvalidOffset(0),
                )
            };
            partition
                .on_request(append(&partition, 2, b"old pending prepare"), None)
                .await;
            let (sender, mut receiver) = oneshot_channel();
            partition.on_request(mutation, Some(sender)).await;
            assert!(
                (&mut receiver).now_or_never().is_none(),
                "the mutation must wait behind the pending prepare"
            );

            partition
                .purge_at(
                    &config,
                    1,
                    partition.consensus().sequencer().current_sequence(),
                )
                .await
                .unwrap();
            commit_pending(&mut partition, &config).await;
            assert_eq!(
                reply_status(receiver),
                refusal.as_code(),
                "a retry would replay the mutation into the replacement history ({kind:?}, delete: {delete_offset})"
            );

            if kind == ConsumerKind::ConsumerGroup {
                install_owner(&mut partition, &config).await;
            }
            partition
                .on_request(append(&partition, 3, REPLACEMENT), None)
                .await;
            commit_pending(&mut partition, &config).await;
            let consumer = polling_consumer(kind);
            assert_eq!(partition.get_consumer_offset(consumer), None);
            let (_, next) = poll(
                partition,
                &config,
                consumer,
                &PollingArgs::new(PollingStrategy::next(), 10, false),
            );
            assert_eq!(next, vec![REPLACEMENT.to_vec()]);
        }
    }
}

#[compio::test]
async fn given_waiting_no_ack_store_when_dispatched_purge_starts_should_reject_without_replay() {
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(SINGLE_REPLICA)).await;
    partition.set_io_notifier(Rc::new(|_, _| {}), largest_legal_job_charge().unwrap());
    partition
        .on_request(append(&partition, 2, b"old pending prepare"), None)
        .await;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            store_with_ack(
                &partition,
                42,
                1,
                ConsumerKind::Consumer,
                0,
                AckLevel::NoAck,
            ),
            Some(sender),
        )
        .await;

    assert!(matches!(
        partition
            .purge_at(
                &config,
                1,
                partition.consensus().sequencer().current_sequence()
            )
            .await,
        Err(PurgeError::Pending)
    ));
    settle_io(&mut partition, &config).await;
    assert_eq!(
        reply_status(receiver),
        IggyError::InvalidOffset(0).as_code(),
        "the store predates the purge, so a retry would replay it into the replacement history"
    );
}

#[compio::test]
async fn given_queued_offset_mutation_when_shutdown_follows_purge_should_reject_without_replay() {
    for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
        for delete_offset in [false, true] {
            let (mut partition, config, _directory) =
                Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
            let (sender, receiver) = oneshot_channel();
            partition
                .on_request(store(&partition, 42, 1, kind, 0), Some(sender))
                .await;
            commit_pending(&mut partition, &config).await;
            assert_success(receiver);
            partition
                .on_request(append(&partition, 2, b"old pending prepare"), None)
                .await;
            assert!(partition.consensus().pipeline_is_full());
            let (mutation, refusal) = if delete_offset {
                (
                    delete(&partition, kind, 2),
                    IggyError::ConsumerOffsetNotFound(CONSUMER_ID as usize),
                )
            } else {
                (
                    store(&partition, 42, 2, kind, 0),
                    IggyError::InvalidOffset(0),
                )
            };
            let (sender, receiver) = oneshot_channel();
            partition.on_request(mutation, Some(sender)).await;
            assert_eq!(partition.consensus().request_queue_len(), 1);
            partition
                .purge_at(
                    &config,
                    1,
                    partition.consensus().sequencer().current_sequence(),
                )
                .await
                .unwrap();
            let (sender, published) = oneshot_channel();
            partition
                .on_request(append(&partition, 3, REPLACEMENT), Some(sender))
                .await;
            assert_eq!(partition.consensus().request_queue_len(), 2);

            partition.begin_shutdown_io();
            settle_io(&mut partition, &config).await;
            assert_eq!(partition.consensus().request_queue_len(), 0);
            assert_eq!(reply_status(receiver), refusal.as_code());
            assert_eq!(
                reply_status(published),
                IggyError::TransientNotAccepted.as_code(),
                "a request admitted into the current history stays retryable"
            );
        }
    }
}

#[allow(clippy::future_not_send)]
async fn partition_with_old_record(
    replica_count: u8,
) -> (TestPartition, PartitionsConfig, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let segment_size = IggyByteSize::from(1_048_576_u64);
    let config = PartitionsConfig {
        messages_required_to_save: 100,
        size_of_messages_required_to_save: segment_size,
        validate_checksum: true,
        segment_size,
        preallocate_segments: false,
        encryptor: None,
        path_layout: PartitionPathLayout::default(),
    };
    let consensus = VsrConsensus::new(
        1,
        0,
        replica_count,
        namespace().inner(),
        Rc::new(IggyMessageBus::new(0)),
        LocalPipeline::with_capacities(1, 8),
    );
    consensus.init();
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        segment_size,
    );
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_owner(&mut partition, &config).await;
    partition
        .on_request(append(&partition, 1, b"old record"), None)
        .await;
    commit_pending(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 2);
    (partition, config, directory)
}

// Drive the actual commit and queue-promotion paths with deterministic quorum
// completion. Remote replicas and timing do not decide when a prepare advances.
#[allow(clippy::future_not_send)]
async fn commit_pending(partition: &mut TestPartition, config: &PartitionsConfig) {
    for _ in 0..16 {
        process_self_acknowledgments(partition, config).await;
        let current = partition.consensus().sequencer().current_sequence();
        if partition.consensus().commit_min() == current {
            return;
        }
        partition.consensus().advance_commit_max(current);
        partition.commit_journal(config).await;
    }
    panic!("the bounded request queue did not drain");
}

#[allow(clippy::future_not_send)]
async fn process_self_acknowledgments(partition: &mut TestPartition, config: &PartitionsConfig) {
    let mut queued = Vec::new();
    partition.consensus().drain_loopback_into(&mut queued);
    for message in queued {
        let mut typed = Message::<PrepareOkHeader>::new(message.as_slice().len());
        typed.as_mut_slice().copy_from_slice(message.as_slice());
        partition.on_ack(typed, config).await;
    }
}

// Stand in for the shard I/O loop: execute every planned job until the
// partition has no work left.
#[allow(clippy::future_not_send)]
async fn settle_io(partition: &mut TestPartition, config: &PartitionsConfig) {
    for _ in 0..IO_STEPS_MAX {
        match partition.resume_io(config).await {
            PartitionIoStep::Ready(plan) => {
                let captured = partition.capture_io(plan, config).unwrap().unwrap();
                let result = captured.job.execute().await;
                partition.accept_io(captured.identity, result).unwrap();
                if let Some(gate) = captured.gate {
                    gate.release();
                }
            }
            PartitionIoStep::Pending => return,
            _ => {}
        }
    }
    panic!("partition I/O did not settle");
}

fn poll(
    partition: TestPartition,
    config: &PartitionsConfig,
    consumer: PollingConsumer,
    args: &PollingArgs,
) -> (TestPartition, Vec<Vec<u8>>) {
    let owner = IggyPartitions::new(ShardId::new(0), config.clone());
    owner.insert(namespace(), partition);
    let read = owner
        .build_poll_snapshot(&namespace(), consumer, args)
        .unwrap()
        .execute_resident();
    let completion = owner.complete_poll(&namespace(), read).unwrap();
    assert!(
        completion.replication.is_none(),
        "automatic commits in this fixture must queue"
    );
    let payloads = completion
        .fragments
        .iter()
        .flat_map(|fragment| {
            let batch =
                decode_batch_slice_with(fragment.as_slice(), BatchIntegrity::Verify).unwrap();
            batch
                .iter_with_offsets()
                .map(|record| record.message.payload.to_vec())
                .collect::<Vec<_>>()
        })
        .collect();
    (owner.remove(&namespace()).unwrap(), payloads)
}

fn namespace() -> IggyNamespace {
    IggyNamespace::new(1, 1, 0)
}

const fn polling_consumer(kind: ConsumerKind) -> PollingConsumer {
    match kind {
        ConsumerKind::Consumer => PollingConsumer::Consumer(CONSUMER_ID as usize, 0),
        ConsumerKind::ConsumerGroup => PollingConsumer::ConsumerGroup(CONSUMER_ID as usize, 0),
        ConsumerKind::ExternalGroup => panic!("an external group is never polled"),
    }
}

fn append(partition: &TestPartition, request: u64, payload: &[u8]) -> Message<RoutedRequestHeader> {
    let mut messages = IggyMessages::with_capacity(1);
    messages.push(IggyMessage {
        header: IggyMessageHeader {
            id: u128::from(request),
            payload_length: u32::try_from(payload.len()).unwrap(),
            ..Default::default()
        },
        payload: payload.to_vec().into(),
        user_headers: None,
    });
    SendMessagesOwned::from_messages(namespace(), &messages)
        .unwrap()
        .encode_request(RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: u128::from(request),
            session: 1,
            request,
            group: namespace().inner(),
            partition_incarnation: partition.history().incarnation,
            purge_generation: partition.history().purge_generation,
            metadata_watermark: METADATA_WATERMARK,
            ..Default::default()
        })
        .unwrap()
}

fn store(
    partition: &TestPartition,
    client: u128,
    request: u64,
    kind: ConsumerKind,
    offset: u64,
) -> Message<RoutedRequestHeader> {
    store_with_ack(partition, client, request, kind, offset, AckLevel::Quorum)
}

fn store_with_ack(
    partition: &TestPartition,
    client: u128,
    request: u64,
    kind: ConsumerKind,
    offset: u64,
    ack: AckLevel,
) -> Message<RoutedRequestHeader> {
    let body = StoreConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id: WireIdentifier::numeric(CONSUMER_ID),
        },
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(1),
        partition_id: Some(0),
        offset,
        ack,
    }
    .to_bytes();
    offset_request(
        partition,
        Operation::StoreConsumerOffset,
        client,
        request,
        &body,
        owner_generation(partition, kind),
    )
}

fn delete(
    partition: &TestPartition,
    kind: ConsumerKind,
    request: u64,
) -> Message<RoutedRequestHeader> {
    delete_with_ack(partition, kind, request, AckLevel::Quorum)
}

fn delete_with_ack(
    partition: &TestPartition,
    kind: ConsumerKind,
    request: u64,
    ack: AckLevel,
) -> Message<RoutedRequestHeader> {
    let body = DeleteConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id: WireIdentifier::numeric(CONSUMER_ID),
        },
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(1),
        partition_id: Some(0),
        ack,
    }
    .to_bytes();
    offset_request(
        partition,
        Operation::DeleteConsumerOffset,
        42,
        request,
        &body,
        owner_generation(partition, kind),
    )
}

fn offset_request(
    partition: &TestPartition,
    operation: Operation,
    client: u128,
    request: u64,
    body: &[u8],
    owner_generation: u64,
) -> Message<RoutedRequestHeader> {
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(size);
    message.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(body);
    message.transmute_header(|_, header: &mut RoutedRequestHeader| {
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation,
            size: u32::try_from(size).unwrap(),
            client,
            session: 1,
            request,
            group: namespace().inner(),
            owner_generation,
            partition_incarnation: partition.history().incarnation,
            purge_generation: partition.history().purge_generation,
            metadata_watermark: METADATA_WATERMARK,
            ..Default::default()
        };
    })
}

fn owner_generation(partition: &TestPartition, kind: ConsumerKind) -> u64 {
    match kind {
        ConsumerKind::Consumer | ConsumerKind::ExternalGroup => 0,
        ConsumerKind::ConsumerGroup => {
            partition
                .consumer_group_owner(u64::from(CONSUMER_ID))
                .unwrap()
                .generation
        }
    }
}

fn assert_success(receiver: consensus::Receiver<Message<ReplyHeader>>) {
    assert_status(receiver, 0);
}

fn assert_status(receiver: consensus::Receiver<Message<ReplyHeader>>, status: u32) {
    assert_eq!(reply_status(receiver), status);
}

fn reply_status(receiver: consensus::Receiver<Message<ReplyHeader>>) -> u32 {
    receiver
        .now_or_never()
        .expect("the request must finish")
        .unwrap()
        .header()
        .status
}
