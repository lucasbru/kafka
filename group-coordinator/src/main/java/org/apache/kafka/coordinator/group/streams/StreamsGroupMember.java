/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupMemberMetadataValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains all information related to a member within a streams group.
 * <p>
 * This class is immutable and is fully backed by records stored in the __consumer_offsets topic.
 *
 * @param memberId                      The ID of the member.
 * @param memberEpoch                   The current epoch of the member.
 * @param previousMemberEpoch           The previous epoch of the member.
 * @param state                         The current state of the member.
 * @param instanceId                    The instance ID of the member.
 * @param rackId                        The rack ID of the member.
 * @param clientId                      The client ID of the member.
 * @param clientHost                    The host of the member.
 * @param rebalanceTimeoutMs            The rebalance timeout in milliseconds.
 * @param topologyEpoch                 The epoch of the topology the member uses.
 * @param processId                     The ID of the Streams client that contains the member.
 * @param userEndpoint                  The user endpoint exposed for Interactive Queries by the Streams client that
 *                                      contains the member.
 * @param clientTags                    Tags of the client of the member used for rack-aware assignment.
 * @param assignedTasks                 Tasks assigned to the member.
 * @param tasksPendingRevocation        Tasks owned by the member pending revocation.
 * @param assignmentEpochs              The assignment epoch for each active task partition. The outer map key is the
 *                                      subtopology ID, and the inner map key is the partition ID. Used to fence
 *                                      zombie commit requests.
 */
@SuppressWarnings("checkstyle:JavaNCSS")
public record StreamsGroupMember(String memberId,
                                 Integer memberEpoch,
                                 Integer previousMemberEpoch,
                                 MemberState state,
                                 Optional<String> instanceId,
                                 Optional<String> rackId,
                                 String clientId,
                                 String clientHost,
                                 Integer rebalanceTimeoutMs,
                                 Integer topologyEpoch,
                                 String processId,
                                 Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint,
                                 Map<String, String> clientTags,
                                 TasksTuple assignedTasks,
                                 TasksTuple tasksPendingRevocation,
                                 Map<String, Map<Integer, Integer>> assignmentEpochs) {

    public StreamsGroupMember {
        Objects.requireNonNull(memberId, "memberId cannot be null");
        clientTags = clientTags != null ? Collections.unmodifiableMap(clientTags) : null;
    }

    /**
     * A builder that facilitates the creation of a new member or the update of an existing one.
     * <p>
     * Please refer to the javadoc of {{@link StreamsGroupMember}} for the definition of the fields.
     */
    public static class Builder {

        private final String memberId;
        private Integer memberEpoch = null;
        private Integer previousMemberEpoch = null;
        private MemberState state = null;
        private Optional<String> instanceId = null;
        private Optional<String> rackId = null;
        private Integer rebalanceTimeoutMs = null;
        private String clientId = null;
        private String clientHost = null;
        private Integer topologyEpoch = null;
        private String processId = null;
        private Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint = null;
        private Map<String, String> clientTags = null;
        private TasksTuple assignedTasks = null;
        private TasksTuple tasksPendingRevocation = null;
        private Map<String, Map<Integer, Integer>> assignmentEpochs = null;

        public Builder(String memberId) {
            this.memberId = Objects.requireNonNull(memberId, "memberId cannot be null");
        }

        public Builder(StreamsGroupMember member) {
            Objects.requireNonNull(member, "member cannot be null");

            this.memberId = member.memberId;
            this.memberEpoch = member.memberEpoch;
            this.previousMemberEpoch = member.previousMemberEpoch;
            this.assignmentEpochs = member.assignmentEpochs;
            this.instanceId = member.instanceId;
            this.rackId = member.rackId;
            this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
            this.clientId = member.clientId;
            this.clientHost = member.clientHost;
            this.topologyEpoch = member.topologyEpoch;
            this.processId = member.processId;
            this.userEndpoint = member.userEndpoint;
            this.clientTags = member.clientTags;
            this.state = member.state;
            this.assignedTasks = member.assignedTasks;
            this.tasksPendingRevocation = member.tasksPendingRevocation;
        }

        public Builder updateMemberEpoch(int memberEpoch) {
            int currentMemberEpoch = this.memberEpoch;
            this.memberEpoch = memberEpoch;
            this.previousMemberEpoch = currentMemberEpoch;
            return this;
        }

        public Builder setMemberEpoch(int memberEpoch) {
            this.memberEpoch = memberEpoch;
            return this;
        }

        public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
            this.previousMemberEpoch = previousMemberEpoch;
            return this;
        }

        public Builder setAssignmentEpochs(Map<String, Map<Integer, Integer>> assignmentEpochs) {
            this.assignmentEpochs = assignmentEpochs;
            return this;
        }

        public Builder setInstanceId(String instanceId) {
            this.instanceId = Optional.ofNullable(instanceId);
            return this;
        }

        public Builder maybeUpdateInstanceId(Optional<String> instanceId) {
            instanceId.ifPresent(this::setInstanceId);
            return this;
        }

        public Builder setRackId(String rackId) {
            this.rackId = Optional.ofNullable(rackId);
            return this;
        }

        public Builder maybeUpdateRackId(Optional<String> rackId) {
            rackId.ifPresent(this::setRackId);
            return this;
        }

        public Builder setRebalanceTimeoutMs(int rebalanceTimeoutMs) {
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            return this;
        }

        public Builder maybeUpdateRebalanceTimeoutMs(OptionalInt rebalanceTimeoutMs) {
            this.rebalanceTimeoutMs = rebalanceTimeoutMs.orElse(this.rebalanceTimeoutMs);
            return this;
        }

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setClientHost(String clientHost) {
            this.clientHost = clientHost;
            return this;
        }

        public Builder setState(MemberState state) {
            this.state = state;
            return this;
        }

        public Builder setTopologyEpoch(int topologyEpoch) {
            this.topologyEpoch = topologyEpoch;
            return this;
        }

        public Builder maybeUpdateTopologyEpoch(OptionalInt topologyEpoch) {
            this.topologyEpoch = topologyEpoch.orElse(this.topologyEpoch);
            return this;
        }

        public Builder setProcessId(String processId) {
            this.processId = processId;
            return this;
        }

        public Builder maybeUpdateProcessId(Optional<String> processId) {
            this.processId = processId.orElse(this.processId);
            return this;
        }

        public Builder setUserEndpoint(StreamsGroupMemberMetadataValue.Endpoint userEndpoint) {
            this.userEndpoint = Optional.ofNullable(userEndpoint);
            return this;
        }

        public Builder maybeUpdateUserEndpoint(Optional<StreamsGroupMemberMetadataValue.Endpoint> userEndpoint) {
            userEndpoint.ifPresent(this::setUserEndpoint);
            return this;
        }

        public Builder setClientTags(Map<String, String> clientTags) {
            this.clientTags = clientTags;
            return this;
        }

        public Builder maybeUpdateClientTags(Optional<Map<String, String>> clientTags) {
            this.clientTags = clientTags.orElse(this.clientTags);
            return this;
        }

        public Builder setAssignedTasks(TasksTuple assignedTasks) {
            this.assignedTasks = assignedTasks;
            return this;
        }

        public Builder setTasksPendingRevocation(TasksTuple tasksPendingRevocation) {
            this.tasksPendingRevocation = tasksPendingRevocation;
            return this;
        }

        public Builder updateWith(StreamsGroupMemberMetadataValue record) {
            setInstanceId(record.instanceId());
            setRackId(record.rackId());
            setClientId(record.clientId());
            setClientHost(record.clientHost());
            setRebalanceTimeoutMs(record.rebalanceTimeoutMs());
            setTopologyEpoch(record.topologyEpoch());
            setProcessId(record.processId());
            setUserEndpoint(record.userEndpoint());
            setClientTags(record.clientTags().stream().collect(Collectors.toMap(
                StreamsGroupMemberMetadataValue.KeyValue::key,
                StreamsGroupMemberMetadataValue.KeyValue::value
            )));
            return this;
        }

        public Builder updateWith(StreamsGroupCurrentMemberAssignmentValue record) {
            setMemberEpoch(record.memberEpoch());
            setPreviousMemberEpoch(record.previousMemberEpoch());
            setState(MemberState.fromValue(record.state()));
            setAssignedTasks(
                new TasksTuple(
                    assignmentFromTaskIds(record.activeTasks()),
                    assignmentFromTaskIds(record.standbyTasks()),
                    assignmentFromTaskIds(record.warmupTasks())
                )
            );
            setTasksPendingRevocation(
                new TasksTuple(
                    assignmentFromTaskIds(record.activeTasksPendingRevocation()),
                    assignmentFromTaskIds(record.standbyTasksPendingRevocation()),
                    assignmentFromTaskIds(record.warmupTasksPendingRevocation())
                )
            );
            setAssignmentEpochs(assignmentEpochsFromRecord(record));
            return this;
        }

        private static Map<String, Set<Integer>> assignmentFromTaskIds(
            List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> topicPartitionsList
        ) {
            return topicPartitionsList.stream().collect(Collectors.toMap(
                StreamsGroupCurrentMemberAssignmentValue.TaskIds::subtopologyId,
                taskIds -> Set.copyOf(taskIds.partitions())));
        }

        /**
         * Gets assignment epochs from a current assignment record.
         * 
         * @param record
         * @return A map from subtopology ID to partition-to-epoch map
         */
        private static Map<String, Map<Integer, Integer>> assignmentEpochsFromRecord(
            StreamsGroupCurrentMemberAssignmentValue record
        ) {
            Map<String, Map<Integer, Integer>> result = new HashMap<>();
            int memberEpoch = record.memberEpoch();

            for (List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIdsList : 
                 List.of(record.activeTasks(), record.activeTasksPendingRevocation())) {
                for (StreamsGroupCurrentMemberAssignmentValue.TaskIds taskIds : taskIdsList) {
                    List<Integer> partitions = taskIds.partitions();
                    List<Integer> epochs = taskIds.assignmentEpochs();

                    String subtopologyId = taskIds.subtopologyId();
                    Map<Integer, Integer> partitionEpochMap = result.computeIfAbsent(
                        subtopologyId, 
                        k -> new HashMap<>()
                    );

                    if (epochs != null) {
                        if (epochs.size() != partitions.size()) {
                            throw new IllegalStateException(
                                "Assignment epochs must be provided for all partitions. " +
                                "Subtopology " + subtopologyId + " has " + partitions.size() + " partitions but " + epochs.size() + " epochs"
                            );
                        }

                        for (int i = 0; i < partitions.size(); i++) {
                            partitionEpochMap.putIfAbsent(partitions.get(i), epochs.get(i));
                        }
                    } else {
                        // Legacy record without epochs: use member epoch from record for all partitions without explicit epochs
                        for (int partition : partitions) {
                            partitionEpochMap.putIfAbsent(partition, memberEpoch);
                        }
                    }
                }
            }

            return result;
        }

        public static Builder withDefaults(String memberId) {
            return new Builder(memberId)
                .setRebalanceTimeoutMs(-1)
                .setTopologyEpoch(-1)
                .setInstanceId(null)
                .setRackId(null)
                .setClientId("")
                .setClientHost("")
                .setProcessId("")
                .setClientTags(Collections.emptyMap())
                .setState(MemberState.STABLE)
                .setMemberEpoch(0)
                .setAssignedTasks(TasksTuple.EMPTY)
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setUserEndpoint(null)
                .setAssignmentEpochs(Collections.emptyMap());
        }

        public StreamsGroupMember build() {
            // Validate that every active task has a corresponding assignment epoch
            if (assignmentEpochs != null && assignedTasks != null) {
                validateAssignmentEpochs(assignedTasks.activeTasks(), assignmentEpochs, "assignedTasks");
            }
            if (assignmentEpochs != null && tasksPendingRevocation != null) {
                validateAssignmentEpochs(tasksPendingRevocation.activeTasks(), assignmentEpochs, "tasksPendingRevocation");
            }
            
            return new StreamsGroupMember(
                memberId,
                memberEpoch,
                previousMemberEpoch,
                state,
                instanceId,
                rackId,
                clientId,
                clientHost,
                rebalanceTimeoutMs,
                topologyEpoch,
                processId,
                userEndpoint,
                clientTags,
                assignedTasks,
                tasksPendingRevocation,
                assignmentEpochs
            );
        }
        
        private void validateAssignmentEpochs(
            Map<String, Set<Integer>> activeTasks,
            Map<String, Map<Integer, Integer>> assignmentEpochs,
            String fieldName
        ) {
            for (Map.Entry<String, Set<Integer>> entry : activeTasks.entrySet()) {
                String subtopologyId = entry.getKey();
                Set<Integer> partitions = entry.getValue();
                
                Map<Integer, Integer> subtopologyEpochs = assignmentEpochs.get(subtopologyId);
                if (subtopologyEpochs == null) {
                    throw new IllegalStateException(
                        "Subtopology " + subtopologyId + " in " + fieldName + " does not have assignment epochs"
                    );
                }
                
                for (Integer partition : partitions) {
                    if (!subtopologyEpochs.containsKey(partition)) {
                        throw new IllegalStateException(
                            "Partition " + partition + " in subtopology " + subtopologyId + 
                            " in " + fieldName + " does not have an assignment epoch"
                        );
                    }
                }
            }
        }
    }

    /**
     * @return True if the member is in the Stable state and at the desired epoch.
     */
    public boolean isReconciledTo(int targetAssignmentEpoch) {
        return state == MemberState.STABLE && memberEpoch == targetAssignmentEpoch;
    }

    /**
     * Creates a member description for the streams group describe response from this member.
     *
     * @param targetAssignment The target assignment of this member in the corresponding group.
     *
     * @return The StreamsGroupMember mapped as StreamsGroupDescribeResponseData.Member.
     */
    public StreamsGroupDescribeResponseData.Member asStreamsGroupDescribeMember(TasksTuple targetAssignment) {
        final StreamsGroupDescribeResponseData.Assignment describedTargetAssignment =
            new StreamsGroupDescribeResponseData.Assignment();

        if (targetAssignment != null) {
            describedTargetAssignment
                .setActiveTasks(taskIdsFromMap(targetAssignment.activeTasks()))
                .setStandbyTasks(taskIdsFromMap(targetAssignment.standbyTasks()))
                .setWarmupTasks(taskIdsFromMap(targetAssignment.warmupTasks()));
        }

        return new StreamsGroupDescribeResponseData.Member()
            .setMemberEpoch(memberEpoch)
            .setMemberId(memberId)
            .setAssignment(
                new StreamsGroupDescribeResponseData.Assignment()
                    .setActiveTasks(taskIdsFromMap(assignedTasks.activeTasks()))
                    .setStandbyTasks(taskIdsFromMap(assignedTasks.standbyTasks()))
                    .setWarmupTasks(taskIdsFromMap(assignedTasks.warmupTasks())))
            .setTargetAssignment(describedTargetAssignment)
            .setClientHost(clientHost)
            .setClientId(clientId)
            .setInstanceId(instanceId.orElse(null))
            .setRackId(rackId.orElse(null))
            .setClientTags(clientTags.entrySet().stream().map(
                entry -> new StreamsGroupDescribeResponseData.KeyValue()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue())
            ).toList())
            .setProcessId(processId)
            .setTopologyEpoch(topologyEpoch)
            .setUserEndpoint(
                userEndpoint.map(
                    endpoint -> new StreamsGroupDescribeResponseData.Endpoint()
                        .setHost(endpoint.host())
                        .setPort(endpoint.port())
                    ).orElse(null)
            );
    }

    private static List<StreamsGroupDescribeResponseData.TaskIds> taskIdsFromMap(Map<String, Set<Integer>> tasks) {
        List<StreamsGroupDescribeResponseData.TaskIds> taskIds = new ArrayList<>();
        tasks.keySet().stream().sorted().forEach(subtopologyId -> {
            taskIds.add(new StreamsGroupDescribeResponseData.TaskIds()
                .setSubtopologyId(subtopologyId)
                .setPartitions(tasks.get(subtopologyId).stream().sorted().toList()));
        });
        return taskIds;
    }

    /**
     * @return True if the two provided members have different assigned tasks.
     */
    public static boolean hasAssignedTasksChanged(StreamsGroupMember member1, StreamsGroupMember member2) {
        return !member1.assignedTasks().equals(member2.assignedTasks());
    }
}
