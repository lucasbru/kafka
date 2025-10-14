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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.TaskRole;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasks;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksPerSubtopology;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    private static final String SUBTOPOLOGY_ID1 = Uuid.randomUuid().toString();
    private static final String SUBTOPOLOGY_ID2 = Uuid.randomUuid().toString();
    private static final String PROCESS_ID = "process_id";
    private static final String MEMBER_NAME = "member";

    /**
     * Creates assignment epochs for active tasks based on task assignments.
     * All partitions are assigned the given epoch.
     */
    private static Map<String, Map<Integer, Integer>> mkAssignmentEpochs(int epoch, TaskRole taskRole, Map<String, Set<Integer>>... subtopologyTasks) {
        if (taskRole != TaskRole.ACTIVE) {
            return Collections.emptyMap();
        }
        Map<String, Map<Integer, Integer>> result = new HashMap<>();
        for (Map<String, Set<Integer>> subtopology : subtopologyTasks) {
            for (Map.Entry<String, Set<Integer>> entry : subtopology.entrySet()) {
                Map<Integer, Integer> partitionEpochs = new HashMap<>();
                for (Integer partition : entry.getValue()) {
                    partitionEpochs.put(partition, epoch);
                }
                result.put(entry.getKey(), partitionEpochs);
            }
        }
        return result;
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStable(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        // Create assignment epochs for active tasks if needed
        Map<String, Map<Integer, Integer>> assignmentEpochs;
        if (taskRole == TaskRole.ACTIVE) {
            assignmentEpochs = new HashMap<>();
            Map<Integer, Integer> sub1Epochs = new HashMap<>();
            sub1Epochs.put(1, memberEpoch);
            sub1Epochs.put(2, memberEpoch);
            assignmentEpochs.put(SUBTOPOLOGY_ID1, sub1Epochs);
            
            Map<Integer, Integer> sub2Epochs = new HashMap<>();
            sub2Epochs.put(3, memberEpoch);
            sub2Epochs.put(4, memberEpoch);
            assignmentEpochs.put(SUBTOPOLOGY_ID2, sub2Epochs);
        } else {
            assignmentEpochs = Collections.emptyMap();
        }

        StreamsGroupMember member =
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(
                    taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)  // Tasks didn't change, so assignment epochs remain the same
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStableAtTargetEpoch(TaskRole taskRole) {
        final int memberEpoch = 10;
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );

        StreamsGroupMember member =
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(
                    taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToStableWithNewTasks(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        // Create assignment epochs for the initial assigned tasks
        Map<String, Map<Integer, Integer>> initialAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(initialAssignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();
        
        // Expected assignment epochs: existing tasks keep their epochs, new tasks get the new epoch
        // Build expected epochs manually - existing tasks keep epoch 10, new tasks get epoch 11
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs;
        if (taskRole == TaskRole.ACTIVE) {
            expectedAssignmentEpochs = new HashMap<>();
            Map<Integer, Integer> sub1Epochs = new HashMap<>();
            sub1Epochs.put(1, memberEpoch);  // existing
            sub1Epochs.put(2, memberEpoch);  // existing
            sub1Epochs.put(4, memberEpoch + 1);  // new
            expectedAssignmentEpochs.put(SUBTOPOLOGY_ID1, sub1Epochs);
            
            Map<Integer, Integer> sub2Epochs = new HashMap<>();
            sub2Epochs.put(3, memberEpoch);  // existing
            sub2Epochs.put(4, memberEpoch);  // existing
            sub2Epochs.put(7, memberEpoch + 1);  // new
            expectedAssignmentEpochs.put(SUBTOPOLOGY_ID2, sub2Epochs);
        } else {
            expectedAssignmentEpochs = Collections.emptyMap();
        }

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnrevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        // Initial assignment epochs for all tasks
        Map<String, Map<Integer, Integer>> initialAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );
        
        // After revocation, tasks 2,4 remain assigned and tasks 1,3 are pending revocation
        // All keep their original epochs
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(initialAssignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 4, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 4)))
                .setTasksPendingRevocation(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1),
                    mkTasks(SUBTOPOLOGY_ID2, 3)))
                .setAssignmentEpochs(assignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnrevokedWithEmptyAssignment(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );

        StreamsGroupMember member =
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, TasksTuple.EMPTY)
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(TasksTuple.EMPTY)
                .setTasksPendingRevocation(
                    mkTasksTuple(
                        taskRole,
                        mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                        mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setAssignmentEpochs(assignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnreleasedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2, 4),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testStableToUnreleasedTasksWithOwnedTasksNotHavingRevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 3, 4))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.STABLE)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 4)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 3, 5)))
            .withCurrentActiveTaskProcessId((subtopologyId, __) ->
                SUBTOPOLOGY_ID2.equals(subtopologyId) ? PROCESS_ID : null
            )
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(taskRole))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 3)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToStable(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 4, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .build();

        // Expected assignment epochs only for remaining tasks (not revoked ones)
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testRemainsInUnrevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        // Assignment epochs for all tasks (assigned + pending revocation)
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 4, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        CurrentAssignmentBuilder currentAssignmentBuilder = new CurrentAssignmentBuilder(
            member)
            .withTargetAssignment(memberEpoch + 2, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of());

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(null)
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedAssignment(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 4, 5, 6)))
                .build()
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToUnrevokedTasks(TaskRole taskRole) {
        final int memberEpoch = 10;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 4, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 2, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withOwnedAssignment(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .build();

        // Expected assignment epochs only for currently assigned and pending revocation tasks
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 5)))
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnrevokedTasksToUnreleasedTasks(TaskRole taskRole) {
        final int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 4, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNREVOKED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch - 1)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 1),
                mkTasks(SUBTOPOLOGY_ID2, 4)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6))
            )
            .build();

        // Expected assignment epochs only for currently assigned tasks (not revoked ones)
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNRELEASED_TASKS)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToStable(TaskRole taskRole) {
        final int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) ->
                Set.of(PROCESS_ID))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(assignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToStableWithNewTasks(TaskRole taskRole) {
        int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();
        
        // Expected assignment epochs include both existing and new tasks
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7))
        );

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                    mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnreleasedTasks(TaskRole taskRole) {
        int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of(PROCESS_ID))
            .build();

        assertEquals(member, updatedMember);
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnreleasedTasksOtherUnreleasedTaskRole(TaskRole taskRole) {
        int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        // The unreleased task is owned by a task of a different role on the same process.
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Set.of() : Set.of(PROCESS_ID))
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> (taskRole == TaskRole.STANDBY)
                    ? Set.of(PROCESS_ID) : Set.of())
            .build();

        assertEquals(member, updatedMember);
    }

    @Test
    public void testUnreleasedTasksToUnreleasedTasksAnyActiveOwner() {
        int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            TaskRole.ACTIVE,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        // The unreleased task remains unreleased, because it is owned by any other instance in
        // an active role, no matter the process.
        // The task that is not unreleased can be assigned.
        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();
        
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            TaskRole.ACTIVE,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7))
        );

        StreamsGroupMember expectedMember = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId(PROCESS_ID)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .setTasksPendingRevocation(TasksTuple.EMPTY)
            .setAssignmentEpochs(expectedAssignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch, mkTasksTuple(TaskRole.ACTIVE,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) ->
                (subtopologyId.equals(SUBTOPOLOGY_ID1) && partitionId == 4) ? "anyOtherProcess"
                    : null)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .build();

        assertEquals(expectedMember, updatedMember);
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnreleasedTasksToUnrevokedTasks(TaskRole taskRole) {
        int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNRELEASED_TASKS)
            .setProcessId("process1")
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2, 3),
                mkTasks(SUBTOPOLOGY_ID2, 5, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 4),
                mkTasks(SUBTOPOLOGY_ID2, 7)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .build();

        // Expected assignment epochs only for currently assigned and pending revocation tasks
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6))
        );

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.UNREVOKED_TASKS)
                .setProcessId("process1")
                .setMemberEpoch(memberEpoch)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 2),
                    mkTasks(SUBTOPOLOGY_ID2, 5)))
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    @ParameterizedTest
    @EnumSource(TaskRole.class)
    public void testUnknownState(TaskRole taskRole) {
        int memberEpoch = 11;
        
        Map<String, Map<Integer, Integer>> assignmentEpochs = mkAssignmentEpochs(
            memberEpoch + 1,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 2, 3, 4)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 5, 6, 7))
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder(MEMBER_NAME)
            .setState(MemberState.UNKNOWN)
            .setMemberEpoch(memberEpoch)
            .setPreviousMemberEpoch(memberEpoch)
            .setProcessId(PROCESS_ID)
            .setAssignedTasks(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .setTasksPendingRevocation(mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 2),
                mkTasks(SUBTOPOLOGY_ID2, 5)))
            .setAssignmentEpochs(assignmentEpochs)
            .build();

        // When the member is in an unknown state, the member is first to force
        // a reset of the client side member state.
        assertThrows(FencedMemberEpochException.class, () -> new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .build());

        // Then the member rejoins with no owned tasks.
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(memberEpoch + 1, mkTasksTuple(taskRole,
                mkTasks(SUBTOPOLOGY_ID1, 3),
                mkTasks(SUBTOPOLOGY_ID2, 6)))
            .withCurrentActiveTaskProcessId((subtopologyId, partitionId) -> PROCESS_ID)
            .withCurrentStandbyTaskProcessIds(
                (subtopologyId, partitionId) -> Set.of())
            .withCurrentWarmupTaskProcessIds((subtopologyId, partitionId) -> Set.of())
            .withOwnedAssignment(mkTasksTuple(taskRole))
            .build();

        // Expected assignment epochs only for assigned tasks
        Map<String, Map<Integer, Integer>> expectedAssignmentEpochs = mkAssignmentEpochs(
            memberEpoch + 1,
            taskRole,
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID1, 3)),
            mkTasksPerSubtopology(mkTasks(SUBTOPOLOGY_ID2, 6))
        );

        assertEquals(
            new StreamsGroupMember.Builder(MEMBER_NAME)
                .setState(MemberState.STABLE)
                .setProcessId(PROCESS_ID)
                .setMemberEpoch(memberEpoch + 1)
                .setPreviousMemberEpoch(memberEpoch)
                .setAssignedTasks(mkTasksTuple(taskRole,
                    mkTasks(SUBTOPOLOGY_ID1, 3),
                    mkTasks(SUBTOPOLOGY_ID2, 6)))
                .setTasksPendingRevocation(TasksTuple.EMPTY)
                .setAssignmentEpochs(expectedAssignmentEpochs)
                .build(),
            updatedMember
        );
    }

    /**
     * Helper method to create assignment epochs from task entries.
     * All partitions get the same epoch value.
     */
    @SafeVarargs
    private final Map<String, Map<Integer, Integer>> mkAssignmentEpochs(
        int epoch,
        Map.Entry<String, Set<Integer>>... taskEntries
    ) {
        Map<String, Map<Integer, Integer>> result = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : taskEntries) {
            Map<Integer, Integer> partitionEpochs = new HashMap<>();
            for (Integer partition : entry.getValue()) {
                partitionEpochs.put(partition, epoch);
            }
            result.put(entry.getKey(), partitionEpochs);
        }
        return result;
    }
}
