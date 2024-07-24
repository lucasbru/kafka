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
import org.apache.kafka.common.message.StreamsHeartbeatRequestData;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTaskAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurrentAssignmentBuilderTest {

    // TODO refine test name after semantics of `withCurrentActiveTaskEpoch` are clarified
    @Test
    public void shouldStayInStableStateAndBumpEpochOnUnmodifiedNewAssignment() {
        int originalEpoch = 10;
        int bumpedEpoch = originalEpoch + 1;

        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        Map<String, Set<Integer>> originalAssigment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 4, 5, 6)
        );
        Map<String, Set<Integer>> unmodifiedAssignment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 4, 5, 6)
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(originalEpoch)
            .setPreviousMemberEpoch(originalEpoch)
            .setAssignedActiveTasks(originalAssigment)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(bumpedEpoch, new Assignment(unmodifiedAssignment))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> originalEpoch)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(bumpedEpoch)
                .setPreviousMemberEpoch(originalEpoch)
                .setAssignedActiveTasks(unmodifiedAssignment)
                .build(),
            updatedMember
        );
    }

    // TODO refine test name after semantics of `withCurrentActiveTaskEpoch` are clarified
    @Test
    public void shouldStayInStableStateAndBumpEpochOnNewAssignmentWithoutTaskRevocation() {
        int originalEpoch = 10;
        int bumpedEpoch = originalEpoch + 1;

        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        Map<String, Set<Integer>> originalAssigment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 10, 11, 12)
        );
        Map<String, Set<Integer>> newAssignment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3, 4),
            mkTaskAssignment(subtopologyId2, 10, 11, 12, 13)
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(originalEpoch)
            .setPreviousMemberEpoch(originalEpoch)
            .setAssignedActiveTasks(originalAssigment)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(bumpedEpoch, new Assignment(newAssignment))
            // what does `-1` mean, and why is this not `originalEpoch` ?
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(bumpedEpoch)
                .setPreviousMemberEpoch(originalEpoch)
                .setAssignedActiveTasks(newAssignment)
                .build(),
            updatedMember
        );
    }

    // TODO refine test name after semantics of `withCurrentActiveTaskEpoch` are clarified
    @Test
    public void shouldTransitFromStableToUnrevokedStateAndBumpEpochTasksOnNewAssignmentWithRevokedTasks() {
        int originalEpoch = 10;
        int bumpedEpoch = originalEpoch + 1;

        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        Map<String, Set<Integer>> originalAssigment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 10, 11, 12)
        );
        Map<String, Set<Integer>> newAssignment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 2, 3, 4),
            mkTaskAssignment(subtopologyId2, 11, 12, 13)
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(originalEpoch)
            .setPreviousMemberEpoch(originalEpoch)
            .setAssignedActiveTasks(originalAssigment)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(bumpedEpoch, new Assignment(newAssignment))
            // -1 ?
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(originalEpoch)
                .setPreviousMemberEpoch(originalEpoch)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 11, 12)))
                .setActiveTasksPendingRevocation(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1),
                    mkTaskAssignment(subtopologyId2, 10)))
                .build(),
            updatedMember
        );
    }

    // TODO refine test name after semantics of `withCurrentActiveTaskEpoch` are clarified
    @Test
    public void shouldTransitFromStableToUnreleasedStateAndBumpEpochOnNewAssignment() {
        int originalEpoch = 10;
        int bumpedEpoch = originalEpoch + 1;

        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        Map<String, Set<Integer>> originalAssigment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 10, 11, 12)
        );
        Map<String, Set<Integer>> newAssignment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3, 4),
            mkTaskAssignment(subtopologyId2, 10, 11, 12, 13)
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(originalEpoch)
            .setPreviousMemberEpoch(originalEpoch)
            .setAssignedActiveTasks(originalAssigment)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(bumpedEpoch, new Assignment(newAssignment))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> originalEpoch)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(bumpedEpoch)
                .setPreviousMemberEpoch(originalEpoch)
                .setAssignedActiveTasks(originalAssigment)
                .build(),
            updatedMember
        );
    }

    // TODO refine test name after semantics of `withCurrentActiveTaskEpoch` are clarified
    @Test
    public void shouldTransitFromStableToUnreleasedStateOnNewAssignmentWithRevokedTasksForOneSubtopologyOnly() {
        int originalEpoch = 10;
        int bumpedEpoch = originalEpoch + 1;

        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        Map<String, Set<Integer>> originalAssigment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 10, 11, 12)
        );
        Map<String, Set<Integer>> newAssignment = mkAssignment(
            mkTaskAssignment(subtopologyId1, 1, 2, 3),
            mkTaskAssignment(subtopologyId2, 10, 11, 13)
        );

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(originalEpoch)
            .setPreviousMemberEpoch(originalEpoch)
            .setAssignedActiveTasks(originalAssigment)
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(bumpedEpoch, new Assignment(newAssignment))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> subtopologyId2.equals(subtopologyId) ? originalEpoch : -1
            )
            .withOwnedActiveTasks(Collections.emptyList())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(bumpedEpoch)
                .setPreviousMemberEpoch(originalEpoch)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 10, 11)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedTasksToStable() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1),
                mkTaskAssignment(subtopologyId2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testRemainsInUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1),
                mkTaskAssignment(subtopologyId2, 4)))
            .build();

        CurrentAssignmentBuilder currentAssignmentBuilder = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1);

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedActiveTasks(null)
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedActiveTasks(Arrays.asList(
                    new StreamsHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId1)
                        .setPartitions(Arrays.asList(1, 2, 3)),
                    new StreamsHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId2)
                        .setPartitions(Arrays.asList(5, 6))))
                .build()
        );

        assertEquals(
            member,
            currentAssignmentBuilder
                .withOwnedActiveTasks(Arrays.asList(
                    new StreamsHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId1)
                        .setPartitions(Arrays.asList(2, 3)),
                    new StreamsHeartbeatRequestData.TaskIds()
                        .setSubtopology(subtopologyId2)
                        .setPartitions(Arrays.asList(4, 5, 6))))
                .build()
        );
    }

    @Test
    public void testUnrevokedTasksToUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 1),
                mkTaskAssignment(subtopologyId2, 4)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 3),
                    mkTaskAssignment(subtopologyId2, 6)))
                .setActiveTasksPendingRevocation(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2),
                    mkTaskAssignment(subtopologyId2, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnrevokedTasksToUnreleasedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNREVOKED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .withOwnedActiveTasks(Arrays.asList(
                new StreamsHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId1)
                    .setPartitions(Arrays.asList(2, 3)),
                new StreamsHeartbeatRequestData.TaskIds()
                    .setSubtopology(subtopologyId2)
                    .setPartitions(Arrays.asList(5, 6))))
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNRELEASED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedTasksToStable() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3),
                    mkTaskAssignment(subtopologyId2, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedTasksToStableWithNewTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> -1)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2, 3, 4),
                    mkTaskAssignment(subtopologyId2, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnreleasedTasksToUnreleasedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3, 4),
                mkTaskAssignment(subtopologyId2, 5, 6, 7))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(member, updatedMember);
    }

    @Test
    public void testUnreleasedTasksToUnrevokedTasks() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNRELEASED_TASKS)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2, 3),
                mkTaskAssignment(subtopologyId2, 5, 6)))
            .build();

        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.UNREVOKED_TASKS)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 3),
                    mkTaskAssignment(subtopologyId2, 6)))
                .setActiveTasksPendingRevocation(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 2),
                    mkTaskAssignment(subtopologyId2, 5)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testUnknownState() {
        String subtopologyId1 = Uuid.randomUuid().toString();
        String subtopologyId2 = Uuid.randomUuid().toString();

        StreamsGroupMember member = new StreamsGroupMember.Builder("member")
            .setState(MemberState.UNKNOWN)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(11)
            .setAssignedActiveTasks(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6)))
            .setActiveTasksPendingRevocation(mkAssignment(
                mkTaskAssignment(subtopologyId1, 2),
                mkTaskAssignment(subtopologyId2, 5)))
            .build();

        // When the member is in an unknown state, the member is first to force
        // a reset of the client side member state.
        assertThrows(FencedMemberEpochException.class, () -> new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 10)
            .build());

        // Then the member rejoins with no owned tasks.
        StreamsGroupMember updatedMember = new CurrentAssignmentBuilder(member)
            .withTargetAssignment(12, new Assignment(mkAssignment(
                mkTaskAssignment(subtopologyId1, 3),
                mkTaskAssignment(subtopologyId2, 6))))
            .withCurrentActiveTaskEpoch((subtopologyId, partitionId) -> 11)
            .withOwnedActiveTasks(Collections.emptyList())
            .build();

        assertEquals(
            new StreamsGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setAssignedActiveTasks(mkAssignment(
                    mkTaskAssignment(subtopologyId1, 3),
                    mkTaskAssignment(subtopologyId2, 6)))
                .build(),
            updatedMember
        );
    }
}
