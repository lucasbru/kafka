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

package org.apache.kafka.coordinator.group.taskassignor;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StickyTaskAssignor implements TaskAssignor {

    public static final String STICKY_ASSIGNOR_NAME = "sticky";
    private final boolean mustPreserveActiveTaskAssignment;
    private static final Logger log = LoggerFactory.getLogger(StickyTaskAssignor.class);

    // helper data structures:
    private TaskPairs taskPairs;
    Map<TaskId, Owner> activeTaskToPrevOwner;
    Map<TaskId, Set<Owner>> standbyTaskToPrevOwner;
    Map<String, ProcessState> processIdToState;

    int allTasks;
    int totalCapacity;
    int tasksPerProcess;

    // results/outputs:
    /**
     * The standby assignments keyed by member id
     */
    Map<String, Set<TaskId>> standbyTasksAssignments;

    /**
     * The active assignments keyed by member id
     */
    Map<String, Set<TaskId>> activeTasksAssignments;


    public StickyTaskAssignor() {
        this(false);
    }
    public StickyTaskAssignor(boolean mustPreserveActiveTaskAssignment) {
        this.mustPreserveActiveTaskAssignment = mustPreserveActiveTaskAssignment;
    }

    @Override
    public String name() {
        return STICKY_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) throws TaskAssignorException {

        initialize(groupSpec, topologyDescriber);

        //active
        Set<TaskId> activeTasks = buildTasks(groupSpec, topologyDescriber, true);
        assignActive(activeTasks);

        //standby
        final int numStandbyReplicas =
                groupSpec.assignmentConfigs().isEmpty() ? 0
                        : Integer.parseInt(groupSpec.assignmentConfigs().get("numStandbyReplicas"));
        if (numStandbyReplicas > 0) {
            Set<TaskId> statefulTasks = buildTasks(groupSpec, topologyDescriber, false);
            assignStandby(statefulTasks, numStandbyReplicas);
        }

        return buildGroupAssignment(groupSpec.members().keySet());
    }

    private Set<TaskId> buildTasks(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber, final boolean isActive) {
        Set<TaskId> ret = new HashSet<>();
        for (String subtopology : groupSpec.subtopologies()) {
            if (isActive || topologyDescriber.isStateful(subtopology)) {
                int numberOfPartitions = topologyDescriber.numPartitions(subtopology);
                for (int i = 0; i < numberOfPartitions; i++) {
                    ret.add(new TaskId(subtopology, i));
                }
            }
        }
        return ret;
    }

    private void initialize(final GroupSpec groupSpec, final TopologyDescriber topologyDescriber) {
        activeTasksAssignments = new HashMap<>();
        standbyTasksAssignments = new HashMap<>();

        allTasks = 0;
        for (String subtopology : groupSpec.subtopologies()) {
            int numberOfPartitions = topologyDescriber.numPartitions(subtopology);
            allTasks += numberOfPartitions;
        }
        totalCapacity = groupSpec.members().size();
        computeTasksPerProcess();

        taskPairs = new TaskPairs(allTasks * (allTasks - 1) / 2);

        processIdToState = new HashMap<>();
        activeTaskToPrevOwner = new HashMap<>();
        standbyTaskToPrevOwner = new HashMap<>();
        for (Map.Entry<String, AssignmentMemberSpec> memberEntry : groupSpec.members().entrySet()) {
            final String memberId = memberEntry.getKey();
            final String processId = memberEntry.getValue().processId();
            final Owner owner = new Owner(processId, memberId);
            final AssignmentMemberSpec memberSpec = memberEntry.getValue();

            processIdToState.putIfAbsent(processId, new ProcessState(processId));
            processIdToState.get(processId).addMember(memberId);

            // prev active tasks
            for (Map.Entry<String, Set<Integer>> entry : memberSpec.activeTasks().entrySet()) {
                Set<Integer> partitionNoSet = entry.getValue();
                for (int partitionNo : partitionNoSet) {
                    activeTaskToPrevOwner.put(new TaskId(entry.getKey(), partitionNo), owner);
                }
            }

            // prev standby tasks
            for (Map.Entry<String, Set<Integer>> entry : memberSpec.standbyTasks().entrySet()) {
                Set<Integer> partitionNoSet = entry.getValue();
                for (int partitionNo : partitionNoSet) {
                    TaskId taskId = new TaskId(entry.getKey(), partitionNo);
                    standbyTaskToPrevOwner.putIfAbsent(taskId, new HashSet<>());
                    standbyTaskToPrevOwner.get(taskId).add(owner);
                }
            }
        }
    }

    private GroupAssignment buildGroupAssignment(final Set<String> members) {
        final Map<String, MemberAssignment> memberAssignments = new HashMap<>();

        for (String memberId : members) {
            Map<String, Set<Integer>> activeTasks = new HashMap<>();
            if (activeTasksAssignments.containsKey(memberId)) {
                activeTasks = buildTasks(activeTasksAssignments.get(memberId));
            }
            Map<String, Set<Integer>> standByTasks = new HashMap<>();
            if (standbyTasksAssignments.containsKey(memberId)) {
                standByTasks = buildTasks(standbyTasksAssignments.get(memberId));
            }
            memberAssignments.put(memberId, new MemberAssignment(activeTasks, standByTasks, new HashMap<>()));
        }

        return new GroupAssignment(memberAssignments);
    }

    private Map<String, Set<Integer>> buildTasks(final Set<TaskId> taskIds) {
        Map<String, Set<Integer>> ret = new HashMap<>();
        for (TaskId taskId : taskIds) {
            ret.putIfAbsent(taskId.subtopologyId(), new HashSet<>());
            ret.get(taskId.subtopologyId()).add(taskId.partition());
        }
        return ret;
    }

    private void assignActive(final Set<TaskId> activeTasks) {

        // 1. re-assigning existing active tasks to clients that previously had the same active tasks
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Owner prevOwner = activeTaskToPrevOwner.get(task);
            if (prevOwner != null && (hasCapacity(prevOwner) || mustPreserveActiveTaskAssignment)) {
                activeTasksAssignments.putIfAbsent(prevOwner.memberId, new HashSet<>());
                activeTasksAssignments.get(prevOwner.memberId).add(task);
                updateHelpers(prevOwner, task, true);
                it.remove();
            }
        }

        // 2. re-assigning tasks to clients that previously have seen the same task (as standby task)
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Set<Owner> prevOwners = standbyTaskToPrevOwner.get(task);
            if (prevOwners != null && !prevOwners.isEmpty()) {
                final Owner prevOwner = findOwnerWithLeastLoad(prevOwners, task, true);
                if (prevOwner != null && hasCapacity(prevOwner)) {
                    activeTasksAssignments.putIfAbsent(prevOwner.memberId, new HashSet<>());
                    activeTasksAssignments.get(prevOwner.memberId).add(task);
                    updateHelpers(prevOwner, task, true);
                    it.remove();
                }
            }
        }

        // 3. assign any remaining unassigned tasks
        for (Iterator<TaskId> it = activeTasks.iterator(); it.hasNext();) {
            final TaskId task = it.next();
            final Owner owner = findOwnerWithLeastLoad(task);
            if (owner != null) {
                activeTasksAssignments.putIfAbsent(owner.memberId, new HashSet<>());
                activeTasksAssignments.get(owner.memberId).add(task);
                it.remove();
                updateHelpers(owner, task, true);
            }
        }
    }

    private void maybeUpdateTasksPerProcess(final int activeTasksNo) {
        if (activeTasksNo == tasksPerProcess) {
            totalCapacity--;
            allTasks -= activeTasksNo;
            computeTasksPerProcess();
        }
    }

    private Owner findOwnerWithLeastLoad(final Set<Owner> owners, TaskId taskId, final boolean returnSameMember) {
        Set<Owner> rightPairs = owners.stream()
                .filter(owner -> taskPairs.hasNewPair(taskId, processIdToState.get(owner.processId).assignedTasks()))
                .collect(Collectors.toSet());
        if (rightPairs.isEmpty()) {
            rightPairs = owners;
        }
        Optional<ProcessState> processWithLeastLoad = rightPairs.stream()
                .map(owner -> processIdToState.get(owner.processId))
                .min(Comparator.comparingDouble(ProcessState::load));

        assert processWithLeastLoad.isPresent();
        // if the same exact former member is needed
        if (returnSameMember) {
            return standbyTaskToPrevOwner.get(taskId).stream()
                    .filter(standby -> standby.processId.equals(processWithLeastLoad.get().processId()))
                    .findFirst()
                    .orElseGet(() -> ownerWithLeastLoad(processWithLeastLoad.get()));
        }
        return ownerWithLeastLoad(processWithLeastLoad.get());
    }

    private Owner findOwnerWithLeastLoad(final TaskId taskId) {
        Set<Owner> allOwners = processIdToState.entrySet().stream()
                .flatMap(entry -> entry.getValue().memberToTaskCounts().keySet().stream()
                .map(memberId -> new Owner(entry.getKey(), memberId)))
                .collect(Collectors.toSet());
        return findOwnerWithLeastLoad(allOwners, taskId, false);
    }

    private Owner findOwnerWithLeastLoad(final TaskId taskId, final Set<String> processes) {
        Set<Owner> allOwners = processes.stream()
                .flatMap(processId -> processIdToState.get(processId).memberToTaskCounts().keySet().stream()
                .map(memberId -> new Owner(processId, memberId)))
                .collect(Collectors.toSet());
        return findOwnerWithLeastLoad(allOwners, taskId, false);
    }

    private Owner ownerWithLeastLoad(final ProcessState processWithLeastLoad) {
        Optional<String> memberWithLeastLoad = processWithLeastLoad.memberToTaskCounts().entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
        return memberWithLeastLoad.map(memberId -> new Owner(processWithLeastLoad.processId(), memberId)).orElse(null);
    }

    private boolean hasCapacity(final Owner owner) {
        return processIdToState.get(owner.processId).assignedActiveTasks().size() < tasksPerProcess;
    }

    private void assignStandby(final Set<TaskId> standbyTasks, final int numStandbyReplicas) {
        for (TaskId task : standbyTasks) {
            for (int i = 0; i < numStandbyReplicas; i++) {
                final Set<String> availableProcesses = findAllowedProcesses(task);
                if (availableProcesses.isEmpty()) {
                    log.warn("Unable to assign " + (numStandbyReplicas - i) +
                            " of " + numStandbyReplicas + " standby tasks for task [" + task + "]. " +
                            "There is not enough available capacity. You should " +
                            "increase the number of threads and/or application instances " +
                            "to maintain the requested number of standby replicas.");
                    break;
                }
                Owner standby = null;

                // prev active task
                Owner prevOwner = activeTaskToPrevOwner.get(task);
                if (prevOwner != null && availableProcesses.contains(prevOwner.processId) && isLoadBalanced(prevOwner.processId)
                        && taskPairs.hasNewPair(task, processIdToState.get(prevOwner.processId).assignedTasks())) {
                    standby = prevOwner;
                }

                // prev standby tasks
                if (standby == null) {
                    final Set<Owner> prevOwners = standbyTaskToPrevOwner.get(task);
                    if (prevOwners != null && !prevOwners.isEmpty()) {
                        prevOwners.removeIf(owner -> !availableProcesses.contains(owner.processId));
                        prevOwner = findOwnerWithLeastLoad(prevOwners, task, true);
                        if (prevOwner != null && isLoadBalanced(prevOwner.processId)) {
                            standby = prevOwner;
                        }
                    }
                }

                // others
                if (standby == null) {
                    standby = findOwnerWithLeastLoad(task, availableProcesses);
                }

                standbyTasksAssignments.putIfAbsent(standby.memberId, new HashSet<>());
                standbyTasksAssignments.get(standby.memberId).add(task);
                updateHelpers(standby, task, false);
            }

        }
    }

    private boolean isLoadBalanced(final String processId) {
        final ProcessState process = processIdToState.get(processId);
        return process.hasCapacity() || isLeastLoadedProcess(process.load());
    }

    private boolean isLeastLoadedProcess(final double load) {
        return processIdToState.values().stream()
                .allMatch(process -> process.load() >= load);
    }

    private Set<String> findAllowedProcesses(final TaskId taskId) {
        return processIdToState.values().stream()
                .filter(process -> !process.hasTask(taskId))
                .map(ProcessState::processId)
                .collect(Collectors.toSet());
    }

    private void updateHelpers(final Owner owner, final TaskId taskId, final boolean isActive) {
        // add all pair combinations: update taskPairs
        taskPairs.addPairs(taskId, processIdToState.get(owner.processId).assignedTasks());

        ProcessState process = processIdToState.get(owner.processId);
        process.addTask(owner.memberId, taskId, isActive);

        if (isActive) {
            // update task per process
            maybeUpdateTasksPerProcess(process.assignedActiveTasks().size());
        }
    }

    private void computeTasksPerProcess() {
        if (totalCapacity == 0) {
            tasksPerProcess = 0;
            return;
        }
        tasksPerProcess = allTasks / totalCapacity;
        if (allTasks - (tasksPerProcess * totalCapacity) > 0) {
            tasksPerProcess++;
        }
    }

    private static class TaskPairs {
        private final Set<Pair> pairs;
        private final int maxPairs;

        TaskPairs(final int maxPairs) {
            this.maxPairs = maxPairs;
            this.pairs = new HashSet<>(maxPairs);
        }

        boolean hasNewPair(final TaskId task1,
                           final Set<TaskId> taskIds) {
            if (pairs.size() == maxPairs) {
                return false;
            }
            if (taskIds.size() == 0) {
                return true;
            }
            for (final TaskId taskId : taskIds) {
                if (!pairs.contains(pair(task1, taskId))) {
                    return true;
                }
            }
            return false;
        }

        void addPairs(final TaskId taskId, final Set<TaskId> assigned) {
            for (final TaskId id : assigned) {
                if (!id.equals(taskId))
                    pairs.add(pair(id, taskId));
            }
        }

        Pair pair(final TaskId task1, final TaskId task2) {
            if (task1.compareTo(task2) < 0) {
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }


        private static class Pair {
            private final TaskId task1;
            private final TaskId task2;

            Pair(final TaskId task1, final TaskId task2) {
                this.task1 = task1;
                this.task2 = task2;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final Pair pair = (Pair) o;
                return Objects.equals(task1, pair.task1) &&
                        Objects.equals(task2, pair.task2);
            }

            @Override
            public int hashCode() {
                return Objects.hash(task1, task2);
            }
        }
    }

    static class Owner {
        private final String processId;
        private final String memberId;

        public Owner(final String processId, final String memberId) {
            this.processId = processId;
            this.memberId = memberId;
        }
    }
}
