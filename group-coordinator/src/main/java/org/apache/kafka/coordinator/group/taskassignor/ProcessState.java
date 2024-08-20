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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.apache.kafka.common.utils.Utils.union;

public class ProcessState {
    private final String processId;
    // number of members
    private int capacity;
    private double load;
    private final Map<String, Integer> memberToTaskCounts;

    private final Set<TaskId> assignedActiveTasks;
    private final Set<TaskId> assignedStandbyTasks;


    ProcessState(final String processId) {
        this.processId = processId;
        this.capacity = 0;
        this.load = Double.MAX_VALUE;
        this.assignedActiveTasks = new HashSet<>();
        this.assignedStandbyTasks = new HashSet<>();
        this.memberToTaskCounts = new HashMap<>();
    }


    public String processId() {
        return processId;
    }

    public int capacity() {
        return capacity;
    }

    public int totalTaskCount() {
        return assignedStandbyTasks.size() + assignedActiveTasks.size();
    }

    public double load() {
        return load;
    }

    public Map<String, Integer> memberToTaskCounts() {
        return memberToTaskCounts;
    }

    public Set<TaskId> assignedActiveTasks() {
        return assignedActiveTasks;
    }

    public Set<TaskId> assignedStandbyTasks() {
        return assignedStandbyTasks;
    }

    public void addTask(final String memberId, final TaskId taskId, final boolean isActive) {
        if (isActive) {
            assignedActiveTasks.add(taskId);
        } else {
            assignedStandbyTasks.add(taskId);
        }
        memberToTaskCounts.put(memberId, memberToTaskCounts.get(memberId) + 1);
        computeLoad();
    }

    private void incrementCapacity() {
        capacity++;
        computeLoad();
    }
    public void computeLoad() {
        if (capacity <= 0) {
            this.load = -1;
        } else {
            this.load = (double) totalTaskCount() / capacity;
        }
    }

    public void addMember(final String member) {
        this.memberToTaskCounts.put(member, 0);
        incrementCapacity();
    }

    public boolean hasCapacity() {
        return totalTaskCount() < capacity;
    }

    public int compareTo(final ProcessState other) {
        int loadCompare = Double.compare(this.load, other.load());
        if (loadCompare == 0) {
            return Integer.compare(other.capacity, this.capacity);
        }
        return loadCompare;
    }

    public boolean hasTask(final TaskId taskId) {
        return assignedActiveTasks.contains(taskId) || assignedStandbyTasks.contains(taskId);    }


    Set<TaskId> assignedTasks() {
        final Set<TaskId> assignedActiveTaskIds = assignedActiveTasks();
        final Set<TaskId> assignedStandbyTaskIds = assignedStandbyTasks();
        return unmodifiableSet(
                union(
                        () -> new HashSet<>(assignedActiveTaskIds.size() + assignedStandbyTaskIds.size()),
                        assignedActiveTaskIds,
                        assignedStandbyTaskIds
                )
        );
    }
}
