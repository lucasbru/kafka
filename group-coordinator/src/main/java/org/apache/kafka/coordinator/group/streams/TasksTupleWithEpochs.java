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

import org.apache.kafka.coordinator.group.generated.StreamsGroupCurrentMemberAssignmentValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An immutable tuple containing active, standby and warm-up tasks with assignment epochs.
 * <p>
 * Active tasks include epoch information to support fencing of zombie commits.
 * Standby and warmup tasks do not have epochs as they don't commit offsets.
 *
 * @param activeTasksWithEpochs Active tasks with their assignment epochs.
 *                              The outer map key is the subtopology ID, the inner map key is the partition ID,
 *                              and the inner map value is the assignment epoch.
 * @param standbyTasks          Standby tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 * @param warmupTasks           Warm-up tasks.
 *                              The key of the map is the subtopology ID, and the value is the set of partition IDs.
 */
public record TasksTupleWithEpochs(Map<String, Map<Integer, Integer>> activeTasksWithEpochs,
                                   Map<String, Set<Integer>> standbyTasks,
                                   Map<String, Set<Integer>> warmupTasks) {

    public TasksTupleWithEpochs {
        activeTasksWithEpochs = deepUnmodifiableMapOfMaps(Objects.requireNonNull(activeTasksWithEpochs));
        standbyTasks = Collections.unmodifiableMap(Objects.requireNonNull(standbyTasks));
        warmupTasks = Collections.unmodifiableMap(Objects.requireNonNull(warmupTasks));
    }

    private static Map<String, Map<Integer, Integer>> deepUnmodifiableMapOfMaps(Map<String, Map<Integer, Integer>> map) {
        Map<String, Map<Integer, Integer>> result = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Integer>> entry : map.entrySet()) {
            result.put(entry.getKey(), Collections.unmodifiableMap(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * An empty task tuple.
     */
    public static final TasksTupleWithEpochs EMPTY = new TasksTupleWithEpochs(
        Map.of(),
        Map.of(),
        Map.of()
    );

    /**
     * Returns a map of active tasks (subtopology ID to partition IDs) by extracting just the keys
     * from the activeTasksWithEpochs map, discarding epoch information.
     * <p>
     * This method creates a new map on each call. Consider using {@link #activeTasksWithEpochs()}
     * directly when possible to avoid the conversion.
     *
     * @return A map of active task partition IDs keyed by subtopology ID.
     */
    public Map<String, Set<Integer>> activeTasks() {
        return activeTasksWithEpochs.entrySet().stream()
            .collect(Collectors.toUnmodifiableMap(
                Map.Entry::getKey,
                entry -> entry.getValue().keySet()
            ));
    }

    /**
     * @return true if all collections in the tuple are empty.
     */
    public boolean isEmpty() {
        return activeTasksWithEpochs.isEmpty() && standbyTasks.isEmpty() && warmupTasks.isEmpty();
    }

    /**
     * Merges this task tuple with another task tuple.
     * For overlapping active tasks, epochs from this tuple take precedence.
     *
     * @param other The other task tuple.
     * @return A new task tuple, containing all active tasks, standby tasks and warm-up tasks from both tuples.
     */
    public TasksTupleWithEpochs merge(TasksTupleWithEpochs other) {
        Map<String, Map<Integer, Integer>> mergedActive = new HashMap<>();
        
        // Add all tasks from this tuple
        this.activeTasksWithEpochs.forEach((subtopologyId, partitionsWithEpochs) -> {
            mergedActive.put(subtopologyId, new HashMap<>(partitionsWithEpochs));
        });
        
        // Add tasks from other tuple, but don't overwrite existing epochs
        other.activeTasksWithEpochs.forEach((subtopologyId, partitionsWithEpochs) -> {
            mergedActive.computeIfAbsent(subtopologyId, k -> new HashMap<>())
                .putAll(partitionsWithEpochs);
        });

        Map<String, Set<Integer>> mergedStandby = mergeSimpleTasks(
            this.standbyTasks,
            other.standbyTasks
        );

        Map<String, Set<Integer>> mergedWarmup = mergeSimpleTasks(
            this.warmupTasks,
            other.warmupTasks
        );

        return new TasksTupleWithEpochs(mergedActive, mergedStandby, mergedWarmup);
    }

    /**
     * Converts this TasksTupleWithEpochs to a TasksTuple by stripping epoch information.
     *
     * @return A TasksTuple containing the same tasks but without epochs.
     */
    public TasksTuple toTasksTuple() {
        Map<String, Set<Integer>> activeTasksOnly = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Integer>> entry : activeTasksWithEpochs.entrySet()) {
            activeTasksOnly.put(entry.getKey(), new HashSet<>(entry.getValue().keySet()));
        }
        return new TasksTuple(activeTasksOnly, standbyTasks, warmupTasks);
    }

    /**
     * Creates a TasksTupleWithEpochs from a TasksTuple and a map of assignment epochs.
     * This is useful for converting from the legacy format or when building from target assignments.
     *
     * @param tasks            The tasks without epochs.
     * @param assignmentEpochs The assignment epochs for active tasks.
     * @return A TasksTupleWithEpochs.
     */
    public static TasksTupleWithEpochs fromTasksAndEpochs(
        TasksTuple tasks,
        Map<String, Map<Integer, Integer>> assignmentEpochs
    ) {
        Map<String, Map<Integer, Integer>> activeTasksWithEpochs = new HashMap<>();
        
        for (Map.Entry<String, Set<Integer>> entry : tasks.activeTasks().entrySet()) {
            String subtopologyId = entry.getKey();
            Set<Integer> partitions = entry.getValue();
            Map<Integer, Integer> partitionsWithEpochs = new HashMap<>();
            
            Map<Integer, Integer> epochsForSubtopology = assignmentEpochs.getOrDefault(subtopologyId, Map.of());
            
            for (Integer partition : partitions) {
                Integer epoch = epochsForSubtopology.get(partition);
                if (epoch == null) {
                    throw new IllegalStateException(
                        "No epoch found for partition " + partition + " in subtopology " + subtopologyId
                    );
                }
                partitionsWithEpochs.put(partition, epoch);
            }
            
            activeTasksWithEpochs.put(subtopologyId, partitionsWithEpochs);
        }
        
        return new TasksTupleWithEpochs(activeTasksWithEpochs, tasks.standbyTasks(), tasks.warmupTasks());
    }

    /**
     * Creates a TasksTupleWithEpochs from a TasksTuple assigning the same default epoch to all active tasks.
     *
     * @param tasks The tasks without epochs.
     * @param defaultEpoch The epoch to assign to all active task partitions.
     * @return A TasksTupleWithEpochs with epochs for all active tasks.
     */
    public static TasksTupleWithEpochs fromTasksWithDefaultEpoch(TasksTuple tasks, int defaultEpoch) {
        Map<String, Map<Integer, Integer>> activeTasksWithEpochs = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> entry : tasks.activeTasks().entrySet()) {
            Map<Integer, Integer> partitionsWithEpochs = new HashMap<>();
            for (Integer partition : entry.getValue()) {
                partitionsWithEpochs.put(partition, defaultEpoch);
            }
            activeTasksWithEpochs.put(entry.getKey(), partitionsWithEpochs);
        }
        return new TasksTupleWithEpochs(activeTasksWithEpochs, tasks.standbyTasks(), tasks.warmupTasks());
    }

    /**
     * Creates a TasksTupleWithEpochs from a current assignment record.
     *
     * @param activeTasks                    The active tasks from the record.
     * @param standbyTasks                   The standby tasks from the record.
     * @param warmupTasks                    The warmup tasks from the record.
     * @param memberEpoch                    The member epoch to use as default for tasks without explicit epochs.
     * @return The TasksTupleWithEpochs
     */
    public static TasksTupleWithEpochs fromCurrentAssignmentRecord(
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> activeTasks,
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> standbyTasks,
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> warmupTasks,
        int memberEpoch
    ) {
        return new TasksTupleWithEpochs(
            parseActiveTasksWithEpochs(activeTasks, memberEpoch),
            parseSimpleTasks(standbyTasks),
            parseSimpleTasks(warmupTasks)
        );
    }

    private static Map<String, Set<Integer>> mergeSimpleTasks(final Map<String, Set<Integer>> tasks1, final Map<String, Set<Integer>> tasks2) {
        HashMap<String, Set<Integer>> result = new HashMap<>();
        tasks1.forEach((subtopologyId, tasks) ->
            result.put(subtopologyId, new HashSet<>(tasks)));
        tasks2.forEach((subtopologyId, tasks) -> result
            .computeIfAbsent(subtopologyId, __ -> new HashSet<>())
            .addAll(tasks));
        return result;
    }

    private static Map<String, Map<Integer, Integer>> parseActiveTasksWithEpochs(
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIdsList,
        int defaultEpoch
    ) {
        Map<String, Map<Integer, Integer>> result = new HashMap<>();
        
        for (StreamsGroupCurrentMemberAssignmentValue.TaskIds taskIds : taskIdsList) {
            String subtopologyId = taskIds.subtopologyId();
            List<Integer> partitions = taskIds.partitions();
            List<Integer> epochs = taskIds.assignmentEpochs();
            
            Map<Integer, Integer> partitionsWithEpochs = new HashMap<>();
            
            if (epochs != null && !epochs.isEmpty()) {
                if (epochs.size() != partitions.size()) {
                    throw new IllegalStateException(
                        "Assignment epochs must be provided for all partitions. " +
                        "Subtopology " + subtopologyId + " has " + partitions.size() + 
                        " partitions but " + epochs.size() + " epochs"
                    );
                }
                
                for (int i = 0; i < partitions.size(); i++) {
                    partitionsWithEpochs.put(partitions.get(i), epochs.get(i));
                }
            } else {
                // Legacy record without epochs: use member epoch as default
                for (Integer partition : partitions) {
                    partitionsWithEpochs.put(partition, defaultEpoch);
                }
            }
            
            result.put(subtopologyId, partitionsWithEpochs);
        }
        
        return result;
    }

    private static Map<String, Set<Integer>> parseSimpleTasks(
        List<StreamsGroupCurrentMemberAssignmentValue.TaskIds> taskIdsList
    ) {
        Map<String, Set<Integer>> result = new HashMap<>();
        
        for (StreamsGroupCurrentMemberAssignmentValue.TaskIds taskIds : taskIdsList) {
            result.put(taskIds.subtopologyId(), new HashSet<>(taskIds.partitions()));
        }
        
        return result;
    }

    @Override
    public String toString() {
        return "(active=" + taskAssignmentToString(activeTasksWithEpochs) +
            ", standby=" + TasksTuple.simpleTaskAssignmentToString(standbyTasks) +
            ", warmup=" + TasksTuple.simpleTaskAssignmentToString(warmupTasks) +
            ')';
    }

    private static String taskAssignmentToString(Map<String, Map<Integer, Integer>> assignment) {
        StringBuilder builder = new StringBuilder("[");
        Iterator<Map.Entry<String, Map<Integer, Integer>>> subtopologyIterator = assignment.entrySet().iterator();
        while (subtopologyIterator.hasNext()) {
            Map.Entry<String, Map<Integer, Integer>> entry = subtopologyIterator.next();
            Iterator<Map.Entry<Integer, Integer>> partitionsIterator = entry.getValue().entrySet().iterator();
            while (partitionsIterator.hasNext()) {
                Map.Entry<Integer, Integer> partitionEpochEntry = partitionsIterator.next();
                builder.append(entry.getKey());
                builder.append("-");
                builder.append(partitionEpochEntry.getKey());
                builder.append("@");
                builder.append(partitionEpochEntry.getValue());
                if (partitionsIterator.hasNext() || subtopologyIterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }
        builder.append("]");
        return builder.toString();
    }
}
