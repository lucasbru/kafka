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
package org.apache.kafka.jmh.coordinator;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.SnapshottableCoordinator;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.streams.AssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.AssignmentRefinerImpl;
import org.apache.kafka.coordinator.group.streams.MemberTaskOffsets;
import org.apache.kafka.coordinator.group.streams.NoOpAssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Measures the group coordinator's handling of streams group heartbeats end to end, including the derivation of the
 * intermediate assignment with warm-up tasks by the configured {@link AssignmentRefiner}.
 * <p>
 * A real {@link GroupCoordinatorShard} is driven through {@link GroupCoordinatorShard#streamsGroupHeartbeat} by
 * simulated Kafka Streams clients, and every record a heartbeat returns is replayed into the shard, with a snapshot
 * per heartbeat that is committed right away, as the coordinator runtime would do. Serializing the records and
 * appending them to the log is not part of the measurement.
 * <p>
 * The simulated clients follow the streams rebalance protocol as the Java client implements it: a member joins with
 * member epoch 0 and the topology, reports the tasks it owns only when its reconciled assignment changed since it last
 * reported it, and reconciles every assignment it receives before its next heartbeat. It reports its task changelog
 * offsets and end offsets when it joins, when its reconciled assignment changed, every
 * {@code task.offset.interval.ms / heartbeat.interval.ms} heartbeats (12 with the defaults), and on every heartbeat
 * while it holds a warm-up task within the acceptable recovery lag, and in each of these cases only the ones that
 * changed since it last sent them. Like the client, a member reports an offset for every stateful task it has state
 * for except the active tasks it is processing, which includes the state left behind by tasks it no longer owns, and
 * reports an end offset for every task it is restoring.
 * <p>
 * Restoration is modelled in heartbeat rounds, in each of which every member sends one heartbeat. Every changelog grows
 * by {@link #CHANGELOG_GROWTH_PER_ROUND} records per round, and every standby task, warm-up task and active task that
 * is still restoring catches up from scratch in {@code restoreRounds} rounds. Coordinator time advances by the
 * heartbeat interval per round, spread evenly over the heartbeats of the round, so the assignment interval batches
 * group epoch bumps as it would in a real cluster. The initial rebalance delay is disabled, and timers never fire: a
 * member never misses its session timeout in this model.
 * <p>
 * Every second subtopology is stateful, with one changelog topic, as in AssignmentRefinerBenchmark. With 100
 * partitions per topic, 10 and 100 subtopologies make 500 and 5000 stateful tasks. The refiner is a
 * {@link CountingAssignmentRefiner} wrapping either the {@link NoOpAssignmentRefiner} or the
 * {@link AssignmentRefinerImpl}; each benchmark reports through auxiliary counters how often the refiner was invoked.
 * A group is set up with the {@link NoOpAssignmentRefiner} regardless of the parameter, which is a fixpoint of both
 * refiners once the group has converged, and switched to the configured refiner when the measurement begins.
 * <ul>
 *     <li>{@code stableHeartbeat} measures one heartbeat of a fully converged, stable group. Each operation is the
 *     next heartbeat in a round robin over the members. The counters {@code refineCalls} and {@code heartbeats}
 *     give the refiner invocations per heartbeat.</li>
 *     <li>{@code scaleOutLifecycle} measures the whole migration that follows adding 10% more members, each in a
 *     process of its own, to a converged group: heartbeat rounds run until every member is reconciled to the final
 *     target assignment, no warm-up task is left and the group is stable. The counters give the refiner invocations,
 *     heartbeats and rounds that took.</li>
 * </ul>
 * The default grid takes long; narrow it with -p. With the {@link AssignmentRefinerImpl}, a scale-out of 1000 members
 * and 5000 stateful tasks runs for more than an hour, because two warm-up tasks at a time move about 450 tasks and
 * the refiner runs on every heartbeat of the stable group in between. A fast pass for a quick feedback loop, of about
 * a minute:
 * <pre>
 * java -jar jmh-benchmarks/build/libs/kafka-jmh-benchmarks-*.jar StreamsGroupHeartbeatBenchmark.stableHeartbeat \
 *     -p memberCount=1000 -p subtopologyCount=100 -f 1 -wi 1 -w 2s -i 3 -r 2s
 * java -jar jmh-benchmarks/build/libs/kafka-jmh-benchmarks-*.jar StreamsGroupHeartbeatBenchmark.scaleOutLifecycle \
 *     -p memberCount=100 -p subtopologyCount=10 -f 1 -wi 1 -i 3
 * </pre>
 * The counters of the summary rows are summed over the iterations, so for the single shot lifecycle benchmark they
 * have to be divided by the iteration count to get the counts per operation.
 */
@Fork(value = 1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class StreamsGroupHeartbeatBenchmark {

    private static final String GROUP_ID = "streams-group";

    /**
     * The fraction of members a scale-out adds to the group.
     */
    private static final double SCALE_OUT_FRACTION = 0.1;

    /**
     * The number of rounds after which a lifecycle that has not converged fails the benchmark.
     */
    private static final int MAX_ROUNDS = 2000;

    /**
     * The end offset every changelog starts at, which is the number of records a copy of a task restores from scratch.
     */
    private static final long CHANGELOG_START_END_OFFSET = 1_000_000L;

    /**
     * The number of records every changelog grows by per round, through the active tasks processing records.
     */
    private static final long CHANGELOG_GROWTH_PER_ROUND = 1_000L;

    private static final int HEARTBEAT_INTERVAL_MS = GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT;

    private static final long ACCEPTABLE_RECOVERY_LAG = GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_DEFAULT;

    /**
     * The number of heartbeats after which a member reports its task offsets even if nothing else made it do so.
     */
    private static final int HEARTBEATS_PER_TASK_OFFSET_INTERVAL =
        GroupCoordinatorConfig.STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS_DEFAULT / HEARTBEAT_INTERVAL_MS;

    /**
     * The default of max.poll.interval.ms, which the client sends as rebalance timeout.
     */
    private static final int REBALANCE_TIMEOUT_MS = 300_000;

    public enum RefinerType {
        NO_OP,
        WARMUP
    }

    /**
     * An {@link AssignmentRefiner} that counts its invocations and delegates to the refiner selected through
     * {@link #configure}. The group coordinator instantiates the refiner from its configuration, so the selection and
     * the counter are static.
     */
    public static class CountingAssignmentRefiner implements AssignmentRefiner {

        private static final AssignmentRefiner NO_OP = new NoOpAssignmentRefiner();

        private static volatile RefinerType refinerType = RefinerType.NO_OP;

        /**
         * Whether to delegate to the {@link NoOpAssignmentRefiner} without counting, which is used while setting up a
         * group.
         */
        private static volatile boolean settingUp = true;

        private static long refineCalls = 0;

        private final AssignmentRefiner delegate;

        public CountingAssignmentRefiner() {
            this.delegate = refinerType == RefinerType.WARMUP ? new AssignmentRefinerImpl() : new NoOpAssignmentRefiner();
        }

        static void configure(RefinerType type) {
            refinerType = type;
            settingUp = true;
        }

        static void startMeasuring() {
            settingUp = false;
        }

        static long refineCalls() {
            return refineCalls;
        }

        @Override
        public Map<String, TasksTuple> refine(
            Map<String, StreamsGroupMember> members,
            Map<String, TasksTuple> targetAssignment,
            Map<String, MemberTaskOffsets> taskOffsets,
            SortedMap<String, ConfiguredSubtopology> subtopologies,
            int numWarmupReplicas,
            long acceptableRecoveryLag
        ) {
            if (settingUp) {
                return NO_OP.refine(members, targetAssignment, taskOffsets, subtopologies, numWarmupReplicas, acceptableRecoveryLag);
            }
            refineCalls++;
            return delegate.refine(members, targetAssignment, taskOffsets, subtopologies, numWarmupReplicas, acceptableRecoveryLag);
        }

        @Override
        public boolean isAffectedByTaskOffsetsChange(
            MemberTaskOffsets previous,
            MemberTaskOffsets current,
            long acceptableRecoveryLag
        ) {
            return delegate.isAffectedByTaskOffsetsChange(previous, current, acceptableRecoveryLag);
        }
    }

    @State(Scope.Benchmark)
    public static class GroupParams {

        @Param({"100", "1000"})
        int memberCount;

        @Param({"1"})
        int membersPerProcess;

        @Param({"10", "100"})
        int subtopologyCount;

        @Param({"100"})
        int partitionCount;

        @Param({"1"})
        int standbyReplicas;

        @Param({"2"})
        int numWarmupReplicas;

        @Param({"3"})
        int restoreRounds;

        @Param({"NO_OP", "WARMUP"})
        RefinerType refiner;
    }

    /**
     * Counters reported next to the time of each benchmark. They are totals per iteration; for the single shot
     * lifecycle benchmark an iteration is one operation.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class Counters {
        public long refineCalls;
        public long heartbeats;
        public long rounds;

        @Setup(Level.Iteration)
        public void reset() {
            refineCalls = 0;
            heartbeats = 0;
            rounds = 0;
        }
    }

    /**
     * A fully converged, stable group whose heartbeats are measured one by one.
     */
    @State(Scope.Benchmark)
    public static class StableGroup {
        GroupSimulation simulation;
        int nextMember = 0;

        @Setup(Level.Trial)
        public void setup(GroupParams params) {
            simulation = GroupSimulation.converged(params);
            // A round with the configured refiner checks that the converged group is a fixpoint of it.
            int changed = simulation.runRound();
            if (changed != 0) {
                throw new IllegalStateException(changed + " members got a new assignment in a converged group.");
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            simulation.close();
        }
    }

    /**
     * A converged group that is rebuilt for every scale-out.
     */
    @State(Scope.Thread)
    public static class ScaleOutGroup {
        GroupParams params;
        GroupSimulation simulation;

        @Setup(Level.Invocation)
        public void setup(GroupParams params) {
            this.params = params;
            simulation = GroupSimulation.converged(params);
        }

        @TearDown(Level.Invocation)
        public void tearDown() {
            simulation.close();
        }
    }

    @Benchmark
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void stableHeartbeat(StableGroup group, Counters counters) {
        GroupSimulation simulation = group.simulation;
        long refineCallsBefore = CountingAssignmentRefiner.refineCalls();
        SimulatedMember member = simulation.members.get(group.nextMember);
        if (simulation.heartbeat(member)) {
            throw new IllegalStateException("Member " + member.memberId + " got a new assignment in a converged group.");
        }
        if (++group.nextMember == simulation.members.size()) {
            group.nextMember = 0;
            simulation.round++;
            counters.rounds++;
        }
        counters.heartbeats++;
        counters.refineCalls += CountingAssignmentRefiner.refineCalls() - refineCallsBefore;
    }

    @Benchmark
    @Threads(1)
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void scaleOutLifecycle(ScaleOutGroup group, Counters counters) {
        GroupSimulation simulation = group.simulation;
        long refineCallsBefore = CountingAssignmentRefiner.refineCalls();
        long heartbeatsBefore = simulation.heartbeats;
        int newMembers = Math.max(1, (int) Math.round(group.params.memberCount * SCALE_OUT_FRACTION));
        simulation.addMembers(newMembers);
        counters.rounds += simulation.runUntilConverged(false);
        counters.heartbeats += simulation.heartbeats - heartbeatsBefore;
        counters.refineCalls += CountingAssignmentRefiner.refineCalls() - refineCallsBefore;
    }

    /**
     * A task of the topology.
     */
    private record Task(String subtopologyId, int partition) { }

    /**
     * The tasks assigned to a member.
     */
    private record Assignment(Set<Task> active, Set<Task> standby, Set<Task> warmup) {
        static final Assignment EMPTY = new Assignment(Set.of(), Set.of(), Set.of());
    }

    /**
     * A group coordinator shard hosting one streams group, and the simulated clients of the group's members.
     */
    static final class GroupSimulation {
        private final GroupParams params;
        private final MockTime time = new MockTime();
        private final Metrics metrics = new Metrics();
        private final GroupCoordinatorShard shard;
        private final SnapshottableCoordinator<GroupCoordinatorShard, CoordinatorRecord> coordinator;
        private final RequestContext requestContext;
        private final StreamsGroupHeartbeatRequestData.Topology topology;
        private final Set<String> statefulSubtopologies = new HashSet<>();
        final List<SimulatedMember> members = new ArrayList<>();
        private long nextOffset = 1;
        private long lastCommittedOffset = 0;
        private long timeInRoundMs = 0;
        long round = 0;
        long heartbeats = 0;
        private int processCount = 0;

        private GroupSimulation(GroupParams params) {
            this.params = params;

            List<String> sourceTopics = new ArrayList<>();
            for (int i = 0; i < params.subtopologyCount; i++) {
                sourceTopics.add("topic-" + i);
            }
            List<String> allTopics = new ArrayList<>(sourceTopics);
            List<StreamsGroupHeartbeatRequestData.Subtopology> subtopologies = new ArrayList<>();
            for (int i = 0; i < sourceTopics.size(); i++) {
                String subtopologyId = String.valueOf(i);
                StreamsGroupHeartbeatRequestData.Subtopology subtopology = new StreamsGroupHeartbeatRequestData.Subtopology()
                    .setSubtopologyId(subtopologyId)
                    .setSourceTopics(List.of(sourceTopics.get(i)));
                if (i % 2 == 0) {
                    String changelogTopic = sourceTopics.get(i) + "-store-changelog";
                    allTopics.add(changelogTopic);
                    subtopology.setStateChangelogTopics(List.of(
                        new StreamsGroupHeartbeatRequestData.TopicInfo().setName(changelogTopic)));
                    statefulSubtopologies.add(subtopologyId);
                }
                subtopologies.add(subtopology);
            }
            topology = new StreamsGroupHeartbeatRequestData.Topology().setEpoch(0).setSubtopologies(subtopologies);
            // The changelog topics exist already, so that the topology is ready without creating internal topics.
            CoordinatorMetadataImage metadataImage = createMetadataImage(allTopics, params.partitionCount);

            Map<String, Object> props = new HashMap<>();
            props.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_REFINER_CLASS_CONFIG, CountingAssignmentRefiner.class);
            props.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, params.standbyReplicas);
            props.put(GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_CONFIG, Math.max(params.standbyReplicas,
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT));
            props.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_WARMUP_REPLICAS_CONFIG, params.numWarmupReplicas);
            props.put(GroupCoordinatorConfig.STREAMS_GROUP_MAX_WARMUP_REPLICAS_CONFIG, Math.max(params.numWarmupReplicas,
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_WARMUP_REPLICAS_DEFAULT));
            props.put(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 0);
            GroupCoordinatorConfig config = GroupCoordinatorConfig.fromProps(props);

            LogContext logContext = new LogContext();
            SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
            TopicPartition topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);
            shard = new GroupCoordinatorShard.Builder(config, new GroupConfigManager(config, ShareGroupConfig.fromProps(props)))
                .withAuthorizerPlugin(Optional.empty())
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withTimer(new ParkedTimer())
                .withExecutor(new MockCoordinatorExecutor<>())
                .withCoordinatorMetrics(new GroupCoordinatorMetrics(new MetricsRegistry(), metrics))
                .withTopicPartition(topicPartition)
                .build();
            coordinator = new SnapshottableCoordinator<>(logContext, snapshotRegistry, shard, topicPartition);
            shard.onLoaded(metadataImage);

            requestContext = new RequestContext(
                new RequestHeader(ApiKeys.STREAMS_GROUP_HEARTBEAT, ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion(), "client", 0),
                "1",
                InetAddress.getLoopbackAddress(),
                KafkaPrincipal.ANONYMOUS,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                ClientInformation.EMPTY,
                false
            );
        }

        private static CoordinatorMetadataImage createMetadataImage(List<String> topics, int partitionCount) {
            MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
            for (String topic : topics) {
                Uuid topicId = Uuid.randomUuid();
                delta.replay(new TopicRecord().setTopicId(topicId).setName(topic));
                for (int partition = 0; partition < partitionCount; partition++) {
                    delta.replay(new PartitionRecord()
                        .setTopicId(topicId)
                        .setPartitionId(partition)
                        .setReplicas(List.of(partition % 4, (partition + 1) % 4)));
                }
            }
            return new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
        }

        /**
         * Sets up a group of params.memberCount members that has converged and whose members all have restored their
         * state and reported it, and switches to the configured refiner.
         */
        static GroupSimulation converged(GroupParams params) {
            CountingAssignmentRefiner.configure(params.refiner);
            GroupSimulation simulation = new GroupSimulation(params);
            simulation.addMembers(params.memberCount);
            simulation.runUntilConverged(true);
            // Let every member report the state it restored, and spread the members' task offset reports over the
            // interval, as they would be after members joined at different times.
            simulation.runRounds(HEARTBEATS_PER_TASK_OFFSET_INTERVAL);
            for (SimulatedMember member : simulation.members) {
                member.heartbeatsSinceOffsets = member.index % HEARTBEATS_PER_TASK_OFFSET_INTERVAL;
            }
            CountingAssignmentRefiner.startMeasuring();
            return simulation;
        }

        void close() {
            metrics.close();
        }

        long changelogEndOffset() {
            return changelogEndOffset(round);
        }

        static long changelogEndOffset(long round) {
            return CHANGELOG_START_END_OFFSET + CHANGELOG_GROWTH_PER_ROUND * round;
        }

        long restoredPerRound() {
            return CHANGELOG_START_END_OFFSET / params.restoreRounds + CHANGELOG_GROWTH_PER_ROUND;
        }

        boolean isStateful(Task task) {
            return statefulSubtopologies.contains(task.subtopologyId());
        }

        void addMembers(int count) {
            int firstProcess = processCount;
            for (int i = 0; i < count; i++) {
                int index = members.size();
                String processId = "process-" + (firstProcess + i / params.membersPerProcess);
                members.add(new SimulatedMember(index, "member-" + index, processId, round));
            }
            processCount = firstProcess + (count + params.membersPerProcess - 1) / params.membersPerProcess;
        }

        void runRounds(int rounds) {
            for (int i = 0; i < rounds; i++) {
                runRound();
            }
        }

        /**
         * Lets every member heartbeat once, in order.
         *
         * @return The number of members whose assignment changed.
         */
        int runRound() {
            int changed = 0;
            for (SimulatedMember member : members) {
                if (heartbeat(member)) {
                    changed++;
                }
            }
            round++;
            return changed;
        }

        /**
         * Runs heartbeat rounds until the group has converged.
         *
         * @param restored Whether every member must also have restored all of its tasks.
         *
         * @return The number of rounds it took.
         */
        int runUntilConverged(boolean restored) {
            for (int rounds = 1; rounds <= MAX_ROUNDS; rounds++) {
                int changed = runRound();
                if (changed == 0 && membersLookConverged(restored) && isConverged()) {
                    return rounds;
                }
            }
            throw new IllegalStateException("The group did not converge within " + MAX_ROUNDS + " rounds: " + describeProgress());
        }

        private String describeProgress() {
            StreamsGroupDescribeResponseData.DescribedGroup group =
                shard.streamsGroupDescribe(List.of(GROUP_ID), lastCommittedOffset).describedGroups().get(0);
            int unreconciled = 0;
            for (StreamsGroupDescribeResponseData.Member member : group.members()) {
                if (!describedTasks(member.assignment().activeTasks()).equals(describedTasks(member.targetAssignment().activeTasks()))
                    || !describedTasks(member.assignment().standbyTasks()).equals(describedTasks(member.targetAssignment().standbyTasks()))
                    || !member.assignment().warmupTasks().isEmpty()) {
                    unreconciled++;
                }
            }
            int delayed = 0;
            int withWarmups = 0;
            int notRestored = 0;
            for (SimulatedMember member : members) {
                delayed += member.assignmentDelayed ? 1 : 0;
                withWarmups += member.assignment.warmup().isEmpty() ? 0 : 1;
                notRestored += member.isRestored(this) ? 0 : 1;
            }
            return "group state " + group.groupState() + ", group epoch " + group.groupEpoch() + ", assignment epoch "
                + group.assignmentEpoch() + ", " + unreconciled + " members not reconciled to their target assignment, "
                + delayed + " members with a delayed assignment, " + withWarmups + " members with warm-up tasks, "
                + notRestored + " members still restoring.";
        }

        private boolean membersLookConverged(boolean restored) {
            int epoch = members.get(0).memberEpoch;
            for (SimulatedMember member : members) {
                if (member.memberEpoch != epoch || member.assignmentDelayed || !member.assignment.warmup().isEmpty()) {
                    return false;
                }
                if (restored && !member.isRestored(this)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Whether the group is stable and every member owns exactly its final target assignment.
         */
        private boolean isConverged() {
            StreamsGroupDescribeResponseData.DescribedGroup group =
                shard.streamsGroupDescribe(List.of(GROUP_ID), lastCommittedOffset).describedGroups().get(0);
            if (!group.groupState().equals("Stable") || group.groupEpoch() != group.assignmentEpoch()) {
                return false;
            }
            for (StreamsGroupDescribeResponseData.Member member : group.members()) {
                StreamsGroupDescribeResponseData.Assignment assignment = member.assignment();
                StreamsGroupDescribeResponseData.Assignment target = member.targetAssignment();
                if (!assignment.warmupTasks().isEmpty()
                    || !describedTasks(assignment.activeTasks()).equals(describedTasks(target.activeTasks()))
                    || !describedTasks(assignment.standbyTasks()).equals(describedTasks(target.standbyTasks()))) {
                    return false;
                }
            }
            return true;
        }

        private static Set<Task> describedTasks(List<StreamsGroupDescribeResponseData.TaskIds> taskIds) {
            Set<Task> tasks = new HashSet<>();
            for (StreamsGroupDescribeResponseData.TaskIds ids : taskIds) {
                for (int partition : ids.partitions()) {
                    tasks.add(new Task(ids.subtopologyId(), partition));
                }
            }
            return tasks;
        }

        /**
         * Sends one heartbeat of the given member, replays the records it produced and lets the member reconcile the
         * assignment it got back.
         *
         * @return Whether the member's assignment changed.
         */
        boolean heartbeat(SimulatedMember member) {
            // Coordinator time advances by one heartbeat interval per round.
            long nextTimeInRoundMs = (long) HEARTBEAT_INTERVAL_MS * (member.index + 1) / members.size();
            if (nextTimeInRoundMs > timeInRoundMs) {
                time.sleep(nextTimeInRoundMs - timeInRoundMs);
            }
            timeInRoundMs = member.index + 1 == members.size() ? 0 : nextTimeInRoundMs;

            member.restore(this);
            StreamsGroupHeartbeatRequestData request = member.buildRequest(this);
            CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
                shard.streamsGroupHeartbeat(requestContext, request);
            replay(result);
            heartbeats++;
            return member.onResponse(this, result.response().data());
        }

        private void replay(CoordinatorResult<?, CoordinatorRecord> result) {
            List<CoordinatorRecord> records = result.records();
            if (records.isEmpty() || !result.replayRecords()) {
                return;
            }
            for (CoordinatorRecord record : records) {
                coordinator.replay(nextOffset++, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, record);
            }
            coordinator.updateLastWrittenOffset(nextOffset);
            coordinator.updateLastCommittedOffset(nextOffset);
            lastCommittedOffset = nextOffset;
        }
    }

    /**
     * The client of one member, modelled after the Java client's streams membership and heartbeat managers.
     */
    static final class SimulatedMember {
        final int index;
        final String memberId;
        final String processId;
        int memberEpoch = 0;
        int endpointInformationEpoch = 0;
        boolean assignmentDelayed = false;
        /**
         * The assignment the member reconciled, which is always the last one it received.
         */
        Assignment assignment = Assignment.EMPTY;
        /**
         * The assignment the member last reported as owned, or null before it joined.
         */
        Assignment lastSentAssignment = null;
        /**
         * The changelog position of every stateful task the member has state for, including tasks it no longer owns.
         */
        final Map<Task, Long> positions = new HashMap<>();
        /**
         * The active tasks that finished restoring and are processing records.
         */
        final Set<Task> processing = new HashSet<>();
        long lastRestoreRound;
        Map<Task, Long> lastSentOffsets = Map.of();
        Map<Task, Long> lastSentEndOffsets = Map.of();
        int heartbeatsSinceOffsets = 0;

        SimulatedMember(int index, String memberId, String processId, long round) {
            this.index = index;
            this.memberId = memberId;
            this.processId = processId;
            this.lastRestoreRound = round;
        }

        /**
         * Advances the restoration of the member's tasks to the current round.
         */
        void restore(GroupSimulation simulation) {
            long rounds = simulation.round - lastRestoreRound;
            if (rounds <= 0) {
                return;
            }
            lastRestoreRound = simulation.round;
            long endOffset = simulation.changelogEndOffset();
            long restored = simulation.restoredPerRound() * rounds;
            for (Task task : assignment.active()) {
                if (!simulation.isStateful(task)) {
                    continue;
                }
                if (processing.contains(task) || restoreTo(task, endOffset, restored) == endOffset) {
                    positions.put(task, endOffset);
                    processing.add(task);
                }
            }
            for (Task task : assignment.standby()) {
                if (simulation.isStateful(task)) {
                    restoreTo(task, endOffset, restored);
                }
            }
            for (Task task : assignment.warmup()) {
                if (simulation.isStateful(task)) {
                    restoreTo(task, endOffset, restored);
                }
            }
        }

        private long restoreTo(Task task, long endOffset, long restored) {
            long position = Math.min(endOffset, positions.getOrDefault(task, 0L) + restored);
            positions.put(task, position);
            return position;
        }

        /**
         * Whether the member is processing all of its stateful active tasks and all of its standby tasks were caught up
         * with the changelog when it last restored.
         */
        boolean isRestored(GroupSimulation simulation) {
            long endOffset = GroupSimulation.changelogEndOffset(lastRestoreRound);
            for (Task task : assignment.standby()) {
                Long position = positions.get(task);
                if (simulation.isStateful(task) && (position == null || position != endOffset)) {
                    return false;
                }
            }
            for (Task task : assignment.active()) {
                if (simulation.isStateful(task) && !processing.contains(task)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * The offsets of all state the member holds, except for the active tasks it is processing.
         */
        private Map<Task, Long> taskOffsets() {
            Map<Task, Long> offsets = new HashMap<>(positions);
            offsets.keySet().removeAll(processing);
            return offsets;
        }

        /**
         * The end offsets of all tasks the member is restoring.
         */
        private Map<Task, Long> taskEndOffsets(GroupSimulation simulation) {
            long endOffset = simulation.changelogEndOffset();
            Map<Task, Long> endOffsets = new HashMap<>();
            for (Task task : assignment.active()) {
                if (simulation.isStateful(task) && !processing.contains(task)) {
                    endOffsets.put(task, endOffset);
                }
            }
            for (Task task : assignment.standby()) {
                if (simulation.isStateful(task)) {
                    endOffsets.put(task, endOffset);
                }
            }
            for (Task task : assignment.warmup()) {
                if (simulation.isStateful(task)) {
                    endOffsets.put(task, endOffset);
                }
            }
            return endOffsets;
        }

        private boolean hasCaughtUpWarmupTask(GroupSimulation simulation) {
            long endOffset = simulation.changelogEndOffset();
            for (Task task : assignment.warmup()) {
                Long position = positions.get(task);
                if (position != null && endOffset - position <= ACCEPTABLE_RECOVERY_LAG) {
                    return true;
                }
            }
            return false;
        }

        StreamsGroupHeartbeatRequestData buildRequest(GroupSimulation simulation) {
            StreamsGroupHeartbeatRequestData request = new StreamsGroupHeartbeatRequestData()
                .setGroupId(GROUP_ID)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setEndpointInformationEpoch(endpointInformationEpoch);

            Map<Task, Long> offsets = taskOffsets();
            Map<Task, Long> endOffsets = taskEndOffsets(simulation);
            if (lastSentAssignment == null) {
                request
                    .setTopology(simulation.topology)
                    .setRebalanceTimeoutMs(REBALANCE_TIMEOUT_MS)
                    .setProcessId(processId)
                    .setClientTags(List.of())
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())
                    .setTaskOffsets(toTaskOffsets(offsets))
                    .setTaskEndOffsets(toTaskOffsets(endOffsets));
                lastSentAssignment = Assignment.EMPTY;
                lastSentOffsets = offsets;
                lastSentEndOffsets = endOffsets;
                heartbeatsSinceOffsets = 0;
                return request;
            }

            heartbeatsSinceOffsets++;
            boolean assignmentChanged = !assignment.equals(lastSentAssignment);
            if (assignmentChanged) {
                request
                    .setActiveTasks(toTaskIds(assignment.active()))
                    .setStandbyTasks(toTaskIds(assignment.standby()))
                    .setWarmupTasks(toTaskIds(assignment.warmup()));
                lastSentAssignment = assignment;
            }
            if (assignmentChanged
                || heartbeatsSinceOffsets >= HEARTBEATS_PER_TASK_OFFSET_INTERVAL
                || hasCaughtUpWarmupTask(simulation)) {
                if (!offsets.equals(lastSentOffsets)) {
                    request.setTaskOffsets(toTaskOffsets(offsets));
                    lastSentOffsets = offsets;
                    heartbeatsSinceOffsets = 0;
                }
                if (!endOffsets.equals(lastSentEndOffsets)) {
                    request.setTaskEndOffsets(toTaskOffsets(endOffsets));
                    lastSentEndOffsets = endOffsets;
                    heartbeatsSinceOffsets = 0;
                }
            }
            return request;
        }

        /**
         * Takes the response to a heartbeat and reconciles the assignment in it, if any.
         *
         * @return Whether the member's assignment changed.
         */
        boolean onResponse(GroupSimulation simulation, StreamsGroupHeartbeatResponseData response) {
            assignmentDelayed = false;
            for (StreamsGroupHeartbeatResponseData.Status status : response.status()) {
                if (status.statusCode() == StreamsGroupHeartbeatResponse.Status.ASSIGNMENT_DELAYED.code()) {
                    assignmentDelayed = true;
                } else {
                    throw new IllegalStateException("Member " + memberId + " got status " + status.statusCode()
                        + ": " + status.statusDetail());
                }
            }
            memberEpoch = response.memberEpoch();
            endpointInformationEpoch = response.endpointInformationEpoch();
            if (response.activeTasks() == null) {
                return false;
            }
            Assignment newAssignment = new Assignment(
                fromTaskIds(response.activeTasks()),
                fromTaskIds(response.standbyTasks()),
                fromTaskIds(response.warmupTasks())
            );
            if (newAssignment.equals(assignment)) {
                return false;
            }
            processing.retainAll(newAssignment.active());
            assignment = newAssignment;
            return true;
        }

        private static List<StreamsGroupHeartbeatRequestData.TaskOffset> toTaskOffsets(Map<Task, Long> offsets) {
            List<StreamsGroupHeartbeatRequestData.TaskOffset> taskOffsets = new ArrayList<>(offsets.size());
            offsets.forEach((task, offset) -> taskOffsets.add(new StreamsGroupHeartbeatRequestData.TaskOffset()
                .setSubtopologyId(task.subtopologyId())
                .setPartition(task.partition())
                .setOffset(offset)));
            return taskOffsets;
        }

        private static List<StreamsGroupHeartbeatRequestData.TaskIds> toTaskIds(Set<Task> tasks) {
            Map<String, List<Integer>> partitionsBySubtopology = new HashMap<>();
            for (Task task : tasks) {
                partitionsBySubtopology.computeIfAbsent(task.subtopologyId(), id -> new ArrayList<>()).add(task.partition());
            }
            List<StreamsGroupHeartbeatRequestData.TaskIds> taskIds = new ArrayList<>(partitionsBySubtopology.size());
            partitionsBySubtopology.forEach((subtopologyId, partitions) -> taskIds.add(
                new StreamsGroupHeartbeatRequestData.TaskIds().setSubtopologyId(subtopologyId).setPartitions(partitions)));
            return taskIds;
        }

        private static Set<Task> fromTaskIds(List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds) {
            Set<Task> tasks = new HashSet<>();
            for (StreamsGroupHeartbeatResponseData.TaskIds ids : taskIds) {
                for (int partition : ids.partitions()) {
                    tasks.add(new Task(ids.subtopologyId(), partition));
                }
            }
            return tasks;
        }
    }

    /**
     * A timer that keeps track of the scheduled timeouts without ever firing them. Within the simulated rounds every
     * member heartbeats well within its session timeout, so no timeout would fire anyway.
     */
    private static final class ParkedTimer implements CoordinatorTimer<CoordinatorRecord> {
        private final Set<String> scheduled = new HashSet<>();

        @Override
        public void schedule(String key, long delay, TimeUnit unit, boolean retry, TimeoutOperation<CoordinatorRecord> operation) {
            scheduled.add(key);
        }

        @Override
        public void schedule(String key, long delay, TimeUnit unit, boolean retry, long retryBackoff,
                             TimeoutOperation<CoordinatorRecord> operation) {
            scheduled.add(key);
        }

        @Override
        public void scheduleIfAbsent(String key, long delay, TimeUnit unit, boolean retry,
                                     TimeoutOperation<CoordinatorRecord> operation) {
            scheduled.add(key);
        }

        @Override
        public void cancel(String key) {
            scheduled.remove(key);
        }

        @Override
        public boolean isScheduled(String key) {
            return scheduled.contains(key);
        }
    }
}
