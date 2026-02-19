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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.apache.kafka.coordinator.group.streams.topics.InternalTopicManager;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark for measuring the performance of {@link InternalTopicManager#configureTopics}.
 *
 * <p>Quick run with a representative subset of parameters:
 * <pre>
 * java -jar jmh-benchmarks/build/libs/kafka-jmh-benchmarks-*.jar \
 *     InternalTopicManagerBenchmark \
 *     -p subtopologyCount=10,100,500 \
 *     -p partitionsPerTopic=100 \
 *     -p topologyType=MIXED \
 *     -p repartitionType=CHAIN \
 *     -p topicExistence=ALL_EXIST \
 *     -f 1 -wi 3 -i 5
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class InternalTopicManagerBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(InternalTopicManagerBenchmark.class);

    @Param({"10", "100", "500"})
    private int subtopologyCount;

    @Param({"10", "100"})
    private int partitionsPerTopic;

    @Param({"MIXED"})
    private InternalTopicManagerBenchmarkUtils.TopologyType topologyType;

    @Param({"NONE", "CHAIN"})
    private InternalTopicManagerBenchmarkUtils.RepartitionType repartitionType;

    @Param({"ALL_EXIST"})
    private InternalTopicManagerBenchmarkUtils.TopicExistence topicExistence;

    private Time time;
    private StreamsTopology topology;
    private CoordinatorMetadataImage metadataImage;

    @Setup(Level.Trial)
    public void setup() {
        time = new MockTime();

        InternalTopicManagerBenchmarkUtils.TopologyAndMetadata topologyAndMetadata =
            InternalTopicManagerBenchmarkUtils.createTopologyAndMetadata(
                subtopologyCount,
                partitionsPerTopic,
                topologyType,
                repartitionType,
                topicExistence
            );

        topology = topologyAndMetadata.topology();
        metadataImage = topologyAndMetadata.metadataImage();
    }

    @Benchmark
    @Threads(1)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void configureTopics(Blackhole blackhole) {
        ConfiguredTopology result = InternalTopicManager.configureTopics(
            LOG,
            "benchmark-group",
            "benchmark-member",
            0,
            topology,
            metadataImage,
            time
        );
        blackhole.consume(result);
    }
}
