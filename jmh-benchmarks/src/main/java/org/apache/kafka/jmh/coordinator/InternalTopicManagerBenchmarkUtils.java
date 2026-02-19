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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for creating test data for {@link InternalTopicManagerBenchmark}.
 */
public class InternalTopicManagerBenchmarkUtils {

    /**
     * Topology type determines the presence of changelog topics.
     */
    public enum TopologyType {
        /** No changelog topics - only source topics */
        STATELESS,
        /** Every subtopology has a changelog topic */
        STATEFUL,
        /** Every other subtopology has a changelog topic */
        MIXED
    }

    /**
     * Repartition topology structure.
     */
    public enum RepartitionType {
        /** No repartition topics */
        NONE,
        /** Repartition topics form a chain (each subtopology reads from previous subtopology's repartition sink) */
        CHAIN
    }

    /**
     * Internal topic existence in metadata.
     */
    public enum TopicExistence {
        /** All internal topics already exist in metadata */
        ALL_EXIST,
        /** No internal topics exist in metadata (need to be created) */
        ALL_MISSING
    }

    /**
     * Container for topology and metadata image used in benchmarks.
     */
    public record TopologyAndMetadata(StreamsTopology topology, CoordinatorMetadataImage metadataImage) { }

    /**
     * Creates a topology and corresponding metadata image for benchmarking.
     *
     * @param subtopologyCount    The number of subtopologies in the topology.
     * @param partitionsPerTopic  The number of partitions for each topic.
     * @param topologyType        The type of topology (stateless, stateful, or mixed).
     * @param repartitionType     The type of repartition topology (none or chain).
     * @param topicExistence      Whether internal topics exist in the metadata.
     * @return A TopologyAndMetadata containing the topology and metadata image.
     */
    public static TopologyAndMetadata createTopologyAndMetadata(
        int subtopologyCount,
        int partitionsPerTopic,
        TopologyType topologyType,
        RepartitionType repartitionType,
        TopicExistence topicExistence
    ) {
        Map<String, StreamsGroupTopologyValue.Subtopology> subtopologyMap = new HashMap<>();
        MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);

        for (int i = 0; i < subtopologyCount; i++) {
            String subtopologyId = "subtopology-" + i;
            String sourceTopicName = "source-topic-" + i;

            // Add source topic to metadata
            addTopic(delta, Uuid.randomUuid(), sourceTopicName, partitionsPerTopic);

            StreamsGroupTopologyValue.Subtopology subtopology = new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId(subtopologyId)
                .setSourceTopics(List.of(sourceTopicName));

            List<StreamsGroupTopologyValue.TopicInfo> repartitionSourceTopics = new ArrayList<>();
            List<String> repartitionSinkTopics = new ArrayList<>();

            // Add repartition topics based on repartition type
            if (repartitionType == RepartitionType.CHAIN && i > 0) {
                // This subtopology reads from the previous subtopology's repartition sink
                String repartitionSourceTopicName = "repartition-topic-" + (i - 1);
                repartitionSourceTopics.add(
                    new StreamsGroupTopologyValue.TopicInfo()
                        .setName(repartitionSourceTopicName)
                        .setPartitions(0) // Flexible partition count
                );

                // Add repartition source topic to metadata if it should exist
                if (topicExistence == TopicExistence.ALL_EXIST) {
                    addTopic(delta, Uuid.randomUuid(), repartitionSourceTopicName, partitionsPerTopic);
                }
            }

            if (repartitionType == RepartitionType.CHAIN && i < subtopologyCount - 1) {
                // This subtopology writes to a repartition sink
                String repartitionSinkTopicName = "repartition-topic-" + i;
                repartitionSinkTopics.add(repartitionSinkTopicName);
            }

            subtopology.setRepartitionSourceTopics(repartitionSourceTopics);
            subtopology.setRepartitionSinkTopics(repartitionSinkTopics);

            // Add changelog topics based on topology type
            List<StreamsGroupTopologyValue.TopicInfo> changelogTopics = new ArrayList<>();
            boolean hasChangelog = switch (topologyType) {
                case STATELESS -> false;
                case STATEFUL -> true;
                case MIXED -> i % 2 == 0;
            };

            if (hasChangelog) {
                String changelogTopicName = "changelog-topic-" + i;
                changelogTopics.add(
                    new StreamsGroupTopologyValue.TopicInfo()
                        .setName(changelogTopicName)
                        .setPartitions(0) // Partition count derived from source topics
                );

                // Add changelog topic to metadata if it should exist
                if (topicExistence == TopicExistence.ALL_EXIST) {
                    addTopic(delta, Uuid.randomUuid(), changelogTopicName, partitionsPerTopic);
                }
            }
            subtopology.setStateChangelogTopics(changelogTopics);

            // Add copartition groups - all source topics and repartition source topics are copartitioned
            List<StreamsGroupTopologyValue.CopartitionGroup> copartitionGroups = new ArrayList<>();
            if (!repartitionSourceTopics.isEmpty()) {
                List<Short> sourceTopicIndices = new ArrayList<>();
                sourceTopicIndices.add((short) 0); // The source topic at index 0

                List<Short> repartitionSourceTopicIndices = new ArrayList<>();
                for (int j = 0; j < repartitionSourceTopics.size(); j++) {
                    repartitionSourceTopicIndices.add((short) j);
                }

                copartitionGroups.add(
                    new StreamsGroupTopologyValue.CopartitionGroup()
                        .setSourceTopics(sourceTopicIndices)
                        .setRepartitionSourceTopics(repartitionSourceTopicIndices)
                );
            }
            subtopology.setCopartitionGroups(copartitionGroups);

            subtopologyMap.put(subtopologyId, subtopology);
        }

        StreamsTopology topology = new StreamsTopology(1, subtopologyMap);
        CoordinatorMetadataImage metadataImage = new KRaftCoordinatorMetadataImage(
            delta.apply(MetadataProvenance.EMPTY)
        );

        return new TopologyAndMetadata(topology, metadataImage);
    }

    private static void addTopic(
        MetadataDelta delta,
        Uuid topicId,
        String topicName,
        int numPartitions
    ) {
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        for (int i = 0; i < numPartitions; i++) {
            delta.replay(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(i)
                .setReplicas(List.of(i % 4, (i + 1) % 4)));
        }
    }
}
