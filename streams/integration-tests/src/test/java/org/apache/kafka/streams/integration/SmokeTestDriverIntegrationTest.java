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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.tests.SmokeTestClient;
import org.apache.kafka.streams.tests.SmokeTestDriver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.tests.SmokeTestDriver.generate;
import static org.apache.kafka.streams.tests.SmokeTestDriver.verify;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlakyCommitClientSupplier implements KafkaClientSupplier {
    private final DefaultKafkaClientSupplier delegate = new DefaultKafkaClientSupplier();
    private final Random random = new Random();
    private final double failureProbability;

    FlakyCommitClientSupplier(final double failureProbability) {
        this.failureProbability = failureProbability;
    }

    @Override
    public Admin getAdmin(final Map<String, Object> config) {
        return delegate.getAdmin(config);
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
        return delegate.getProducer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        return new FlakyCommitConsumer(delegate.getConsumer(config), random, failureProbability);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
        return delegate.getRestoreConsumer(config);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
        return delegate.getGlobalConsumer(config);
    }

    private static class FlakyCommitConsumer implements Consumer<byte[], byte[]> {
        private final Consumer<byte[], byte[]> delegate;
        private final Random random;
        private final double failureProbability;

        FlakyCommitConsumer(final Consumer<byte[], byte[]> delegate, final Random random, final double failureProbability) {
            this.delegate = delegate;
            this.random = random;
            this.failureProbability = failureProbability;
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
            if (random.nextDouble() < failureProbability) {
                throw new TimeoutException("Randomly injected TimeoutException in committed()");
            }
            return delegate.committed(partitions);
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
            if (random.nextDouble() < failureProbability) {
                throw new TimeoutException("Randomly injected TimeoutException in committed()");
            }
            return delegate.committed(partitions, timeout);
        }

        @Override
        public Set<TopicPartition> assignment() {
            return delegate.assignment();
        }

        @Override
        public Set<String> subscription() {
            return delegate.subscription();
        }

        @Override
        public void subscribe(final Collection<String> topics) {
            delegate.subscribe(topics);
        }

        @Override
        public void subscribe(final Collection<String> topics, final org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
            delegate.subscribe(topics, callback);
        }

        @Override
        public void assign(final Collection<TopicPartition> partitions) {
            delegate.assign(partitions);
        }

        @Override
        public void subscribe(final Pattern pattern, final org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
            delegate.subscribe(pattern, callback);
        }

        @Override
        public void subscribe(final Pattern pattern) {
            delegate.subscribe(pattern);
        }

        @Override
        public void subscribe(final SubscriptionPattern pattern, final org.apache.kafka.clients.consumer.ConsumerRebalanceListener callback) {
            delegate.subscribe(pattern, callback);
        }

        @Override
        public void subscribe(final SubscriptionPattern pattern) {
            delegate.subscribe(pattern);
        }

        @Override
        public void unsubscribe() {
            delegate.unsubscribe();
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
            return delegate.poll(timeout);
        }

        @Override
        public void commitSync() {
            delegate.commitSync();
        }

        @Override
        public void commitSync(final Duration timeout) {
            delegate.commitSync(timeout);
        }

        @Override
        public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
            delegate.commitSync(offsets);
        }

        @Override
        public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
            delegate.commitSync(offsets, timeout);
        }

        @Override
        public void commitAsync() {
            delegate.commitAsync();
        }

        @Override
        public void commitAsync(final OffsetCommitCallback callback) {
            delegate.commitAsync(callback);
        }

        @Override
        public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
            delegate.commitAsync(offsets, callback);
        }

        @Override
        public void registerMetricForSubscription(final KafkaMetric metric) {
            delegate.registerMetricForSubscription(metric);
        }

        @Override
        public void unregisterMetricFromSubscription(final KafkaMetric metric) {
            delegate.unregisterMetricFromSubscription(metric);
        }

        @Override
        public void seek(final TopicPartition partition, final long offset) {
            delegate.seek(partition, offset);
        }

        @Override
        public void seek(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
            delegate.seek(partition, offsetAndMetadata);
        }

        @Override
        public void seekToBeginning(final Collection<TopicPartition> partitions) {
            delegate.seekToBeginning(partitions);
        }

        @Override
        public void seekToEnd(final Collection<TopicPartition> partitions) {
            delegate.seekToEnd(partitions);
        }

        @Override
        public long position(final TopicPartition partition) {
            return delegate.position(partition);
        }

        @Override
        public long position(final TopicPartition partition, final Duration timeout) {
            return delegate.position(partition, timeout);
        }

        @Override
        public Uuid clientInstanceId(final Duration timeout) {
            return delegate.clientInstanceId(timeout);
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return delegate.metrics();
        }

        @Override
        public List<PartitionInfo> partitionsFor(final String topic) {
            return delegate.partitionsFor(topic);
        }

        @Override
        public List<PartitionInfo> partitionsFor(final String topic, final Duration timeout) {
            return delegate.partitionsFor(topic, timeout);
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics() {
            return delegate.listTopics();
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics(final Duration timeout) {
            return delegate.listTopics(timeout);
        }

        @Override
        public Set<TopicPartition> paused() {
            return delegate.paused();
        }

        @Override
        public void pause(final Collection<TopicPartition> partitions) {
            delegate.pause(partitions);
        }

        @Override
        public void resume(final Collection<TopicPartition> partitions) {
            delegate.resume(partitions);
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch) {
            return delegate.offsetsForTimes(timestampsToSearch);
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(final Map<TopicPartition, Long> timestampsToSearch, final Duration timeout) {
            return delegate.offsetsForTimes(timestampsToSearch, timeout);
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions) {
            return delegate.beginningOffsets(partitions);
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
            return delegate.beginningOffsets(partitions, timeout);
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
            return delegate.endOffsets(partitions);
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions, final Duration timeout) {
            return delegate.endOffsets(partitions, timeout);
        }

        @Override
        public OptionalLong currentLag(final TopicPartition topicPartition) {
            return delegate.currentLag(topicPartition);
        }

        @Override
        public ConsumerGroupMetadata groupMetadata() {
            return delegate.groupMetadata();
        }

        @Override
        public void enforceRebalance() {
            delegate.enforceRebalance();
        }

        @Override
        public void enforceRebalance(final String reason) {
            delegate.enforceRebalance(reason);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public void close(final Duration timeout) {
            delegate.close(timeout);
        }

        @Override
        public void close(final CloseOptions option) {
            delegate.close(option);
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }
    }
}

@Timeout(600)
@Tag("integration")
public class SmokeTestDriverIntegrationTest {
    private static EmbeddedKafkaCluster cluster = null;
    public TestInfo testInfo;
    private ArrayList<SmokeTestClient> clients = new ArrayList<>();

    @BeforeAll
    public static void startCluster() throws IOException {
        cluster = new EmbeddedKafkaCluster(3);
        cluster.start();
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
        cluster = null;
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    public void shutDown(final TestInfo testInfo) {
        // Clean up clients in case the test failed or timed out
        for (final SmokeTestClient client : clients) {
            if (!client.closed() && !client.error()) {
                client.close();
            }
        }
    }

    private static class Driver extends Thread {
        private final String bootstrapServers;
        private final int numKeys;
        private final int maxRecordsPerKey;
        private Exception exception = null;
        private SmokeTestDriver.VerificationResult result;

        private Driver(final String bootstrapServers, final int numKeys, final int maxRecordsPerKey) {
            this.bootstrapServers = bootstrapServers;
            this.numKeys = numKeys;
            this.maxRecordsPerKey = maxRecordsPerKey;
        }

        @Override
        public void run() {
            try {
                final Map<String, Set<Integer>> allData =
                    generate(bootstrapServers, numKeys, maxRecordsPerKey, Duration.ofSeconds(20));
                result = verify(bootstrapServers, allData, maxRecordsPerKey, false);

            } catch (final Exception ex) {
                this.exception = ex;
            }
        }

        public Exception exception() {
            return exception;
        }

        SmokeTestDriver.VerificationResult result() {
            return result;
        }

    }

    // In this test, we try to keep creating new stream, and closing the old one, to maintain only 3 streams alive.
    // During the new stream added and old stream left, the stream process should still complete without issue.
    // We set 2 timeout condition to fail the test before passing the verification:
    // (1) 10 min timeout, (2) 30 tries of polling without getting any data
    // The processing thread variations where disabled since they triggered a race condition, see KAFKA-19696
    @ParameterizedTest
    @CsvSource({
        "false, false"
    })
    public void shouldWorkWithRebalance(
        final boolean processingThreadsEnabled,
        final boolean streamsProtocolEnabled
    ) throws InterruptedException {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new AssertionError("Test called exit(). code:" + statusCode + " message:" + message);
        });
        Exit.setHaltProcedure((statusCode, message) -> {
            throw new AssertionError("Test called halt(). code:" + statusCode + " message:" + message);
        });
        int numClientsCreated = 0;
        int numDataRecordsProcessed = 0;
        final int numKeys = 10;
        final int maxRecordsPerKey = 1000;

        // Create a client supplier that randomly throws TimeoutException on committed() calls
        final FlakyCommitClientSupplier clientSupplier = new FlakyCommitClientSupplier(0.5);

        IntegrationTestUtils.cleanStateBeforeTest(cluster, SmokeTestDriver.topics());

        final String bootstrapServers = cluster.bootstrapServers();
        final Driver driver = new Driver(bootstrapServers, numKeys, maxRecordsPerKey);
        driver.start();
        System.out.println("started driver");


        final Properties props = new Properties();
        final String appId = safeUniqueTestName(testInfo);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 0);
        props.put(InternalConfig.PROCESSING_THREADS_ENABLED, processingThreadsEnabled);
        if (streamsProtocolEnabled) {
            props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
            // decrease the session timeout so that we can trigger the rebalance soon after old client left closed
            cluster.setGroupSessionTimeout(appId, 10000);
            cluster.setGroupHeartbeatTimeout(appId, 1000);
        } else {
            // decrease the session timeout so that we can trigger the rebalance soon after old client left closed
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        }
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

        // cycle out Streams instances as long as the test is running.
        while (driver.isAlive()) {
            // take a nap
            Thread.sleep(100);

            // add a new client
            final SmokeTestClient smokeTestClient = new SmokeTestClient("streams-" + numClientsCreated++, clientSupplier);
            clients.add(smokeTestClient);
            smokeTestClient.start(props);

            // let the oldest client die of "natural causes"
            if (clients.size() >= 3) {
                final SmokeTestClient client = clients.remove(0);

                client.closeAsync();
                while (!client.closed()) {
                    assertFalse(client.error(), "The streams application seems to have crashed.");
                    Thread.sleep(100);
                }
                numDataRecordsProcessed += client.totalDataRecordsProcessed();
            }
        }

        try {
            // wait for verification to finish
            driver.join();
        } finally {
            // whether or not the assertions failed, tell all the streams instances to stop
            for (final SmokeTestClient client : clients) {
                client.closeAsync();
            }

            // then, wait for them to stop
            for (final SmokeTestClient client : clients) {
                while (!client.closed()) {
                    assertFalse(client.error(), "The streams application seems to have crashed.");
                    Thread.sleep(100);
                }
                numDataRecordsProcessed += client.totalDataRecordsProcessed();
            }
        }

        // check to make sure that it actually succeeded
        if (driver.exception() != null) {
            driver.exception().printStackTrace();
            throw new AssertionError(driver.exception());
        }
        assertTrue(driver.result().passed(), driver.result().result());

        // The one extra record is a record that the driver produces to flush suppress
        final int expectedRecords = numKeys * maxRecordsPerKey + 1;

        // We check that we did no have to reprocess any records, which would indicate a bug since everything
        // runs locally in this test.
        assertEquals(expectedRecords, numDataRecordsProcessed,
            String.format("It seems we had to reprocess records, expected %d records, processed %d records.",
                expectedRecords,
                numDataRecordsProcessed)
        );
    }
}
