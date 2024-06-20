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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.Assignment;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.HostInfo;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.Subtopology;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.TaskId;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.TopicInfo;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.common.message.StreamsHeartbeatResponseData;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.StreamsHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsHeartbeatResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_MAX_POLL_INTERVAL_MS;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamsHeartbeatRequestManagerTest {

    public static final String TEST_GROUP_ID = "testGroupId";
    public static final String TEST_MEMBER_ID = "testMemberId";
    public static final int TEST_MEMBER_EPOCH = 5;
    public static final String TEST_INSTANCE_ID = "instanceId";
    public static final int TEST_THROTTLE_TIME_MS = 5;
    private StreamsHeartbeatRequestManager heartbeatRequestManager;

    private Time time;

    private StreamsAssignmentInterface streamsAssignmentInterface;

    private ConsumerConfig config;

    @Mock
    private CoordinatorRequestManager coordinatorRequestManager;

    @Mock
    private StreamsInitializeRequestManager streamsInitializeRequestManager;

    @Mock
    private StreamsPrepareAssignmentRequestManager streamsPrepareAssignmentRequestManager;

    @Mock
    private MembershipManager membershipManager;

    @Mock
    private BackgroundEventHandler backgroundEventHandler;

    @Mock
    private Metrics metrics;

    @Mock
    private ConsumerMetadata metadata;

    // Static data for testing
    private final UUID processID = new UUID(1, 1);

    private final HostInfo endPoint = new HostInfo("localhost", 9092);

    private final String assignor = "test";

    private final Map<String, Subtopology> subtopologyMap = new HashMap<>();

    private final Map<String, Object> assignmentConfiguration = new HashMap<>();

    private final Map<String, String> clientTags = new HashMap<>();

    private final Node coordinatorNode = new Node(1, "localhost", 9092);

    @BeforeEach
    void setUp() {
        config = config();

        subtopologyMap.clear();
        assignmentConfiguration.clear();
        clientTags.clear();
        streamsAssignmentInterface =
            new StreamsAssignmentInterface(
                processID,
                endPoint,
                assignor,
                subtopologyMap,
                assignmentConfiguration,
                clientTags
            );
        LogContext logContext = new LogContext("test");
        time = new MockTime();

        MockitoAnnotations.openMocks(this);
        when(metrics.sensor(anyString())).thenReturn(mock(Sensor.class));
        heartbeatRequestManager = new StreamsHeartbeatRequestManager(
            logContext,
            time,
            config,
            coordinatorRequestManager,
            streamsInitializeRequestManager,
            streamsPrepareAssignmentRequestManager,
            membershipManager,
            backgroundEventHandler,
            metrics,
            streamsAssignmentInterface,
            metadata
        );

        when(membershipManager.groupId()).thenReturn(TEST_GROUP_ID);
        when(membershipManager.memberId()).thenReturn(TEST_MEMBER_ID);
        when(membershipManager.memberEpoch()).thenReturn(TEST_MEMBER_EPOCH);
        when(membershipManager.groupInstanceId()).thenReturn(Optional.of(TEST_INSTANCE_ID));
    }


    @Test
    void testNoHeartbeatIfCoordinatorUnknown() {
        when(membershipManager.shouldHeartbeatNow()).thenReturn(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.empty());

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(0, result.unsentRequests.size());
        verify(membershipManager).onHeartbeatRequestSkipped();
    }

    @Test
    void testNoHeartbeatIfHeartbeatSkipped() {
        when(membershipManager.shouldSkipHeartbeat()).thenReturn(true);
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(0, result.unsentRequests.size());
        verify(membershipManager).onHeartbeatRequestSkipped();
    }

    @Test
    void testHeartbeatWhenCoordinatorKnown() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());

        StreamsHeartbeatRequest request = (StreamsHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();

        assertEquals(TEST_GROUP_ID, request.data().groupId());
        assertEquals(TEST_MEMBER_ID, request.data().memberId());
        assertEquals(TEST_MEMBER_EPOCH, request.data().memberEpoch());
        assertEquals(TEST_INSTANCE_ID, request.data().instanceId());

        // Static information is null
        assertNull(request.data().processId());
        assertNull(request.data().hostInfo());
        assertNull(request.data().assignor());
        assertNull(request.data().assignmentConfigs());
        assertNull(request.data().clientTags());
    }

    @Test
    void testFullStaticInformationWhenJoining() {
        mockJoiningState();
        assignmentConfiguration.put("config1", "value1");
        clientTags.put("clientTag1", "value2");

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());

        StreamsHeartbeatRequest request = (StreamsHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();

        assertEquals(processID.toString(), request.data().processId());
        assertEquals(endPoint.host, request.data().hostInfo().host());
        assertEquals(endPoint.port, request.data().hostInfo().port());
        assertEquals(assignor, request.data().assignor());
        assertEquals(1, request.data().assignmentConfigs().size());
        assertEquals("config1", request.data().assignmentConfigs().get(0).key());
        assertEquals("value1", request.data().assignmentConfigs().get(0).value());
        assertEquals(1, request.data().clientTags().size());
        assertEquals("clientTag1", request.data().clientTags().get(0).key());
        assertEquals("value2", request.data().clientTags().get(0).value());
    }

    @Test
    void testShutdownRequested() {
        mockJoiningState();
        streamsAssignmentInterface.requestShutdown();

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        assertEquals(Optional.of(coordinatorNode), result.unsentRequests.get(0).node());

        StreamsHeartbeatRequest request = (StreamsHeartbeatRequest) result.unsentRequests.get(0).requestBuilder().build();

        assertEquals(true, request.data().shutdownApplication());
    }

    @Test
    void testSuccessfulResponse() {
        mockJoiningState();

        final Uuid uuid0 = Uuid.randomUuid();
        final Uuid uuid1 = Uuid.randomUuid();

        final TopicInfo emptyTopicInfo = new TopicInfo(Optional.empty(), Collections.emptyMap());

        when(metadata.topicIds()).thenReturn(
            mkMap(
                mkEntry("source0", uuid0),
                mkEntry("repartition0", uuid1)
            ));

        streamsAssignmentInterface.subtopologyMap().put("0",
            new Subtopology(
                Collections.singleton("source0"),
                Collections.singleton("sink0"),
                Collections.singletonMap("repartition0", emptyTopicInfo),
                Collections.singletonMap("changelog0", emptyTopicInfo)
            ));
        streamsAssignmentInterface.subtopologyMap().put("1",
            new Subtopology(
                Collections.singleton("source1"),
                Collections.singleton("sink1"),
                Collections.singletonMap("repartition1", emptyTopicInfo),
                Collections.singletonMap("changelog1", emptyTopicInfo)
            ));
        streamsAssignmentInterface.subtopologyMap().put("2",
            new Subtopology(
                Collections.singleton("source2"),
                Collections.singleton("sink2"),
                Collections.singletonMap("repartition2", emptyTopicInfo),
                Collections.singletonMap("changelog2", emptyTopicInfo)
            ));

        StreamsHeartbeatResponseData data = new StreamsHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setMemberId(TEST_MEMBER_ID)
            .setMemberEpoch(TEST_MEMBER_EPOCH)
            .setThrottleTimeMs(TEST_THROTTLE_TIME_MS)
            .setHeartbeatIntervalMs(1000)
            .setActiveTasks(Collections.singletonList(
                new StreamsHeartbeatResponseData.TaskIds().setSubtopology("0").setPartitions(Collections.singletonList(0))))
            .setStandbyTasks(Collections.singletonList(
                new StreamsHeartbeatResponseData.TaskIds().setSubtopology("1").setPartitions(Collections.singletonList(1))))
            .setWarmupTasks(Collections.singletonList(
                new StreamsHeartbeatResponseData.TaskIds().setSubtopology("2").setPartitions(Collections.singletonList(2))));

        mockResponse(data);

        ArgumentCaptor<ConsumerGroupHeartbeatResponseData> captor = ArgumentCaptor.forClass(ConsumerGroupHeartbeatResponseData.class);
        verify(membershipManager, times(1)).onHeartbeatSuccess(captor.capture());
        ConsumerGroupHeartbeatResponseData response = captor.getValue();
        assertEquals(Errors.NONE.code(), response.errorCode());
        assertEquals(TEST_MEMBER_ID, response.memberId());
        assertEquals(TEST_MEMBER_EPOCH, response.memberEpoch());
        assertEquals(TEST_THROTTLE_TIME_MS, response.throttleTimeMs());
        assertEquals(1000, response.heartbeatIntervalMs());
        final List<TopicPartitions> tps = response.assignment().topicPartitions();
        assertEquals(2, tps.size());
        assertEquals(mkSet(uuid0, uuid1), tps.stream().map(TopicPartitions::topicId).collect(Collectors.toSet()));
        assertEquals(Collections.singletonList(0), tps.get(0).partitions());
        assertEquals(Collections.singletonList(0), tps.get(1).partitions());

        final Assignment targetAssignment = streamsAssignmentInterface.targetAssignment.get();
        assertEquals(1, targetAssignment.activeTasks.size());
        final TaskId activeTaskId = targetAssignment.activeTasks.stream().findFirst().get();
        assertEquals(activeTaskId.subtopologyId, "0");
        assertEquals(activeTaskId.partitionId, 0);

        assertEquals(1, targetAssignment.standbyTasks.size());
        final TaskId standbyTaskId = targetAssignment.standbyTasks.stream().findFirst().get();
        assertEquals(standbyTaskId.subtopologyId, "1");
        assertEquals(standbyTaskId.partitionId, 1);

        assertEquals(1, targetAssignment.warmupTasks.size());
        final TaskId warmupTaskId = targetAssignment.warmupTasks.stream().findFirst().get();
        assertEquals(warmupTaskId.subtopologyId, "2");
        assertEquals(warmupTaskId.partitionId, 2);

    }


    @Test
    void testPrepareAssignment() {
        mockJoiningState();

        StreamsHeartbeatResponseData data = new StreamsHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setMemberEpoch(TEST_MEMBER_EPOCH)
            .setShouldComputeAssignment(true);

        mockResponse(data);

        verify(streamsPrepareAssignmentRequestManager).prepareAssignment();
    }

    @Test
    void testInitializeTopology() {
        mockJoiningState();

        StreamsHeartbeatResponseData data = new StreamsHeartbeatResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(0)
            .setMemberEpoch(TEST_MEMBER_EPOCH)
            .setShouldInitializeTopology(true);

        mockResponse(data);

        verify(streamsInitializeRequestManager).initialize();
    }

    private void mockResponse(final StreamsHeartbeatResponseData data) {

        NetworkClientDelegate.PollResult result = heartbeatRequestManager.poll(time.milliseconds());

        assertEquals(1, result.unsentRequests.size());
        final UnsentRequest unsentRequest = result.unsentRequests.get(0);
        assertEquals(Optional.of(coordinatorNode), unsentRequest.node());

        ClientResponse response = createHeartbeatResponse(unsentRequest, data);

        unsentRequest.handler().onComplete(response);
    }

    private void mockJoiningState() {
        when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(coordinatorNode));
        when(membershipManager.state()).thenReturn(MemberState.JOINING);
    }

    private ClientResponse createHeartbeatResponse(
        final NetworkClientDelegate.UnsentRequest request,
        final StreamsHeartbeatResponseData data
    ) {
        StreamsHeartbeatResponse response = new StreamsHeartbeatResponse(data);
        return new ClientResponse(
            new RequestHeader(ApiKeys.STREAMS_HEARTBEAT, ApiKeys.STREAMS_HEARTBEAT.latestVersion(), "client-id", 1),
            request.handler(),
            "0",
            time.milliseconds(),
            time.milliseconds(),
            false,
            null,
            null,
            response);
    }

    private ConsumerConfig config() {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS));
        return new ConsumerConfig(prop);
    }
}