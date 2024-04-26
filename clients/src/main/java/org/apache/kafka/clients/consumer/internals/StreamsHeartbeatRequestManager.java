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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.StreamsAssignmentInterface.Assignment;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsHeartbeatRequestData.TaskId;
import org.apache.kafka.common.message.StreamsHeartbeatRequestData.HostInfo;
import org.apache.kafka.common.message.StreamsHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsHeartbeatRequest;
import org.apache.kafka.common.requests.StreamsHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamsHeartbeatRequestManager implements RequestManager {

    private final Logger logger;

    /**
     * CoordinatorRequestManager manages the connection to the group coordinator
     */
    private final CoordinatorRequestManager coordinatorRequestManager;

    /**
     * HeartbeatRequestState manages heartbeat request timing and retries
     */
    private final StreamsHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState;

    /*
     * HeartbeatState manages building the heartbeat requests correctly
     */
    private final StreamsHeartbeatRequestManager.HeartbeatState heartbeatState;

    /**
     * MembershipManager manages member's essential attributes like epoch and id, and its rebalance state
     */
    private final MembershipManager membershipManager;

    private final StreamsPrepareAssignmentRequestManager streamsPrepareAssignmentRequestManager;

    private final StreamsInitializeRequestManager streamsInitializeRequestManager;

    /**
     * ErrorEventHandler allows the background thread to propagate errors back to the user
     */
    private final BackgroundEventHandler backgroundEventHandler;

    /**
     * Timer for tracking the time since the last consumer poll.  If the timer expires, the consumer will stop
     * sending heartbeat until the next poll.
     */
    private final Timer pollTimer;

    /**
     * Holding the heartbeat sensor to measure heartbeat timing and response latency
     */
    private final HeartbeatMetricsManager metricsManager;

    /*
     * StreamsGroupMetadata holds the metadata for the streams group
     */
    private StreamsAssignmentInterface streamsInterface;

    /**
     * Local cache of assigned topic IDs and names. Topics are added here when received in a
     * target assignment, as we discover their names in the Metadata cache, and removed when the
     * topic is not in the subscription anymore. The purpose of this cache is to avoid metadata
     * requests in cases where a currently assigned topic is in the target assignment (new
     * partition assigned, or revoked), but it is not present the Metadata cache at that moment.
     */
    private final Map<String, Uuid> assignedTopicIdCache;

    public StreamsHeartbeatRequestManager(
        final LogContext logContext,
        final Time time,
        final ConsumerConfig config,
        final CoordinatorRequestManager coordinatorRequestManager,
        final StreamsInitializeRequestManager streamsInitializeRequestManager,
        final StreamsPrepareAssignmentRequestManager streamsPrepareAssignmentRequestManager,
        final MembershipManager membershipManager,
        final BackgroundEventHandler backgroundEventHandler,
        final Metrics metrics,
        final StreamsAssignmentInterface streamsAssignmentInterface
    ) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.logger = logContext.logger(getClass());
        this.membershipManager = membershipManager;
        this.streamsInitializeRequestManager = streamsInitializeRequestManager;
        this.streamsPrepareAssignmentRequestManager = streamsPrepareAssignmentRequestManager;
        this.backgroundEventHandler = backgroundEventHandler;
        int maxPollIntervalMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.heartbeatState = new StreamsHeartbeatRequestManager.HeartbeatState(streamsInterface, membershipManager, maxPollIntervalMs);
        this.heartbeatRequestState = new StreamsHeartbeatRequestManager.HeartbeatRequestState(logContext, time, 0, retryBackoffMs,
            retryBackoffMaxMs, maxPollIntervalMs);
        this.pollTimer = time.timer(maxPollIntervalMs);
        this.metricsManager = new HeartbeatMetricsManager(metrics);
        this.streamsInterface = streamsAssignmentInterface;
    }

    /**
     * This will build a heartbeat request if one must be sent, determined based on the member
     * state. A heartbeat is sent in the following situations:
     * <ol>
     *     <li>Member is part of the consumer group or wants to join it.</li>
     *     <li>The heartbeat interval has expired, or the member is in a state that indicates
     *     that it should heartbeat without waiting for the interval.</li>
     * </ol>
     * This will also determine the maximum wait time until the next poll based on the member's
     * state.
     * <ol>
     *     <li>If the member is without a coordinator or is in a failed state, the timer is set
     *     to Long.MAX_VALUE, as there's no need to send a heartbeat.</li>
     *     <li>If the member cannot send a heartbeat due to either exponential backoff, it will
     *     return the remaining time left on the backoff timer.</li>
     *     <li>If the member's heartbeat timer has not expired, It will return the remaining time
     *     left on the heartbeat timer.</li>
     *     <li>If the member can send a heartbeat, the timer is set to the current heartbeat interval.</li>
     * </ol>
     *
     * @return {@link PollResult} that includes a heartbeat request if one must be sent, and the
     * time to wait until the next poll.
     */
    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        if (!coordinatorRequestManager.coordinator().isPresent() ||
            membershipManager.shouldSkipHeartbeat()) {
            membershipManager.onHeartbeatRequestSkipped();
            return NetworkClientDelegate.PollResult.EMPTY;
        }
        pollTimer.update(currentTimeMs);
        if (pollTimer.isExpired() && !membershipManager.isLeavingGroup()) {
            logger.warn("Consumer poll timeout has expired. This means the time between " +
                "subsequent calls to poll() was longer than the configured max.poll.interval.ms, " +
                "which typically implies that the poll loop is spending too much time processing " +
                "messages. You can address this either by increasing max.poll.interval.ms or by " +
                "reducing the maximum size of batches returned in poll() with max.poll.records.");

            membershipManager.transitionToSendingLeaveGroup(true);
            NetworkClientDelegate.UnsentRequest leaveHeartbeat = makeHeartbeatRequest(currentTimeMs, true);

            // We can ignore the leave response because we can join before or after receiving the response.
            heartbeatRequestState.reset();
            heartbeatState.reset();
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(leaveHeartbeat));
        }

        boolean heartbeatNow = membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight();
        if (!heartbeatRequestState.canSendRequest(currentTimeMs) && !heartbeatNow) {
            return new NetworkClientDelegate.PollResult(heartbeatRequestState.nextHeartbeatMs(currentTimeMs));
        }

        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(currentTimeMs, false);
        return new NetworkClientDelegate.PollResult(heartbeatRequestState.heartbeatIntervalMs, Collections.singletonList(request));
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * Similarly, we may have to unblock the application thread to send a `PollApplicationEvent` to make sure
     * our poll timer will not expire while we are polling.
     *
     * <p>In the event that heartbeats are currently being skipped, this still returns the next heartbeat
     * delay rather than {@code Long.MAX_VALUE} so that the application thread remains responsive.
     */
    @Override
    public long maximumTimeToWait(long currentTimeMs) {
        pollTimer.update(currentTimeMs);
        if (
            pollTimer.isExpired() ||
                (membershipManager.shouldHeartbeatNow() && !heartbeatRequestState.requestInFlight())
        ) {
            return 0L;
        }
        return Math.min(pollTimer.remainingMs() / 2, heartbeatRequestState.nextHeartbeatMs(currentTimeMs));
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final long currentTimeMs,
                                                                     final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = makeHeartbeatRequest(ignoreResponse);
        heartbeatRequestState.onSendAttempt(currentTimeMs);
        membershipManager.onHeartbeatRequestSent();
        metricsManager.recordHeartbeatSentMs(currentTimeMs);
        return request;
    }

    private NetworkClientDelegate.UnsentRequest makeHeartbeatRequest(final boolean ignoreResponse) {
        NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new StreamsHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator());
        if (ignoreResponse)
            return logResponse(request);
        else
            return request.whenComplete((response, exception) -> {
                long completionTimeMs = request.handler().completionTimeMs();
                if (response != null) {
                    metricsManager.recordRequestLatency(response.requestLatencyMs());
                    onResponse((StreamsHeartbeatResponse) response.responseBody(), completionTimeMs);
                } else {
                    onFailure(exception, completionTimeMs);
                }
            });
    }

    private NetworkClientDelegate.UnsentRequest logResponse(final NetworkClientDelegate.UnsentRequest request) {
        return request.whenComplete((response, exception) -> {
            if (response != null) {
                metricsManager.recordRequestLatency(response.requestLatencyMs());
                Errors error =
                    Errors.forCode(((StreamsHeartbeatResponse) response.responseBody()).data().errorCode());
                if (error == Errors.NONE)
                    logger.debug("GroupHeartbeat responded successfully: {}", response);
                else
                    logger.error("GroupHeartbeat failed because of {}: {}", error, response);
            } else {
                logger.error("GroupHeartbeat failed because of unexpected exception.", exception);
            }
        });
    }

    private void onFailure(final Throwable exception, final long responseTimeMs) {
        this.heartbeatRequestState.onFailedAttempt(responseTimeMs);
        this.heartbeatState.reset();
        if (exception instanceof RetriableException) {
            String message = String.format("GroupHeartbeatRequest failed because of the retriable exception. " +
                    "Will retry in %s ms: %s",
                heartbeatRequestState.remainingBackoffMs(responseTimeMs),
                exception.getMessage());
            logger.debug(message);
        } else {
            logger.error("GroupHeartbeatRequest failed due to fatal error: " + exception.getMessage());
            handleFatalFailure(exception);
        }
    }

    private void onResponse(final StreamsHeartbeatResponse response, long currentTimeMs) {
        if (Errors.forCode(response.data().errorCode()) == Errors.NONE) {
            final StreamsHeartbeatResponseData data = response.data();

            heartbeatRequestState.updateHeartbeatIntervalMs(data.heartbeatIntervalMs());
            heartbeatRequestState.onSuccessfulAttempt(currentTimeMs);
            heartbeatRequestState.resetTimer();

            if (data.shouldInitializeTopology()) {
                streamsInitializeRequestManager.initialize();
            }
            if (data.shouldComputeAssignment()) {
                streamsPrepareAssignmentRequestManager.prepareAssignment();
            }


            // TODO: Maybe refactor `MembershipManager` to not access `ConsumerGroupHeartbeatResponseData` directly
            ConsumerGroupHeartbeatResponseData cgData = new ConsumerGroupHeartbeatResponseData();
            cgData.setMemberId(data.memberId());
            cgData.setMemberEpoch(data.memberEpoch());
            cgData.setErrorCode(data.errorCode());
            cgData.setErrorMessage(data.errorMessage());
            cgData.setThrottleTimeMs(data.throttleTimeMs());
            cgData.setHeartbeatIntervalMs(data.heartbeatIntervalMs());


            if (data.activeTasks() != null && data.standbyTasks() != null && data.warmupTasks() != null) {
                synchronized (streamsInterface.targetAssignment) {
                    updateTaskIdCollection(data.activeTasks(), streamsInterface.targetAssignment.activeTasks);
                    updateTaskIdCollection(data.standbyTasks(), streamsInterface.targetAssignment.standbyTasks);
                    updateTaskIdCollection(data.warmupTasks(), streamsInterface.targetAssignment.warmupTasks);
                }

                // TODO: Regular expressions and UUIDs

                List<ConsumerGroupHeartbeatResponseData.TopicPartitions> tps = new ArrayList<>();





                data.activeTasks().forEach( taskId -> {
                    streamsInterface.subtopologyMap().get(taskId.subtopology()).sourceTopics.forEach(topic -> {
                        membershipManager.updateTargetAssignment(topic, taskId.partitions());
                        ConsumerGroupHeartbeatResponseData.TopicPartitions tp = new ConsumerGroupHeartbeatResponseData.TopicPartitions();
                        tp.setTopicId(topic); // TODO: Resolve topicId
                        cgAssignment.setTopicPartitions(topic, taskId.partitions());
                    });
                });
                ConsumerGroupHeartbeatResponseData.Assignment cgAssignment = new ConsumerGroupHeartbeatResponseData.Assignment();
                cgAssignment.setTopicPartitions(tps);

            } else {
                // TODO: Partially defined does not make much sense I guess
            }

            membershipManager.onHeartbeatSuccess(cgData);
            return;
        }
        onErrorResponse(response, currentTimeMs);
    }

    private void updateTaskIdCollection(
        final List<StreamsHeartbeatResponseData.TaskId> source,
        final Set<StreamsAssignmentInterface.TaskId> target
    ) {
        target.clear();
        source.forEach(taskId -> {
            taskId.partitions().forEach(partition -> {
                target.add(new StreamsAssignmentInterface.TaskId(taskId.subtopology(), partition));
            });
        });
    }

    private void onErrorResponse(final StreamsHeartbeatResponse response,
                                 final long currentTimeMs) {
        Errors error = Errors.forCode(response.data().errorCode());
        String errorMessage = response.data().errorMessage();
        String message;

        this.heartbeatState.reset();
        this.heartbeatRequestState.onFailedAttempt(currentTimeMs);
        membershipManager.onHeartbeatFailure();

        switch (error) {
            case NOT_COORDINATOR:
                // the manager should retry immediately when the coordinator node becomes available again
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is incorrect. " +
                        "Will attempt to find the coordinator again and retry",
                    coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_NOT_AVAILABLE:
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is not available. " +
                        "Will attempt to find the coordinator again and retry",
                    coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                coordinatorRequestManager.markCoordinatorUnknown(errorMessage, currentTimeMs);
                // Skip backoff so that the next HB is sent as soon as the new coordinator is discovered
                heartbeatRequestState.reset();
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // the manager will backoff and retry
                message = String.format("GroupHeartbeatRequest failed because the group coordinator %s is still loading." +
                        "Will retry",
                    coordinatorRequestManager.coordinator());
                logInfo(message, response, currentTimeMs);
                break;

            case GROUP_AUTHORIZATION_FAILED:
                GroupAuthorizationException exception =
                    GroupAuthorizationException.forGroupId(membershipManager.groupId());
                logger.error("GroupHeartbeatRequest failed due to group authorization failure: {}", exception.getMessage());
                handleFatalFailure(error.exception(exception.getMessage()));
                break;

            case UNRELEASED_INSTANCE_ID:
                logger.error("GroupHeartbeatRequest failed due to the instance id {} was not released: {}",
                    membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(Errors.UNRELEASED_INSTANCE_ID.exception(errorMessage));
                break;

            case INVALID_REQUEST:
            case GROUP_MAX_SIZE_REACHED:
            case UNSUPPORTED_ASSIGNOR:
            case UNSUPPORTED_VERSION:
                logger.error("GroupHeartbeatRequest failed due to error: {}", error);
                handleFatalFailure(error.exception(errorMessage));
                break;

            case FENCED_MEMBER_EPOCH:
                message = String.format("GroupHeartbeatRequest failed for member %s because epoch %s is fenced.",
                    membershipManager.memberId(), membershipManager.memberEpoch());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            case UNKNOWN_MEMBER_ID:
                message = String.format("GroupHeartbeatRequest failed because member %s is unknown.",
                    membershipManager.memberId());
                logInfo(message, response, currentTimeMs);
                membershipManager.transitionToFenced();
                // Skip backoff so that a next HB to rejoin is sent as soon as the fenced member releases its assignment
                heartbeatRequestState.reset();
                break;

            default:
                // If the manager receives an unknown error - there could be a bug in the code or a new error code
                logger.error("GroupHeartbeatRequest failed due to unexpected error: {}", error);
                handleFatalFailure(error.exception(errorMessage));
                break;
        }
    }

    private void logInfo(final String message,
                         final StreamsHeartbeatResponse response,
                         final long currentTimeMs) {
        logger.info("{} in {}ms: {}",
            message,
            heartbeatRequestState.remainingBackoffMs(currentTimeMs),
            response.data().errorMessage());
    }

    private void handleFatalFailure(Throwable error) {
        backgroundEventHandler.add(new ErrorEvent(error));
        membershipManager.transitionToFatal();
    }

    /**
     * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff. The
     * object extends {@link RequestState} to enable exponential backoff and duplicated request handling. The two fields
     * that it holds are:
     */
    static class HeartbeatRequestState extends RequestState {
        /**
         *  heartbeatTimer tracks the time since the last heartbeat was sent
         */
        private final Timer heartbeatTimer;

        /**
         * The heartbeat interval which is acquired/updated through the heartbeat request
         */
        private long heartbeatIntervalMs;

        public HeartbeatRequestState(
            final LogContext logContext,
            final Time time,
            final long heartbeatIntervalMs,
            final long retryBackoffMs,
            final long retryBackoffMaxMs,
            final double jitter) {
            super(logContext, StreamsHeartbeatRequestManager.HeartbeatRequestState.class.getName(), retryBackoffMs, 2, retryBackoffMaxMs, jitter);
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        }

        private void update(final long currentTimeMs) {
            this.heartbeatTimer.update(currentTimeMs);
        }

        public void resetTimer() {
            this.heartbeatTimer.reset(heartbeatIntervalMs);
        }

        @Override
        public boolean canSendRequest(final long currentTimeMs) {
            update(currentTimeMs);
            return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
        }

        public long nextHeartbeatMs(final long currentTimeMs) {
            if (heartbeatTimer.remainingMs() == 0) {
                return this.remainingBackoffMs(currentTimeMs);
            }
            return heartbeatTimer.remainingMs();
        }

        private void updateHeartbeatIntervalMs(final long heartbeatIntervalMs) {
            if (this.heartbeatIntervalMs == heartbeatIntervalMs) {
                // no need to update the timer if the interval hasn't changed
                return;
            }
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatTimer.updateAndReset(heartbeatIntervalMs);
        }
    }


    /**
     * Look for topic in the global metadata cache. If found, add it to the local cache and
     * return it. If not found, look for it in the local metadata cache. Return empty if not
     * found in any of the two.
     */
    private Optional<String> findTopicIdINGlobalOrLocalCache(String topicName) {
        Uuid idFromMetadataCache = metadata.topicIds().getOrDefault(topicName, null);
        if (idFromMetadataCache != null) {
            // Add topic name to local cache, so it can be reused if included in a next target
            // assignment if metadata cache not available.
            assignedTopicNamesCache.put(topicId, idFromMetadataCache);
            return Optional.of(idFromMetadataCache);
        } else {
            // Topic ID was not found in metadata. Check if the topic name is in the local
            // cache of topics currently assigned. This will avoid a metadata request in the
            // case where the metadata cache may have been flushed right before the
            // revocation of a previously assigned topic.
            String nameFromSubscriptionCache = assignedTopicNamesCache.getOrDefault(topicId, null);
            return Optional.ofNullable(nameFromSubscriptionCache);
        }
    }

    /**
     * Builds the heartbeat requests correctly, ensuring that all information is sent according to
     * the protocol, but subsequent requests do not send information which has not changed. This
     * is important to ensure that reconciliation completes successfully.
     */
    static class HeartbeatState {
        private final MembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final StreamsHeartbeatRequestManager.HeartbeatState.SentFields sentFields;

        /*
         * StreamsGroupMetadata holds the metadata for the streams group
         */
        private final StreamsAssignmentInterface streamsInterface;

        public HeartbeatState(
            final StreamsAssignmentInterface streamsInterface,
            final MembershipManager membershipManager,
            final int rebalanceTimeoutMs) {
            this.membershipManager = membershipManager;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.sentFields = new StreamsHeartbeatRequestManager.HeartbeatState.SentFields();
            this.streamsInterface = streamsInterface;
        }

        public void reset() {
            sentFields.reset();
        }

        public StreamsHeartbeatRequestData buildRequestData() {
            StreamsHeartbeatRequestData data = new StreamsHeartbeatRequestData();

            // GroupId - always sent
            data.setGroupId(membershipManager.groupId());

            // MemberId - always sent, empty until it has been received from the coordinator
            data.setMemberId(membershipManager.memberId());

            // MemberEpoch - always sent
            data.setMemberEpoch(membershipManager.memberEpoch());

            // InstanceId - set if present
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

            boolean sendAllFields = membershipManager.state() == MemberState.JOINING;

            // RebalanceTimeoutMs - only sent when joining or if it has changed since the last heartbeat
            if (sendAllFields || sentFields.rebalanceTimeoutMs != rebalanceTimeoutMs) {
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                sentFields.rebalanceTimeoutMs = rebalanceTimeoutMs;
            }

            // Immutable -- only sent when joining
            if (sendAllFields) {
                data.setProcessId(streamsInterface.processID().toString());
                HostInfo hostInfo = new HostInfo();
                hostInfo.setHost(streamsInterface.userEndPointHost());
                hostInfo.setPort(streamsInterface.userEndPointPort());
                data.setHostInfo(hostInfo);
                data.setClientTags(streamsInterface.clientTags().entrySet().stream().map(entry -> {
                    StreamsHeartbeatRequestData.KeyValue tag = new StreamsHeartbeatRequestData.KeyValue();
                    tag.setKey(entry.getKey());
                    tag.setValue(entry.getValue());
                    return tag;
                }).collect(Collectors.toList()));
                data.setAssignmentConfigs(streamsInterface.assignmentConfiguration().entrySet().stream().map(entry -> {
                    StreamsHeartbeatRequestData.KeyValue config = new StreamsHeartbeatRequestData.KeyValue();
                    config.setKey(entry.getKey());
                    config.setValue(entry.getValue().toString());
                    return config;
                }).collect(Collectors.toList()));
            }

            if (streamsInterface.shutdownRequested()) {
                data.setShutdownApplication(true);
            }

            synchronized (streamsInterface.reconciledAssignment) {
                if (!streamsInterface.reconciledAssignment.equals(sentFields.localAssignment)) {
                    data.setActiveTasks(convertTaskIdCollection(streamsInterface.reconciledAssignment.activeTasks));
                    data.setStandbyTasks(convertTaskIdCollection(streamsInterface.reconciledAssignment.standbyTasks));
                    data.setWarmupTasks(convertTaskIdCollection(streamsInterface.reconciledAssignment.warmupTasks));
                    sentFields.localAssignment = streamsInterface.reconciledAssignment;
                }
            }

            return data;
        }

        private List<TaskId> convertTaskIdCollection(final Set<StreamsAssignmentInterface.TaskId> tasks) {
            return tasks.stream()
                .collect(
                    Collectors.groupingBy(StreamsAssignmentInterface.TaskId::subtopologyId,
                    Collectors.mapping(StreamsAssignmentInterface.TaskId::taskId, Collectors.toList()))
                )
                .entrySet()
                .stream()
                .map(entry -> {
                    TaskId id = new TaskId();
                    id.setSubtopology(entry.getKey());
                    id.setPartitions(entry.getValue());
                    return id;
                })
                .collect(Collectors.toList());
        }

        // Fields of StreamsHeartbeatRequest sent in the most recent request
        static class SentFields {
            private int rebalanceTimeoutMs = -1;
            private Assignment localAssignment = null;

            SentFields() {}

            void reset() {
                rebalanceTimeoutMs = -1;
                localAssignment = null;
            }
        }
    }

}
