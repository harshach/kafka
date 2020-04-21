package org.apache.kafka.common.log.remote.storage;

import java.util.*;

import static java.util.Objects.*;
import static java.util.Optional.*;

public final class LocalTieredStorageEvent {

    public enum EventType {
        OFFLOAD_SEGMENT,
        FETCH_SEGMENT,
        FETCH_OFFSET_INDEX,
        FETCH_TIME_INDEX,
        DELETE_SEGMENT
    }

    private final EventType type;
    private final int brokerId;
    private final RemoteLogSegmentId segmentId;
    private final Optional<RemoteLogSegmentFileset> fileset;
    private final Optional<RemoteLogSegmentMetadata> metadata;
    private final Optional<Long> startPosition;
    private final Optional<Long> endPosition;
    private final Optional<Exception> exception;

    public EventType getType() {
        return type;
    }

    public boolean matches(final LocalTieredStorageCondition condition) {
        if (condition.eventType != type) {
            return false;
        }
        if (condition.brokerId != brokerId) {
            return false;
        }
        if (!segmentId.topicPartition().equals(condition.topicPartition)) {
            return false;
        }
        if (!exception.map(e -> condition.failed).orElseGet(() -> !condition.failed)) {
            return false;
        }
        return true;
    }

    public LocalTieredStorageEvent(final EventType eventType,
                                   final int brokerId,
                                   final RemoteLogSegmentId segmentId,
                                   final RemoteLogSegmentFileset fileset,
                                   final RemoteLogSegmentMetadata metadata,
                                   final Long startPosition,
                                   final Long endPosition,
                                   final Exception exception) {

        this.brokerId = brokerId;
        this.type = requireNonNull(eventType);
        this.segmentId = requireNonNull(segmentId);
        this.fileset = ofNullable(fileset);
        this.metadata = ofNullable(metadata);
        this.startPosition = ofNullable(startPosition);
        this.endPosition = ofNullable(endPosition);
        this.exception = ofNullable(exception);
    }

    public LocalTieredStorageEvent(final EventType eventType, final int brokerId,
                                   final RemoteLogSegmentFileset fileset, final RemoteLogSegmentMetadata metadata) {
        this(eventType, brokerId, metadata.remoteLogSegmentId(), fileset, metadata, null, null, null);
    }

    public LocalTieredStorageEvent(final EventType eventType, final int brokerId,
                                   final RemoteLogSegmentMetadata metadata, final Exception e) {
        this(eventType, brokerId, metadata.remoteLogSegmentId(), null, metadata, null, null, e);
    }

    public LocalTieredStorageEvent(final EventType eventType, final int brokerId, final RemoteLogSegmentId segmentId,
                                   final RemoteLogSegmentFileset fileset, final Exception e) {
        this(eventType, brokerId, segmentId, fileset, null, null, null, e);
    }

    public LocalTieredStorageEvent(final EventType eventType, final int brokerId, final RemoteLogSegmentFileset fileset,
                                   final RemoteLogSegmentMetadata metadata, final Long startPosition, final Long endPosition) {
        this(eventType, brokerId, metadata.remoteLogSegmentId(),
                fileset, metadata, startPosition, endPosition, null);
    }

    public LocalTieredStorageEvent(final EventType eventType, final int brokerId, final RemoteLogSegmentMetadata metadata,
                                   final Long startPosition, final Long endPosition, final Exception e) {
        this(eventType, brokerId, metadata.remoteLogSegmentId(), null, metadata, startPosition, endPosition, e);
    }
}