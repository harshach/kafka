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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

public final class LocalTieredStorageEvent implements Comparable<LocalTieredStorageEvent> {

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
    private final int timestamp;
    private final Optional<RemoteLogSegmentFileset> fileset;
    private final Optional<RemoteLogSegmentMetadata> metadata;
    private final Optional<Long> startPosition;
    private final Optional<Long> endPosition;
    private final Optional<Exception> exception;

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

    public EventType getType() {
        return type;
    }

    public TopicPartition getTopicPartition() {
        return segmentId.topicPartition();
    }

    public boolean isAfter(final LocalTieredStorageEvent event) {
        return event.timestamp < timestamp;
    }

    @Override
    public int compareTo(LocalTieredStorageEvent other) {
        requireNonNull(other);

        if (other.timestamp > timestamp) {
            return -1;
        }
        if (other.timestamp < timestamp) {
            return 1;
        }
        return 0;
    }

    private LocalTieredStorageEvent(final Builder builder) {
        this.brokerId = builder.brokerId;
        this.type = builder.eventType;
        this.segmentId = builder.segmentId;
        this.timestamp = builder.timestamp;
        this.fileset = ofNullable(builder.fileset);
        this.metadata = ofNullable(builder.metadata);
        this.startPosition = ofNullable(builder.startPosition);
        this.endPosition = ofNullable(builder.endPosition);
        this.exception = ofNullable(builder.exception);
    }

    public static Builder newBuilder(
            final EventType type, final int time, final int brokerId, final RemoteLogSegmentId segmentId) {
        return new Builder(type, time, brokerId, segmentId);
    }

    public static class Builder {
        private EventType eventType;
        private int brokerId;
        private RemoteLogSegmentId segmentId;
        private int timestamp;
        private RemoteLogSegmentFileset fileset;
        private RemoteLogSegmentMetadata metadata;
        private Long startPosition;
        private Long endPosition;
        private Exception exception;

        public Builder withFileset(final RemoteLogSegmentFileset fileset) {
            this.fileset = fileset;
            return this;
        }

        public Builder withMetadata(final RemoteLogSegmentMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder withStartPosition(final Long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        public Builder withEndPosition(final Long endPosition) {
            this.endPosition = endPosition;
            return this;
        }

        public Builder withException(final Exception exception) {
            this.exception = exception;
            return this;
        }

        public LocalTieredStorageEvent build() {
            return new LocalTieredStorageEvent(this);
        }

        private Builder(final EventType type, final int time, final int brokerId, final RemoteLogSegmentId segId) {
            this.eventType = requireNonNull(type);
            this.timestamp = time;
            this.brokerId = brokerId;
            this.segmentId = requireNonNull(segId);
        }
    }
}