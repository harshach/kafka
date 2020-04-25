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
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class LocalTieredStorageCondition {
    final EventType eventType;
    final int brokerId;
    final TopicPartition topicPartition;
    final boolean failed;

    private final InternalListener listener;
    private final LocalTieredStorageCondition next;

    public static LocalTieredStorageCondition expectEvent(final Iterable<LocalTieredStorage> storages,
                                                          final EventType eventType,
                                                          final int brokerId,
                                                          final TopicPartition tp,
                                                          final boolean failed) {

        final LocalTieredStorageCondition condition = new LocalTieredStorageCondition(eventType, brokerId, tp, failed);
        storages.forEach(storage -> storage.addListener(condition.listener));
        return condition;
    }

    public final LocalTieredStorageCondition and(final LocalTieredStorageCondition last) {
        if (last.next != null) {
            throw new IllegalArgumentException(
                    format("The condition %s is already composed, cannot add it to %s", last, this));
        }
        if (last == this) {
            throw new IllegalArgumentException(
                    format("The condition %s cannot be added to itself", this));
        }
        return new LocalTieredStorageCondition(this, next != null ? next.and(last) : last);
    }

    public void waitUntilTrue(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        final long start = System.currentTimeMillis();

        if (!listener.awaitEvent(timeout, unit)) {
            throw new TimeoutException(format("Time out reached before condition was verified %s", this));
        }

        if (next != null) {
            final long end = System.currentTimeMillis();
            next.waitUntilTrue(Math.max(0, timeout - (end - start)), unit);
        }
    }

    public String toString() {
        return format("Condition[eventType=%s, brokerId=%d, topicPartition=%s, failed=%b]",
                eventType, brokerId, topicPartition, failed);
    }

    private static final class InternalListener implements LocalTieredStorageListener {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final LocalTieredStorageCondition condition;

        @Override
        public void onStorageEvent(final LocalTieredStorageEvent event) {
            if (event.matches(condition)) {
                latch.countDown();
            }
        }

        private boolean awaitEvent(final long timeout, final TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        private InternalListener(final LocalTieredStorageCondition condition) {
            this.condition = requireNonNull(condition);
        }
    }

    private LocalTieredStorageCondition(final EventType type, final int id, final TopicPartition tp, final boolean failed) {
        this.eventType = requireNonNull(type);
        this.brokerId = id;
        this.topicPartition = requireNonNull(tp);
        this.failed = failed;
        this.listener = new InternalListener(this);
        this.next = null;
    }

    private LocalTieredStorageCondition(final LocalTieredStorageCondition h, final LocalTieredStorageCondition next) {
        this.eventType = h.eventType;
        this.brokerId = h.brokerId;
        this.topicPartition = h.topicPartition;
        this.failed = h.failed;
        this.listener = h.listener;
        this.next = requireNonNull(next);
    }
}
