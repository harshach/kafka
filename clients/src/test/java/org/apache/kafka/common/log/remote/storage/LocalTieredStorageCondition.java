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

/**
 * A {@link LocalTieredStorageCondition} embeds a subset of the characterization of an interaction
 * with the {@link LocalTieredStorage} and provides a method which suspends the current thread until
 * an interaction with the storage which matches the characterization is intercepted.
 *
 * In this implementation, the elements characterizing an interaction between the {@link LocalTieredStorage}
 * and a broker include the nature of that interaction (e.g. offload or fetch a segment, fetch a time index, etc.),
 * the broker from which the interaction originates from, the topic-partition the interaction relates
 * to, and whether the interaction results in a successful or failed outcome.
 *
 * The current model for interactions with the {@link LocalTieredStorage} is materialized by a
 * {@link LocalTieredStorageEvent}.
 *
 * Conditions can be chained to formulate logical conjunctions via the use of the method
 * {@link LocalTieredStorageCondition#and(LocalTieredStorageCondition)}.
 */
public final class LocalTieredStorageCondition {
    final EventType eventType;
    final int brokerId;
    final TopicPartition topicPartition;
    final boolean failed;

    private final InternalListener listener;
    private final LocalTieredStorageCondition next;

    /**
     * Constructs a new condition on an interaction of the {@code eventType} nature, from the broker
     * with id {@code brokerId}, for the topic-partition {@code tp} and which is {@code failed} or not.
     *
     * This condition internally observes the provided {@code storages} to intercept such an
     * interaction if it occurs.
     *
     * NOTE: interactions with the {@code storages} is monitored as soon as this method returns.
     *
     * @param storages
     * @param eventType
     * @param brokerId
     * @param tp
     * @param failed
     * @return
     */
    public static LocalTieredStorageCondition expectEvent(final Iterable<LocalTieredStorage> storages,
                                                          final EventType eventType,
                                                          final int brokerId,
                                                          final TopicPartition tp,
                                                          final boolean failed) {

        final LocalTieredStorageCondition condition = new LocalTieredStorageCondition(eventType, brokerId, tp, failed);
        storages.forEach(storage -> storage.addListener(condition.listener));
        return condition;
    }

    /**
     * Formulates a new condition which is true if and only if this condition and the {@code conjuct}
     * provided as argument are both true.
     *
     * @param conjuct Another condition which truth is required for the resulting new condition to be true.
     * @return A new condition which is true iff this condition and {@code conjuct} are both true.
     */
    public final LocalTieredStorageCondition and(final LocalTieredStorageCondition conjuct) {
        //
        // To keep things simple, only authorize append to the condition chain of elementary (not composed)
        // conditions. It also allows to protect from cycles.
        //
        if (conjuct.next != null) {
            throw new IllegalArgumentException(
                    format("The condition %s is already composed, cannot add it to %s", conjuct, this));
        }
        if (conjuct == this) {
            throw new IllegalArgumentException(
                    format("The condition %s cannot be added to itself", this));
        }
        //
        // Note: There is no upper bound enforced on the length of the chain of conditions.
        //       This class is for tests only!
        //
        return new LocalTieredStorageCondition(this, next != null ? next.and(conjuct) : conjuct);
    }

    /**
     * Suspend the current thread until the condition(s) materialized by this instance becomes true.
     * The method returns non-exceptionally only if the truth of the condition is established before
     * the given {@code timeout} elapses.
     *
     * Whether or not this condition has been constructed from multiple conjuncts does not impact the
     * time limit defined by {@code timeout}, which is always honored regardless of how many underlying
     * conditions this condition is composed of.
     *
     * If the truth of this condition could not be established before timeout, a {@link TimeoutException}
     * is thrown.
     *
     * @param timeout The maximum time the calling thread can be suspended until the condition is true.
     * @param unit The unit of the timeout.
     * @throws InterruptedException If this method is interrupted while awaiting the condition to be true.
     * @throws TimeoutException If the truth of the condition could not be verified before {@code timeout}.
     */
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
