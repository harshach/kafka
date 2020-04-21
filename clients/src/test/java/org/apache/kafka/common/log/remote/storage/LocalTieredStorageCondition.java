package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.*;

import java.util.concurrent.*;

import static java.lang.String.*;
import static java.util.Objects.*;

public class LocalTieredStorageCondition {
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
            throw new IllegalArgumentException(format("The condition %s is already composed, cannot add it to %s", last, this));
        }
        if (last == this) {
            throw new IllegalArgumentException(format("The condition %s cannot be added to itself", this));
        }
        return new LocalTieredStorageCondition(this, next != null ? next.and(last) : last);
    }

    public void waitUntilTrue(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        final long start = System.currentTimeMillis();

        if (!listener.latch.await(timeout, unit)) {
            throw new TimeoutException(format("Time out reached before condition was verified %s", this));
        }

        if (next != null) {
            final long end = System.currentTimeMillis();
            next.waitUntilTrue(Math.max(0, timeout - (end - start)), unit);
        }
    }

    public String toString() {
        return format("Condition[eventType=%s, brokerId=%d, topicPartition=%s, failed=%b", eventType, brokerId, topicPartition, failed);
    }

    private final class InternalListener implements LocalTieredStorageListener {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onStorageEvent(final LocalTieredStorageEvent event) {
            if (event.matches(LocalTieredStorageCondition.this)) {
                latch.countDown();
            }
        }
    }

    private LocalTieredStorageCondition(final EventType type, final int id, final TopicPartition tp, final boolean failed) {
        this.eventType = requireNonNull(type);
        this.brokerId = id;
        this.topicPartition = requireNonNull(tp);
        this.failed = failed;
        this.listener = new InternalListener();
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
