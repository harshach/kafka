package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import static java.lang.String.*;
import static java.util.stream.Collectors.*;

public final class LocalRemoteStorageWaiter {
    private final Map<TopicPartition, AtomicInteger> remainingSegments;
    private final CountDownLatch latch;

    public static LocalRemoteStorageWaiter.Builder newWaiter() {
        return new LocalRemoteStorageWaiter.Builder();
    }

    public void waitForSegments(final long timeout, final TimeUnit unit) throws InterruptedException, TimeoutException {
        LOGGER.debug("Waiting on segments from topic-partitions: {}", remainingSegments);

        if (!latch.await(timeout, unit)) {
            throw new TimeoutException(
                    format("Timed out before all segments were offloaded to the remote storage. " +
                            "Remaining segments: %s", remainingSegments));
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalRemoteStorageWaiter.class);

    private final class InternalListener implements LocalRemoteStorageListener {
        @Override
        public void onTopicPartitionCreated(TopicPartition topicPartition) {
        }

        @Override
        public void onSegmentCreated(RemoteLogSegmentId id, File segmentFile) {
            LOGGER.debug("Segment uploaded to remote storage: {}", id);

            final AtomicInteger remaining = remainingSegments.get(id.topicPartition());

            if (remaining != null && remaining.decrementAndGet() >= 0) {
                latch.countDown();
            }
        }
    }

    private LocalRemoteStorageWaiter(final Builder builder) {
        this.remainingSegments = Collections.unmodifiableMap(builder.segmentCountDowns);

        final int segmentCount = remainingSegments.values().stream().collect(summingInt(AtomicInteger::get));

        this.latch = new CountDownLatch(segmentCount);
    }

    public static final class Builder {
        private final Map<TopicPartition, AtomicInteger> segmentCountDowns = new HashMap<>();

        public Builder addSegmentsToWaitFor(final TopicPartition topicPartition, final int numberOfSegments) {
            segmentCountDowns.put(topicPartition, new AtomicInteger(numberOfSegments));
            return this;
        }

        public LocalRemoteStorageWaiter fromStorage(final LocalRemoteStorage storage) {
            final LocalRemoteStorageWaiter waiter = new LocalRemoteStorageWaiter(this);
            storage.addListener(waiter.new InternalListener());
            return waiter;
        }
    }
}
