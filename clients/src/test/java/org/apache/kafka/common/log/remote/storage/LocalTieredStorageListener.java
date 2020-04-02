package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Used to notify users of change in the local storage, such as the addition of a directory,
 * a segment or offset file.
 *
 * Unlike {@link LocalTieredStorageTraverser}, the intent is to generate instant notification,
 * rather than walking the directory structure of the storage at a given point in time.
 */
public interface LocalTieredStorageListener {

    /**
     * Called when the directory hosting segments for a topic-partition has been created.
     */
    void onTopicPartitionCreated(TopicPartition topicPartition);

    /**
     * Called when a segment has been copied to the local remote storage.
     *
     * @param id The remote id assigned to the segment.
     * @param segmentFile The segment's file in the remote storage.
     */
    void onSegmentCreated(RemoteLogSegmentId id, File segmentFile);

    /**
     * Delegates to a list of listeners in insertion order. Failure of one listener does not
     * prevent execution of next ones in the list.
     */
    final class CompositeLocalTieredStorageListener implements LocalTieredStorageListener {
        private static final Logger LOGGER = LoggerFactory.getLogger(LocalTieredStorageListener.class);
        private final List<LocalTieredStorageListener> listeners = new CopyOnWriteArrayList<>();

        public void add(final LocalTieredStorageListener listener) {
            listeners.add(Objects.requireNonNull(listener));
        }

        @Override
        public void onTopicPartitionCreated(final TopicPartition topicPartition) {
            for (final LocalTieredStorageListener listener: listeners) {
                try {
                    listener.onTopicPartitionCreated(topicPartition);

                } catch (Exception e) {
                    LOGGER.error("Caught failure from listener", e);
                }
            }
        }

        @Override
        public void onSegmentCreated(final RemoteLogSegmentId id, final File segmentFile) {
            for (final LocalTieredStorageListener listener: listeners) {
                try {
                    listener.onSegmentCreated(id, segmentFile);

                } catch (Exception e) {
                    LOGGER.error("Caught failure from listener", e);
                }
            }
        }
    }
}
