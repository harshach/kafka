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
 * Unlike {@link LocalRemoteStorageTraverser}, the intent is to generate instant notification,
 * rather than walking the directory structure of the storage at a given point in time.
 */
public interface LocalRemoteStorageListener {

    void onTopicPartitionCreated(TopicPartition topicPartition);

    void onSegmentCreated(RemoteLogSegmentId id, File segmentFile);

    final class CompositeLocalRemoteStorageListener implements LocalRemoteStorageListener {
        private static final Logger LOGGER = LoggerFactory.getLogger(LocalRemoteStorageListener.class);
        private final List<LocalRemoteStorageListener> listeners = new CopyOnWriteArrayList<>();

        public void add(final LocalRemoteStorageListener listener) {
            listeners.add(Objects.requireNonNull(listener));
        }

        @Override
        public void onTopicPartitionCreated(final TopicPartition topicPartition) {
            for (final LocalRemoteStorageListener listener: listeners) {
                try {
                    listener.onTopicPartitionCreated(topicPartition);

                } catch (Exception e) {
                    LOGGER.error("Caught failure from listener", e);
                }
            }
        }

        @Override
        public void onSegmentCreated(final RemoteLogSegmentId id, final File segmentFile) {
            for (final LocalRemoteStorageListener listener: listeners) {
                try {
                    listener.onSegmentCreated(id, segmentFile);

                } catch (Exception e) {
                    LOGGER.error("Caught failure from listener", e);
                }
            }
        }
    }
}
