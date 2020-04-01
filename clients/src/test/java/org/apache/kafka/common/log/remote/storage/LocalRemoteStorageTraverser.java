package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.apache.kafka.common.record.*;

/**
 * Used to walk through a local remote storage, providing a support to tests to explore the content of the storage.
 * This interface is to be used with the {@link LocalRemoteStorage} and is intended for tests only.
 */
public interface LocalRemoteStorageTraverser {

    /**
     * Called when a new topic-partition stored on the remote storage is discovered.
     * @param topicPartition The new topic-partition discovered.
     */
    void visitTopicPartition(TopicPartition topicPartition);

    /**
     * Called when a new segment is discovered for a given topic-partition.
     * This method can only be called after {@link LocalRemoteStorageTraverser#visitTopicPartition(TopicPartition)}
     * for the topic-partition the segment belongs to.
     *
     * @param segmentId The remote id of the segment discovered.
     */
    void visitSegment(RemoteLogSegmentId segmentId);

    /**
     * Called for each record read on a given segment.
     * This method can only be called after {@link LocalRemoteStorageTraverser#visitRecord(RemoteLogSegmentId, Record)}
     * for the segment the record is read from.
     *
     * @param segmentId The remote id of the segment the record is read from.
     * @param record Record read from the segment.
     */
    void visitRecord(RemoteLogSegmentId segmentId, Record record);

}
