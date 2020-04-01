package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.apache.kafka.common.record.*;

import java.nio.*;
import java.util.*;

import static java.lang.String.*;

public final class LocalRemoteStorageSnapshot {

    public static LocalRemoteStorageSnapshot takeSnapshot(final LocalRemoteStorage storage) {
        Snapshot snapshot = new Snapshot();
        storage.traverse(snapshot);
        return new LocalRemoteStorageSnapshot(snapshot);
    }

    public List<TopicPartition> getTopicPartitions() {
        return Collections.unmodifiableList(snapshot.topicPartitions);
    }

    public Map<RemoteLogSegmentId, List<ByteBuffer>> getRecords() {
        return Collections.unmodifiableMap(snapshot.records);
    }

    private final Snapshot snapshot;

    private LocalRemoteStorageSnapshot(final Snapshot snapshot) {
        Objects.requireNonNull(this.snapshot = snapshot);
    }

    private static final class Snapshot implements LocalRemoteStorageTraverser {
        private final Map<RemoteLogSegmentId, List<ByteBuffer>> records = new HashMap<>();
        private final List<TopicPartition> topicPartitions = new ArrayList<>();

        @Override
        public void visitTopicPartition(TopicPartition topicPartition) {
            if (topicPartitions.contains(topicPartition)) {
                throw new IllegalStateException(format("Topic-partition %s was already visited", topicPartition));
            }

            this.topicPartitions.add(topicPartition);
        }

        @Override
        public void visitSegment(RemoteLogSegmentId segmentId) {
            if (records.containsKey(segmentId)) {
                throw new IllegalStateException(format("Segment with id %s was already visited", segmentId));
            }

            records.put(segmentId, new ArrayList<>());
        }

        @Override
        public void visitRecord(RemoteLogSegmentId segmentId, Record record) {
            final List<ByteBuffer> segmentRecords = records.get(segmentId);

            if (segmentRecords == null) {
                throw new IllegalStateException(format("Segment with id %s was not visited", segmentId));
            }

            segmentRecords.add(record.value());
        }
    }
}
