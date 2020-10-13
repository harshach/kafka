package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.IOException;
import java.util.NavigableMap;

public interface MetadataStore {
    void flush() throws IOException;

    void load() throws IOException;

    void update(TopicPartition tp, RemoteLogSegmentMetadata metadata);

    NavigableMap<Long, RemoteLogSegmentId> getSegmentIds(TopicPartition tp);

    RemoteLogSegmentMetadata getMetaData(RemoteLogSegmentId id);
}
