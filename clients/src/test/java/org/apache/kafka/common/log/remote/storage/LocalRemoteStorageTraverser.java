package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.record.*;

public interface LocalRemoteStorageTraverser {

    void visitRecord(RemoteLogSegmentId segmentId, Record record);

}
