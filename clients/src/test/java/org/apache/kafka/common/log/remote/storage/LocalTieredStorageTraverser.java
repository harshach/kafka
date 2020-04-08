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

import org.apache.kafka.common.*;
import org.apache.kafka.common.record.*;

/**
 * Used to walk through a local remote storage, providing a support to tests to explore the content of the storage.
 * This interface is to be used with the {@link LocalTieredStorage} and is intended for tests only.
 */
public interface LocalTieredStorageTraverser {

    /**
     * Called when a new topic-partition stored on the remote storage is discovered.
     * @param topicPartition The new topic-partition discovered.
     */
    void visitTopicPartition(TopicPartition topicPartition);

    /**
     * Called when a new segment is discovered for a given topic-partition.
     * This method can only be called after {@link LocalTieredStorageTraverser#visitTopicPartition(TopicPartition)}
     * for the topic-partition the segment belongs to.
     *
     * @param segmentId The remote id of the segment discovered.
     */
    void visitSegment(RemoteLogSegmentId segmentId);

    /**
     * Called for each record read on a given segment.
     * This method can only be called after {@link LocalTieredStorageTraverser#visitRecord(RemoteLogSegmentId, Record)}
     * for the segment the record is read from.
     *
     * @param segmentId The remote id of the segment the record is read from.
     * @param record Record read from the segment.
     */
    void visitRecord(RemoteLogSegmentId segmentId, Record record);

}
