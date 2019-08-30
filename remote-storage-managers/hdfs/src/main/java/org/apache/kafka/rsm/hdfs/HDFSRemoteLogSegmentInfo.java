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
package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.common.TopicPartition;

import kafka.log.remote.RemoteLogSegmentInfo;
import org.apache.hadoop.fs.Path;

public class HDFSRemoteLogSegmentInfo implements RemoteLogSegmentInfo {
    private TopicPartition topicPartition;
    private long baseOffset;
    private long endOffset;
    private Path path;

    HDFSRemoteLogSegmentInfo(TopicPartition topicPartition, long baseOffset, long endOffset, Path path) {
        this.topicPartition = topicPartition;
        this.baseOffset = baseOffset;
        this.endOffset = endOffset;
        this.path = path;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public long endOffset() {
        return endOffset;
    }

    public Path getPath() {
        return path;
    }
}
