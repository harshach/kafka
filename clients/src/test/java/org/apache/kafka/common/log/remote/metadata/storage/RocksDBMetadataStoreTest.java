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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class RocksDBMetadataStoreTest {
    TopicPartition tp0 = new TopicPartition("foo", 0);
    TopicPartition tp1 = new TopicPartition("foo", 1);
    TopicPartition tp2 = new TopicPartition("bar", 0);

    long segSize = 128 * 1024 * 1024;
    RemoteLogSegmentId rlSegIdTp0_0_100 = new RemoteLogSegmentId(tp0, UUID.randomUUID());
    RemoteLogSegmentMetadata rlSegMetTp0_0_100 = new RemoteLogSegmentMetadata(rlSegIdTp0_0_100, 0L, 100L, -1L, 1, segSize, Collections.emptyMap());

    RemoteLogSegmentId rlSegIdTp0_101_200 = new RemoteLogSegmentId(tp0, UUID.randomUUID());
    RemoteLogSegmentMetadata rlSegMetTp0_101_200 = new RemoteLogSegmentMetadata(rlSegIdTp0_101_200, 101L, 200L, -1L, 1, segSize, Collections.emptyMap());

    RemoteLogSegmentId rlSegIdTp1_101_300 = new RemoteLogSegmentId(tp1, UUID.randomUUID());
    RemoteLogSegmentMetadata rlSegMetTp1_101_300 = new RemoteLogSegmentMetadata(rlSegIdTp1_101_300, 101L, 300L, -1L, 1, segSize, Collections.emptyMap());

    RemoteLogSegmentId rlSegIdTp2_150_400 = new RemoteLogSegmentId(tp2, UUID.randomUUID());
    RemoteLogSegmentMetadata rlSegMetTp2_150_400 = new RemoteLogSegmentMetadata(rlSegIdTp2_150_400, 150L, 400L, -1L, 1, segSize, Collections.emptyMap());


    private File logDir0;
    private File logDir1;

    @Before
    public void setup() throws Exception {
        RocksDBMetadataStore.loadLibrary();
        logDir0 = TestUtils.tempDirectory();
        logDir1 = TestUtils.tempDirectory();
    }

    @Test
    public void testRocksDBMetadataStore() throws Exception {
        HashMap<String, String> config = new HashMap<>();
        config.put(RocksDBMetadataStore.Config.REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP, "100");

        {
            RocksDBMetadataStore store0 = new RocksDBMetadataStore(logDir0.getAbsolutePath(), config);
            store0.load();

            store0.update(tp0, rlSegMetTp0_0_100);
            store0.update(tp0, rlSegMetTp0_101_200);
            store0.update(tp1, rlSegMetTp1_101_300);

            NavigableMap<Long, RemoteLogSegmentId> segments0_0 = store0.getSegmentIds(tp0);
            assertEquals(2, segments0_0.size());
            assertEquals(Long.valueOf(0), segments0_0.firstEntry().getKey());
            assertEquals(rlSegIdTp0_0_100, segments0_0.firstEntry().getValue());
            assertEquals(Long.valueOf(101), segments0_0.higherKey(0L));
            assertEquals(rlSegIdTp0_101_200, segments0_0.get(101L));

            assertEquals(100L, store0.getMetaData(rlSegIdTp0_0_100).endOffset());

            NavigableMap<Long, RemoteLogSegmentId> segments0_1 = store0.getSegmentIds(tp1);
            assertEquals(1, segments0_1.size());
            assertEquals(Long.valueOf(101), segments0_1.firstEntry().getKey());
            assertEquals(rlSegIdTp1_101_300, segments0_1.firstEntry().getValue());

            assertEquals(rlSegMetTp1_101_300, store0.getMetaData(rlSegIdTp1_101_300));

            store0.flush();
        }

        copyDir(logDir0.toPath(), logDir1.toPath());

        {
            RocksDBMetadataStore store1 = new RocksDBMetadataStore(logDir1.getAbsolutePath(), config);
            store1.load();
            NavigableMap<Long, RemoteLogSegmentId> segments1_0 = store1.getSegmentIds(tp0);
            assertEquals(2, segments1_0.size());
            assertEquals(Long.valueOf(0), segments1_0.firstEntry().getKey());
            assertEquals(rlSegIdTp0_0_100, segments1_0.firstEntry().getValue());
            assertEquals(Long.valueOf(101), segments1_0.higherKey(0L));
            assertEquals(rlSegIdTp0_101_200, segments1_0.get(101L));

            assertEquals(rlSegMetTp0_0_100, store1.getMetaData(rlSegIdTp0_0_100));
            assertEquals(rlSegMetTp0_101_200, store1.getMetaData(rlSegIdTp0_101_200));

            NavigableMap<Long, RemoteLogSegmentId> segments1_1 = store1.getSegmentIds(tp1);
            assertEquals(1, segments1_1.size());
            assertEquals(Long.valueOf(101), segments1_1.firstEntry().getKey());
            assertEquals(rlSegIdTp1_101_300, segments1_1.firstEntry().getValue());

            store1.update(tp2, rlSegMetTp2_150_400);
            NavigableMap<Long, RemoteLogSegmentId> segments1_2 = store1.getSegmentIds(tp2);
            assertEquals(1, segments1_2.size());
            assertEquals(Long.valueOf(150), segments1_2.firstEntry().getKey());
            assertEquals(rlSegIdTp2_150_400, segments1_2.firstEntry().getValue());

            store1.flush();

            assertEquals(rlSegMetTp1_101_300, store1.getMetaData(rlSegIdTp1_101_300));
            assertEquals(rlSegMetTp2_150_400, store1.getMetaData(rlSegIdTp2_150_400));
        }
    }

    private void copyDir(Path src, Path des) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copy(source, des.resolve(src.relativize(source))));
        }
    }

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
