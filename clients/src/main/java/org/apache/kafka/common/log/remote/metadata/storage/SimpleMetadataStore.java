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
import org.apache.kafka.common.utils.Utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SimpleMetadataStore implements MetadataStore {
    private static final String COMMITTED_LOG_METADATA_FILE_NAME = "_rlmm_committed_metadata_log";


    private ConcurrentSkipListMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idWithSegmentMetadata =
        new ConcurrentSkipListMap<>();
    private Map<TopicPartition, NavigableMap<Long, RemoteLogSegmentId>> partitionsWithSegmentIds =
        new ConcurrentHashMap<>();
    private final File metadataStoreFile;

    SimpleMetadataStore(String logDir, Map<String, ?> configs) {
        metadataStoreFile = new File(logDir, COMMITTED_LOG_METADATA_FILE_NAME);

        if (!metadataStoreFile.exists()) {
            try {
                metadataStoreFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        write(idWithSegmentMetadata.values());
    }

    @Override
    public synchronized void load() throws IOException {
        // checking for empty files.
        if (metadataStoreFile.length() == 0) {
            return;
        }

        List<RemoteLogSegmentMetadata> remoteLogSegmentMetadatas;
        try (FileInputStream fis = new FileInputStream(metadataStoreFile);
             RLSMSerDe.RLSMDeserializer deserializer = new RLSMSerDe.RLSMDeserializer()) {

            remoteLogSegmentMetadatas = new ArrayList<>();

            // read version
            ByteBuffer versionBytes = ByteBuffer.allocate(2);
            fis.read(versionBytes.array());
            short version = versionBytes.getShort();

            ByteBuffer lenBuffer = ByteBuffer.allocate(4);

            //read the length of each entry
            while (fis.read(lenBuffer.array()) != -1) {
                final int len = lenBuffer.getInt();
                lenBuffer.flip();

                //read the entry
                byte[] data = new byte[len];
                final int read = fis.read(data);
                if (read != len) {
                    throw new IOException("Invalid amount of data read, file may have been corrupted.");
                }

                final RemoteLogSegmentMetadata rlsm = deserializer.deserialize(null, version,
                    ByteBuffer.wrap(data));
                remoteLogSegmentMetadatas.add(rlsm);
            }
        }

        for (RemoteLogSegmentMetadata entry : remoteLogSegmentMetadatas) {
            partitionsWithSegmentIds.computeIfAbsent(entry.remoteLogSegmentId().topicPartition(),
                k -> new ConcurrentSkipListMap<>()).put(entry.startOffset(), entry.remoteLogSegmentId());
            idWithSegmentMetadata.put(entry.remoteLogSegmentId(), entry);
        }
    }

    @Override
    public void update(TopicPartition tp, RemoteLogSegmentMetadata metadata) {
        final NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds
            .computeIfAbsent(tp, topicPartition -> new ConcurrentSkipListMap<>());
        if (metadata.markedForDeletion()) {
            idWithSegmentMetadata.remove(metadata.remoteLogSegmentId());
            // todo-tier check for concurrent updates when leader/follower switches occur
            map.remove(metadata.startOffset());
        } else {
            map.put(metadata.startOffset(), metadata.remoteLogSegmentId());
            idWithSegmentMetadata.put(metadata.remoteLogSegmentId(), metadata);
        }
    }

    @Override
    public NavigableMap<Long, RemoteLogSegmentId> getSegmentIds(TopicPartition tp) {
        return partitionsWithSegmentIds.get(tp);
    }

    @Override
    public RemoteLogSegmentMetadata getMetaData(RemoteLogSegmentId id) {
        return idWithSegmentMetadata.get(id);
    }

    public synchronized void write(Collection<RemoteLogSegmentMetadata> remoteLogSegmentMetadatas) throws IOException {
        File newMetadataStoreFile = new File(metadataStoreFile.getAbsolutePath() + ".new");
        try (FileOutputStream fileOutputStream = new FileOutputStream(newMetadataStoreFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
             RLSMSerDe.RLSMSerializer serializer = new RLSMSerDe.RLSMSerializer()) {

            //write version
            ByteBuffer versionBuffer = ByteBuffer.allocate(2);
            versionBuffer.putShort((short) 0);
            bufferedOutputStream.write(versionBuffer.array());

            ByteBuffer lenBuffer = ByteBuffer.allocate(4);

            // write each entry
            for (RemoteLogSegmentMetadata remoteLogSegmentMetadata : remoteLogSegmentMetadatas) {
                final byte[] serializedBytes = serializer.serialize(null, remoteLogSegmentMetadata, false);
                // write length
                lenBuffer.putInt(serializedBytes.length);
                bufferedOutputStream.write(lenBuffer.array());
                lenBuffer.flip();

                //write data
                bufferedOutputStream.write(serializedBytes);
            }

            fileOutputStream.getFD().sync();
        }

        Utils.atomicMoveWithFallback(newMetadataStoreFile.toPath(), metadataStoreFile.toPath());
    }
}
