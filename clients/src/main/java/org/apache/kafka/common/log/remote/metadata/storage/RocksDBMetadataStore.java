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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public class RocksDBMetadataStore implements MetadataStore {
    private static String DB_DIR = "remote-segment-metadata";

    public static class Config extends AbstractConfig {
        public static final String REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP = "remote.log.metadata.rocksdb.cache.size.mb";
        public static final String REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_DOC = "Remote segment metadata store RocksDB in-memory cache size in MB";
        private static final ConfigDef CONFIG_DEF;

        static {
            CONFIG_DEF = new ConfigDef().define(REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP, INT, 50, atLeast(1), MEDIUM, REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_DOC);
        }

        public Config(Map<?, ?> originals) {
            super(CONFIG_DEF, originals);
        }
    }

    public static void loadLibrary() {
        RocksDB.loadLibrary();
    }

    RocksDB db;
    Cache cache;
    RLSMSerDe.RLSMSerializer serializer = new RLSMSerDe.RLSMSerializer();
    RLSMSerDe.RLSMDeserializer deserializer = new RLSMSerDe.RLSMDeserializer();

    RocksDBMetadataStore(String logDir, Map<String, ?> configs) {
        Config conf = new Config(configs);

        cache = new LRUCache(conf.getInt(Config.REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP));
        final Options options = new Options();
        options.setCreateIfMissing(true);
        options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
        File dir = new File(logDir + File.separator + DB_DIR);
        try {
            Files.createDirectories(dir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, dir.getAbsolutePath());
        } catch(IOException | RocksDBException ex) {
            throw new KafkaException("Failed to initialize RocksDB: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void flush() throws IOException {
        FlushOptions options = new FlushOptions();
        options.setWaitForFlush(true);
        try {
            db.flush(options);
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to persist remote segment metadata to local storage: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void load() throws IOException {
    }

    @Override
    public void update(TopicPartition tp, RemoteLogSegmentMetadata metadata) {
        try {
            WriteBatch batch = new WriteBatch();
            final NavigableMap<Long, RemoteLogSegmentId> segmentIds = getSegmentIds(tp);
            if (metadata.markedForDeletion()) {
                batch.delete(segmentKey(metadata.remoteLogSegmentId().id()));
                // todo-tier check for concurrent updates when leader/follower switches occur
                segmentIds.remove(metadata.startOffset());
            } else {
                segmentIds.put(metadata.startOffset(), metadata.remoteLogSegmentId());
                batch.put(segmentKey(metadata.remoteLogSegmentId().id()), serializer.serialize(null, metadata));
            }
            batch.put(tpKey(tp), RemoteSegmentMapSerDe.serialize(segmentIds));
            db.write(new WriteOptions(), batch);
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to update remote segment metadata in RocksDB: " + ex.getMessage(), ex);
        }
    }

    @Override
    public NavigableMap<Long, RemoteLogSegmentId> getSegmentIds(TopicPartition tp) {
        try {
            byte[] map_bytes = db.get(tpKey(tp));
            if (map_bytes != null) {
                return RemoteSegmentMapSerDe.deserialize(tp, map_bytes);
            } else {
                return new ConcurrentSkipListMap<>();
            }
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to read from RocksDB: " + ex.getMessage(), ex);
        }
    }

    @Override
    public RemoteLogSegmentMetadata getMetaData(RemoteLogSegmentId id) {
        try {
            byte[] bytes = db.get(segmentKey(id.id()));
            return deserializer.deserialize(null, bytes);
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to get remote segment metadata from RocksDB: " + ex.getMessage(), ex);
        }
    }

    /**
     * Segment UUID to RocksDB Key
     */
    private byte[] segmentKey(UUID id) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[17]);
        bb.put((byte)0xFF); // magic byte for segment UUID (topic-partition is UTF8 encoded, which cannot start with 0xFF)
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * TopicPartition to RocksDB Key
     *
     * @return UTF-8 encoded "topic-partition" string.
     */
    private byte[] tpKey(TopicPartition tp) {
        return tp.toString().getBytes(StandardCharsets.UTF_8);
    }
}
