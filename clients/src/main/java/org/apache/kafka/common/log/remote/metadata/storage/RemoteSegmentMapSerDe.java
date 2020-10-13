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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

public class RemoteSegmentMapSerDe {
    private static final Field.Int64 START_OFFSET_FIELD = new Field.Int64("start-offset",
        "Start offset of the remote log segment");
    private static final Field.UUID SEGMENT_ID_FIELD = new Field.UUID("segment-id",
        "UUID of the remote log segment");
    private static final Schema SEGMENT_MAP_ENTRY_SCHEMA_V0 = new Schema(
        START_OFFSET_FIELD,
        SEGMENT_ID_FIELD
    );
    private static final String SEGMENT_MAP_FIELD_NAME = "segment-map";
    private static final Schema SCHEMA_V0 = new Schema(new Field(SEGMENT_MAP_FIELD_NAME, new ArrayOf(SEGMENT_MAP_ENTRY_SCHEMA_V0)));
    private static final Schema[] SCHEMAS = {SCHEMA_V0};

    public static byte[] serialize(NavigableMap<Long, RemoteLogSegmentId> data) {
        Struct struct = new Struct(SCHEMA_V0);
        Struct[] entry_structs = new Struct[data.size()];
        int i = 0;
        for (Map.Entry<Long, RemoteLogSegmentId> entry : data.entrySet()) {
            entry_structs[i] = new Struct(SEGMENT_MAP_ENTRY_SCHEMA_V0);
            entry_structs[i].set(START_OFFSET_FIELD, entry.getKey());
            entry_structs[i].set(SEGMENT_ID_FIELD, entry.getValue().id());
            i++;
        }
        struct.set(SEGMENT_MAP_FIELD_NAME, entry_structs);

        final int size = SCHEMA_V0.sizeOf(struct);
        ByteBuffer byteBuffer;
        byteBuffer = ByteBuffer.allocate(size + 2);
        byteBuffer.putShort((short)0); // v0
        SCHEMA_V0.write(byteBuffer, struct);

        return byteBuffer.array();
    }

    public static NavigableMap<Long, RemoteLogSegmentId> deserialize(TopicPartition tp, byte[] data) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        short version = byteBuffer.getShort();
        return deserialize(tp, version, byteBuffer);
    }

    private static NavigableMap<Long, RemoteLogSegmentId> deserialize(TopicPartition tp, short version, ByteBuffer byteBuffer) {
        NavigableMap<Long, RemoteLogSegmentId> map = new ConcurrentSkipListMap<>();
        final Struct struct = SCHEMAS[version].read(byteBuffer);
        Object[] segments = struct.getArray(SEGMENT_MAP_FIELD_NAME);
        for (Object seg : segments) {
            Struct seg_struct = (Struct) seg;
            Long startOffset = seg_struct.get(START_OFFSET_FIELD);
            UUID id = seg_struct.get(SEGMENT_ID_FIELD);
            map.put(startOffset, new RemoteLogSegmentId(tp, id));
        }
        return map;
    }
}
