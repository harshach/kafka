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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.*;
import org.junit.*;
import org.junit.rules.TestName;

import static java.lang.String.format;
import static java.nio.ByteBuffer.*;
import static java.util.Arrays.*;
import static org.apache.kafka.common.log.remote.storage.LocalRemoteStorageSnapshot.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public final class LocalRemoteStorageManagerTest {
    @Rule
    public final TestName testName = new TestName();

    private final LocalLogSegments localLogSegments = new LocalLogSegments();
    private final TopicPartition topicPartition = new TopicPartition("my-topic", 1);

    private LocalRemoteStorage remoteStorage;
    private LocalRemoteStorageVerifier remoteStorageVerifier;

    private void init(Map<String, Object> extraConfig) {
        remoteStorage = new LocalRemoteStorage();
        remoteStorageVerifier = new LocalRemoteStorageVerifier(remoteStorage, topicPartition);

        Map<String, Object> config = new HashMap<>();
        config.put(LocalRemoteStorage.STORAGE_ID_PROP, generateStorageId());
        config.put(LocalRemoteStorage.DELETE_ON_CLOSE_PROP, "true");
        config.putAll(extraConfig);

        remoteStorage.configure(config);
    }

    @Before
    public void before() {
        init(Collections.emptyMap());
    }

    @After
    public void after() {
        remoteStorage.close();
        localLogSegments.deleteAll();
    }

    @Test
    public void copyEmptyLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);
    }

    @Test
    public void copyDataFromLogSegment() throws RemoteStorageException {
        final byte[] data = new byte[]{0, 1, 2};
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(data);

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyRemoteLogSegmentMatchesLocal(id, segment);
    }

    @Test
    public void fetchLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(new byte[]{0, 1, 2});

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedLogSegment(id, 0, new byte[]{0, 1, 2});
        //FIXME: Fetch at arbitrary index does not work as proper support for records need to be added.
    }

    @Test
    public void fetchOffsetIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedOffsetIndex(id, LocalLogSegments.OFFSET_FILE_BYTES);
    }

    @Test
    public void fetchTimeIndex() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);

        remoteStorageVerifier.verifyFetchedTimeIndex(id, LocalLogSegments.TIME_FILE_BYTES);
    }

    @Test
    public void deleteLogSegment() throws RemoteStorageException {
        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);

        remoteStorage.deleteLogSegment(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyLogSegmentFilesAbsent(id, segment);
    }

    @Test
    public void segmentsAreNotDeletedIfDeleteApiIsDisabled() throws RemoteStorageException {
        init(Collections.singletonMap(LocalRemoteStorage.ENABLE_DELETE_API_PROP, "false"));

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment();

        remoteStorage.copyLogSegment(id, segment);
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);

        remoteStorage.deleteLogSegment(newRemoteLogSegmentMetadata(id));
        remoteStorageVerifier.verifyContainsLogSegmentFiles(id, segment);
    }

    @Test
    public void traverseSingleOffloadedRecord() throws RemoteStorageException {
        final byte[] bytes = new byte[]{0, 1, 2};

        final RemoteLogSegmentId id = newRemoteLogSegmentId();
        final LogSegmentData segment = localLogSegments.nextSegment(bytes);

        remoteStorage.copyLogSegment(id, segment);

        remoteStorage.traverse(new LocalRemoteStorageTraverser() {
            @Override
            public void visitTopicPartition(TopicPartition topicPartition) {
                assertEquals(LocalRemoteStorageManagerTest.this.topicPartition, topicPartition);
            }

            @Override
            public void visitRecord(RemoteLogSegmentId segmentId, Record record) {
                assertEquals(wrap(bytes), record.value());
            }

            @Override
            public void visitSegment(RemoteLogSegmentId segmentId) {
                assertEquals(id, segmentId);
            }
        });
    }

    @Test
    public void traverseMultipleOffloadedRecordsInOneSegment() throws RemoteStorageException {
        final byte[] record1 = new byte[]{0, 1, 2};
        final byte[] record2 = new byte[]{3, 4, 5};
        final RemoteLogSegmentId id = newRemoteLogSegmentId();

        remoteStorage.copyLogSegment(id, localLogSegments.nextSegment(record1, record2));

        final LocalRemoteStorageSnapshot snapshot = takeSnapshot(remoteStorage);

        final Map<RemoteLogSegmentId, List<ByteBuffer>> expected = new HashMap<>();
        expected.put(id, asList(wrap(record1), wrap(record2)));

        assertEquals(asList(topicPartition), snapshot.getTopicPartitions());
        assertEquals(expected, snapshot.getRecords());
    }

    @Test
    public void traverseMultipleOffloadedRecordsInTwoSegments() throws RemoteStorageException {
        final byte[] record1a = new byte[]{0, 1, 2};
        final byte[] record2a = new byte[]{3, 4, 5};
        final byte[] record1b = new byte[]{6, 7, 8};
        final byte[] record2b = new byte[]{9, 10, 11};

        final RemoteLogSegmentId idA = newRemoteLogSegmentId();
        final RemoteLogSegmentId idB = newRemoteLogSegmentId();

        remoteStorage.copyLogSegment(idA, localLogSegments.nextSegment(record1a, record2a));
        remoteStorage.copyLogSegment(idB, localLogSegments.nextSegment(record1b, record2b));

        final LocalRemoteStorageSnapshot snapshot = takeSnapshot(remoteStorage);

        final Map<RemoteLogSegmentId, List<ByteBuffer>> expected = new HashMap<>();
        expected.put(idA, asList(wrap(record1a), wrap(record2a)));
        expected.put(idB, asList(wrap(record1b), wrap(record2b)));

        assertEquals(asList(topicPartition), snapshot.getTopicPartitions());
        assertEquals(expected, snapshot.getRecords());
    }

    @Test
    public void fetchThrowsIfDataDoesNotExist() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(RemoteResourceNotFoundException.class,
            () -> remoteStorage.fetchLogSegmentData(metadata, 0L, null));
        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchOffsetIndex(metadata));
        assertThrows(RemoteResourceNotFoundException.class, () -> remoteStorage.fetchTimestampIndex(metadata));
    }

    @Test
    public void assertStartAndEndPositionConsistency() {
        final RemoteLogSegmentMetadata metadata = newRemoteLogSegmentMetadata(newRemoteLogSegmentId());

        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, -1L, null));
        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 1L, -1L));
        assertThrows(IllegalArgumentException.class, () -> remoteStorage.fetchLogSegmentData(metadata, 2L, 1L));
    }

    private RemoteLogSegmentMetadata newRemoteLogSegmentMetadata(final RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, new byte[0]);
    }

    private RemoteLogSegmentId newRemoteLogSegmentId() {
        return new RemoteLogSegmentId(topicPartition, UUID.randomUUID());
    }

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss");

    private String generateStorageId() {
        return format("%s-%s-%s",
                getClass().getSimpleName(), testName.getMethodName(), DATE_TIME_FORMATTER.format(LocalDateTime.now()));
    }

    private static final class LocalLogSegments {
        private static final byte[] OFFSET_FILE_BYTES = "offset".getBytes();
        private static final byte[] TIME_FILE_BYTES = "time".getBytes();

        private static final NumberFormat OFFSET_FORMAT = NumberFormat.getInstance();

        static {
            OFFSET_FORMAT.setMaximumIntegerDigits(20);
            OFFSET_FORMAT.setMaximumFractionDigits(0);
            OFFSET_FORMAT.setGroupingUsed(false);
        }

        private final File segmentDir = new File("local-segments");
        private long baseOffset = 0;

        LocalLogSegments() {
            if (!segmentDir.exists()) {
                segmentDir.mkdir();
            }
        }

        LogSegmentData nextSegment() {
            return nextSegment(new byte[0]);
        }

        LogSegmentData nextSegment(final byte[]... data) {
            final String offset = OFFSET_FORMAT.format(baseOffset);

            try {
                final FileChannel channel = FileChannel.open(
                        Paths.get(segmentDir.getAbsolutePath(), offset + ".log"),
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

                final SimpleRecord[] records = Arrays.stream(data).map(SimpleRecord::new).toArray(SimpleRecord[]::new);

                MemoryRecords.withRecords(CompressionType.NONE, records).writeFullyTo(channel);
                channel.force(true);

                final File segment = new File(segmentDir, offset + ".log");
                final File offsetIndex = new File(segmentDir, offset + ".index");
                final File timeIndex = new File(segmentDir, offset + ".time");

                Files.write(offsetIndex.toPath(), OFFSET_FILE_BYTES);
                Files.write(timeIndex.toPath(), TIME_FILE_BYTES);

                baseOffset += data.length;

                return new LogSegmentData(segment, offsetIndex, timeIndex);

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        void deleteAll() {
            Arrays.stream(segmentDir.listFiles()).forEach(File::delete);
            segmentDir.delete();
        }
    }
}
