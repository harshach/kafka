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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.*;

import java.io.*;
import java.nio.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public final class LocalRemoteStorageVerifier {
    private final LocalRemoteStorage remoteStorage;
    private final TopicPartition topicPartition;

    public LocalRemoteStorageVerifier(final LocalRemoteStorage remoteStorage,
                                      final TopicPartition topicPartition) {

        this.remoteStorage = requireNonNull(remoteStorage);
        this.topicPartition = requireNonNull(topicPartition);
    }

    private List<String> expectedPaths(final RemoteLogSegmentId id, final LogSegmentData data) {
        final String rootPath = getStorageRootDirectory();
        final String topicPartitionSubpath = format("%s-%d", topicPartition.topic(), topicPartition.partition());
        final String uuid = id.id().toString();

        return Arrays.asList(
                Paths.get(rootPath, topicPartitionSubpath, uuid + "-segment").toString(),
                Paths.get(rootPath, topicPartitionSubpath, uuid + "-offset").toString(),
                Paths.get(rootPath, topicPartitionSubpath, uuid + "-time").toString()
        );
    }

    /**
     * Verify the remote storage contains remote log segment and associated files for the provided {@code id}.
     *
     * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
     * @param segment The segment stored on Kafka's local storage.
     */
    public void verifyContainsLogSegmentFiles(final RemoteLogSegmentId id, final LogSegmentData segment) {
        expectedPaths(id, segment).forEach(LocalRemoteStorageVerifier::assertFileExists);
    }

    /**
     * Verify the remote storage does NOT contain remote log segment and associated files for the provided {@code id}.
     *
     * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
     * @param segment The segment stored on Kafka's local storage.
     */
    public void verifyLogSegmentFilesAbsent(final RemoteLogSegmentId id, final LogSegmentData segment) {
        expectedPaths(id, segment).forEach(LocalRemoteStorageVerifier::assertFileDoesNotExist);
    }

    /**
     * Compare the content of the remote segment with the provided {@code data} array.
     * This method does not fetch from the remote storage.
     *
     * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
     * @param seg The segment stored on Kafka's local storage.
     */
    public void verifyRemoteLogSegmentMatchesLocal(final RemoteLogSegmentId id, final LogSegmentData seg) {
        final String remoteSegmentPath = expectedPaths(id, seg).get(0);
        assertFileDataEquals(remoteSegmentPath, seg.logSegment().getAbsolutePath());
    }

    /**
     * Verifies the content of the remote segment matches with the {@code expected} array.
     *
     * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
     * @param startPosition The position in the segment to fetch from.
     * @param expected The expected content.
     */
    public void verifyFetchedLogSegment(final RemoteLogSegmentId id, final long startPosition, final byte[] expected) {
        try {
            final InputStream in = remoteStorage.fetchLogSegmentData(newMetadata(id), startPosition, null);
            final ByteBuffer buffer = ByteBuffer.wrap(readFully(in));
            Iterator<Record> records = MemoryRecords.readableRecords(buffer).records().iterator();

            assertTrue(records.hasNext());
            assertEquals(ByteBuffer.wrap(expected), records.next().value());

        } catch (RemoteStorageException | IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Verifies the content of the remote offset index matches with the {@code expected} array.
     *
     * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
     * @param expected The expected content.
     */
    public void verifyFetchedOffsetIndex(final RemoteLogSegmentId id, final byte[] expected) {
        try {
            final InputStream in = remoteStorage.fetchOffsetIndex(newMetadata(id));
            assertArrayEquals(expected, readFully(in));

        } catch (RemoteStorageException | IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Verifies the content of the remote time index matches with the {@code expected} array.
     *
     * @param id The unique ID of the remote log segment and associated resources (e.g. offset and time indexes).
     * @param expected The expected content.
     */
    public void verifyFetchedTimeIndex(final RemoteLogSegmentId id, final byte[] expected) {
        try {
            final InputStream in = remoteStorage.fetchTimestampIndex(newMetadata(id));
            assertArrayEquals(expected, readFully(in));

        } catch (RemoteStorageException | IOException e) {
            throw new AssertionError(e);
        }
    }

    private RemoteLogSegmentMetadata newMetadata(final RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(id, 0, 0, -1L, -1, new byte[0]);
    }

    private String getStorageRootDirectory() {
        try {
            return remoteStorage.getStorageDirectoryRoot();

        } catch (RemoteStorageException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertFileExists(final String path) {
        if (!Paths.get(path).toFile().exists()) {
            throw new AssertionError(format("File %s does not exist", path));
        }
    }

    private static void assertFileDoesNotExist(final String path) {
        if (Paths.get(path).toFile().exists()) {
            throw new AssertionError(format("File %s should not exist", path));
        }
    }

    private static void assertFileDataEquals(final String path1, final String path2) {
        try {
            assertFileExists(path1);
            assertArrayEquals(Files.readAllBytes(Paths.get(path1)), Files.readAllBytes(Paths.get(path2)));

        } catch (final IOException e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] readFully(final InputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] buffer = new byte[1024];
        int len;

        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
        }

        return out.toByteArray();
    }
}
