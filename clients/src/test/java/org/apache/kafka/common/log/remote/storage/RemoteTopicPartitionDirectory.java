package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.slf4j.*;

import java.io.*;
import java.util.*;
import java.util.stream.*;

import static java.lang.String.*;
import static java.util.Arrays.*;
import static java.util.Objects.*;
import static java.util.stream.Collectors.*;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.*;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.*;
import static org.slf4j.LoggerFactory.*;

/**
 * Represents a topic-partition directory in the local tiered storage under which filesets for
 * log segments are stored.
 *
 * /storage-directory/topic-partition/-| uuid1-segment
 *                                     | uuid1-offset_index
 *                                     | uuid1-time_index
 *                                     | uuid2-segment
 *                                     | uuid2-offset_index
 *                                     | uuid2-offset_index
 */
public final class RemoteTopicPartitionDirectory {
    private static final Logger LOGGER = getLogger(RemoteLogSegmentFileset.class);

    private final File directory;
    private final boolean existed;
    private final TopicPartition topicPartition;

    RemoteTopicPartitionDirectory(final TopicPartition topicPartition, final File directory, final boolean existed) {
        this.topicPartition = requireNonNull(topicPartition);
        this.directory = requireNonNull(directory);
        this.existed = existed;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    boolean didExist() {
        return existed;
    }

    public File getDirectory() {
        return directory;
    }

    boolean delete() {
        return deleteFilesOnly(asList(directory.listFiles())) && deleteQuietly(directory);
    }

    void traverse(final LocalTieredStorageTraverser traverser) {
        traverser.visitTopicPartition(topicPartition);
        listFilesets().stream().forEach(fileset -> traverser.visitSegment(fileset));
    }

    private List<RemoteLogSegmentFileset> listFilesets() {
        Set<UUID> uuids = Arrays.stream(directory.listFiles())
                .map(file -> getUUID(file.getName()))
                .collect(toSet());

        return uuids.stream()
                .map(uuid -> RemoteLogSegmentFileset.openExistingFileset(this, uuid))
                .collect(Collectors.toList());
    }

    public static RemoteTopicPartitionDirectory openTopicPartitionDirectory(
            final TopicPartition topicPartition, final File storageDir) {

        final File directory = new File(storageDir, topicPartition.toString());
        final boolean existed = directory.exists();

        if (!existed) {
            LOGGER.info("Creating directory: " + directory.getAbsolutePath());
            directory.mkdirs();
        }

        return new RemoteTopicPartitionDirectory(topicPartition, directory, existed);
    }

    public static RemoteTopicPartitionDirectory openTopicPartitionDirectory(
            final String dirname, final File parentDir) {

        final char topicParitionSeparator = '-';
        final int separatorIndex = dirname.lastIndexOf(topicParitionSeparator);

        if (separatorIndex == -1) {
            throw new IllegalArgumentException(format(
                    "Invalid format for topic-partition directory: %s", dirname));
        }

        final String topic = dirname.substring(0, separatorIndex);
        final int partition;

        try {
            partition = Integer.parseInt(dirname.substring(separatorIndex + 1));

        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(format(
                    "Invalid format for topic-partition directory: %s", dirname), ex);
        }

        return openTopicPartitionDirectory(new TopicPartition(topic, partition), parentDir);
    }
}
