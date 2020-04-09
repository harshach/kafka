package org.apache.kafka.common.log.remote.storage;

import org.slf4j.*;

import java.io.*;
import java.util.*;

import static java.lang.String.*;
import static java.util.Arrays.*;
import static java.util.Collections.*;
import static java.util.Objects.*;
import static java.util.function.Function.*;
import static java.util.stream.Collectors.*;
import static org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.*;
import static org.apache.kafka.common.log.remote.storage.RemoteTopicPartitionDirectory.*;
import static org.slf4j.LoggerFactory.*;

public final class RemoteLogSegmentFileset {

    public enum RemoteLogSegmentFileType {
        SEGMENT,
        OFFSET_INDEX,
        TIME_INDEX;

        public String toFilename(final UUID uuid) {
            return format("%s-%s", uuid.toString(), name().toLowerCase());
        }

        private static final char separator = '-';

        public static RemoteLogSegmentFileType getFileType(final String filename) {
            final int separatorIndex = filename.lastIndexOf(separator);
            if (separatorIndex == -1) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename));
            }

            try {
                return RemoteLogSegmentFileType.valueOf(filename.substring(1 + separatorIndex).toUpperCase());

            } catch (final EnumConstantNotPresentException e) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename));
            }
        }

        public static UUID getUUID(final String filename) {
            final int separatorIndex = filename.lastIndexOf(separator);
            if (separatorIndex == -1) {
                throw new IllegalArgumentException(format("Not a remote log segment file: %s", filename));
            }

            return UUID.fromString(filename.substring(0, separatorIndex));
        }
    }

    private static final Logger LOGGER = getLogger(RemoteLogSegmentFileset.class);

    public static RemoteLogSegmentFileset openFileset(final File storageDir, final RemoteLogSegmentId id) {
        final RemoteTopicPartitionDirectory tpDir = openTopicPartitionDirectory(id.topicPartition(), storageDir);
        final File partitionDirectory = tpDir.getDirectory();
        final UUID uuid = id.id();

        final Map<RemoteLogSegmentFileType, File> files = stream(RemoteLogSegmentFileType.values())
                .collect(toMap(identity(), type -> new File(partitionDirectory, type.toFilename(uuid))));

        return new RemoteLogSegmentFileset(tpDir, id, files);
    }

    private final RemoteTopicPartitionDirectory partitionDirectory;
    private final RemoteLogSegmentId remoteLogSegmentId;
    private final Map<RemoteLogSegmentFileType, File> files;

    RemoteLogSegmentFileset(final RemoteTopicPartitionDirectory topicPartitionDirectory,
                            final RemoteLogSegmentId remoteLogSegmentId,
                            final Map<RemoteLogSegmentFileType, File> files) {

        this.partitionDirectory = requireNonNull(topicPartitionDirectory);
        this.remoteLogSegmentId = requireNonNull(remoteLogSegmentId);
        this.files = unmodifiableMap(files);
    }

    public RemoteTopicPartitionDirectory getPartitionDirectory() {
        return partitionDirectory;
    }

    public RemoteLogSegmentId getRemoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    public File getFile(final RemoteLogSegmentFileType type) {
        return files.get(type);
    }

    public boolean delete() {
        return deleteFilesOnly(files.values());
    }

    public void copy(final Transferer transferer, final LogSegmentData data) throws IOException {
        transferer.transfer(data.logSegment(), files.get(SEGMENT));
        transferer.transfer(data.offsetIndex(), files.get(OFFSET_INDEX));
        transferer.transfer(data.timeIndex(), files.get(TIME_INDEX));
    }

    public static RemoteLogSegmentFileset openFileset(final RemoteTopicPartitionDirectory tpDirectory, final UUID uuid) {
        final Map<RemoteLogSegmentFileType, File> files =
                stream(tpDirectory.getDirectory().listFiles())
                        .filter(file -> file.getName().startsWith(uuid.toString()))
                        .collect(toMap(file -> getFileType(file.getName()), identity()));

        final RemoteLogSegmentId id = new RemoteLogSegmentId(tpDirectory.getTopicPartition(), uuid);
        return new RemoteLogSegmentFileset(tpDirectory, id, files);
    }

    public static boolean deleteFilesOnly(final Collection<File> files) {
        final Optional<File> notAFile = files.stream().filter(f -> !f.isFile()).findAny();

        if (notAFile.isPresent()) {
            LOGGER.warn(format("Found unexpected directory %s. Will not delete.", notAFile.get().getAbsolutePath()));
            return false;
        }

        return files.stream().map(RemoteLogSegmentFileset::deleteQuietly).reduce(true, Boolean::logicalAnd);
    }

    public static boolean deleteQuietly(final File file) {
        try {
            LOGGER.trace("Deleting " + file.getAbsolutePath());
            return file.delete();

        } catch (final Exception e) {
            LOGGER.error(format("Encountered error while deleting %s", file.getAbsolutePath()));
        }

        return false;
    }
}
