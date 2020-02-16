package org.apache.kafka.common.log.remote.storage;

import static java.lang.String.format;

public class RemoteResourceNotFoundException extends RemoteStorageException {

    public RemoteResourceNotFoundException(final RemoteLogSegmentId id, final String resourceName) {
        super(format("One of the resource associated to the remote log segment was not found. " +
                "ID: %s Resource name: %s", id.id(), resourceName));
    }
}
