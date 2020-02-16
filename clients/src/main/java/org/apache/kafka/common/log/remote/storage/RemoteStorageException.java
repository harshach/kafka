package org.apache.kafka.common.log.remote.storage;

public class RemoteStorageException extends Exception {

    public RemoteStorageException(final String message) {
        super(message);
    }

    public RemoteStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
