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
package org.apache.kafka.rsm.s3;

import org.apache.kafka.common.log.remote.storage.LogSegmentData;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentContext;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.log.remote.storage.RemoteStorageManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import com.amazonaws.SdkClientException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManager.class);

    private AwsClientBuilder.EndpointConfiguration endpointConfiguration = null;

    private String bucket;
    private AmazonS3 s3Client;
    private TransferManager transferManager;

    public S3RemoteStorageManager() {
    }

    // for testing
    S3RemoteStorageManager(final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
        Objects.requireNonNull(endpointConfiguration, "endpointConfiguration must not be null");
        this.endpointConfiguration = endpointConfiguration;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");

        final S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(configs);
        this.bucket = config.s3BucketName();

        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        if (this.endpointConfiguration == null) {
            s3ClientBuilder = s3ClientBuilder.withRegion(config.s3Region());
        } else {
            s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        }

        // It's fine to pass null in here.
        s3ClientBuilder.setCredentials(config.awsCredentialsProvider());

        s3Client = s3ClientBuilder.build();
        transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
    }

    @Override
    public RemoteLogSegmentContext copyLogSegment(final RemoteLogSegmentId remoteLogSegmentId,
                                                  final LogSegmentData logSegmentData) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentId, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        try {
            final String logFileKey = s3Key(remoteLogSegmentId, logSegmentData.logSegment().getName());
            log.debug("Uploading log file: {}", logFileKey);
            final Upload logFileUpload = transferManager.upload(this.bucket, logFileKey, logSegmentData.logSegment());

            final String offsetIndexFileKey = s3Key(remoteLogSegmentId, logSegmentData.offsetIndex().getName());
            log.debug("Uploading offset index file: {}", offsetIndexFileKey);
            final Upload offsetIndexFileUpload = transferManager.upload(this.bucket, offsetIndexFileKey, logSegmentData.offsetIndex());

            final String timeIndexFileKey = s3Key(remoteLogSegmentId, logSegmentData.timeIndex().getName());
            log.debug("Uploading time index file: {}", timeIndexFileKey);
            final Upload timeIndexFileUpload = transferManager.upload(this.bucket, timeIndexFileKey, logSegmentData.timeIndex());

            logFileUpload.waitForUploadResult();
            offsetIndexFileUpload.waitForUploadResult();
            timeIndexFileUpload.waitForUploadResult();

            // TODO clean up in case of interruption

            return new S3RemoteLogSegmentContext(
                logSegmentData.logSegment().getName(),
                logSegmentData.offsetIndex().getName(),
                logSegmentData.timeIndex().getName()
            );
        } catch (final SdkClientException e) {
            throw new RemoteStorageException("Error uploading remote log segment " + remoteLogSegmentId, e);
        } catch (final InterruptedException e) {
            throw new RemoteStorageException("Uploading remote log segment " + remoteLogSegmentId + " interrupted", e);
        }
    }

    @Override
    public InputStream fetchLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                           final Long startPosition, final Long endPosition) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");
        Objects.requireNonNull(startPosition, "startPosition must not be null");

        if (startPosition < 0) {
            throw new IllegalArgumentException("startPosition must be non-negative");
        }

        if (endPosition != null && endPosition < startPosition) {
            throw new IllegalArgumentException("endPosition must >= startPosition");
        }

        final S3RemoteLogSegmentContext context = deserializeS3RemoteLogSegmentContext(remoteLogSegmentMetadata.remoteLogSegmentContext());
        final String logFileKey = s3Key(remoteLogSegmentMetadata.remoteLogSegmentId(), context.logFileName());

        try {
            final GetObjectRequest getObjectRequest;
            if (endPosition != null) {
                getObjectRequest = new GetObjectRequest(bucket, logFileKey).withRange(startPosition, endPosition);
            } else {
                getObjectRequest = new GetObjectRequest(bucket, logFileKey).withRange(startPosition);
            }
            final S3Object s3Object = s3Client.getObject(getObjectRequest);
            return s3Object.getObjectContent();
        } catch (final Exception e) {
            throw new RemoteStorageException("Error fetching log segment data from " + logFileKey, e);
        }
    }

    @Override
    public InputStream fetchOffsetIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final S3RemoteLogSegmentContext context = deserializeS3RemoteLogSegmentContext(remoteLogSegmentMetadata.remoteLogSegmentContext());
        final String offsetIndexFileKey = s3Key(remoteLogSegmentMetadata.remoteLogSegmentId(), context.offsetIndexFileName());

        try {
            final S3Object s3Object = s3Client.getObject(bucket, offsetIndexFileKey);
            return s3Object.getObjectContent();
        } catch (final SdkClientException e) {
            throw new RemoteStorageException("Error fetching offset index from " + offsetIndexFileKey, e);
        }
    }

    @Override
    public InputStream fetchTimestampIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final S3RemoteLogSegmentContext context = deserializeS3RemoteLogSegmentContext(remoteLogSegmentMetadata.remoteLogSegmentContext());
        final String timeIndexFileKey = s3Key(remoteLogSegmentMetadata.remoteLogSegmentId(), context.timeIndexFileName());

        try {
            final S3Object s3Object = s3Client.getObject(bucket, timeIndexFileKey);
            return s3Object.getObjectContent();
        } catch (final SdkClientException e) {
            throw new RemoteStorageException("Error fetching timestamp index from " + timeIndexFileKey, e);
        }
    }

    @Override
    public void deleteLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata must not be null");

        final S3RemoteLogSegmentContext context = deserializeS3RemoteLogSegmentContext(remoteLogSegmentMetadata.remoteLogSegmentContext());
        final String logFileKey = s3Key(remoteLogSegmentMetadata.remoteLogSegmentId(), context.logFileName());
        final String offsetIndexFileKey = s3Key(remoteLogSegmentMetadata.remoteLogSegmentId(), context.offsetIndexFileName());
        final String timeIndexFileKey = s3Key(remoteLogSegmentMetadata.remoteLogSegmentId(), context.timeIndexFileName());

        try {
            final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket)
                .withKeys(logFileKey, offsetIndexFileKey, timeIndexFileKey);
            s3Client.deleteObjects(deleteObjectsRequest);
        } catch (final SdkClientException e) {
            throw new RemoteStorageException(String.format("Error deleting %s, %s or %s", logFileKey, offsetIndexFileKey, timeIndexFileKey), e);
        }
    }

    @Override
    public void close() throws IOException {
        // intentionally left blank
    }

    private String s3Key(final RemoteLogSegmentId remoteLogSegmentId, final String fileName) {
        return remoteLogSegmentId.topicPartition().toString() + "/" + fileName + "." + remoteLogSegmentId.id();
    }

    private S3RemoteLogSegmentContext deserializeS3RemoteLogSegmentContext(final byte[] contextBytes) throws RemoteStorageException {
        try {
            return S3RemoteLogSegmentContext.fromBytes(contextBytes);
        } catch (final Exception e) {
            throw new RemoteStorageException("Error deserializing S3RemoteLogSegmentContext", e);
        }
    }
}
