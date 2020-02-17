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
package integration.kafka.remote

import java.io.{File, InputStream}
import java.nio.file.Files.newInputStream
import java.nio.file.StandardOpenOption.READ
import java.nio.file.Files
import java.{lang, util}
import java.util.Optional

import integration.kafka.remote.LocalRemoteStorageManager.{checkArgument, deleteFilesOnly, deleteQuietly, directory, emptyContext, offsetSuffix, segmentSuffix, timestampSuffix, transfer, defaultTransferer}
import kafka.utils.Logging
import org.apache.kafka.common.log.remote.storage._

import scala.compat.java8.OptionConverters._

/**
  * An implementation of [[RemoteStorageManager]] which relies on the local file system to store offloaded log
  * segments and associated data.
  *
  * Due to the consistency semantic of POSIX-compliant file systems, this remote storage provides strong
  * read-after-write consistency and a segment's data can be accessed once the copy to the storage succeeded.
  *
  * In order to guarantee isolation, independence, reproducibility and consistency of unit and integration
  * tests, the scope of a storage implemented by this class, and identified via the storage ID provided to the
  * constructor, should be limited to a test or well-defined self-contained use-case.
  *
  * @param storageId The ID assigned to this storage and used for reference. Using descriptive, unique ID is
  *                  recommended to ensure a storage can be unambiguously associated to the test which created it.
  *
  * @param deleteOnClose Delete all files and directories from this storage on close, substantially removing it
  *                      entirely from the file system.
  *
  * @param transferer The implementation of the transfer of the data of the canonical segment and index files to
  *                   this storage.
  *                   @todo use this to test failure mode in copyLogSegment.
  */
final class LocalRemoteStorageManager(val storageId: String,
                                      val deleteOnClose: Boolean = false,
                                      val transferer: (File, File) => Unit = defaultTransferer)
  extends RemoteStorageManager with Logging {

  val storageDirectory = {
    val (dir, existed) = directory(s"remote-storage/$storageId")
    if (existed) {
      logger.warn(s"Remote storage with ID $storageId already exists on the file system. Any data already in the " +
        s"remote storage will not be deleted and may result in an inconsistent state and/or provide stale data.")
    }
    dir
  }

  logger.info(s"Created local remote storage: $storageId.")

  override def copyLogSegment(id: RemoteLogSegmentId, data: LogSegmentData): RemoteLogSegmentContext = {
    wrap { () =>
      val remote = new RemoteLogSegmentFiles(id, shouldExist = false)
      try {
        remote.copyAll(data)

      } catch {
        case e: Exception =>
          //
          // Keep the storage in a consistent state, i.e. a segment stored should always have with its
          // associated offset and time indexes stored as well. Here, delete any file which was copied
          // before the exception was hit. The exception is re-thrown as no remediation is expected in
          // the current scope.
          //
          remote.deleteAll()
          throw e
      }

      emptyContext
    }
  }

  override def fetchLogSegmentData(metadata: RemoteLogSegmentMetadata,
                                   startPosition: lang.Long,
                                   endPosition: Optional[lang.Long]): InputStream = {

    checkArgument(startPosition >= 0, s"Start position must be positive: $startPosition")
    endPosition.asScala.foreach { pos =>
      checkArgument(pos >= startPosition,
        s"end position $endPosition cannot be less than start position $startPosition")
    }

    wrap { () =>
      val remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), shouldExist = true)
      val inputStream = newInputStream(remote.logSegment.toPath, READ)
      inputStream.skip(startPosition)

      // endPosition is ignored at this stage. A wrapper around the file input stream can implement
      // the upper bound on the stream.

      inputStream
    }
  }

  override def fetchOffsetIndex(metadata: RemoteLogSegmentMetadata): InputStream = {
    wrap { () =>
      val remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), shouldExist = true)
      newInputStream(remote.offsetIndex.toPath, READ)
    }
  }

  override def fetchTimestampIndex(metadata: RemoteLogSegmentMetadata): InputStream = {
    wrap { () =>
      val remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), shouldExist = true)
      newInputStream(remote.timeIndex.toPath, READ)
    }
  }

  override def deleteLogSegment(metadata: RemoteLogSegmentMetadata): Boolean = {
    wrap { () =>
      val remote = new RemoteLogSegmentFiles(metadata.remoteLogSegmentId(), shouldExist = true)
      remote.deleteAll()
    }
  }

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def close(): Unit = {
    wrap { () =>
      if (deleteOnClose) {
        try {
          val success = !storageDirectory.listFiles().map { topicPartitionDirectory =>

            if (!topicPartitionDirectory.isDirectory) {
              logger.warn(
                s"Found file $topicPartitionDirectory which is not a remote topic-partition directory. " +
                s"Stopping the deletion process.")
              //
              // If an unexpected state is encountered, do not proceed with the delete of the local storage,
              // keeping it for post-mortem analysis. Do not throw either, in an attempt to keep the close()
              // method quiet.
              //
              return
            }

            //
            // The topic-partition directory is deleted only if all files inside it have been deleted successfully
            // thanks to the short-circuit operand. Yes, this is bad to rely on that to drive the execution flow.
            //
            deleteFilesOnly(topicPartitionDirectory.listFiles()) && deleteQuietly(topicPartitionDirectory)

          }.exists(!_)

          if (success) {
            deleteQuietly(storageDirectory)
          }

        } catch {
          case e: Exception =>
            logger.error("Error while deleting remote storage. Stopping the deletion process.", e)
        }
      }
    }
  }

  private def wrap[U](f: () => U): U = {
    try { f() }
    catch {
      case rse: RemoteStorageException => throw rse
      case e: Exception => throw new RemoteStorageException("Internal error in local remote storage", e)
    }
  }

  final class RemoteLogSegmentFiles(id: RemoteLogSegmentId, shouldExist: Boolean) {
    val partitionDirectory = {
      val tp = id.topicPartition()
      directory(s"${tp.topic()}-${tp.partition()}", storageDirectory)._1
    }

    var logSegment = remoteFile(id, segmentSuffix, shouldExist)
    var offsetIndex = remoteFile(id, offsetSuffix, shouldExist)
    var timeIndex = remoteFile(id, timestampSuffix, shouldExist)

    def copyAll(data: LogSegmentData): Unit = {
      transfer(transferer) {
        Seq((data.logSegment(), logSegment), (data.offsetIndex(), offsetIndex), (data.timeIndex(), timeIndex))
      }
    }

    def deleteAll(): Boolean = {
      deleteFilesOnly(Seq(logSegment, offsetIndex, timeIndex)) && deleteQuietly(partitionDirectory)
    }

    private def remoteFile(id: RemoteLogSegmentId, suffix: String, shouldExist: Boolean): File = {
      val file = new File(partitionDirectory, s"${id.id().toString}-$suffix")
      if (!file.exists() && shouldExist) {
        throw new RemoteResourceNotFoundException(id, s"File ${file.getName} not found")
      }

      file
    }
  }
}

object LocalRemoteStorageManager extends Logging {
  private val segmentSuffix = "segment"
  private val offsetSuffix = "offset"
  private val timestampSuffix = "timestamp"

  private val emptyContext = new RemoteLogSegmentContext {
    override def asBytes(): Array[Byte] = Array.emptyByteArray
  }

  private def directory(relativePath: String, parent: File = new File(".")): (File, Boolean) = {
    val directory = new File(parent, relativePath)
    val existed = directory.exists()
    if (!existed) {
      logger.trace(s"Creating directory: ${directory.getAbsolutePath}")
      directory.mkdirs()
    }

    (directory, existed)
  }

  private def defaultTransferer(from: File, to: File): Unit = Files.copy(from.toPath, to.toPath)

  private def transfer(transferer: (File, File) => Unit)(sourceToDest: Seq[(File, File)]) = {
    sourceToDest.foreach(x => {
      val (from, to) = (x._1.getAbsolutePath, x._2.getAbsolutePath)
      try {
        transferer.apply(x._1, x._2)
        logger.trace(s"Copied $from to remote storage $to")

      } catch {
        case e: Exception => throw new RemoteStorageException(s"Could not copy $from to remote storage $to", e)
      }
    })
  }

  private def deleteFilesOnly(files: Seq[File]): Boolean = {
    !files.map(file => {
      if (file.isDirectory) {
        logger.warn(s"Found unexpected directory ${file.getAbsolutePath}. Will not delete.")
        false

      } else {
        deleteQuietly(file)
      }
    }).exists(!_)
  }

  private def deleteQuietly(file: File): Boolean = {
    try {
      logger.trace(s"Deleting ${file.getAbsolutePath}")
      file.delete()

    } catch {
      case e: Exception =>
        logger.error(s"Encountered error while deleting ${file.getAbsolutePath}", e)
        false
    }
  }

  private def checkArgument(valid: Boolean, message: String): Unit = {
    if (!valid) throw new IllegalArgumentException(message)
  }
}
