package integration.kafka.remote

import java.io.{File, InputStream}
import java.nio.file.Files.newInputStream
import java.nio.file.StandardOpenOption.READ
import java.nio.file.Files
import java.{lang, util}
import java.util.Optional

import integration.kafka.remote.LocalRemoteStorageManager.{checkArgument, copy, deleteFilesOnly, deleteQuietly,
  directory, emptyContext, offsetSuffix, segmentSuffix, timestampSuffix}
import kafka.utils.Logging
import org.apache.kafka.common.log.remote.storage._

import scala.compat.java8.OptionConverters._

final class LocalRemoteStorageManager(val storageId: String,
                                      val deleteOnClose: Boolean = false) extends RemoteStorageManager with Logging {

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
              return
            }

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

    val logSegment = remoteFile(id, segmentSuffix, shouldExist)
    val offsetIndex = remoteFile(id, offsetSuffix, shouldExist)
    val timeIndex = remoteFile(id, timestampSuffix, shouldExist)

    def copyAll(data: LogSegmentData): Unit = {
      copy {
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

  private def copy(sourceToDest: Seq[(File, File)]) = {
    sourceToDest.foreach(x => {
      val (from, to) = (x._1.getAbsolutePath, x._2.getAbsolutePath)
      try {
        Files.copy(x._1.toPath, x._2.toPath)
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
