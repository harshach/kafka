package integration.kafka.remote

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Optional, UUID}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteResourceNotFoundException}
import org.junit.rules.TestName
import org.junit.{After, Before, Rule, Test}
import org.scalatest.Assertions.assertThrows
import org.scalatestplus.mockito.MockitoSugar

import scala.Array.emptyByteArray
import scala.annotation.meta.getter

class LocalRemoteStorageManagerTest extends MockitoSugar {
  @(Rule @getter)
  val testName = new TestName

  private var remoteStorageManager: LocalRemoteStorageManager = _
  private var metadata: RemoteLogSegmentMetadata = _

  @Before
  def before(): Unit = {
    remoteStorageManager = newRemoteStorageManager()
    metadata = newRemoteLogSegmentMetata()
  }

  @After
  def after(): Unit = {
    Option(remoteStorageManager).foreach(_.close())
  }

  @Test
  def deleteThrowsIfDataDoesNotExist(): Unit = {
    assertThrows[RemoteResourceNotFoundException] {
      remoteStorageManager.deleteLogSegment(metadata)
    }
  }

  @Test
  def fetchSegmentThrowsIfDataDoesNotExist(): Unit = {
    assertThrows[RemoteResourceNotFoundException] {
      remoteStorageManager.fetchLogSegmentData(metadata, 0L, Optional.of(1L))
    }
  }

  @Test
  def fetchOffsetIndexThrowsIfDataDoesNotExist(): Unit = {
    assertThrows[RemoteResourceNotFoundException] {
      remoteStorageManager.fetchOffsetIndex(metadata)
    }
  }

  @Test
  def fetchTimeIndexThrowsfDataDoesNotExist(): Unit = {
    assertThrows[RemoteResourceNotFoundException] {
      remoteStorageManager.fetchTimestampIndex(metadata)
    }
  }

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss")

  private def newRemoteStorageManager(): LocalRemoteStorageManager = {
    val storageId = s"${getClass.getSimpleName}-${testName.getMethodName}-${formatter.format(LocalDateTime.now())}"
    new LocalRemoteStorageManager(storageId, false)
  }

  private def newRemoteLogSegmentMetata(): RemoteLogSegmentMetadata = {
    val id = new RemoteLogSegmentId(topicPartition, UUID.randomUUID())
    new RemoteLogSegmentMetadata(id, 0, 0, -1, emptyByteArray)
  }

  private val topicPartition = new TopicPartition("aTopic", 1)

}
