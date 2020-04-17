package integration.kafka.tiered.storage

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorageListener, RemoteLogSegmentMetadata}

import scala.collection.{Seq, mutable}

final class TieredStorageFetchCaptor extends LocalTieredStorageListener {
  /* @GuardedBy("this") */
  private val events = mutable.Map[TopicPartition, mutable.Buffer[TieredStorageFetchEvent]]()

  override def onSegmentFetched(metadata: RemoteLogSegmentMetadata,
                                startPosition: java.lang.Long,
                                endPosition: java.lang.Long): Unit = {

    val topicPartition = metadata.remoteLogSegmentId().topicPartition()

    this.synchronized {
      if (!events.contains(topicPartition)) {
        events += topicPartition -> mutable.Buffer[TieredStorageFetchEvent]()
      }

      events(topicPartition) += new TieredStorageFetchEvent(metadata, startPosition, endPosition)
    }
  }

  def getEvents(topicPartition: TopicPartition): Seq[TieredStorageFetchEvent] =
    this.synchronized {
      events(topicPartition)
    }
}

final case class TieredStorageFetchEvent(metadata: RemoteLogSegmentMetadata,
                                         startPosition: Long,
                                         endPosition: Long)

