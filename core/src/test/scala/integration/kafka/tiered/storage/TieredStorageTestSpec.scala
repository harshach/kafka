package integration.kafka.tiered.storage

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageCondition.expectEvent
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType.OFFLOAD_SEGMENT
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorageSnapshot, RemoteLogSegmentFileset}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.assertEquals
import unit.kafka.utils.RecordsMatcher.correspondTo

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}

final case class OffloadedSegmentSpec(val sourceBrokerId: Int,
                                      val topicPartition: TopicPartition,
                                      val baseOffset: Int,
                                      val records: Seq[ProducerRecord[String, String]])

final class RemoteFetchRequestSpec(val fetchOffset: Long, val leaderEpoch: Long)

trait TieredStorageTestAction {

  def execute(context: TieredStorageTestContext)

}

final class CreateTopicAction(val topicName: String,
                              val partitionCount: Int,
                              val replicationFactor: Int,
                              val segmentSize: Int) extends TieredStorageTestAction {

  override def execute(context: TieredStorageTestContext): Unit = {
    val topicProps = new Properties()

    //
    // Ensure offset and time indexes are generated for every record.
    //
    topicProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    if (segmentSize != -1) {
      topicProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * segmentSize).toString)
    }

    //
    // To verify records physically absent from Kafka's storage can be consumed via the tiered storage, we
    // want to delete log segments as soon as possible. When the tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    topicProps.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)

    context.createTopic(topicName, partitionCount, replicationFactor, topicProps)
  }
}

final class ProduceOffloadAction(val offloadedSegmentSpecs: Map[TopicPartition, Seq[OffloadedSegmentSpec]],
                                 val recordsToProduce: Map[TopicPartition, Seq[ProducerRecord[String, String]]])
  extends TieredStorageTestAction {

  private val offloadWaitTimeoutSec = 20
  private implicit val serdes: Serde[String] = Serdes.String()

  override def execute(context: TieredStorageTestContext): Unit = {
    val tieredStorages = context.getTieredStorages
    val localStorages = context.getLocalStorages

    val tieredStorageConditions = offloadedSegmentSpecs.values.flatten.map { spec =>
      expectEvent(tieredStorages.asJava, OFFLOAD_SEGMENT, spec.sourceBrokerId, spec.topicPartition, false)
    }

    context.produce(recordsToProduce.values.flatten)

    tieredStorageConditions.reduce(_ and _).waitUntilTrue(offloadWaitTimeoutSec, TimeUnit.SECONDS)

    recordsToProduce.foreach {
      case (topicPartition, producedRecords) =>
        localStorages.map(_.waitForEarliestOffset(topicPartition, 1L))

        val consumedRecords = context.consume(topicPartition, producedRecords.length)
        assertThat(consumedRecords, correspondTo(producedRecords, topicPartition))
    }

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(tieredStorages(0))

    offloadedSegmentSpecs.foreach {
      case (topicPartition: TopicPartition, specs: Seq[OffloadedSegmentSpec]) =>
        snapshot.getFilesets(topicPartition).asScala
          .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
          .zip(specs)
          .foreach {
            pair => compareRecords(pair._1, pair._2, topicPartition)
          }
    }
  }

  private def compareRecords(fileset: RemoteLogSegmentFileset,
                             spec: OffloadedSegmentSpec,
                             topicPartition: TopicPartition): Unit = {

    // Records found in the local tiered storage.
    val discoveredRecords = fileset.getRecords.asScala

    // Records expected to be found, based on what was sent by the producer.
    val producerRecords = spec.records

    assertThat(producerRecords, correspondTo(discoveredRecords, topicPartition))
    assertEquals("Base offset of segment mismatch", spec.baseOffset, discoveredRecords(0).offset())
  }
}

final class BounceBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.bounce(brokerId)
}

/**
  * This builder helps to formulate a test case exercising the tiered storage functionality and formulate
  * the expectations following the execution of the test.
  */
final class TieredStorageTestBuilder {
  private var producables: mutable.Map[TopicPartition, mutable.Buffer[ProducerRecord[String, String]]] = mutable.Map()
  private var offloadables: mutable.Map[TopicPartition, mutable.Buffer[(Int, Int, Int)]] = mutable.Map()

  private val actions = mutable.Buffer[TieredStorageTestAction]()

  def createTopic(topic: String, partitionsCount: Int, replicationFactor: Int, segmentSize: Int): this.type = {
    assert(segmentSize >= 1, s"Segments size for topic ${topic} needs to be >= 1")
    assert(partitionsCount >= 1, s"Partition count for topic ${topic} needs to be >= 1")
    assert(replicationFactor >= 1, s"Replication factor for topic ${topic} needs to be >= 1")

    maybeCreateProduceOffloadAction()
    actions += new CreateTopicAction(topic, partitionsCount, replicationFactor, segmentSize)
    this
  }

  def produce(topic: String, partition: Int, key: String, value: String): this.type = {
    val topicPartition = new TopicPartition(topic, partition)

    if (!producables.contains(topicPartition)) {
      producables += (topicPartition -> mutable.Buffer())
    }

    producables(topicPartition) += new ProducerRecord[String, String](topic, partition, key, value)
    this
  }

  def expectSegmentToBeOffloaded(fromBroker: Int, topic: String, partition: Int,
                                 baseOffset: Int, segmentSize: Int): this.type = {

    val topicPartition = new TopicPartition(topic, partition)
    val attr = (fromBroker, baseOffset, segmentSize)

    offloadables.get(topicPartition) match {
      case Some(buffer) => buffer += attr
      case None => offloadables += topicPartition -> mutable.Buffer(attr)
    }

    this
  }

  def bounce(brokerId: Int): this.type = {
    maybeCreateProduceOffloadAction()
    actions += new BounceBrokerAction(brokerId)
    this
  }

  def complete(): Seq[TieredStorageTestAction] = {
    maybeCreateProduceOffloadAction()
    actions
  }

  private def maybeCreateProduceOffloadAction(): Unit = {
    if (!producables.isEmpty) {
      // Creates the map of records to produce. Order of records is preserved at partition level.
      val recordsToProduce = Map() ++ producables.mapValues(Seq() ++ _)

      /**
        * Builds a specification of an offloaded segment.
        * This method modifies this builder's sequence of records to produce.
        */
      def makeSpec(topicPartition: TopicPartition, attr: (Int, Int, Int)): OffloadedSegmentSpec = {
        attr match {
          case (sourceBroker:Int, baseOffset: Int, segmentSize: Int) =>
            val segments = (0 until segmentSize).map(_ => producables(topicPartition).remove(0))
            new OffloadedSegmentSpec(sourceBroker, topicPartition, baseOffset, segments)
        }
      }

      // Creates the map of specifications of segments expected to be offloaded.
      val offloadedSegmentSpecs = Map() ++ offloadables.map { case (p, attrs) => (p, attrs.map(makeSpec (p, _))) }

      actions += new ProduceOffloadAction(offloadedSegmentSpecs, recordsToProduce)
      producables = mutable.Map()
      offloadables = mutable.Map()
    }
  }
}
