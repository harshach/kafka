package integration.kafka.tiered.storage

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageCondition.expectEvent
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType.OFFLOAD_SEGMENT
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import unit.kafka.utils.RecordsMatcher.correspondTo

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}

final case class OffloadedSegmentSpec(val sourceBrokerId: Int,
                                      val topicPartition: TopicPartition,
                                      val baseOffset: Int,
                                      val records: Seq[ProducerRecord[String, String]])

final case class TopicSpec(val topicName: String,
                           val partitionCount: Int,
                           val replicationFactor: Int,
                           val segmentSize: Int,
                           val properties: Properties = new Properties)

final class RemoteFetchRequestSpec(val brokerId: Int, val fetchOffset: Long)

trait TieredStorageTestAction {

  def execute(context: TieredStorageTestContext): Unit

}

final class CreateTopicAction(val spec: TopicSpec) extends TieredStorageTestAction {

  override def execute(context: TieredStorageTestContext): Unit = {
    //
    // Ensure offset and time indexes are generated for every record.
    //
    spec.properties.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    if (spec.segmentSize != -1) {
      spec.properties.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * spec.segmentSize).toString)
    }

    //
    // To verify records physically absent from Kafka's storage can be consumed via the tiered storage, we
    // want to delete log segments as soon as possible. When the tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    spec.properties.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)

    context.createTopic(spec)
  }
}

final class ProduceAction(val offloadedSegmentSpecs: Map[TopicPartition, Seq[OffloadedSegmentSpec]],
                          val recordsToProduce: Map[TopicPartition, Seq[ProducerRecord[String, String]]])
  extends TieredStorageTestAction {

  private val offloadWaitTimeoutSec = 20
  private implicit val serde: Serde[String] = Serdes.String()

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
        val topicSpec = context.topicSpec(topicPartition.topic())
        val expectedEarliestOffset = producedRecords.size - (producedRecords.size % topicSpec.segmentSize) - 1

        localStorages.map(_.waitForEarliestOffset(topicPartition, expectedEarliestOffset))

        val consumedRecords = context.consume(topicPartition, producedRecords.length)
        assertThat(consumedRecords, correspondTo(producedRecords, topicPartition))
    }

    val snapshot = context.takeTieredStorageSnapshot()

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

final class ConsumeAction(val topicPartition: TopicPartition,
                          val fetchOffset: Long,
                          val expectedTotalCount: Int,
                          val expectedFromTieredStorageCount: Int) extends TieredStorageTestAction {

  private implicit val serde: Serde[String] = Serdes.String()

  override def execute(context: TieredStorageTestContext): Unit = {
    val consumedRecords = context.consume(topicPartition, expectedTotalCount, fetchOffset)

    if (expectedFromTieredStorageCount == 0) {
      return
    }

    val snapshot = context.takeTieredStorageSnapshot()
    val tieredStorageRecords = snapshot
        .getFilesets(topicPartition).asScala
        .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
        .map(_.getRecords.asScala).flatten

    val firstExpectedRecordOpt = tieredStorageRecords.find(_.offset() == fetchOffset)
    assertTrue(s"Could not find record with offset $fetchOffset", firstExpectedRecordOpt.isDefined)

    val indexOfFetchOffsetInTieredStorage = tieredStorageRecords.indexOf(firstExpectedRecordOpt.get)
    val recordsCountFromFirstIndex = tieredStorageRecords.size - indexOfFetchOffsetInTieredStorage

    assertFalse(
      s"Not enough records found in tiered storage from offset $fetchOffset. " +
      s"Expected: $expectedFromTieredStorageCount, Was $recordsCountFromFirstIndex",
      expectedFromTieredStorageCount > recordsCountFromFirstIndex)

    assertFalse(
      s"Too many records found in tiered storage from offset $fetchOffset. " +
      s"Expected: $expectedFromTieredStorageCount, Was $recordsCountFromFirstIndex",
      expectedFromTieredStorageCount < recordsCountFromFirstIndex
    )

    val storedRecords = tieredStorageRecords.splitAt(indexOfFetchOffsetInTieredStorage)._2
    val readRecords = consumedRecords.take(expectedFromTieredStorageCount)

    assertThat(storedRecords, correspondTo(readRecords, topicPartition))
  }
}

final class BounceBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.bounce(brokerId)
}

final class StopBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.stop(brokerId)
}

final class StartBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.start(brokerId)
}

final class EraseBrokerStorageAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.eraseBrokerStorage(brokerId)
}

/**
  * This builder helps to formulate a test case exercising the tiered storage functionality and formulate
  * the expectations following the execution of the test.
  */
final class TieredStorageTestBuilder {
  private var producables: mutable.Map[TopicPartition, mutable.Buffer[ProducerRecord[String, String]]] = mutable.Map()
  private var offloadables: mutable.Map[TopicPartition, mutable.Buffer[(Int, Int, Int)]] = mutable.Map()

  private var consumables: mutable.Map[TopicPartition, (Long, Int, Int)] = mutable.Map()
  //private var fetchables: mutable.Map[TopicPartition, ()] = mutable.Map()

  private val actions = mutable.Buffer[TieredStorageTestAction]()

  def createTopic(topic: String, partitionsCount: Int, replicationFactor: Int, segmentSize: Int): this.type = {
    assert(segmentSize >= 1, s"Segments size for topic ${topic} needs to be >= 1")
    assert(partitionsCount >= 1, s"Partition count for topic ${topic} needs to be >= 1")
    assert(replicationFactor >= 1, s"Replication factor for topic ${topic} needs to be >= 1")

    maybeCreateProduceAction()
    maybeCreateConsumeActions()

    actions += new CreateTopicAction(TopicSpec(topic, partitionsCount, replicationFactor, segmentSize))
    this
  }

  def produce(topic: String, partition: Int, key: String, value: String): this.type = {
    assert(partition >= 0, "Partition must be >= 0")
    val topicPartition = new TopicPartition(topic, partition)

    if (!producables.contains(topicPartition)) {
      producables += (topicPartition -> mutable.Buffer())
    }

    producables(topicPartition) += new ProducerRecord[String, String](topic, partition, key, value)
    this
  }

  def consume(topic: String,
              partition: Int,
              fetchOffset: Long,
              expectedTotalCount: Int,
              expectedFromTieredStorageCount: Int): this.type = {

    assert(partition >= 0, "Partition must be >= 0")
    assert(fetchOffset >= 0, "Fecth offset must be >=0")
    assert(expectedTotalCount >= 1, "Must read at least one record")
    assert(expectedFromTieredStorageCount >= 0, "Expected read cannot be < 0")
    assert(expectedFromTieredStorageCount <= expectedTotalCount, "Cannot fetch more records than consumed")

    val topicPartition = new TopicPartition(topic, partition)

    assert(!consumables.contains(topicPartition), s"Consume already in progress for $topicPartition")
    consumables += topicPartition -> (fetchOffset, expectedTotalCount, expectedFromTieredStorageCount)
    this
  }

  def expectSegmentToBeOffloaded(fromBroker: Int, topic: String, partition: Int,
                                 baseOffset: Int, segmentSize: Int): this.type = {

    val topicPartition = new TopicPartition(topic, partition)
    val attrs = (fromBroker, baseOffset, segmentSize)

    offloadables.get(topicPartition) match {
      case Some(buffer) => buffer += attrs
      case None => offloadables += topicPartition -> mutable.Buffer(attrs)
    }

    this
  }

  def bounce(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new BounceBrokerAction(brokerId)
    this
  }

  def stop(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new StopBrokerAction(brokerId)
    this
  }

  def start(brokerId: Int): this.type = {
    actions += new StartBrokerAction(brokerId)
    this
  }

  def eraseBrokerStorage(brokerId: Int): this.type = {
    actions += new EraseBrokerStorageAction(brokerId)
    this
  }

  def complete(): Seq[TieredStorageTestAction] = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions
  }

  private def maybeCreateProduceAction(): Unit = {
    if (!producables.isEmpty) {
      // Creates the map of records to produce. Order of records is preserved at partition level.
      val recordsToProduce = Map() ++ producables.mapValues(Seq() ++ _)

      /**
        * Builds a specification of an offloaded segment.
        * This method modifies this builder's sequence of records to produce.
        */
      def makeSpec(topicPartition: TopicPartition, attrs: (Int, Int, Int)): OffloadedSegmentSpec = {
        attrs match {
          case (sourceBroker:Int, baseOffset: Int, segmentSize: Int) =>
            val segments = (0 until segmentSize).map(_ => producables(topicPartition).remove(0))
            new OffloadedSegmentSpec(sourceBroker, topicPartition, baseOffset, segments)
        }
      }

      // Creates the map of specifications of segments expected to be offloaded.
      val offloadedSegmentSpecs = Map() ++ offloadables.map { case (p, attrs) => (p, attrs.map(makeSpec (p, _))) }

      actions += new ProduceAction(offloadedSegmentSpecs, recordsToProduce)
      producables = mutable.Map()
      offloadables = mutable.Map()
    }
  }

  private def maybeCreateConsumeActions(): Unit = {
    if (!consumables.isEmpty) {
      consumables.foreach {
        case (topicPartition, spec) =>
          actions += new ConsumeAction(topicPartition, spec._1, spec._2, spec._3)
      }
      consumables = mutable.Map()
    }
  }
}
