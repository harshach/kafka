package integration.kafka.tiered.storage

import java.util.Properties

import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig

import scala.collection.{Seq, mutable}

sealed trait StorageType
case object LocalStorage extends StorageType
case object RemoteStorage extends StorageType

/**
  * Defines a complete use case which exercises the tiered storage functionality in Apache Kafka.
  * It is used by the [[TieredStorageTestOrchestrator]] to provide the configurations, records
  * to create, and the expectations on the result of the test.
  */
final class TieredStorageTestSpec(builder: TieredStorageTestSpecBuilder) {
  val topic: String = builder.topic
  val partitionCount: Int = builder.partitionsCount
  val replicationFactor: Int = builder.replicationFactor
  val segmentSize: Int = builder.segmentSize
  val fetchMaxBytes: Int = builder.fetchMaxBytes
  val deleteSegmentsAfterOffload: Boolean = builder.deleteSegmentsAfterOffload
  val recordsToProduce: Map[Int, Seq[ProducerRecord[String, String]]] = builder.recordsToProduce
  val offloadedSegments: Map[Int, Seq[OffloadedSegmentSpec]] = builder.offloadedSegmentSpec

  def configure(zookeeperClient: KafkaZkClient, kafkaServers: Seq[KafkaServer]): Unit = {
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
    if (deleteSegmentsAfterOffload) {
      topicProps.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)
    }

    TestUtils.createTopic(zookeeperClient, topic, partitionCount, replicationFactor, kafkaServers, topicProps)
  }

  def getNumberOfRecordsToProduce(): Int = recordsToProduce.size

  def produce(producer: KafkaProducer[String, String]): Unit = {
    recordsToProduce.values.flatten.foreach(producer.send(_).get())
  }

  def consume(consumer: KafkaConsumer[String, String], partition: Int): Seq[ConsumerRecord[String, String]] = {
    TestUtils.consumeRecords(consumer, recordsToProduce(partition).length)
  }
}

final case class OffloadedSegmentSpec(val partition: Int,
                                      val baseOffset: Int,
                                      val records: Seq[ProducerRecord[String, String]])

final class RemoteFetchRequestSpec(val fetchOffset: Long, val leaderEpoch: Long)

/**
  * This builder helps to formulate a test case exercising the tiered storage functionality and formulate
  * the expectations following the execution of the test.
  */
final class TieredStorageTestSpecBuilder(val topic: String,
                                         val partitionsCount: Int,
                                         val replicationFactor: Int) {

  private val producables: mutable.Map[Int, mutable.Buffer[ProducerRecord[String, String]]] = mutable.Map()
  private val offloadables: mutable.Map[Int, mutable.Buffer[(Int, Int)]] = mutable.Map()

  var segmentSize: Int = -1
  var deleteSegmentsAfterOffload = true
  var fetchMaxBytes: Int = -1

  var recordsToProduce: Map[Int, Seq[ProducerRecord[String, String]]] = _
  var offloadedSegmentSpec: Map[Int, Seq[OffloadedSegmentSpec]] = _

  def withSegmentSize(segmentSize: Int): this.type = {
    assert(segmentSize >= 1, s"Segments size for topic ${topic} needs to be >= 1")
    this.segmentSize = segmentSize
    this
  }

  def deleteSegmentsOnceOffloaded(deleteSegmentsAfterOffload: Boolean): this.type = {
    this.deleteSegmentsAfterOffload = deleteSegmentsAfterOffload
    this
  }

  def producing(partition: Int, key: String, value: String): this.type = {
    if (!producables.contains(partition)) {
      producables += (partition -> mutable.Buffer())
    }

    producables(partition) += new ProducerRecord[String, String](topic, partition, key, value)
    this
  }

  def expectingSegmentToBeOffloaded(partition: Int, baseOffset: Int, segmentSize: Int): this.type = {
    offloadables.get(partition) match {
      case Some(buffer) => buffer += ((baseOffset, segmentSize))
      case None => offloadables += partition -> mutable.Buffer((baseOffset, segmentSize))
    }

    this
  }

  def build(): TieredStorageTestSpec = {
    // Creates the map of records to produce. Order of records is preserved at partition level.
    recordsToProduce = Map() ++ producables.mapValues(Seq() ++ _)

    /**
      * Builds a specification of an offloaded segment.
      * This method modifies this builder's sequence of records to produce.
      */
    def makeSpec(partition: Int, sizeAndOffset: (Int, Int)): OffloadedSegmentSpec = {
      sizeAndOffset match {
        case (baseOffset: Int, segmentSize: Int) =>
          val segments = (0 until segmentSize).map(_ => producables(partition).remove(0))
          new OffloadedSegmentSpec(partition, baseOffset, segments)
      }
    }

    // Creates the map of specifications of segments expected to be offloaded.
    offloadedSegmentSpec = Map() ++
      offloadables.map { case (p, offsetAndSizes) => (p, offsetAndSizes.map(makeSpec (p, _))) }

    new TieredStorageTestSpec(this)
  }

}

object TieredStorageTestSpecBuilder {

  def newSpec(topic: String, partitionsCount: Int, replicationFactor: Int): TieredStorageTestSpecBuilder =
    new TieredStorageTestSpecBuilder(topic, partitionsCount, replicationFactor)

}