package integration.kafka.tiered.storage

import java.nio.ByteBuffer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageSnapshot, RemoteLogSegmentFileset}
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageWaiter.newWaiterBuilder
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Assert

import scala.collection.{Seq, mutable}

final class TieredStorageTestCase(val recordsToProduce: Map[TopicPartition, Seq[ProducerRecord[String, String]]],
                                  val offloadedSegments: Map[TopicPartition, Seq[OffloadedSegmentSpec]],
                                  val producer: KafkaProducer[String, String],
                                  val storage: LocalTieredStorage) {

  val offloadWaitTimeoutSec = 5

  def exercise(): Unit = {
    val waiter = {
      val waiterBuilder = newWaiterBuilder()
      offloadedSegments.foreach { x => waiterBuilder.addSegmentsToWaitFor(x._1, x._2.length) }
      waiterBuilder.create(storage)
    }

    recordsToProduce.values.flatten.foreach(producer.send(_).get())

    waiter.waitForSegments(offloadWaitTimeoutSec, TimeUnit.SECONDS)

    producer.close(Duration.ZERO)
  }

  def verify(): Unit = {
    import scala.collection.JavaConverters._

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(storage)

    offloadedSegments.foreach { x: (TopicPartition, Seq[OffloadedSegmentSpec]) =>
      val filesets = snapshot.getFilesets(x._1)
      filesets.asScala.zip(x._2).foreach { y: (RemoteLogSegmentFileset, OffloadedSegmentSpec) =>

        val actual = y._1.getRecords.asScala.map(record => (record.key(), record.value()))
        val expected = y._2.records.map(
          r => (ByteBuffer.wrap(r.key().getBytes()), ByteBuffer.wrap(r.value().getBytes())))

        Assert.assertEquals(expected, actual)
      }
    }
  }

  def close(): Unit = {

  }
}

final case class OffloadedSegmentSpec(val topicPartition: TopicPartition,
                                      val records: Seq[ProducerRecord[String, String]])

final class TieredStorageTestCaseBuilder(private val kafkaServers: Seq[KafkaServer],
                                         private val zookeeperClient: KafkaZkClient,
                                         private val producerConfig: Properties,
                                         private val storage: LocalTieredStorage) {

  val producables: mutable.Map[TopicPartition, mutable.Buffer[ProducerRecord[String, String]]] = mutable.LinkedHashMap()
  val offloadables: mutable.Map[TopicPartition, mutable.Buffer[Int]] = mutable.Map()

  def withTopic(name: String, partitions: Int, segmentSize: Int = -1): this.type = {
    val brokerCount = kafkaServers.length
    val topicProps = new Properties()

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, with 12, we can guarantee no more than one entry in both the offset and time index.
    //
    if (segmentSize != -1) {
      topicProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (segmentSize * 12).toString)
    }

    TestUtils.createTopic(zookeeperClient, name, partitions, brokerCount, kafkaServers, topicProps)
    this
  }

  def producing(topic: String, partition: Int, key: String, value: String): this.type = {
    val topicPartition = new TopicPartition(topic, partition)
    if (!producables.contains(topicPartition)) {
      producables += (topicPartition -> mutable.Buffer())
    }

    producables(topicPartition) += new ProducerRecord[String, String](topic, partition, key, value)
    this
  }

  def expectingSegmentToBeOffloaded(topic: String, partition: Int, segmentSize: Int): this.type = {
    val tp = new TopicPartition(topic, partition)

    offloadables.get(tp) match {
      case Some(buffer) => buffer += segmentSize
      case None => offloadables += tp -> mutable.Buffer(segmentSize)
    }

    this
  }

  def create(): TieredStorageTestCase = {
    val recordsToProduce = producables.mapValues(_.toSeq)
    val recordsOffloaded = offloadables.map {x: ((TopicPartition, mutable.Buffer[Int])) =>
      (x._1, x._2.map(size => new OffloadedSegmentSpec(x._1, producables(x._1).take(size))))
    }

    val producer = new KafkaProducer[String, String](producerConfig, new StringSerializer, new StringSerializer)

    new TieredStorageTestCase(recordsToProduce.toMap, recordsOffloaded.toMap, producer, storage)
  }
}

object TieredStorageTestCaseBuilder {

  def newTestCase(kafkaServers: Seq[KafkaServer],
                  zookeeperClient: KafkaZkClient,
                  producerConfig: Properties,
                  storage: LocalTieredStorage): TieredStorageTestCaseBuilder = {

    new TieredStorageTestCaseBuilder(kafkaServers, zookeeperClient, producerConfig, storage)
  }
}
