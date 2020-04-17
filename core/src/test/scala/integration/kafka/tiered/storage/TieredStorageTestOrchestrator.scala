package integration.kafka.tiered.storage

import java.util.Properties
import java.util.concurrent.TimeUnit

import integration.kafka.tiered.storage.RecordsMatcher.correspondTo
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageWatcher.newWatcherBuilder
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageSnapshot, RemoteLogSegmentFileset}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer}
import org.apache.kafka.common.utils.Utils
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.assertEquals
import unit.kafka.utils.StorageWatcher

import scala.collection.JavaConverters._
import scala.collection.Seq

/**
  * Helps define the specifications of a test case to exercise the support for tiered storage in Apache Kafka.
  * This class encapsulates the execution of the tests and the assertions which ensure the tiered storage was
  * exercised as expected.
  */
final class TieredStorageTestOrchestrator(val specs: Seq[TieredStorageTestSpec],
                                          val tieredStorage: LocalTieredStorage,
                                          val kafkaStorageWatcher: StorageWatcher,
                                          val producerConfig: Properties,
                                          val consumerConfig: Properties) {

  private val offloadWaitTimeoutSec = 20

  private val fetchCaptor = new TieredStorageFetchCaptor()

  val producer = new KafkaProducer[String, String](producerConfig,
    Serdes.String().serializer(), Serdes.String().serializer())

  tieredStorage.addListener(fetchCaptor)

  def execute(zookeeperClient: KafkaZkClient, kafkaServers: Seq[KafkaServer]): Unit = {
    specs.foreach { spec =>
      spec.configure(zookeeperClient, kafkaServers)
      execute(spec)
    }
  }

  private def execute(spec: TieredStorageTestSpec): Unit = {
    //
    // The watcher subscribes to modifications of the local tiered storage and waits until
    // the expected segments are all found.
    //
    val tieredStorageWatcher = {
      val watcherBuilder = newWatcherBuilder()
      spec.offloadedSegments.foreach {
        x => watcherBuilder.addSegmentsToWaitFor(new TopicPartition(spec.topic, x._1), x._2.length)
      }
      watcherBuilder.create(tieredStorage)
    }

    spec.produce(producer)

    tieredStorageWatcher.watch(offloadWaitTimeoutSec, TimeUnit.SECONDS)

    spec.recordsToProduce.foreach {
      _ match {
        case (partition, producedRecords) =>
          val topicPartition = new TopicPartition(spec.topic, partition)
          kafkaStorageWatcher.waitForEarliestOffset(topicPartition, 1L)

          val consumer = new KafkaConsumer[String, String](consumerConfig, new StringDeserializer, new StringDeserializer)

          consumer.assign(Seq(topicPartition).asJava)
          val consumedRecords = TestUtils.consumeRecords(consumer, producedRecords.length)

          assertThat(consumedRecords, correspondTo(producedRecords, spec.topic, Serdes.String(), Serdes.String()))
      }
    }

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(tieredStorage)

    spec.offloadedSegments.foreach {
      _ match {
        case (partition: Int, specs: Seq[OffloadedSegmentSpec]) =>
          val topicPartition = new TopicPartition(spec.topic, partition)

          snapshot.getFilesets(topicPartition).asScala
            .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
            .zip(specs)
            .foreach {
              pair => compareRecords(pair._1, pair._2)
            }

          fetchCaptor.getEvents(topicPartition)
            .zip(specs)
            .foreach { pair =>
              val fetchEvent = pair._1
              val spec = pair._2
              assertEquals(spec.baseOffset, fetchEvent.metadata.startOffset())
          }
      }
    }
  }

  private def compareRecords(fileset: RemoteLogSegmentFileset, spec: OffloadedSegmentSpec): Unit = {
    // Records found in the local tiered storage.
    val discoveredRecords = fileset.getRecords.asScala

    // Records expected to be found, based on what was sent by the producer.
    val producerRecords = spec.records

    assertThat(producerRecords, correspondTo(discoveredRecords, "", Serdes.String(), Serdes.String()))
    assertEquals("Base offset of segment mismatch", spec.baseOffset, discoveredRecords(0).offset())
  }

  def tearDown(): Unit = {
    Utils.closeAll(producer)
  }

}
