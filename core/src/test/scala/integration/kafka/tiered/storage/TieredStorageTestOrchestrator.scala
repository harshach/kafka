package integration.kafka.tiered.storage

import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageCondition, LocalTieredStorageSnapshot, RemoteLogSegmentFileset}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer}
import org.apache.kafka.common.utils.Utils
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.assertEquals
import unit.kafka.utils.RecordsMatcher.correspondTo
import unit.kafka.utils.BrokerLocalStorage

import scala.collection.JavaConverters._
import scala.collection.Seq

/**
  * This orchestrator is responsible for the execution of a test case against physical Apache Kafka broker(s)
  * backed by a [[LocalTieredStorage]].
  *
  * It collaborates with [[TieredStorageTestSpec]] which provides it with the configuration of the topic to
  * create, the records to send, and expectations on the results.
  *
  * As of now, the orchestrator always goes through the following steps:
  *
  * 1) Create a topic;
  * 2) Produce records;
  * 3) Consume records;
  * 4) Verify records from Kafka and the [[LocalTieredStorage]].
  */
// TODO(duprie) Document.
final class TieredStorageTestOrchestrator(val admin: Admin,
                                          val zookeeperClient: KafkaZkClient,
                                          val kafkaServers: Seq[KafkaServer],
                                          val tieredStorages: Map[Int, LocalTieredStorage],
                                          val localStorages: Map[Int, BrokerLocalStorage],
                                          val producerConfig: Properties,
                                          val consumerConfig: Properties) {

  private val offloadWaitTimeoutSec = 20

  val producer = new KafkaProducer[String, String](producerConfig,
    Serdes.String().serializer(), Serdes.String().serializer())

  def execute(specs: Seq[TieredStorageTestSpec]): Unit = {
    specs.foreach { spec =>
      spec.configure(admin, zookeeperClient, kafkaServers)
      execute(spec)
    }
  }

  private def execute(spec: TieredStorageTestSpec): Unit = {
    //
    // The watcher subscribes to modifications of the local tiered storage and waits until
    // the expected segments are all found.
    //
    val tieredStorageConditions = spec.offloadedSegments.values.flatten.map { segment => segment
      LocalTieredStorageCondition.expectEvent(tieredStorages.values.asJava,
        EventType.OFFLOAD_SEGMENT, 0, new TopicPartition(spec.topic, segment.partition), false)
    }

    val condition = tieredStorageConditions.reduce(_ and _)

    spec.produce(producer)

    condition.waitUntilTrue(offloadWaitTimeoutSec, TimeUnit.SECONDS)

    spec.recordsToProduce.foreach {
      _ match {
        case (partition, producedRecords) =>
          val topicPartition = new TopicPartition(spec.topic, partition)
          localStorages.map(_._2.waitForEarliestOffset(topicPartition, 1L))

          val consumer = new KafkaConsumer[String, String](consumerConfig, new StringDeserializer, new StringDeserializer)

          consumer.assign(Seq(topicPartition).asJava)
          val consumedRecords = TestUtils.consumeRecords(consumer, producedRecords.length)

          assertThat(consumedRecords, correspondTo(producedRecords, spec.topic, Serdes.String(), Serdes.String()))
      }
    }

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(tieredStorages(0))

    spec.offloadedSegments.foreach {
      _ match {
        case (partition: Int, specs: Seq[OffloadedSegmentSpec]) =>
          val topicPartition = new TopicPartition(spec.topic, partition)

          snapshot.getFilesets(topicPartition).asScala
            .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
            .zip(specs)
            .foreach {
              pair => compareRecords(pair._1, pair._2, spec.topic)
            }

         /* tieredStorageFetchCaptors.last._2.getEvents(topicPartition)
            .zip(specs)
            .foreach { pair =>
              val fetchEvent = pair._1
              val spec = pair._2
              assertEquals(spec.baseOffset, fetchEvent.metadata.startOffset())
          }*/
      }
    }
  }

  private def compareRecords(fileset: RemoteLogSegmentFileset, spec: OffloadedSegmentSpec, topic: String): Unit = {
    // Records found in the local tiered storage.
    val discoveredRecords = fileset.getRecords.asScala

    // Records expected to be found, based on what was sent by the producer.
    val producerRecords = spec.records

    assertThat(producerRecords, correspondTo(discoveredRecords, topic, Serdes.String(), Serdes.String()))
    assertEquals("Base offset of segment mismatch", spec.baseOffset, discoveredRecords(0).offset())
  }

  def tearDown(): Unit = {
    Utils.closeAll(producer)
  }

}
