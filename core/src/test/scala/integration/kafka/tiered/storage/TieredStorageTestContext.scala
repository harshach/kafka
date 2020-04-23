package integration.kafka.tiered.storage

import java.util.Properties

import kafka.admin.AdminUtils.assignReplicasToBrokers
import kafka.admin.BrokerMetadata
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Utils
import unit.kafka.utils.BrokerLocalStorage

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}

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
final class TieredStorageTestContext(private val admin: Admin,
                                     private val zookeeperClient: KafkaZkClient,
                                     private val brokers: Seq[KafkaServer],
                                     private val tieredStorages: Map[Int, LocalTieredStorage],
                                     private val localStorages: Map[Int, BrokerLocalStorage],
                                     private val producerConfig: Properties,
                                     private val consumerConfig: Properties) {

  private val serde = Serdes.String()
  private val producer = new KafkaProducer[String, String](producerConfig, serde.serializer(), serde.serializer())
  private val consumer = new KafkaConsumer[String, String](consumerConfig, serde.deserializer(), serde.deserializer())

  private val topicSpecs = mutable.Map[String, TopicSpec]()

  def createTopic(spec: TopicSpec): Unit = {
    val metadata = brokers(0).metadataCache.getAliveBrokers.map(b => BrokerMetadata(b.id, b.rack))
    val assignments = assignReplicasToBrokers(metadata, spec.partitionCount, spec.replicationFactor, 0, 0)
    TestUtils.createTopic(zookeeperClient, spec.topicName, assignments, brokers, spec.properties)

    topicSpecs.synchronized { topicSpecs += spec.topicName -> spec }
  }

  def produce(records: Iterable[ProducerRecord[String, String]]) = {
    records.foreach(producer.send(_).get())
  }

  def consume(topicPartition: TopicPartition, numberOfRecords: Int): Seq[ConsumerRecord[String, String]] = {
    consumer.assign(Seq(topicPartition).asJava)
    TestUtils.consumeRecords(consumer, numberOfRecords)
  }

  def bounce(brokerId: Int): Unit = {
    val broker = brokers(brokerId)
    broker.shutdown()
    broker.awaitShutdown()
    broker.startup()
  }

  def getTieredStorages: Seq[LocalTieredStorage] = tieredStorages.values.toSeq

  def getLocalStorages: Seq[BrokerLocalStorage] = localStorages.values.toSeq

  def topicSpec(topicName: String) = topicSpecs.synchronized { topicSpecs(topicName) }

  def close(): Unit = {
    Utils.closeAll(producer, consumer)
  }

}
