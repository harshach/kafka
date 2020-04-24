package integration.kafka.tiered.storage

import java.time.Duration
import java.util.Properties

import kafka.admin.AdminUtils.assignReplicasToBrokers
import kafka.admin.BrokerMetadata
import kafka.server.KafkaServer
import kafka.tiered.storage.TieredStorageTestHarness
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.admin.{Admin, AdminClient}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageHistory, LocalTieredStorageSnapshot}
import org.apache.kafka.common.security.auth.SecurityProtocol
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
final class TieredStorageTestContext(private val zookeeperClient: KafkaZkClient,
                                     private val brokers: Seq[KafkaServer],
                                     private val producerConfig: Properties,
                                     private val consumerConfig: Properties,
                                     private val securityProtocol: SecurityProtocol) {

  private val (ser, de) = (Serdes.String().serializer(), Serdes.String().deserializer())
  private val topicSpecs = mutable.Map[String, TopicSpec]()

  @volatile private var producer: KafkaProducer[String, String] = _
  @volatile private var consumer: KafkaConsumer[String, String] = _
  @volatile private var admin: Admin = _

  @volatile private var tieredStorages: Seq[LocalTieredStorage] = _
  @volatile private var localStorages: Seq[BrokerLocalStorage] = _

  initContext()

  def initContext(): Unit = {
    val bootstrapServerString = TestUtils.getBrokerListStrFromServers(brokers, securityProtocol)
    producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)
    consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)

    val adminConfig = new Properties()
    adminConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)

    producer = new KafkaProducer[String, String](producerConfig, ser, ser)
    consumer = new KafkaConsumer[String, String](consumerConfig, de, de)
    admin = AdminClient.create(adminConfig)

    tieredStorages = TieredStorageTestHarness.getTieredStorages(brokers)
    localStorages = TieredStorageTestHarness.getLocalStorages(brokers)
  }

  def createTopic(spec: TopicSpec): Unit = {
    val metadata = brokers(0).metadataCache.getAliveBrokers.map(b => BrokerMetadata(b.id, b.rack))
    val assignments = assignReplicasToBrokers(metadata, spec.partitionCount, spec.replicationFactor, 0, 0)
    TestUtils.createTopic(zookeeperClient, spec.topicName, assignments, brokers, spec.properties)

    topicSpecs.synchronized { topicSpecs += spec.topicName -> spec }
  }

  def produce(records: Iterable[ProducerRecord[String, String]]) = {
    records.foreach(producer.send(_).get())
  }

  def consume(topicPartition: TopicPartition,
              numberOfRecords: Int,
              fetchOffset: Long = 0): Seq[ConsumerRecord[String, String]] = {

    consumer.assign(Seq(topicPartition).asJava)
    consumer.seek(topicPartition, fetchOffset)
    TestUtils.consumeRecords(consumer, numberOfRecords)
  }

  def bounce(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    producer.close(Duration.ofSeconds(5))
    consumer.close(Duration.ofSeconds(5))
    admin.close()

    broker.shutdown()
    broker.awaitShutdown()
    broker.startup()

    initContext()
  }

  def stop(brokerId: Int): Unit = {
    val broker = brokers(brokerId)
    broker.shutdown()
    broker.awaitShutdown()
  }

  def start(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    producer.close(Duration.ofSeconds(5))
    consumer.close(Duration.ofSeconds(5))

    broker.startup()

    initContext()
  }

  def eraseBrokerStorage(brokerId: Int): Unit = {
    localStorages(brokerId).eraseStorage()
  }

  def topicSpec(topicName: String) = topicSpecs.synchronized { topicSpecs(topicName) }

  def takeTieredStorageSnapshot(): LocalTieredStorageSnapshot = {
    LocalTieredStorageSnapshot.takeSnapshot(tieredStorages(0))
  }

  def getTieredStorageHistory(brokerId: Int): LocalTieredStorageHistory = tieredStorages(brokerId).getHistory

  def getTieredStorages: Seq[LocalTieredStorage] = tieredStorages

  def getLocalStorages: Seq[BrokerLocalStorage] = localStorages

  def getAdmin() = admin

  def close(): Unit = {
    getTieredStorages.find(_ => true).foreach(_.clear())
    Utils.closeAll(producer, consumer)
    admin.close()
  }

}
