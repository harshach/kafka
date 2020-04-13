package integration.kafka.tiered.storage

import java.util.Properties

import integration.kafka.tiered.storage.TieredStorageTestCaseBuilder.newTestCase
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_ID_PROP}
import org.junit.Test

class TieredStorageTest extends IntegrationTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 1.toString)

    overridingProps.setProperty(STORAGE_ID_PROP, "tiered-storage-tests")
    overridingProps.setProperty(DELETE_ON_CLOSE_PROP, "false")

    createBrokerConfigs(numConfigs = 1, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override protected def brokerCount: Int = 1

  @Test
  def test(): Unit = {

    val topicName = "aTopic"

    val remoteStorage = serverForId(0).get.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage]

    val testCase = newTestCase(servers, zkClient, producerConfig, remoteStorage)
      .withTopic(topicName, 1, 1)
      .producing(topicName, 0, "key1", "value1")
      .producing(topicName, 0, "key2", "value2")
      .expectingSegmentToBeOffloaded(topicName, 0, 1)
      .create()

    testCase.exercise()

    testCase.verify()

    /*val topicProps = new Properties()
    topicProps.setProperty(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 12.toString)

    TestUtils.createTopic(zkClient, "phoque", brokerCount, brokerCount, servers, topicProps)

    val tp = new TopicPartition("phoque", 0)

    val waiter = newWaiterBuilder().addSegmentsToWaitFor(tp, 1).create(remoteStorage)

    val producer = createProducer(new StringSerializer, new StringSerializer)
    producer.send(new ProducerRecord[String, String]("phoque", "a")).get()
    producer.send(new ProducerRecord[String, String]("phoque", "b")).get()

    waiter.waitForSegments(5, TimeUnit.SECONDS)

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(remoteStorage)

    assertEquals(util.Arrays.asList(tp), snapshot.getTopicPartitions)

    assertEquals(1, snapshot.size())

    val record = snapshot.getFilesets(tp).get(0).getRecords.get(0)

    Assert.assertEquals(ByteBuffer.wrap("a".getBytes), record.value())

    topicProps.setProperty(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 24.toString)
    TestUtils.createTopic(zkClient, "phoque2", brokerCount, brokerCount, servers, topicProps)

    val w = newWaiterBuilder().addSegmentsToWaitFor(tp, 1).create(remoteStorage)

    producer.send(new ProducerRecord[String, String]("phoque2", "a")).get()
    producer.send(new ProducerRecord[String, String]("phoque2", "b")).get()
    producer.send(new ProducerRecord[String, String]("phoque2", "c")).get()*/




  }

}
