package integration.kafka.tiered.storage

import java.nio.ByteBuffer
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_ID_PROP}
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageWaiter.newWaiter
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageSnapshot}
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.Assert.assertEquals
import org.junit.{Assert, Test}

class TieredStorageTest extends IntegrationTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 1.toString)

    overridingProps.setProperty(STORAGE_ID_PROP, "tiered-storage")
    overridingProps.setProperty(DELETE_ON_CLOSE_PROP, "true")

    createBrokerConfigs(numConfigs = 1, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override protected def brokerCount: Int = 1

  @Test
  def test(): Unit = {
    val logDirectory = configs(0).get(KafkaConfig.LogDirProp)

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, with 12, we can guarantee no more than one entry in both the offset and time index.
    //
    val topicProps = new Properties()
    topicProps.setProperty(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 12.toString)

    TestUtils.createTopic(zkClient, "phoque", brokerCount, brokerCount, servers, topicProps)

    val remoteStorage = serverForId(0).get.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage]
    val tp = new TopicPartition("phoque", 0)

    val waiter = newWaiter().addSegmentsToWaitFor(tp, 1).fromStorage(remoteStorage)

    val producer = createProducer(new StringSerializer, new StringSerializer)
    producer.send(new ProducerRecord[String, String]("phoque", "a")).get()
    producer.send(new ProducerRecord[String, String]("phoque", "b")).get()

    waiter.waitForSegments(5, TimeUnit.SECONDS)

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(remoteStorage)

    assertEquals(util.Arrays.asList(tp), snapshot.getTopicPartitions)

    val records = snapshot.getRemoteLogSegmentFilesets
    assertEquals(1, records.size())

    Assert.assertEquals(util.Arrays.asList(ByteBuffer.wrap("a".getBytes)), records.values().iterator().next())
  }

}
