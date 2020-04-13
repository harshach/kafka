package integration.kafka.tiered.storage

import java.util.Properties

import integration.kafka.tiered.storage.TieredStorageTestCaseBuilder.newTestCase
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_ID_PROP}
import org.junit.{After, Test}

import scala.collection.mutable

class TieredStorageTest extends IntegrationTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 1.toString)

    overridingProps.setProperty(STORAGE_ID_PROP, "tiered-storage-tests")
    overridingProps.setProperty(DELETE_ON_CLOSE_PROP, "true")

    createBrokerConfigs(numConfigs = 1, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override protected def brokerCount: Int = 1

  private val testCases = mutable.Buffer[TieredStorageTestCase]()

  @Test
  def test(): Unit = {
    val topicA = "topicA"
    val topicB = "topicB"

    val remoteStorage = serverForId(0).get.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage]

    val testCase = newTestCase(servers, zkClient, producerConfig, remoteStorage)

      .withTopic(topicA, 1, 1)
      .producing(topicA, 0, "k1", "v1")
      .producing(topicA, 0, "k2", "v2")
      .producing(topicA, 0, "k3", "v3")
      .expectingSegmentToBeOffloaded(topicA, 0, 1)
      .expectingSegmentToBeOffloaded(topicA, 0, 1)

   /*   .withTopic(topicB, 1, 2)
      .producing(topicB, 0, "k1", "v1")
      .producing(topicB, 0, "k2", "v2")
      .producing(topicB, 0, "k3", "v3")
      .expectingSegmentToBeOffloaded(topicB, 0, 2)*/

      .create()

    testCases += testCase

    testCase.exercise()
    testCase.verify()

  }

  @After
  override def tearDown(): Unit = {
    testCases.foreach(_.close())

    super.tearDown()
  }

}
