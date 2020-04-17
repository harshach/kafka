/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.tiered.storage

import java.util.Properties

import integration.kafka.tiered.storage.TieredStorageTestSpecBuilder.newSpec

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_DIR_PROP}
import org.junit.{After, Test}
import unit.kafka.utils.StorageWatcher

import scala.collection.{Seq, mutable}

class TieredStorageTest extends IntegrationTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 1.toString)

    overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp, 1000.toString)

    overridingProps.setProperty(STORAGE_DIR_PROP, "tiered-storage-tests")
    overridingProps.setProperty(DELETE_ON_CLOSE_PROP, "true")

    createBrokerConfigs(numConfigs = 1, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override protected def brokerCount: Int = 1

  private var orchestrator: TieredStorageTestOrchestrator = _

  @Test
  def test(): Unit = {
    val remoteStorage = serverForId(0).get.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage]
    val storageWatcher = new StorageWatcher(configs(0).get(KafkaConfig.LogDirProp).asInstanceOf[String])

    val specs = mutable.Buffer[TieredStorageTestSpec]()

    specs += newSpec(topic = "topicA", partitionsCount = 1, replicationFactor = 1)
      .withSegmentSize(1)
      .producing(0, "k1", "v1")
      .producing(0, "k2", "v2")
      .producing(0, "k3", "v3")
      .expectingSegmentToBeOffloaded(0, baseOffset = 0, segmentSize = 1)
      .expectingSegmentToBeOffloaded(0, baseOffset = 1, segmentSize = 1)
      .build()

    specs += newSpec(topic = "topicB", partitionsCount = 1, replicationFactor = 1)
      .withSegmentSize(2)
      .producing(0, "k1", "v1")
      .producing(0, "k2", "v2")
      .producing(0, "k3", "v3")
      .expectingSegmentToBeOffloaded(0, baseOffset = 0, segmentSize = 2)
      .build()

    orchestrator = new TieredStorageTestOrchestrator(specs, remoteStorage, storageWatcher, producerConfig, consumerConfig)
    orchestrator.execute(zkClient, servers)
  }

  @After
  override def tearDown(): Unit = {
    if (orchestrator != null) {
      orchestrator.tearDown()
    }
    super.tearDown()
  }

}
