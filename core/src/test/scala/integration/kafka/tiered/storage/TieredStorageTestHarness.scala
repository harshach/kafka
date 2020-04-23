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

import integration.kafka.tiered.storage.{TieredStorageTestBuilder, TieredStorageTestContext}
import kafka.api.IntegrationTestHarness
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils.createBrokerConfigs
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.{DELETE_ON_CLOSE_PROP, STORAGE_DIR_PROP}
import org.apache.kafka.common.replica.ReplicaSelector
import org.junit.{After, Before, Test}
import unit.kafka.utils.BrokerLocalStorage

import scala.collection.Seq

/**
  * Base class for integration tests exercising the tiered storage functionality in Apache Kafka.
  */
abstract class TieredStorageTestHarness extends IntegrationTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 1000.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 1.toString)

    overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp, 1000.toString)

    readReplicaSelectorClass.foreach(c => overridingProps.put(KafkaConfig.ReplicaSelectorClassProp, c.getName))

    overridingProps.setProperty(STORAGE_DIR_PROP, "tiered-storage-tests")
    overridingProps.setProperty(DELETE_ON_CLOSE_PROP, "true")

    createBrokerConfigs(numConfigs = brokerCount, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  private var context: TieredStorageTestContext = _

  protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] = None

  protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit

  @Before
  override def setUp(): Unit = {
    super.setUp()
    context = new TieredStorageTestContext(zkClient, servers, producerConfig, consumerConfig, securityProtocol)
  }

  @Test
  def executeTieredStorageTest(): Unit = {
    val builder = new TieredStorageTestBuilder
    writeTestSpecifications(builder)
    builder.complete().foreach(_.execute(context))
  }

  @After
  override def tearDown(): Unit = {
    if (context != null) {
      context.close()
    }
    super.tearDown()
  }
}

object TieredStorageTestHarness {

  def getTieredStorages(brokers: Seq[KafkaServer]): Seq[LocalTieredStorage] = {
    brokers.map(_.remoteLogManager.get.storageManager().asInstanceOf[LocalTieredStorage])
  }

  def getLocalStorages(brokers: Seq[KafkaServer]): Seq[BrokerLocalStorage] = {
    brokers.map(b => new BrokerLocalStorage(b.config.logDirs(0)))
  }
}
