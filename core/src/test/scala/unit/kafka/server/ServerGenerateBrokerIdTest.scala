/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.utils.{TestUtils, Utils}
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import junit.framework.Assert._
import java.io.File

class ServerGenerateBrokerIdTest extends JUnit3Suite with KafkaServerTestHarness {
  val configs = List(new KafkaConfig(TestUtils.createBrokerConfig(-1, TestUtils.choosePort)),
    new KafkaConfig(TestUtils.createBrokerConfig(0, TestUtils.choosePort)),
    new KafkaConfig(TestUtils.createBrokerConfig(-1, TestUtils.choosePort)))
  val brokerMetaPropsFile = "meta.properties"


  @Test
  def testAutoGenerateBrokerId() {
    servers.head.shutdown()
    assertTrue(verifyBrokerMetadata(servers.head.config.logDirs, 1001))
    // restart the server check to see if it uses the brokerId generated previously
    servers.head.startup()
    assertEquals(servers.head.config.brokerId, 1001)

  }

  @Test
  def testUserConfigAndGeneratedBrokerId() {
    // start the server with broker.id as part of config
    val expectedBrokerIds = List(1001, 0, 1002)
      (servers zip expectedBrokerIds).map {
        case (server, expectedBrokerId) =>
          assertEquals(server.config.brokerId, expectedBrokerId)
          assertTrue(verifyBrokerMetadata(server.config.logDirs, expectedBrokerId))
      }
  }

  @Test
  def testMultipleLogDirsMetaProps() {
    // add multiple logDirs and check if the generate brokerId is stored in all of them
    var server = servers.head
    assertEquals(server.config.brokerId, 1001)
    val logDirs = server.config.logDirs.mkString(",") + "," + TestUtils.tempDir().getAbsolutePath +
    "," + TestUtils.tempDir().getAbsolutePath
    val kafkaConfigProps = TestUtils.createBrokerConfig(-1, TestUtils.choosePort)
    kafkaConfigProps.setProperty("log.dir", logDirs)
    val kafkaConfig = new KafkaConfig(kafkaConfigProps)
    server.shutdown()
    server = new KafkaServer(kafkaConfig)
    server.startup()
    assertTrue(verifyBrokerMetadata(server.config.logDirs, 1001))
    server.shutdown()
    Utils.rm(server.config.logDirs)
  }

  @Test
  def testConsistentBrokerIdFromUserConfigAndMetaProps() {
    // check if configured brokerId and stored brokerId are equal or throw InconsistentBrokerException
    var server = servers.head
    val kafkaConfigProps = TestUtils.createBrokerConfig(0, TestUtils.choosePort())
    kafkaConfigProps.setProperty("log.dir", server.config.logDirs.mkString(","))
    server.shutdown()
    server = new KafkaServer(new KafkaConfig(kafkaConfigProps)) // user specified broker id
    try {
      server.startup()
    } catch {
      case e: kafka.common.InconsistentBrokerIdException => //success
    }
    server.shutdown()
    Utils.rm(server.config.logDirs)
  }

  def verifyBrokerMetadata(logDirs: Seq[String], brokerId: Int): Boolean = {
    for(logDir <- logDirs) {
      val brokerMetadataOpt = (new BrokerMetadataCheckpoint(
        new File(logDir + File.separator + brokerMetaPropsFile))).read()
      brokerMetadataOpt match {
        case Some(brokerMetadata: BrokerMetadata) =>
          if (brokerMetadata.brokerId != brokerId)  return false
        case _ => return false
      }
    }
    true
  }

}
