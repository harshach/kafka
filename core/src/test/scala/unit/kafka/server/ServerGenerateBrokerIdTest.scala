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

import kafka.zk.ZooKeeperTestHarness
import kafka.utils.{IntEncoder, TestUtils, Utils, VerifiableProperties}
import kafka.utils.TestUtils._
import java.io.File
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import junit.framework.Assert._

class ServerGenerateBrokerIdTest extends JUnit3Suite with ZooKeeperTestHarness {
  val port = TestUtils.choosePort
  var props1 = TestUtils.createBrokerConfig(-1, port)
  var config1 = new KafkaConfig(props1)
  var props2 = TestUtils.createBrokerConfig(0, port)
  var config2 = new KafkaConfig(props2)

  @Test
  def testAutoGenerateBrokerId() {
    var server1 = new KafkaServer(config1)
    server1.startup()
    // do a clean shutdown and check that offset checkpoint file exists
    server1.shutdown()
    for(logDir <- config1.logDirs) {
      val metaProps = new VerifiableProperties(Utils.loadProps(logDir+"/meta.properties"))
      assertTrue(metaProps.containsKey("broker.id"))
      assertEquals(metaProps.getInt("broker.id"),1001)
    }
    // restart the server check to see if it uses the brokerId generated previously
    server1 = new KafkaServer(config1)
    assertEquals(server1.config.brokerId, 1001)
    server1.shutdown()
    Utils.rm(server1.config.logDirs)


    // start the server with broker.id as part of config
    var server2 = new KafkaServer(config2)
    assertEquals(server2.config.brokerId,0)
    server2.shutdown()
    Utils.rm(server2.config.logDirs)

    // add multiple logDirs and check if the generate brokerId is stored in all of them
    props2 = TestUtils.createBrokerConfig(-1, port)
    var logDirs = props2.getProperty("log.dir")+","+TestUtils.tempDir().getAbsolutePath
    props2.setProperty("log.dir",logDirs)
    config2 = new KafkaConfig(props2)
    server1 = new KafkaServer(config2)
    server1.startup()
    server1.shutdown()
    for(logDir <- config2.logDirs) {
      val metaProps = new VerifiableProperties(Utils.loadProps(logDir+"/meta.properties"))
      assertTrue(metaProps.containsKey("broker.id"))
      assertEquals(metaProps.getInt("broker.id"),1002)
    }
    Utils.rm(server1.config.logDirs)

    // addition to log.dirs after generation of a broker.id from zk should be copied over
    props2 = TestUtils.createBrokerConfig(-1,port)
    config2 = new KafkaConfig(props2)
    server1 = new KafkaServer(config2)
    server1.startup()
    server1.shutdown()
    logDirs = props2.getProperty("log.dir")+","+TestUtils.tempDir().getAbsolutePath
    server1.startup()
    for(logDir <- config2.logDirs) {
      val metaProps = new VerifiableProperties(Utils.loadProps(logDir+"/meta.properties"))
      assertTrue(metaProps.containsKey("broker.id"))
      assertEquals(metaProps.getInt("broker.id"),1003)
    }
    server1.shutdown()
    Utils.rm(server1.config.logDirs)
    verifyNonDaemonThreadsStatus
  }

  def verifyNonDaemonThreadsStatus() {
    assertEquals(0, Thread.getAllStackTraces.keySet().toArray
      .map(_.asInstanceOf[Thread])
      .count(t => !t.isDaemon && t.isAlive && t.getClass.getCanonicalName.toLowerCase.startsWith("kafka")))
  }

}
