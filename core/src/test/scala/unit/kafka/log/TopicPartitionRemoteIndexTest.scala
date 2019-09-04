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
package kafka.log

import java.io.File
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import kafka.log.remote.{RemoteLogIndexEntry, TopicPartitionRemoteIndex}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import unit.kafka.log.RemoteLogIndexTest.generateEntries

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class TopicPartitionRemoteIndexTest extends JUnitSuite with Logging {

  @Test
  def testAppendLookupIndexWithBase(): Unit = {
    val entriesCt = 10
    val offsetStep = 100
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(new TopicPartition("topic", 0), dir)

    // check for baseOffset with 0
    doTestIndexes(entriesCt, offsetStep, 0L, dir, rlmIndex)

    // check for baseOffset with 10000
    doTestIndexes(entriesCt, offsetStep, 10000L, dir, rlmIndex)
  }

  @Test
  def testNegativeOffset(): Unit = {
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(new TopicPartition("topic", 0), dir)

    assertThrows[IllegalArgumentException](doTestIndexes(10, 50, -1L, dir, rlmIndex))
  }

  private def doTestIndexes(entriesCt: Int, offsetStep: Integer, baseOffset: Long, dir: File,
                            rlmIndex: TopicPartitionRemoteIndex) = {
    val entries = generateEntries(entriesCt, offsetStep, baseOffset)
    rlmIndex.appendEntries(entries, baseOffset.toString)

    entries.forEach(new Consumer[RemoteLogIndexEntry] {
      override def accept(entry: RemoteLogIndexEntry): Unit = {
        assertEquals(entry, rlmIndex.lookupEntryForOffset(entry.firstOffset).get)
      }
    })
  }

  @Test
  def testConcurrentAppendLookupsInIndex(): Unit = {
    val threadCt = 64
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(new TopicPartition("topic", 0), dir)
    var lastOffset = 1000L
    val stepOffset = 100
    val entriesCt = 100000
    val workers = new ListBuffer[Runnable]
    val failed: AtomicBoolean = new AtomicBoolean(false)
    val latch: CountDownLatch = new CountDownLatch(threadCt)

    for (i <- 1 to threadCt) {
      val baseOffset = lastOffset + 1
      val entries = generateEntries(entriesCt, stepOffset, baseOffset)
      lastOffset = entries.get(entries.size() - 1).lastOffset
      workers += new Runnable() {
        override def run(): Unit = {
          val firstOffset = entries.get(0).firstOffset
          val mayBeAddedOffset = rlmIndex.appendEntries(entries, Log.filenamePrefixFromOffset(firstOffset))

          val result = if (mayBeAddedOffset.isDefined) {
            entries.iterator().asScala.count(entry => {
              entry.equals(rlmIndex.lookupEntryForOffset(entry.firstOffset).get)
            }) == entries.size
          } else {
            true
          }
          if (!result) failed.compareAndSet(false, true)
          latch.countDown()
        }
      }
    }
    val executorService = Executors.newFixedThreadPool(threadCt)

    try {
      workers.toList.map(worker => executorService.submit(worker))
      latch.await(2, TimeUnit.MINUTES)
      assert(!failed.get())
    } finally {
      executorService.shutdownNow()
    }
  }

  @Test
  def testCleanupIndexUntilOffset(): Unit = {
    val dir = TestUtils.tempDir()
    val rlmIndex = TopicPartitionRemoteIndex.open(new TopicPartition("topic", 0), dir)
    List(100, 300, 500, 700, 900).foreach(x =>
      rlmIndex.appendEntries(generateEntries(100, 1, x), x.toString))

    val indexes = rlmIndex.cleanupIndexesUntil(300)
    assertEquals(3, indexes.size)

    indexes.foreach( x=> assert(x.file.getName.endsWith(Log.DeletedFileSuffix)))

    assert(rlmIndex.lookupEntryForOffset(100).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(150).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(200).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(250).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(300).isDefined)
    assert(rlmIndex.lookupEntryForOffset(450).isDefined)

    val indexesTill750 = rlmIndex.cleanupIndexesUntil(750)
    assertEquals(6, indexesTill750.size)

    assert(rlmIndex.lookupEntryForOffset(100).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(250).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(500).isEmpty)
    assert(rlmIndex.lookupEntryForOffset(700).isDefined)
    assert(rlmIndex.lookupEntryForOffset(950).isDefined)

  }
}