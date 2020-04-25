/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.tiered.storage

import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.utils.{TestUtils, nonthreadsafe}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageCondition.expectEvent
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType.{FETCH_SEGMENT, OFFLOAD_SEGMENT}
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.hamcrest.MatcherAssert.assertThat
import org.junit.Assert.{assertEquals, assertFalse, fail}
import unit.kafka.utils.RecordsKeyValueMatcher.correspondTo

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._
import scala.collection.{Seq, mutable}

/**
  * Specifies a remote log segment expected to be found in a second-tier storage.
  *
  * @param sourceBrokerId The broker which offloaded (uploaded) the segment to the second-tier storage.
  * @param topicPartition The topic-partition which the remote log segment belongs to.
  * @param baseOffset The base offset of the remote log segment.
  * @param records The records *expected* in the remote log segment.
  */
final case class OffloadedSegmentSpec(val sourceBrokerId: Int,
                                      val topicPartition: TopicPartition,
                                      val baseOffset: Int,
                                      val records: Seq[ProducerRecord[String, String]])

/**
  * Specifies a topic-partition with attributes customized for the purpose of tiered-storage tests.
  *
  * @param topicName The name of the topic.
  * @param partitionCount The number of partitions for the topic.
  * @param replicationFactor The replication factor of the topic.
  * @param segmentSize The size of segment for the topic, in number of records.
  *                    A fixed, pre-determined size for the segment is important to enforce to ease exercising
  *                    tiered-storages and reason on expected state in Kafka and tiered-storages.
  * @param properties Configuration of the topic customized for the purpose of tiered-storage tests.
  */
final case class TopicSpec(val topicName: String,
                           val partitionCount: Int,
                           val replicationFactor: Int,
                           val segmentSize: Int,
                           val properties: Properties = new Properties)

/**
  * Specifies a fetch (download) event from a second-tier storage. This is used to ensure the
  * interactions between Kafka and the second-tier storage match expectations.
  *
  * @param sourceBrokerId The broker which fetched (a) remote log segment(s) from the second-tier storage.
  * @param topicPartition The topic-partition which segment(s) were fetched.
  * @param count The number of remote log segment(s) fetched.
  */
// TODO Add more details on the specifications to perform more robust tests.
final case class RemoteFetchSpec(val sourceBrokerId: Int,
                                 val topicPartition: TopicPartition,
                                 val count: Int)

/**
  * An action, or step, taken during a test.
  */
trait TieredStorageTestAction {

  def execute(context: TieredStorageTestContext): Unit

}

final class CreateTopicAction(val spec: TopicSpec) extends TieredStorageTestAction {

  override def execute(context: TieredStorageTestContext): Unit = {
    //
    // Ensure offset and time indexes are generated for every record.
    //
    spec.properties.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    if (spec.segmentSize != -1) {
      spec.properties.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * spec.segmentSize).toString)
    }

    //
    // To verify records physically absent from Kafka's storage can be consumed via the second tier storage, we
    // want to delete log segments as soon as possible. When tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    spec.properties.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)

    context.createTopic(spec)
  }
}

/**
  * Produce records and verify resulting states in the first and second-tier storage.
  *
  * @param offloadedSegmentSpecs The segments expected to be offloaded to the second-tier storage.
  * @param recordsToProduce The records to produce per topic-partition.
  */
final class ProduceAction(val offloadedSegmentSpecs: Map[TopicPartition, Seq[OffloadedSegmentSpec]],
                          val recordsToProduce: Map[TopicPartition, Seq[ProducerRecord[String, String]]])
  extends TieredStorageTestAction {

  /**
    * How much time to wait for all remote log segments of a topic-partition to be offloaded
    * to the second-tier storage.
    */
  private val offloadWaitTimeoutSec = 20

  private implicit val serde: Serde[String] = Serdes.String()

  override def execute(context: TieredStorageTestContext): Unit = {
    val tieredStorages = context.getTieredStorages
    val localStorages = context.getLocalStorages

    val tieredStorageConditions = offloadedSegmentSpecs.values.flatten.map { spec =>
      expectEvent(tieredStorages.asJava, OFFLOAD_SEGMENT, spec.sourceBrokerId, spec.topicPartition, false)
    }

    //
    // Records are produced here.
    //
    context.produce(recordsToProduce.values.flatten)

    tieredStorageConditions.reduce(_ and _).waitUntilTrue(offloadWaitTimeoutSec, TimeUnit.SECONDS)

    //
    // At this stage, records were produced and the expected remote log segments found in the second-tier storage.
    // Further steps are:
    //
    // 1) verify the local (first-tier) storages contain only the expected log segments - that is to say,
    //    in the special case of these integration tests, only the active segment.
    // 2) consume the records and verify they match the produced records.
    //
    // TODO: Handle consume from any offset.
    //
    recordsToProduce.foreach {
      case (topicPartition, producedRecords) =>
        val topicSpec = context.topicSpec(topicPartition.topic())
        val expectedEarliestOffset = producedRecords.size - (producedRecords.size % topicSpec.segmentSize) - 1

        localStorages.foreach(_.waitForEarliestOffset(topicPartition, expectedEarliestOffset))

        val consumedRecords = context.consume(topicPartition, producedRecords.length)
        assertThat(consumedRecords, correspondTo(producedRecords, topicPartition))
    }

    //
    // Take a physical snapshot of the second-tier storage, and compare the records found with
    // those of the expected log segments.
    //
    // TODO: Handle incremental population of the second-tier storage.
    //       Currently all of records found are considered.
    //
    val snapshot = context.takeTieredStorageSnapshot()

    offloadedSegmentSpecs.foreach {
      case (topicPartition: TopicPartition, specs: Seq[OffloadedSegmentSpec]) =>
        snapshot.getFilesets(topicPartition).asScala
          .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
          .zip(specs)
          .foreach {
            pair => compareRecords(pair._1, pair._2, topicPartition)
          }
    }
  }

  private def compareRecords(fileset: RemoteLogSegmentFileset,
                             spec: OffloadedSegmentSpec,
                             topicPartition: TopicPartition): Unit = {

    // Records found in the local tiered storage.
    val discoveredRecords = fileset.getRecords.asScala

    // Records expected to be found, based on what was sent by the producer.
    val producerRecords = spec.records

    assertThat(producerRecords, correspondTo(discoveredRecords, topicPartition))
    assertEquals("Base offset of segment mismatch", spec.baseOffset, discoveredRecords(0).offset())
  }
}

/**
  * Consume records for the topic-partition and verify they match the formulated expectation.
  *
  * @param topicPartition The topic-partition which to consume records from.
  * @param fetchOffset The first offset to consume from.
  * @param expectedTotalCount The number of records expected to be consumed.
  * @param expectedFromSecondTierCount The number of records expected to be retrieved from the second-tier storage.
  * @param remoteFetchSpec Specifies the interactions required with the second-tier storage (if any)
  *                        to fulfill the consumer fetch request.
  */
final class ConsumeAction(val topicPartition: TopicPartition,
                          val fetchOffset: Long,
                          val expectedTotalCount: Int,
                          val expectedFromSecondTierCount: Int,
                          val remoteFetchSpec: RemoteFetchSpec) extends TieredStorageTestAction {

  private implicit val serde: Serde[String] = Serdes.String()

  override def execute(context: TieredStorageTestContext): Unit = {
    //
    // Retrieve the history (which stores the chronological sequence of interactions with the second-tier
    // storage) for the expected broker. Note that while the second-tier storage is unique, each broker
    // maintains a local instance of LocalTieredStorage, which is the server-side plug-in interface which
    // allows Kafka to interact with that storage. These instances record the interactions (or events)
    // between the broker which they belong to and the second-tier storage.
    //
    // The latest event at the time of invocation for the interaction of type "FETCH_SEGMENT" between the
    // given broker and the second-tier storage is retrieved. It can be empty if an interaction of this
    // type has yet to happen.
    //
    val history = context.getTieredStorageHistory(remoteFetchSpec.sourceBrokerId)
    val latestEventSoFar = history.latestEvent(FETCH_SEGMENT, topicPartition).asScala

    //
    // Records are consumed here.
    //
    val consumedRecords = context.consume(topicPartition, expectedTotalCount, fetchOffset)

    //
    // (A) Comparison of records consumed with records in the second-tier storage.
    //
    // Reads all records physically found in the second-tier storage ∂for the given topic-partition.
    // The resulting sequence is sorted by records offset, as there is no guarantee on ordering from
    // the LocalTieredStorageSnapshot.
    //
    val tieredStorageRecords = context.takeTieredStorageSnapshot()
        .getFilesets(topicPartition).asScala
        .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
        .map(_.getRecords.asScala).flatten

    //
    // Try to find a record from the second-tier storage which should be included in the
    // sequence of records consumed.
    //
    val firstExpectedRecordOpt = tieredStorageRecords.find(_.offset() >= fetchOffset)

    if (!firstExpectedRecordOpt.isDefined) {
      //
      // If no records could be found in the second-tier storage or their offset are less
      // than the consumer fetch offset, no record would be consumed from that storage.
      //
      if (expectedFromSecondTierCount > 0) {
        fail(s"Could not find record with offset $fetchOffset from the second-tier storage.")
      }
      return
    }

    val indexOfFetchOffsetInTieredStorage = tieredStorageRecords.indexOf(firstExpectedRecordOpt.get)
    val recordsCountFromFirstIndex = tieredStorageRecords.size - indexOfFetchOffsetInTieredStorage

    assertFalse(
      s"Not enough records found in tiered storage from offset $fetchOffset for $topicPartition. " +
      s"Expected: $expectedFromSecondTierCount, Was $recordsCountFromFirstIndex",
      expectedFromSecondTierCount > recordsCountFromFirstIndex)

    assertFalse(
      s"Too many records found in tiered storage from offset $fetchOffset for $topicPartition. " +
      s"Expected: $expectedFromSecondTierCount, Was $recordsCountFromFirstIndex",
      expectedFromSecondTierCount < recordsCountFromFirstIndex
    )

    val storedRecords = tieredStorageRecords.splitAt(indexOfFetchOffsetInTieredStorage)._2
    val readRecords = consumedRecords.take(expectedFromSecondTierCount)

    assertThat(storedRecords, correspondTo(readRecords, topicPartition))

    //
    // (B) Assessment of the interactions between the source broker and the second-tier storage.
    //     Events which occurred before the consumption of records are discarded.
    //
    val events = history.getEvents(FETCH_SEGMENT, topicPartition).asScala
    val eventsInScope = latestEventSoFar.map(e => events.filter(_.isAfter(e))).getOrElse(events)

    assertEquals(remoteFetchSpec.count, eventsInScope.size)
  }
}

final class BounceBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.bounce(brokerId)
}

final class StopBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.stop(brokerId)
}

final class StartBrokerAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.start(brokerId)
}

final class EraseBrokerStorageAction(val brokerId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = context.eraseBrokerStorage(brokerId)
}

final class ExpectLeaderAction(val topicPartition: TopicPartition, val replicaId: Int, val electLeader: Boolean)
  extends TieredStorageTestAction {

  override def execute(context: TieredStorageTestContext): Unit = {
    if (electLeader) {
      context.admin().electLeaders(ElectionType.PREFERRED, Set(topicPartition).asJava)
    }

    // TODO Provide a valid error message.
    TestUtils.assertLeader(context.admin(), topicPartition, replicaId)
  }
}

final class ExpectBrokerInISR(val topicPartition: TopicPartition, replicaId: Int) extends TieredStorageTestAction {
  override def execute(context: TieredStorageTestContext): Unit = {
    TestUtils.waitForBrokersInIsr(context.admin(), topicPartition, Set(replicaId))
  }
}

/**
  * This builder helps to formulate a test case exercising the tiered storage functionality and formulate
  * the expectations following the execution of the test.
  */
@nonthreadsafe
final class TieredStorageTestBuilder {
  private var producables: mutable.Map[TopicPartition, mutable.Buffer[ProducerRecord[String, String]]] = mutable.Map()
  private var offloadables: mutable.Map[TopicPartition, mutable.Buffer[(Int, Int, Int)]] = mutable.Map()

  private var consumables: mutable.Map[TopicPartition, (Long, Int, Int)] = mutable.Map()
  private var fetchables: mutable.Map[TopicPartition, (Int, Int)] = mutable.Map()

  private val actions = mutable.Buffer[TieredStorageTestAction]()

  def createTopic(topic: String, partitionsCount: Int, replicationFactor: Int, segmentSize: Int): this.type = {
    assert(segmentSize >= 1, s"Segments size for topic ${topic} needs to be >= 1")
    assert(partitionsCount >= 1, s"Partition count for topic ${topic} needs to be >= 1")
    assert(replicationFactor >= 1, s"Replication factor for topic ${topic} needs to be >= 1")

    maybeCreateProduceAction()
    maybeCreateConsumeActions()

    actions += new CreateTopicAction(TopicSpec(topic, partitionsCount, replicationFactor, segmentSize))
    this
  }

  def produce(topic: String, partition: Int, keyValues: (String, String)*): this.type = {
    assert(partition >= 0, "Partition must be >= 0")
    val topicPartition = new TopicPartition(topic, partition)

    keyValues.foreach {
      case (key, value) =>
        if (!producables.contains(topicPartition)) {
          producables += (topicPartition -> mutable.Buffer())
        }

        producables(topicPartition) += new ProducerRecord[String, String](topic, partition, key, value)
    }

    this
  }

  def expectSegmentToBeOffloaded(fromBroker: Int, topic: String, partition: Int,
                                 baseOffset: Int, segmentSize: Int): this.type = {

    val topicPartition = new TopicPartition(topic, partition)
    val attrs = (fromBroker, baseOffset, segmentSize)

    offloadables.get(topicPartition) match {
      case Some(buffer) => buffer += attrs
      case None => offloadables += topicPartition -> mutable.Buffer(attrs)
    }

    this
  }

  def consume(topic: String,
              partition: Int,
              fetchOffset: Long,
              expectedTotalRecord: Int,
              expectedRecordsFromSecondTier: Int): this.type = {

    assert(partition >= 0, "Partition must be >= 0")
    assert(fetchOffset >= 0, "Fecth offset must be >=0")
    assert(expectedTotalRecord >= 1, "Must read at least one record")
    assert(expectedRecordsFromSecondTier >= 0, "Expected read cannot be < 0")
    assert(expectedRecordsFromSecondTier <= expectedTotalRecord, "Cannot fetch more records than consumed")

    val topicPartition = new TopicPartition(topic, partition)

    assert(!consumables.contains(topicPartition), s"Consume already in progress for $topicPartition")
    consumables += topicPartition -> (fetchOffset, expectedTotalRecord, expectedRecordsFromSecondTier)
    this
  }

  def expectLeader(topic: String, partition: Int, brokerId: Int, electLeader: Boolean = false): this.type = {
    actions += new ExpectLeaderAction(new TopicPartition(topic, partition), brokerId, electLeader)
    this
  }

  def expectInIsr(topic: String, partition: Int, brokerId: Int): this.type = {
    actions += new ExpectBrokerInISR(new TopicPartition(topic, partition), brokerId)
    this
  }

  def expectFetchFromTieredStorage(fromBroker: Int, topic: String, partition: Int, recordCount: Int): this.type = {
    assert(partition >= 0, "Partition must be >= 0")
    assert(recordCount >= 0, "Expected fetch count from tiered storage must be >= 0")

    val topicPartition = new TopicPartition(topic, partition)

    assert(!fetchables.contains(topicPartition), s"Consume already in progress for $topicPartition")
    fetchables += topicPartition -> (fromBroker, recordCount)
    this
  }

  def bounce(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new BounceBrokerAction(brokerId)
    this
  }

  def stop(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new StopBrokerAction(brokerId)
    this
  }

  def start(brokerId: Int): this.type = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions += new StartBrokerAction(brokerId)
    this
  }

  def eraseBrokerStorage(brokerId: Int): this.type = {
    actions += new EraseBrokerStorageAction(brokerId)
    this
  }

  def complete(): Seq[TieredStorageTestAction] = {
    maybeCreateProduceAction()
    maybeCreateConsumeActions()
    actions
  }

  private def maybeCreateProduceAction(): Unit = {
    if (!producables.isEmpty) {
      // Creates the map of records to produce. Order of records is preserved at partition level.
      val recordsToProduce = Map() ++ producables.view.mapValues(Seq() ++ _)

      /**
        * Builds a specification of an offloaded segment.
        * This method modifies this builder's sequence of records to produce.
        */
      def makeSpec(topicPartition: TopicPartition, attrs: (Int, Int, Int)): OffloadedSegmentSpec = {
        attrs match {
          case (sourceBroker:Int, baseOffset: Int, segmentSize: Int) =>
            val segments = (0 until segmentSize).map(_ => producables(topicPartition).remove(0))
            new OffloadedSegmentSpec(sourceBroker, topicPartition, baseOffset, segments)
        }
      }

      // Creates the map of specifications of segments expected to be offloaded.
      val offloadedSegmentSpecs = Map[TopicPartition, Seq[OffloadedSegmentSpec]]() ++ offloadables.map {
        case (tp: TopicPartition, attrs: mutable.Buffer[(Int, Int, Int)]) => (tp, attrs.map(makeSpec (tp, _)))
      }

      actions += new ProduceAction(offloadedSegmentSpecs, recordsToProduce)
      producables = mutable.Map()
      offloadables = mutable.Map()
    }
  }

  private def maybeCreateConsumeActions(): Unit = {
    if (!consumables.isEmpty) {
      consumables.foreach {
        case (topicPartition, spec) =>
          val (sourceBroker, fetchCount) = fetchables.get(topicPartition).getOrElse((0, 0))
          val remoteFetchSpec = RemoteFetchSpec(sourceBroker, topicPartition, fetchCount)
          actions += new ConsumeAction(topicPartition, spec._1, spec._2, spec._3, remoteFetchSpec)
      }
      consumables = mutable.Map()
      fetchables = mutable.Map()
    }
  }
}
