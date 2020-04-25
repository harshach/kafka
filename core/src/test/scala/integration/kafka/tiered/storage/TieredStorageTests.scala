package integration.kafka.tiered.storage

import java.util.Optional

import integration.kafka.tiered.storage.TieredStorageTests.{CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest, OffloadAndConsumeFromLeaderTest}
import kafka.tiered.storage.TieredStorageTestHarness
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.replica.{ClientMetadata, PartitionView, ReplicaSelector, ReplicaView}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Suite.SuiteClasses
import org.junit.runners.Suite

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

/**
  * The integration tests for the tiered-storage functionality are gathered here.
  */
@SuiteClasses(Array[Class[_]](
  classOf[OffloadAndConsumeFromLeaderTest],
  classOf[CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest]
/*  classOf[OffloadAndConsumeFromFollowerTest] */
))
@RunWith(classOf[Suite])
object TieredStorageTests {

  /**
    * Test Cases (A):
    *
    *    Elementary offloads and fetches from tiered storage.
    */
  final class OffloadAndConsumeFromLeaderTest extends TieredStorageTestHarness {
    private val (broker, topicA, topicB, partition) = (0, "topicA", "topicB", 0)

    override protected def brokerCount: Int = 1

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      builder
        /*
         * (A.1) Create a topic which segments contain only one record and produce three.
         *
         *       The topic and broker are configured so that the two rolled segments are picked from
         *       the offloaded to the tiered storage and not present in the first-tier broker storage.
         *
         *       Acceptance:
         *       -----------
         *       State of the storages after production of the records and propagation of the log
         *       segment lifecycles to peer subsystems (log cleaner, remote log manager).
         *
         *           First-tier storage                Second-tier storage
         *          *-------------------*             *-------------------*
         *          | base offset = 2   |             |  base offset = 0  |
         *          | (k3, v3)          |             |  (k1, v1)         |
         *          *-------------------*             *-------------------*
         *                                            *-------------------*
         *                                            |  base offset = 1  |
         *                                            |  (k2, v2)         |
         *                                            *-------------------*
         */
        .createTopic(topicA, partitionsCount = 1, replicationFactor = 1, segmentSize = 1)
        .produce(topicA, partition, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(broker, topicA, partition, baseOffset = 0, segmentSize = 1)
        .expectSegmentToBeOffloaded(broker, topicA, partition, baseOffset = 1, segmentSize = 1)

        /*
         * (A.2) Similar scenario as above, but with segments of two records.
         *
         *       Acceptance:
         *       -----------
         *       State of the storages after production of the records and propagation of the log
         *       segment lifecycles to peer subsystems (log cleaner, remote log manager).
         *
         *           First-tier storage                Second-tier storage
         *          *-------------------*             *-------------------*
         *          | base offset = 3   |             |  base offset = 0  |
         *          | (k3, v3)          |             |  (k1, v1)         |
         *          *-------------------*             |  (k2, v2)         |
         *                                            *-------------------*
         */
        .createTopic(topicB, partitionsCount = 1, replicationFactor = 1, segmentSize = 2)
        .produce(topicB, 0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(broker, topicB, partition, baseOffset = 0, segmentSize = 2)

        /*
         * (A.3) Stops and restarts the broker. The purpose of this test is to a) exercise consumption
         *       from a given offset and b) verify that upon broker start, existing remote log segments
         *       metadata are loaded by Kafka and these log segments available.
         *
         *       Acceptance:
         *       -----------
         *       - For topic A, this offset is defined such that only the second segment is fetched from
         *         the tiered storage.
         *       - For topic B, only one segment is present in the tiered storage, as asserted by the
         *         previous sub-test-case.
         */
        .bounce(brokerId = 0)
        .consume(topicA, partition, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromTieredStorage = 1)
        .consume(topicB, partition, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromTieredStorage = 1)
        .expectFetchFromTieredStorage(broker, topicA, partition, recordCount = 1)
        .expectFetchFromTieredStorage(broker, topicB, partition, recordCount = 1)
    }
  }

  /**
    * Test Cases (B):
    *
    *    Given a cluster of brokers {B0, B1} and a topic-partition Ta-p0.
    *    The purpose of this test is to exercise multiple failure scenarios on the cluster upon
    *    on a single-broker outage and loss of the first-tiered storage on one broker, that is:
    *
    *    - Loss of the remote log segment metadata on B0;
    *    - Loss of the active log segment on B0;
    *    - Loss of availability of broker B0;
    *
    *    Acceptance:
    *    -----------
    *    - Remote log segments of Ta-p0 are fetched from B1 when B0 is offline.
    *    - B0 restores the availability both active and remote log segments upon restart.
    */
  final class CanFetchFromTieredStorageAfterRecoveryOfLocalSegmentsTest extends TieredStorageTestHarness {
    private val (broker0, broker1, topic, partition) = (0, 1, "topicA", 0)

    /* Cluster of two brokers */
    override protected def brokerCount: Int = 2

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      builder
        /* (A.1) */
        .createTopic(topic, partitionsCount = 1, replicationFactor = 2, segmentSize = 1)
        .produce(topic, partition, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(broker0, topic, partition, baseOffset = 0, segmentSize = 1)
        .expectSegmentToBeOffloaded(broker0, topic, partition, baseOffset = 1, segmentSize = 1)

        /* (B.1) Stop B0 and read remote log segments from the leader replica which moved to B1. */
        .expectLeader(topic, partition, broker0)
        .stop(broker0)
        .expectLeader(topic, partition, broker1)
        .consume(topic, partition, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromTieredStorage = 2)
        .expectFetchFromTieredStorage(broker1, topic, partition, recordCount = 2)

        /*
         * (B.2) Restore previous leader with an empty storage. The active segment is expected to be
         *       replicated from the new leader.
         *       Note that a preferred leader election is manually triggered for broker 0 to avoid
         *       waiting on the election which would be automatically triggered.
         */
        .eraseBrokerStorage(broker0)
        .start(broker0)
        .expectLeader(topic, partition, broker0, electLeader = true)
        .consume(topic, partition, fetchOffset = 0, expectedTotalRecord = 3, expectedRecordsFromTieredStorage = 2)
        .expectFetchFromTieredStorage(broker0, topic, partition, recordCount = 2)
    }
  }

  // TODO: fetch from follower not implemented.
  final class OffloadAndConsumeFromFollowerTest extends TieredStorageTestHarness {

    override protected def brokerCount: Int = 2

    override protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] =
      Some(classOf[ConsumeFromFollowerInDualBrokerCluster])

    override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
      builder
        .createTopic("topicA", partitionsCount = 1, replicationFactor = 2, segmentSize = 1)
        .expectLeader("topicA", partition = 0, brokerId = 0)
        .expectInIsr("topicA", partition = 0, brokerId = 1)
        .produce("topicA", 0, ("k1", "v1"), ("k2", "v2"), ("k3", "v3"))
        .expectSegmentToBeOffloaded(fromBroker = 0, "topicA", partition = 0, baseOffset = 0, segmentSize = 1)
        .expectSegmentToBeOffloaded(fromBroker = 0, "topicA", partition = 0, baseOffset = 1, segmentSize = 1)
        .consume("topicA", partition = 0, fetchOffset = 1, expectedTotalRecord = 2, expectedRecordsFromTieredStorage = 1)

        .stop(brokerId = 1)
        .eraseBrokerStorage(brokerId = 1)
        .start(brokerId = 1)
        .expectLeader("topicA", partition = 0, brokerId = 0)
        .expectInIsr("topicA", partition = 0, brokerId = 1)
        .consume("topicA", partition = 0, fetchOffset = 0, expectedTotalRecord = 2, expectedRecordsFromTieredStorage = 1)
        .expectFetchFromTieredStorage(fromBroker = 1, "topicA", partition = 0, recordCount = 2)
    }
  }
}

final class ConsumeFromFollowerInDualBrokerCluster extends ReplicaSelector {

  override def select(topicPartition: TopicPartition,
                      clientMetadata: ClientMetadata,
                      partitionView: PartitionView): Optional[ReplicaView] = {

    if (Topic.isInternal(topicPartition.topic())) {
      Some(partitionView.leader()).asJava

    } else {
      assertEquals(
        s"Replicas for the topic-partition $topicPartition need to be assigned to exactly two brokers.",
        2,
        partitionView.replicas().size()
      )

      partitionView.replicas().asScala.find(_ != partitionView.leader()).asJava
    }
  }
}

