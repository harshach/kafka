package integration.kafka.tiered.storage

import java.util.Optional

import integration.kafka.tiered.storage.TieredStorageTestSpecBuilder.newSpec
import integration.kafka.tiered.storage.TieredStorageTests.{OffloadAndConsumeFromFollowerTest, OffloadAndConsumeFromLeaderTest}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.replica.{ClientMetadata, PartitionView, ReplicaSelector, ReplicaView}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Suite.SuiteClasses
import org.junit.runners.Suite

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.collection.mutable

@SuiteClasses(Array[Class[_]](
  classOf[OffloadAndConsumeFromLeaderTest],
  classOf[OffloadAndConsumeFromFollowerTest]
))
@RunWith(classOf[Suite])
object TieredStorageTests {

  final class OffloadAndConsumeFromLeaderTest extends TieredStorageTestHarness {

    override protected def brokerCount: Int = 1

    override protected def writeTestSpecifications(specs: mutable.Buffer[TieredStorageTestSpec]): Unit = {
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
    }
  }

  final class OffloadAndConsumeFromFollowerTest extends TieredStorageTestHarness {

    override protected def brokerCount: Int = 2

    override protected def readReplicaSelectorClass: Option[Class[_ <: ReplicaSelector]] =
      Some(classOf[ConsumeFromFollowerInDualBrokerCluster])

    override protected def writeTestSpecifications(specs: mutable.Buffer[TieredStorageTestSpec]): Unit = {
      specs += newSpec(topic = "topicA", partitionsCount = 1, replicationFactor = 2)
        .withSegmentSize(1)
        .producing(0, "k1", "v1")
        .producing(0, "k2", "v2")
        .producing(0, "k3", "v3")
        .expectingSegmentToBeOffloaded(0, baseOffset = 0, segmentSize = 1)
        .expectingSegmentToBeOffloaded(0, baseOffset = 1, segmentSize = 1)
        .build()
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

