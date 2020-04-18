package integration.kafka.tiered.storage

import integration.kafka.tiered.storage.TieredStorageTestSpecBuilder.newSpec

import scala.collection.mutable

object TieredStorageTests {

  final class TieredStorageConsumeFromLeaderTest extends TieredStorageTestHarness {

    override protected def brokerCount: Int = 1

    //TODO(duprie): Document and explain.
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


}

