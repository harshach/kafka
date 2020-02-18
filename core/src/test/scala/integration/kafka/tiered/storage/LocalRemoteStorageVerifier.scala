/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.tiered.storage

import java.io.InputStream
import java.nio.file.{Files, Paths}
import java.util.Optional.empty
import java.util.UUID

import LocalRemoteStorageVerifier.{assertFileDataEquals, assertFileDoesNotExist, assertFileExists}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{LogSegmentData, RemoteLogSegmentId, RemoteLogSegmentMetadata}
import org.junit.Assert.assertArrayEquals

import scala.Array.emptyByteArray
import scala.io.Source

class LocalRemoteStorageVerifier(val remoteStorage: LocalRemoteStorageManager,
                                 val defaultTopicPartition: Option[TopicPartition] = None) {

  private def getDefaultTopicPartition(): TopicPartition = {
    defaultTopicPartition.getOrElse {
      throw new IllegalArgumentException("No topic-partition was provided")
    }
  }

  private def expectedPaths(id: RemoteLogSegmentId,
                            data: LogSegmentData,
                            topicPartition: TopicPartition = getDefaultTopicPartition()): Seq[String] = {

    val rootPath = remoteStorage.getStorageDirectoryRoot
    val topicPartitionSubpath = s"${topicPartition.topic()}-${topicPartition.partition()}"
    val uuid = id.id().toString

    Seq(
      Paths.get(rootPath, topicPartitionSubpath, s"$uuid-segment").toString,
      Paths.get(rootPath, topicPartitionSubpath, s"$uuid-offset").toString,
      Paths.get(rootPath, topicPartitionSubpath, s"$uuid-timestamp").toString
    )
  }

  def assertContainsLogSegmentFiles(id: RemoteLogSegmentId, segment: LogSegmentData): Unit = {
    expectedPaths(id, segment).foreach(assertFileExists _)
  }

  def assertLogSegmentFilesAbsent(id: RemoteLogSegmentId, segment: LogSegmentData): Unit = {
    expectedPaths(id, segment).foreach(assertFileDoesNotExist _)
  }

  def assertLogSegmentDataEquals(id: RemoteLogSegmentId, segment: LogSegmentData, data: Array[Byte]): Unit = {
    val remoteSegmentPath = expectedPaths(id, segment)(0)
    assertFileDataEquals(remoteSegmentPath, data)
  }

  def assertFetchLogSegment(id: RemoteLogSegmentId, startPosition: Long, data: Byte*): Unit = {
    assertContent(id, remoteStorage.fetchLogSegmentData(_, startPosition, empty()), data.toArray)
  }

  def assertFetchOffsetIndex(id: RemoteLogSegmentId, data: Array[Byte]): Unit = {
    assertContent(id, remoteStorage.fetchOffsetIndex(_), data)
  }

  def assertFetchTimeIndex(id: RemoteLogSegmentId, data: Array[Byte]): Unit = {
    assertContent(id, remoteStorage.fetchTimestampIndex(_), data)
  }

  def newRemoteLogSegmentMetata(id: RemoteLogSegmentId = newRemoteLogSegmentId()): RemoteLogSegmentMetadata = {
    new RemoteLogSegmentMetadata(id, 0, 0, -1, emptyByteArray)
  }

  def newRemoteLogSegmentId(topicPartition: TopicPartition = getDefaultTopicPartition()): RemoteLogSegmentId = {
    new RemoteLogSegmentId(topicPartition, UUID.randomUUID())
  }

  def assertContent(id: RemoteLogSegmentId,
                    fetcher: RemoteLogSegmentMetadata => InputStream,
                    data: Array[Byte]): Unit = {

    val is = fetcher(newRemoteLogSegmentMetata(id))
    assertArrayEquals(data, Source.fromInputStream(is).mkString.getBytes)
  }
}

object LocalRemoteStorageVerifier {

  private def assertFileExists(path: String): Unit = {
    if (!Paths.get(path).toFile.exists()) {
      throw new AssertionError(s"File $path does not exist")
    }
  }

  private def assertFileDoesNotExist(path: String): Unit = {
    if (Paths.get(path).toFile.exists()) {
      throw new AssertionError(s"File $path should not exist")
    }
  }

  private def assertFileDataEquals(path: String, data: Array[Byte]): Unit = {
    assertFileExists(path)
    assertArrayEquals(data, Files.readAllBytes(Paths.get(path)))
  }

}
