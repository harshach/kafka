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

import java.io.File
import java.nio.file.Files
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Optional.{empty, of}
import java.util.UUID

import LocalLogSegments.{offsetFileContent, timeFileContent}
import integration.kafka.tiered.storage.LocalRemoteStorageManager.{DeleteOnCloseProp, StorageIdProp}
import kafka.log.Log
import kafka.utils.CoreUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{LogSegmentData, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteResourceNotFoundException}
import org.junit.rules.TestName
import org.junit.{After, Before, Rule, Test}
import org.scalatest.Assertions.assertThrows

import scala.Array.emptyByteArray
import scala.annotation.meta.getter

import scala.collection.JavaConverters._

class LocalRemoteStorageManagerTest {
  @(Rule @getter)
  val testName = new TestName

  private val localLogSegments = new LocalLogSegments
  private val topicPartition = new TopicPartition("my-topic", 1)

  private var remoteStorage: LocalRemoteStorageManager = _
  private var remoteStorageVerifier: LocalRemoteStorageVerifier = _

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH:mm:ss")

  private def generateStorageId(): String = {
    s"${getClass.getSimpleName}-${testName.getMethodName}-${formatter.format(LocalDateTime.now())}"
  }

  @Before
  def before(): Unit = {
    remoteStorage = new LocalRemoteStorageManager()
    remoteStorageVerifier = new LocalRemoteStorageVerifier(remoteStorage, Some(topicPartition))

    val config = Map[String, Any](StorageIdProp -> generateStorageId(), DeleteOnCloseProp -> true)
    remoteStorage.configure(config.asJava)
  }

  @After
  def after(): Unit = {
    Option(remoteStorage).foreach(_.close())
    localLogSegments.deleteAll()
  }

  @Test
  def copyEmptyLogSegment(): Unit = {
    val (id, segment) = (newRemoteLogSegmentId(), localLogSegments.nextSegment())

    remoteStorage.copyLogSegment(id, segment)

    remoteStorageVerifier.assertContainsLogSegmentFiles(id, segment)
  }

  @Test
  def copyDataFromLogSegment(): Unit = {
    val data = Array[Byte](0, 1, 2)
    val (id, segment) = (newRemoteLogSegmentId(), localLogSegments.nextSegment(data))

    remoteStorage.copyLogSegment(id, segment)

    remoteStorageVerifier.assertLogSegmentDataEquals(id, segment, data)
  }

  @Test
  def fetchLogSegment(): Unit = {
    val data = Array[Byte](0, 1, 2)
    val (id, segment) = (newRemoteLogSegmentId(), localLogSegments.nextSegment(data))

    remoteStorage.copyLogSegment(id, segment)

    remoteStorageVerifier.assertFetchLogSegment(id, startPosition = 0, data: _*)
    remoteStorageVerifier.assertFetchLogSegment(id, startPosition = 1, data = 1, 2)
    remoteStorageVerifier.assertFetchLogSegment(id, startPosition = 2, data = 2)
  }

  @Test
  def fetchOffsetIndex(): Unit = {
    val (id, segment) = (newRemoteLogSegmentId(), localLogSegments.nextSegment())

    remoteStorage.copyLogSegment(id, segment)

    remoteStorageVerifier.assertFetchOffsetIndex(id, offsetFileContent)
  }

  @Test
  def fetchTimeIndex(): Unit = {
    val (id, segment) = (newRemoteLogSegmentId(), localLogSegments.nextSegment())

    remoteStorage.copyLogSegment(id, segment)

    remoteStorageVerifier.assertFetchTimeIndex(id, timeFileContent)
  }

  @Test
  def deleteLogSegment(): Unit = {
    val (id, segment) = (newRemoteLogSegmentId(), localLogSegments.nextSegment())

    remoteStorage.copyLogSegment(id, segment)
    remoteStorageVerifier.assertContainsLogSegmentFiles(id, segment)

    remoteStorage.deleteLogSegment(newRemoteLogSegmentMetata(id))
    remoteStorageVerifier.assertLogSegmentFilesAbsent(id, segment)
  }

  @Test
  def fetchThrowsIfDataDoesNotExist(): Unit = {
    val metadata = newRemoteLogSegmentMetata()

    assertThrows[RemoteResourceNotFoundException] { remoteStorage.deleteLogSegment(metadata)    }
    assertThrows[RemoteResourceNotFoundException] { remoteStorage.fetchOffsetIndex(metadata)    }
    assertThrows[RemoteResourceNotFoundException] { remoteStorage.fetchTimestampIndex(metadata) }
  }

  @Test
  def assertStartAndEndPositionConsistency(): Unit = {
    val metadata = newRemoteLogSegmentMetata()

    assertThrows[IllegalArgumentException] { remoteStorage.fetchLogSegmentData(metadata, startPosition = -1L, empty()) }
    assertThrows[IllegalArgumentException] { remoteStorage.fetchLogSegmentData(metadata, startPosition = 1L, of(-1L)) }
    assertThrows[IllegalArgumentException] { remoteStorage.fetchLogSegmentData(metadata, startPosition = 2L, of(1L)) }
  }

  // TODO(duprie): Add test for rollback after failed copy

  def newRemoteLogSegmentMetata(id: RemoteLogSegmentId = newRemoteLogSegmentId()): RemoteLogSegmentMetadata = {
    new RemoteLogSegmentMetadata(id, 0, 0, -1, emptyByteArray)
  }

  def newRemoteLogSegmentId(): RemoteLogSegmentId = {
    new RemoteLogSegmentId(topicPartition, UUID.randomUUID())
  }
}

class LocalLogSegments {
  private val segmentDir = new File("local-segments")

  private var baseOffset = 0

  if (!segmentDir.exists()) {
    segmentDir.mkdir()
  }

  def nextSegment(data: Array[Byte] = emptyByteArray): LogSegmentData = {
    val segment = new File(segmentDir, s"${Log.filenamePrefixFromOffset(baseOffset)}.log")
    val offsetIndex = Log.offsetIndexFile(segmentDir, baseOffset)
    val timeIndex = Log.timeIndexFile(segmentDir, baseOffset)

    Files.write(segment.toPath, data)
    Files.write(offsetIndex.toPath, offsetFileContent)
    Files.write(timeIndex.toPath, timeFileContent)

    baseOffset += 1

    new LogSegmentData(segment, offsetIndex, timeIndex)
  }

  def deleteAll(): Unit = {
    CoreUtils.delete(segmentDir.listFiles().toSeq.map(_.toString))
    segmentDir.delete()
  }
}

object LocalLogSegments {
  val offsetFileContent = "offset".map(_.toByte).toArray
  val timeFileContent = "time".map(_.toByte).toArray
}

