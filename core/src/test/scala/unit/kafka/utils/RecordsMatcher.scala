package unit.kafka.utils

import java.nio.ByteBuffer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.{Record, SimpleRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde}
import org.apache.kafka.common.utils.Utils
import org.hamcrest.{Description, TypeSafeDiagnosingMatcher}

final class RecordsMatcher[R1, R2, K, V](private val expectedRecords: Iterable[R1],
                                         private val topic: String,
                                         private val keySerde: Serde[K],
                                         private val valueSerde: Serde[V])

  extends TypeSafeDiagnosingMatcher[Iterable[R2]] {

  override def describeTo(description: Description): Unit = {
    description.appendText(s"Records correspond (topic: $topic)")
  }

  override def matchesSafely(actualRecord: Iterable[R2], mismatchDescription: Description): Boolean = {
    if (expectedRecords.size != actualRecord.size) {
      mismatchDescription
        .appendText(s"Number of records differ. Expected: ")
        .appendValue(expectedRecords.size)
        .appendText(", Actual: ")
        .appendValue(actualRecord.size)
        .appendText(";")
      return false
    }

    expectedRecords
      .zip(actualRecord)
      .map { case (expected, actual) => matches(expected, actual, mismatchDescription) }
      .reduce(_ && _)
  }

  private def matches(expected: R1, actual: R2, mismatchDescription: Description): Boolean = {
    val expectedRecord = convert(expected).getOrElse {
      mismatchDescription.appendText(s"Invalid expected record type: ${expected.getClass}")
      return false
    }

    val actualRecord = convert(actual).getOrElse {
      mismatchDescription.appendText(s"Invalid actual record type: ${expected.getClass}")
      return false
    }

    def compare(lhs: ByteBuffer, rhs: ByteBuffer, deserializer: Deserializer[_], desc: String): Boolean = {
      if ((lhs != null && !lhs.equals(rhs)) || (lhs == null && rhs != null)) {
        val deserializer = keySerde.deserializer()

        mismatchDescription.appendText(s"$desc mismatch. Expected: ")
          .appendValue(deserializer.deserialize(topic, Utils.toNullableArray(lhs)))
          .appendText("; Actual: ")
          .appendValue(deserializer.deserialize(topic, Utils.toNullableArray(rhs)))
          .appendText(";")

        false
      } else true
    }

    val keq = compare(expectedRecord.key(), actualRecord.key(), keySerde.deserializer(), "Record key")
    val veq = compare(expectedRecord.value(), actualRecord.value(), valueSerde.deserializer(), "Record value")

    keq && veq
  }

  private def convert(recordCandidate: Any): Option[SimpleRecord] = {
    val keySerializer = keySerde.serializer()
    val valueSerializer = valueSerde.serializer()

    if (recordCandidate.isInstanceOf[ProducerRecord[K, V]]) {
      val record = recordCandidate.asInstanceOf[ProducerRecord[K, V]]
      Some(new SimpleRecord(record.timestamp(),
        Utils.wrapNullable(keySerializer.serialize(topic, record.key())),
        Utils.wrapNullable(valueSerializer.serialize(topic, record.value())),
        record.headers().toArray))

    } else if (recordCandidate.isInstanceOf[ConsumerRecord[K, V]]) {
      val record = recordCandidate.asInstanceOf[ConsumerRecord[K, V]]
      Some(new SimpleRecord(record.timestamp(),
        Utils.wrapNullable(keySerializer.serialize(topic, record.key())),
        Utils.wrapNullable(valueSerializer.serialize(topic, record.value())),
        record.headers().toArray))

    } else if (recordCandidate.isInstanceOf[Record]) {
      val record = recordCandidate.asInstanceOf[Record]
      Some(new SimpleRecord(record.timestamp(), record.key(), record.value(), record.headers()))

    } else {
      None
    }
  }
}

object RecordsMatcher {

  def correspondTo[K, V](expectedRecords: Iterable[Any],
                         topic: String,
                         keySerde: Serde[K],
                         valueSerde: Serde[V]) : RecordsMatcher[Any, Any, K, V] = {
    new RecordsMatcher[Any, Any, K, V](expectedRecords, topic, keySerde, valueSerde)
  }

}


