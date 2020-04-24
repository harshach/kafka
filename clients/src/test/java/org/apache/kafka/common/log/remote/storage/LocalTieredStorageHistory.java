package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.*;
import org.slf4j.*;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static java.util.function.Function.*;
import static java.util.stream.Collectors.*;
import static org.slf4j.LoggerFactory.*;

public final class LocalTieredStorageHistory {
    private static final int HARD_EVENT_COUNT_LIMIT = 1_000_000;

    private static final Logger LOGGER = getLogger(LocalTieredStorageHistory.class);

    private final Map<EventType, List<LocalTieredStorageEvent>> history;

    LocalTieredStorageHistory() {
        this.history = unmodifiableMap(stream(EventType.values()).collect(toMap(identity(), t -> new ArrayList<>())));
    }

    public List<LocalTieredStorageEvent> getEvents(final EventType type, final TopicPartition topicPartition) {
        List<LocalTieredStorageEvent> matchingTypeEvents = history.get(type);

        synchronized (matchingTypeEvents) {
            matchingTypeEvents = new ArrayList<>(matchingTypeEvents);
        }

        return matchingTypeEvents.stream().filter(matches(topicPartition)).collect(Collectors.toList());
    }

    public Optional<LocalTieredStorageEvent> latestEvent(final EventType type, final TopicPartition topicPartition) {
        return getEvents(type, topicPartition).stream().max(Comparator.naturalOrder());
    }

    final class InternalListener implements LocalTieredStorageListener {
        @Override
        public void onStorageEvent(LocalTieredStorageEvent event) {
            final List<LocalTieredStorageEvent> events = history.get(event.getType());

            synchronized (events) {
                if (events.size() >= HARD_EVENT_COUNT_LIMIT) {
                    LOGGER.error("Reached max number of historical event of type {}, dropping event {}",
                            event.getType(), event);
                    return;
                }

                events.add(event);
            }
        }
    }

    private static Predicate<LocalTieredStorageEvent> matches(final TopicPartition topicPartition) {
        return event -> event.getTopicPartition().equals(topicPartition);
    }
}
