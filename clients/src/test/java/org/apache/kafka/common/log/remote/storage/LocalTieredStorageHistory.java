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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageEvent.EventType;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

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

    void listenTo(final LocalTieredStorage storage) {
        storage.addListener(new InternalListener());
    }

    private final class InternalListener implements LocalTieredStorageListener {
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
