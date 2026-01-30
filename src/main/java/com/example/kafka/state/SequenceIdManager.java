package com.example.kafka.state;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe manager for Kafka event sequence IDs.
 * Maintains separate sequence counters for each Kafka topic.
 * 
 * Sequence IDs are used to order events and detect duplicates.
 * All events published to the same topic share a single incrementing sequence.
 */
@Component
public class SequenceIdManager {

    private final ConcurrentHashMap<String, AtomicLong> topicSequences = new ConcurrentHashMap<>();

    /**
     * Get the next sequence ID for a given topic.
     * Thread-safe and atomic operation.
     *
     * @param topic the Kafka topic name
     * @return the next sequence ID for this topic
     */
    public long getNextSequenceId(String topic) {
        return topicSequences
                .computeIfAbsent(topic, k -> new AtomicLong(0))
                .incrementAndGet();
    }

    /**
     * Get the current sequence ID for a topic without incrementing.
     *
     * @param topic the Kafka topic name
     * @return the current sequence ID, or 0 if topic hasn't been used yet
     */
    public long getCurrentSequenceId(String topic) {
        AtomicLong sequence = topicSequences.get(topic);
        return sequence != null ? sequence.get() : 0;
    }

    /**
     * Reset all sequence counters. Useful for testing.
     */
    public void reset() {
        topicSequences.clear();
    }
}
