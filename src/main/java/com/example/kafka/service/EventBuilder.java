package com.example.kafka.service;

import com.example.kafka.state.SequenceIdManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

/**
 * Service for building Kafka events with proper headers and structure.
 * Handles both intents (no sequence_id) and regular events (with sequence_id).
 */
@Service
public class EventBuilder {

    private final SequenceIdManager sequenceIdManager;
    private final ObjectMapper objectMapper;

    public EventBuilder(SequenceIdManager sequenceIdManager, ObjectMapper objectMapper) {
        this.sequenceIdManager = sequenceIdManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Build an intent event (player command).
     * Intents do NOT have sequence_id or entity fields.
     *
     * @param topic     Kafka topic to publish to
     * @param eventType Type of the event (e.g., "buy-robot")
     * @param payload   Event payload as a map
     * @param kafkaKey  Kafka message key (usually playerId)
     * @return JSON string representing the complete event
     */
    public String buildIntent(String topic, String eventType, Map<String, Object> payload, String kafkaKey) {
        ObjectNode event = objectMapper.createObjectNode();

        // Set topic and key
        event.put("topic", topic);
        event.put("kafkaKey", kafkaKey);

        // Build headers (intents don't have sequence_id or entity)
        ObjectNode headers = objectMapper.createObjectNode();
        headers.put("event_id", UUID.randomUUID().toString());
        headers.put("event_type", eventType);
        headers.put("event_type_version", 1);
        headers.put("created_at", getCurrentTimestamp());

        event.set("headers", headers);

        // Set payload
        event.set("payload", objectMapper.valueToTree(payload));

        return event.toString();
    }

    /**
     * Build a regular event (service-generated).
     * Events HAVE sequence_id and entity fields.
     *
     * @param topic     Kafka topic to publish to
     * @param eventType Type of the event (e.g., "robot-constructed")
     * @param entity    Entity identifier (e.g., "robot.{uuid}")
     * @param payload   Event payload as a map
     * @param kafkaKey  Kafka message key
     * @return JSON string representing the complete event
     */
    public String buildEvent(String topic, String eventType, String entity,
            Map<String, Object> payload, String kafkaKey) {
        ObjectNode event = objectMapper.createObjectNode();

        // Set topic and key
        event.put("topic", topic);
        event.put("kafkaKey", kafkaKey);

        // Build headers with sequence_id
        ObjectNode headers = objectMapper.createObjectNode();
        headers.put("event_id", UUID.randomUUID().toString());
        headers.put("entity", entity);
        headers.put("sequence_id", sequenceIdManager.getNextSequenceId(topic));
        headers.put("created_at", getCurrentTimestamp());
        headers.put("event_type", eventType);
        headers.put("event_type_version", 1);

        event.set("headers", headers);

        // Set payload
        event.set("payload", objectMapper.valueToTree(payload));

        return event.toString();
    }

    /**
     * Get current timestamp in ISO-8601 format with timezone.
     */
    private String getCurrentTimestamp() {
        return ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
