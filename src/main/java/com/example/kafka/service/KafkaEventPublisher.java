package com.example.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Service for publishing events to Kafka.
 * Wraps KafkaTemplate and provides logging and error handling.
 */
@Service
public class KafkaEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Publish an event to Kafka.
     * The event JSON must contain "topic", "kafkaKey", "headers", and "payload"
     * fields.
     *
     * @param eventJson JSON string containing the complete event
     */
    public void publish(String eventJson) {
        try {
            JsonNode eventNode = objectMapper.readTree(eventJson);

            String topic = eventNode.get("topic").asText();
            String key = eventNode.get("kafkaKey").asText();
            String eventType = eventNode.get("headers").get("event_type").asText();

            // Send to Kafka
            kafkaTemplate.send(topic, key, eventJson);

            logger.info("Published event: topic={}, eventType={}, key={}", topic, eventType, key);

        } catch (Exception e) {
            logger.error("Failed to publish event: {}", eventJson, e);
            throw new RuntimeException("Failed to publish event", e);
        }
    }
}
