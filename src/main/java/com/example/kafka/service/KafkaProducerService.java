package com.example.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Value("${app.topic-name}")
    private String topicName;

    /**
     * Send a message to Kafka with headers
     * 
     * @param key        The message key (String)
     * @param value      The message value (byte array)
     * @param eventType  The event type header value
     * @param sequenceId The sequence ID (will be serialized as 8 bytes)
     * @return true if successful, false otherwise
     */
    public boolean sendMessageWithHeaders(String key, String topic, byte[] value, String eventType, Long sequenceId) {
        try {
            // Create producer record
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);

            // Add headers
            List<Header> headers = new ArrayList<>();

            // Add event_type header
            if (eventType != null) {
                headers.add(new RecordHeader("event_type", eventType.getBytes(StandardCharsets.UTF_8)));
            }

            // Add sequence_id header (serialize as 8 bytes)
            if (sequenceId != null) {
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                buffer.putLong(sequenceId);
                headers.add(new RecordHeader("sequence_id", buffer.array()));
            }

            // Add headers to record
            headers.forEach(record.headers()::add);
            log.info("Sending Record to topic {}", record.topic());
            // Send the record
            SendResult<String, byte[]> result = kafkaTemplate.send(record).get();
            log.info("Message sent - Key: {}, event_type: {}, sequence_id: {}, Partition: {}, Offset: {}",
                    key, eventType, sequenceId,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            return true;
        } catch (Exception e) {
            log.error("Failed to send message - Key: {}, event_type: {}, sequence_id: {}",
                    key, eventType, sequenceId, e);
            return false;
        }
    }

    /**
     * Send a message to Kafka asynchronously
     * 
     * @param key   The message key (String)
     * @param value The message value (byte array)
     */
    public void sendMessage(String key, byte[] value) {
        CompletableFuture<SendResult<String, byte[]>> future = kafkaTemplate.send(topicName, key, value);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Message sent successfully - Key: {}, Partition: {}, Offset: {}",
                        key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message - Key: {}", key, ex);
            }
        });
    }

    /**
     * Send a message to Kafka synchronously
     * 
     * @param key   The message key (String)
     * @param value The message value (byte array)
     * @return true if successful, false otherwise
     */
    public boolean sendMessageSync(String key, byte[] value) {
        try {
            SendResult<String, byte[]> result = kafkaTemplate.send(topicName, key, value).get();
            log.info("Message sent successfully - Key: {}, Partition: {}, Offset: {}",
                    key,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            return true;
        } catch (Exception e) {
            log.error("Failed to send message - Key: {}", key, e);
            return false;
        }
    }
}
