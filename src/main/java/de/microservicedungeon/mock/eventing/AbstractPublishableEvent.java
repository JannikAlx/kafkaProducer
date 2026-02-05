package de.microservicedungeon.mock.eventing;

import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

@ToString
public abstract class AbstractPublishableEvent<T> {
    // Convention: <db/bl>.<aggregate>.<type>.<version>
    @NotNull
    @Getter
    private final String TOPIC_NAME;
    @NotNull
    private final String AGGREGATE_NAME;
    @NotNull
    private final String SCHEMA;
    @NotNull
    @Min(1)
    private final int SCHEMA_VERSION;
    private final String eventKey;
    @Valid
    @Nullable
    private final T payloadData;
    private final String traceId;
    private final JsonSerializationStrategy jsonSerializationStrategy;
    private final SequenceIdManager sequenceIdManager;

    public AbstractPublishableEvent(String topicName, String aggregateName, String schema, int schemaVersion, String eventKey, T payloadData, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        this.sequenceIdManager = sequenceIdManager;
        if (schemaVersion < 1){
            throw new IllegalArgumentException("Version has to be greater than 0");
        }
        this.TOPIC_NAME = topicName; // not null
        this.AGGREGATE_NAME = aggregateName; // not null
        this.SCHEMA = schema; // not null
        this.SCHEMA_VERSION = schemaVersion; // not null
        this.eventKey = eventKey; //optional in case of error events
        this.payloadData = payloadData; // not null
        this.traceId = null;
        this.jsonSerializationStrategy = jsonSerializationStrategy; // not null
    }

    public Optional<String> getKey() {
        if (eventKey != null && !eventKey.isBlank()){
            return Optional.of(eventKey);
        }
        return Optional.empty();
    }

    public String getEntityIdentifier() {
        if (AGGREGATE_NAME != null && !AGGREGATE_NAME.isBlank() && eventKey != null && !eventKey.isBlank()) {
            return AGGREGATE_NAME+"."+eventKey;
        }
        return "unknown.unknown";
    }

    public byte[] getPayload() {
        if (payloadData == null) {
            return new byte[0];
        }
        try {
            return jsonSerializationStrategy.serialize(payloadData);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize payload", e);
        }
    }

    /**
     * Builds a Kafka ProducerRecord with proper headers and payload.
     * Uses the SequenceIdManager to automatically assign sequence IDs.
     *
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> toProducerRecord() {
        long sequenceId = sequenceIdManager.getNextSequenceId(TOPIC_NAME);
        long createdAt = System.currentTimeMillis();
        UUID eventId = UUID.randomUUID();

        CommonHeaders headers = CommonHeaders.builder()
                .eventId(eventId)
                .entity(getEntityIdentifier())
                .sequenceId(sequenceId)
                .createdAt(createdAt)
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                TOPIC_NAME,
                eventKey,
                getPayload()
        );

        // Add headers as bytes
        record.headers().add(new RecordHeader("event_id", eventId.toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("entity", headers.getEntity().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("sequence_id", longToBytes(sequenceId)));
        record.headers().add(new RecordHeader("created_at", longToBytes(createdAt)));
        record.headers().add(new RecordHeader("event_type", SCHEMA.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("event_type_version", intToBytes(SCHEMA_VERSION)));

        return record;
    }

    /**
     * Convert long to byte array using ByteBuffer.
     */
    private byte[] longToBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * Convert int to byte array using ByteBuffer.
     */
    private byte[] intToBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

}
