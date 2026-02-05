package de.microservicedungeon.mock.eventing;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Common headers for Kafka events as defined in AsyncAPI specifications.
 * These headers are used across all event messages for tracking, debugging, and deduplication.
 */
@Getter
@Builder
@ToString
@EqualsAndHashCode
public class CommonHeaders {

    @JsonProperty("event_id")
    @NotNull
    private final UUID eventId;

    @JsonProperty("entity")
    @NotNull
    private final String entity;

    @JsonProperty("sequence_id")
    @NotNull
    @Min(1)
    private final Long sequenceId;

    @JsonProperty("created_at")
    @NotNull
    private final Long createdAt;

    @JsonProperty("event_type")
    @NotNull
    private final String eventType;

    @JsonProperty("event_type_version")
    @NotNull
    @Min(1)
    private final Integer eventTypeVersion;

    /**
     * Creates a new CommonHeaders instance.
     *
     * @param eventId           Uniquely identifies an event globally across topics and partitions
     * @param entity            Identifies the entity an event belongs to (format: entity.id)
     * @param sequenceId        Sequence number for ordering and duplicate detection
     * @param createdAt         Timestamp in milliseconds since epoch when the event was created
     * @param eventType         The type or schema name used for the payload
     * @param eventTypeVersion  The version of the event type/schema
     */
    private CommonHeaders(UUID eventId, String entity, Long sequenceId, Long createdAt,
                         String eventType, Integer eventTypeVersion) {
        if (sequenceId != null && sequenceId < 1) {
            throw new IllegalArgumentException("sequenceId must be greater than 0");
        }
        if (eventTypeVersion != null && eventTypeVersion < 1) {
            throw new IllegalArgumentException("eventTypeVersion must be greater than 0");
        }
        this.eventId = eventId;
        this.entity = entity;
        this.sequenceId = sequenceId;
        this.createdAt = createdAt;
        this.eventType = eventType;
        this.eventTypeVersion = eventTypeVersion;
    }

    /**
     * Converts the common headers to Kafka headers format.
     * Only non-null fields are included in the returned list.
     *
     * @return a list of Kafka headers with field names as keys and their byte representations as values
     */
    public List<Header> toKafkaHeaders() {
        List<Header> headers = new java.util.ArrayList<>();

        if (eventId != null) {
            headers.add(new RecordHeader("event_id", eventId.toString().getBytes(StandardCharsets.UTF_8)));
        }
        if (entity != null) {
            headers.add(new RecordHeader("entity", entity.getBytes(StandardCharsets.UTF_8)));
        }
        if (sequenceId != null) {
            headers.add(new RecordHeader("sequence_id", ByteBuffer.allocate(Long.BYTES).putLong(sequenceId).array()));
        }
        if (createdAt != null) {
            headers.add(new RecordHeader("created_at", ByteBuffer.allocate(Long.BYTES).putLong(createdAt).array()));
        }
        if (eventType != null) {
            headers.add(new RecordHeader("event_type", eventType.getBytes(StandardCharsets.UTF_8)));
        }
        if (eventTypeVersion != null) {
            headers.add(new RecordHeader("event_type_version", ByteBuffer.allocate(Integer.BYTES).putInt(eventTypeVersion).array()));
        }

        return headers;
    }


}
