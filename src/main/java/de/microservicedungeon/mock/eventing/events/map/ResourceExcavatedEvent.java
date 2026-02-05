package de.microservicedungeon.mock.eventing.events.map;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) for resources being excavated and available for pickup.
 */
@Component
public class ResourceExcavatedEvent extends AbstractEventFactory<ResourceExcavatedEvent.ResourceExcavatedPayload> {

    private static final String TOPIC_NAME = "bl.map.events.v1";
    private static final String AGGREGATE_NAME = "planet";
    private static final String SCHEMA = "resource-excavated";
    private static final int SCHEMA_VERSION = 1;

    public ResourceExcavatedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record ResourceExcavatedPayload(
            @JsonProperty("planetId")
            @NotNull
            UUID planetId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("robotId")
            @NotNull
            UUID robotId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("resourceType")
            @NotNull
            String resourceType,
            @JsonProperty("amount")
            @NotNull
            @Min(1)
            Integer amount,
            @JsonProperty("remainingAmount")
            @NotNull
            @Min(0)
            Integer remainingAmount
    ) {}

    /**
     * Builds a new {@code ResourceExcavatedEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(ResourceExcavatedPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.planetId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.planetId().toString(), payload, headers);
    }
}
