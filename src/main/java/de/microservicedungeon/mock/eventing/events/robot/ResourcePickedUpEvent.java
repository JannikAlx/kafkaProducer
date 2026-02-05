package de.microservicedungeon.mock.eventing.events.robot;

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
 * Event(factory) for robot picking up resources from a planet.
 */
@Component
public class ResourcePickedUpEvent extends AbstractEventFactory<ResourcePickedUpEvent.ResourcePickedUpPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "resource-picked-up";
    private static final int SCHEMA_VERSION = 1;

    public ResourcePickedUpEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record ResourcePickedUpPayload(
            @JsonProperty("robotId")
            @NotNull
            UUID robotId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("planetId")
            @NotNull
            UUID planetId,
            @JsonProperty("resourceType")
            @NotNull
            String resourceType,
            @JsonProperty("amount")
            @NotNull
            @Min(1)
            Integer amount
    ) {}

    /**
     * Builds a new {@code ResourcePickedUpEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(ResourcePickedUpPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.robotId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.robotId().toString(), payload, headers);
    }
}
