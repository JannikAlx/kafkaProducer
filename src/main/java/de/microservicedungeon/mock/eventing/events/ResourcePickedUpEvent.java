package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Notifies about a robot picking up resources from a planet.
 */
public class ResourcePickedUpEvent extends AbstractPublishableEvent<ResourcePickedUpEvent.ResourcePickedUpPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "resource-picked-up";
    private static final int SCHEMA_VERSION = 1;

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

    protected ResourcePickedUpEvent(ResourcePickedUpPayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.robotId().toString(), payload, jsonSerializationStrategy, sequenceIdManager);
    }

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code ResourcePickedUpEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(ResourcePickedUpPayload payload) {
            ResourcePickedUpEvent event = new ResourcePickedUpEvent(payload, jsonSerializer, sequenceIdManager);
            return event.toProducerRecord();
        }
    }
}
