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
 * Notifies about resources being excavated and available for pickup.
 */
public class ResourceExcavatedEvent extends AbstractPublishableEvent<ResourceExcavatedEvent.ResourceExcavatedPayload> {

    private static final String TOPIC_NAME = "bl.map.events.v1";
    private static final String AGGREGATE_NAME = "planet";
    private static final String SCHEMA = "resource-excavated";
    private static final int SCHEMA_VERSION = 1;

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

    protected ResourceExcavatedEvent(ResourceExcavatedPayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.planetId().toString(), payload, jsonSerializationStrategy, sequenceIdManager);
    }

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code ResourceExcavatedEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(ResourceExcavatedPayload payload) {
            ResourceExcavatedEvent event = new ResourceExcavatedEvent(payload, jsonSerializer, sequenceIdManager);
            return event.toProducerRecord();
        }
    }
}
