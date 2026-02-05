package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Keyed ECST event publishing the planet's current state.
 */
public class PlanetECSTEvent extends AbstractPublishableEvent<PlanetECSTEvent.PlanetECSTPayload> {

    private static final String TOPIC_NAME = "db.planet.ecst.v1";
    private static final String AGGREGATE_NAME = "planet";
    private static final String SCHEMA = "planet-ecst";
    private static final int SCHEMA_VERSION = 1;

    public record PlanetECSTPayload(
            @JsonProperty("planetId")
            @NotNull
            UUID planetId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("position")
            @NotNull
            @Valid
            PositionPayload position,
            @JsonProperty("resourceDeposits")
            @NotNull
            List<@Valid ResourceDepositPayload> resourceDeposits
    ) {}

    public record PositionPayload(
            @JsonProperty("x")
            @NotNull
            Integer x,
            @JsonProperty("y")
            @NotNull
            Integer y
    ) {}

    public record ResourceDepositPayload(
            @JsonProperty("resourceType")
            @NotNull
            String resourceType,
            @JsonProperty("currentAmount")
            @NotNull
            @Min(0)
            Integer currentAmount,
            @JsonProperty("maxAmount")
            @NotNull
            @Min(0)
            Integer maxAmount
    ) {}

    protected PlanetECSTEvent(PlanetECSTPayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.planetId().toString(), payload, jsonSerializationStrategy, sequenceIdManager);
    }

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code PlanetECSTEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(PlanetECSTPayload payload) {
            PlanetECSTEvent event = new PlanetECSTEvent(payload, jsonSerializer, sequenceIdManager);
            return event.toProducerRecord();
        }
    }
}
