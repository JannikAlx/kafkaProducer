package de.microservicedungeon.mock.eventing.events.map;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Event(factory) for planet ECST (Entity Current State Transfer).
 */
@Component
public class PlanetECSTEvent extends AbstractEventFactory<PlanetECSTEvent.PlanetECSTPayload> {

    private static final String TOPIC_NAME = "db.planet.ecst.v1";
    private static final String AGGREGATE_NAME = "planet";
    private static final String SCHEMA = "planet-ecst";
    private static final int SCHEMA_VERSION = 1;

    public PlanetECSTEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

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

    /**
     * Builds a new {@code PlanetECSTEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(PlanetECSTPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.planetId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.planetId().toString(), payload, headers);
    }
}
