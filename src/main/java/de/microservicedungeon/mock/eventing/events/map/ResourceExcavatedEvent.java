package de.microservicedungeon.mock.eventing.events.map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
     * Fluent builder for creating ResourceExcavatedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class ResourceExcavatedEventBuilder {
        private UUID planetId;
        private UUID gameId;
        private UUID robotId;
        private UUID playerId;
        private String resourceType;
        private Integer amount;
        private Integer remainingAmount;

        public ResourceExcavatedEventBuilder forPlanet(UUID planetId) {
            this.planetId = planetId;
            return this;
        }

        public ResourceExcavatedEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public ResourceExcavatedEventBuilder byRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public ResourceExcavatedEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public ResourceExcavatedEventBuilder withResourceType(String resourceType) {
            this.resourceType = resourceType;
            return this;
        }

        public ResourceExcavatedEventBuilder withAmount(Integer amount) {
            this.amount = amount;
            return this;
        }

        public ResourceExcavatedEventBuilder withRemainingAmount(Integer remainingAmount) {
            this.remainingAmount = remainingAmount;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            ResourceExcavatedPayload payload = new ResourceExcavatedPayload(
                    planetId, gameId, robotId, playerId, resourceType, amount, remainingAmount);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + planetId.toString())
                    .createdAt(System.currentTimeMillis())
                    .build();

            return buildRecord(TOPIC_NAME, planetId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing ResourceExcavatedEvent ProducerRecords.
     * @return a new ResourceExcavatedEventBuilder
     */
    public ResourceExcavatedEventBuilder builder() {
        return new ResourceExcavatedEventBuilder();
    }
}
