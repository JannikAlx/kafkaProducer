package de.microservicedungeon.mock.eventing.events.robot;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
     * Fluent builder for creating ResourcePickedUpEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class ResourcePickedUpEventBuilder {
        private UUID robotId;
        private UUID playerId;
        private UUID gameId;
        private UUID planetId;
        private String resourceType;
        private Integer amount;

        public ResourcePickedUpEventBuilder forRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public ResourcePickedUpEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public ResourcePickedUpEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public ResourcePickedUpEventBuilder fromPlanet(UUID planetId) {
            this.planetId = planetId;
            return this;
        }

        public ResourcePickedUpEventBuilder withResourceType(String resourceType) {
            this.resourceType = resourceType;
            return this;
        }

        public ResourcePickedUpEventBuilder withAmount(Integer amount) {
            this.amount = amount;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            ResourcePickedUpPayload payload = new ResourcePickedUpPayload(
                    robotId, playerId, gameId, planetId, resourceType, amount);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + robotId.toString())
                    .createdAt(System.currentTimeMillis())
                    .build();

            return buildRecord(TOPIC_NAME, robotId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing ResourcePickedUpEvent ProducerRecords.
     * @return a new ResourcePickedUpEventBuilder
     */
    public ResourcePickedUpEventBuilder builder() {
        return new ResourcePickedUpEventBuilder();
    }
}
