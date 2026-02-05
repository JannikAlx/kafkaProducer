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
 * Event(factory) for robot delivering resources to be sold.
 */
@Component
public class RobotDeliveredResourcesEvent extends AbstractEventFactory<RobotDeliveredResourcesEvent.RobotDeliveredResourcesPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-delivered-resources";
    private static final int SCHEMA_VERSION = 1;

    public RobotDeliveredResourcesEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record RobotDeliveredResourcesPayload(
            @JsonProperty("robotId")
            @NotNull
            UUID robotId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("bankAccountId")
            @NotNull
            UUID bankAccountId,
            @JsonProperty("resourceType")
            @NotNull
            String resourceType,
            @JsonProperty("amount")
            @NotNull
            @Min(1)
            Integer amount
    ) {}

    /**
     * Fluent builder for creating RobotDeliveredResourcesEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class RobotDeliveredResourcesEventBuilder {
        private UUID robotId;
        private UUID playerId;
        private UUID gameId;
        private UUID bankAccountId;
        private String resourceType;
        private Integer amount;

        public RobotDeliveredResourcesEventBuilder forRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public RobotDeliveredResourcesEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public RobotDeliveredResourcesEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public RobotDeliveredResourcesEventBuilder toBankAccount(UUID bankAccountId) {
            this.bankAccountId = bankAccountId;
            return this;
        }

        public RobotDeliveredResourcesEventBuilder withResourceType(String resourceType) {
            this.resourceType = resourceType;
            return this;
        }

        public RobotDeliveredResourcesEventBuilder withAmount(Integer amount) {
            this.amount = amount;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            RobotDeliveredResourcesPayload payload = new RobotDeliveredResourcesPayload(
                    robotId, playerId, gameId, bankAccountId, resourceType, amount);

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
     * Creates a new builder instance for constructing RobotDeliveredResourcesEvent ProducerRecords.
     * @return a new RobotDeliveredResourcesEventBuilder
     */
    public RobotDeliveredResourcesEventBuilder builder() {
        return new RobotDeliveredResourcesEventBuilder();
    }
}
