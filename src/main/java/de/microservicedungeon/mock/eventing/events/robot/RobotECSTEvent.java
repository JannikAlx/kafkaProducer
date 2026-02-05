package de.microservicedungeon.mock.eventing.events.robot;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Keyed ECST event publishing the robot's current state.
 */
@Component
public class RobotECSTEvent extends AbstractEventFactory<RobotECSTEvent.RobotECSTPayload> {

    private static final String TOPIC_NAME = "db.robot.ecst.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-ecst";
    private static final int SCHEMA_VERSION = 1;

    public RobotECSTEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record RobotECSTPayload(
            @JsonProperty("robotId")
            @NotNull
            UUID robotId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("position")
            @NotNull
            @Valid
            PositionPayload position,
            @JsonProperty("cargo")
            @NotNull
            List<@Valid CargoPayload> cargo,
            @JsonProperty("health")
            @NotNull
            @Min(0)
            @Max(100)
            Integer health
    ) {}

    public record PositionPayload(
            @JsonProperty("x")
            @NotNull
            Integer x,
            @JsonProperty("y")
            @NotNull
            Integer y
    ) {}

    public record CargoPayload(
            @JsonProperty("resourceType")
            @NotNull
            String resourceType,
            @JsonProperty("amount")
            @NotNull
            @Min(0)
            Integer amount
    ) {}

    /**
     * Fluent builder for creating RobotECSTEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class RobotECSTEventBuilder {
        private UUID robotId;
        private UUID playerId;
        private UUID gameId;
        private PositionPayload position;
        private List<CargoPayload> cargo;
        private Integer health;

        public RobotECSTEventBuilder forRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public RobotECSTEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public RobotECSTEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public RobotECSTEventBuilder atPosition(PositionPayload position) {
            this.position = position;
            return this;
        }

        public RobotECSTEventBuilder atPosition(Integer x, Integer y) {
            this.position = new PositionPayload(x, y);
            return this;
        }

        public RobotECSTEventBuilder withCargo(List<CargoPayload> cargo) {
            this.cargo = cargo;
            return this;
        }

        public RobotECSTEventBuilder withHealth(Integer health) {
            this.health = health;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            RobotECSTPayload payload = new RobotECSTPayload(robotId, playerId, gameId, position, cargo, health);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + robotId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, robotId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing RobotECSTEvent ProducerRecords.
     * @return a new RobotECSTEventBuilder
     */
    public RobotECSTEventBuilder builder() {
        return new RobotECSTEventBuilder();
    }
}
