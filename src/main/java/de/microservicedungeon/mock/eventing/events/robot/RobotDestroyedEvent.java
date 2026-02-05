package de.microservicedungeon.mock.eventing.events.robot;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) for robot being destroyed (e.g., by a black hole).
 */
@Component
public class RobotDestroyedEvent extends AbstractEventFactory<RobotDestroyedEvent.RobotDestroyedPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-destroyed";
    private static final int SCHEMA_VERSION = 1;

    public RobotDestroyedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record RobotDestroyedPayload(
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
            @JsonProperty("reason")
            @NotNull
            String reason
    ) {}

    public record PositionPayload(
            @JsonProperty("x")
            @NotNull
            Integer x,
            @JsonProperty("y")
            @NotNull
            Integer y
    ) {}

    /**
     * Fluent builder for creating RobotDestroyedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class RobotDestroyedEventBuilder {
        private UUID robotId;
        private UUID playerId;
        private UUID gameId;
        private PositionPayload position;
        private String reason;

        public RobotDestroyedEventBuilder forRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public RobotDestroyedEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public RobotDestroyedEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public RobotDestroyedEventBuilder atPosition(PositionPayload position) {
            this.position = position;
            return this;
        }

        public RobotDestroyedEventBuilder atPosition(Integer x, Integer y) {
            this.position = new PositionPayload(x, y);
            return this;
        }

        public RobotDestroyedEventBuilder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            RobotDestroyedPayload payload = new RobotDestroyedPayload(robotId, playerId, gameId, position, reason);

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
     * Creates a new builder instance for constructing RobotDestroyedEvent ProducerRecords.
     * @return a new RobotDestroyedEventBuilder
     */
    public RobotDestroyedEventBuilder builder() {
        return new RobotDestroyedEventBuilder();
    }
}
