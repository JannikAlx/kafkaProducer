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
 * Notifies about a robot moving to a new position.
 */
@Component
public class RobotMovedEvent extends AbstractEventFactory<RobotMovedEvent.RobotMovedPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-moved";
    private static final int SCHEMA_VERSION = 1;

    public RobotMovedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record RobotMovedPayload(
            @JsonProperty("robotId")
            @NotNull
            UUID robotId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("fromPosition")
            @NotNull
            @Valid
            PositionPayload fromPosition,
            @JsonProperty("toPosition")
            @NotNull
            @Valid
            PositionPayload toPosition
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
     * Fluent builder for creating RobotMovedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class RobotMovedEventBuilder {
        private UUID robotId;
        private UUID playerId;
        private UUID gameId;
        private PositionPayload fromPosition;
        private PositionPayload toPosition;

        public RobotMovedEventBuilder forRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public RobotMovedEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public RobotMovedEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public RobotMovedEventBuilder fromPosition(PositionPayload fromPosition) {
            this.fromPosition = fromPosition;
            return this;
        }

        public RobotMovedEventBuilder toPosition(PositionPayload toPosition) {
            this.toPosition = toPosition;
            return this;
        }

        public RobotMovedEventBuilder withPositions(PositionPayload from, PositionPayload to) {
            this.fromPosition = from;
            this.toPosition = to;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            RobotMovedPayload payload = new RobotMovedPayload(robotId, playerId, gameId, fromPosition, toPosition);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + robotId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, robotId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing RobotMovedEvent ProducerRecords.
     * @return a new RobotMovedEventBuilder
     */
    public RobotMovedEventBuilder builder() {
        return new RobotMovedEventBuilder();
    }
}
