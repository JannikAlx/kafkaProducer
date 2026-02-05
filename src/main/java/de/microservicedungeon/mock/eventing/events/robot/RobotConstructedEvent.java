package de.microservicedungeon.mock.eventing.events.robot;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) used to publish robot constructed events.
 */
@Component
public class RobotConstructedEvent extends AbstractEventFactory<RobotConstructedEvent.RobotConstructedPayload> {
    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-constructed";
    private static final int SCHEMA_VERSION = 1;

    public RobotConstructedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record RobotConstructedPayload(
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
            PositionPayload position
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
     * Fluent builder for creating RobotConstructedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class RobotConstructedEventBuilder {
        private UUID robotId;
        private UUID playerId;
        private UUID gameId;
        private PositionPayload position;

        public RobotConstructedEventBuilder forRobot(UUID robotId) {
            this.robotId = robotId;
            return this;
        }

        public RobotConstructedEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public RobotConstructedEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public RobotConstructedEventBuilder atPosition(PositionPayload position) {
            this.position = position;
            return this;
        }

        public RobotConstructedEventBuilder atPosition(Integer x, Integer y) {
            this.position = new PositionPayload(x, y);
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            RobotConstructedPayload payload = new RobotConstructedPayload(robotId, playerId, gameId, position);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + robotId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, gameId.toString() + "." + playerId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing RobotConstructedEvent ProducerRecords.
     * @return a new RobotConstructedEventBuilder
     */
    public RobotConstructedEventBuilder builder() {
        return new RobotConstructedEventBuilder();
    }
}
