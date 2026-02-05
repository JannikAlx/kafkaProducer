package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import org.springframework.validation.Validator;

import java.util.UUID;

/**
 * Notifies about a robot being destroyed (e.g., by a black hole).
 */
public class RobotDestroyedEvent extends AbstractPublishableEvent<RobotDestroyedEvent.RobotDestroyedPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-destroyed";
    private static final int SCHEMA_VERSION = 1;

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

    protected RobotDestroyedEvent(RobotDestroyedPayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.robotId().toString(), payload, jsonSerializationStrategy, sequenceIdManager);
    }

    @Component
    @AllArgsConstructor
    public static class Factory {
        private Validator validator;
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code RobotDestroyedEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(RobotDestroyedPayload payload) {
            RobotDestroyedEvent event = new RobotDestroyedEvent(payload, jsonSerializer,sequenceIdManager);
            return event.toProducerRecord();
        }
    }
}
