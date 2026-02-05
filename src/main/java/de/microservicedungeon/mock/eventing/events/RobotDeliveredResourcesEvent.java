package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Notifies about a robot delivering resources to be sold.
 */
public class RobotDeliveredResourcesEvent extends AbstractPublishableEvent<RobotDeliveredResourcesEvent.RobotDeliveredResourcesPayload> {

    private static final String TOPIC_NAME = "bl.robot.events.v1";
    private static final String AGGREGATE_NAME = "robot";
    private static final String SCHEMA = "robot-delivered-resources";
    private static final int SCHEMA_VERSION = 1;

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

    protected RobotDeliveredResourcesEvent(RobotDeliveredResourcesPayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.robotId().toString(), payload, jsonSerializationStrategy, sequenceIdManager);
    }

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code RobotDeliveredResourcesEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(RobotDeliveredResourcesPayload payload) {
            RobotDeliveredResourcesEvent event = new RobotDeliveredResourcesEvent(payload, jsonSerializer, sequenceIdManager);
            return event.toProducerRecord();
        }
    }
}
