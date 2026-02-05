package de.microservicedungeon.mock.eventing.events.robot;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
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
     * Builds a new {@code RobotDeliveredResourcesEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(RobotDeliveredResourcesPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.robotId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.robotId().toString(), payload, headers);
    }
}
