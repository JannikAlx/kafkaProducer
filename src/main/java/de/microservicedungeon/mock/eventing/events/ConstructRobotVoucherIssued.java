package de.microservicedungeon.mock.eventing.events;

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
 * Event(factory) used to publish construct robot voucher issued events.
 */
@Component
public class ConstructRobotVoucherIssued extends AbstractEventFactory<ConstructRobotVoucherIssued.ConstructRobotVoucherPayload> {
    private static final String TOPIC_NAME = "bl.voucher.events.v1";
    private static final String AGGREGATE_NAME = "voucher";
    private static final String SCHEMA = "construct-robot-voucher-issued";
    private static final int SCHEMA_VERSION = 1;

    public ConstructRobotVoucherIssued(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record ConstructRobotVoucherPayload(
            @JsonProperty("voucherId")
            @NotNull
            UUID voucherId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("totalPrice")
            @NotNull
            int totalPrice,
            @JsonProperty("numberOfRobots")
            @NotNull
            @Min(value = 1)
            int numberOfRobots
    ) {}

    /**
     * Builds a new {@code ConstructRobotVoucherIssued} ProducerRecord from a given payload.
     *
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(ConstructRobotVoucherPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.gameId().toString() + "." + payload.playerId().toString())
                .createdAt(System.currentTimeMillis()).build();

        return buildRecord(TOPIC_NAME, payload.gameId().toString() + "." + payload.playerId().toString(), payload, headers);
    }
}
