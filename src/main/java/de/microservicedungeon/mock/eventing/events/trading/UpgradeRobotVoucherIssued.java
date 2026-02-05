package de.microservicedungeon.mock.eventing.events.trading;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) for upgrade robot voucher issued events.
 */
@Component
public class UpgradeRobotVoucherIssued extends AbstractEventFactory<UpgradeRobotVoucherIssued.UpgradeRobotVoucherPayload> {
    private static final String TOPIC_NAME = "bl.voucher.events.v1";
    private static final String AGGREGATE_NAME = "voucher";
    private static final String SCHEMA = "upgrade-robot-voucher-issued";
    private static final int SCHEMA_VERSION = 1;

    public UpgradeRobotVoucherIssued(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record UpgradeRobotVoucherPayload(
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
            @JsonProperty("upgradeLevelCount")
            @NotNull
            @Positive
            int upgradeLevelCount,
            @JsonProperty("upgradeType")
            @NotNull
            @NotBlank
            String upgradeType
    ) {}

    /**
     * Builds a new {@code UpgradeRobotVoucherIssued} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(UpgradeRobotVoucherPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.gameId().toString() + "." + payload.playerId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.gameId().toString() + "." + payload.playerId().toString(), payload, headers);
    }
}
