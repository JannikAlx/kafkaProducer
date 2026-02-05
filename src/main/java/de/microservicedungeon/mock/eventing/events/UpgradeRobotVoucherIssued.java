package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

public class UpgradeRobotVoucherIssued extends AbstractPublishableEvent<UpgradeRobotVoucherIssued.UpgradeRobotVoucherPayload> {
    private static final String TOPIC = "bl.voucher.events.v1";
    private static final String AGGREGATE = "voucher";
    private static final String SCHEMA = "upgrade-robot-voucher-issued";
    private static final int SCHEMA_VERSION = 1;

    protected UpgradeRobotVoucherIssued(UpgradeRobotVoucherPayload payload,
                                        JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(
                TOPIC,
                AGGREGATE,
                SCHEMA,
                SCHEMA_VERSION,
                payload.gameId().toString() + "." + payload.playerId().toString(),
                payload,
                jsonSerializationStrategy,
                sequenceIdManager
        );
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

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;
        /**
         * Builds a new {@code UpgradeRobotVoucherIssued}
         * @return the newly created {@code UpgradeRobotVoucherIssued}
         */
        public ProducerRecord<String, byte[]> build(UpgradeRobotVoucherPayload payload) {
            var record = new UpgradeRobotVoucherIssued(payload, jsonSerializer, sequenceIdManager);
            return record.toProducerRecord();
        }
    }
}
