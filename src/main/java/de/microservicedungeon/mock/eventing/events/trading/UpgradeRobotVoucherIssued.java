package de.microservicedungeon.mock.eventing.events.trading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
     * Fluent builder for creating UpgradeRobotVoucherIssued ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class UpgradeRobotVoucherIssuedBuilder {
        private UUID voucherId;
        private UUID gameId;
        private UUID playerId;
        private int totalPrice;
        private int upgradeLevelCount;
        private String upgradeType;

        public UpgradeRobotVoucherIssuedBuilder forVoucher(UUID voucherId) {
            this.voucherId = voucherId;
            return this;
        }

        public UpgradeRobotVoucherIssuedBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public UpgradeRobotVoucherIssuedBuilder forPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public UpgradeRobotVoucherIssuedBuilder withTotalPrice(int totalPrice) {
            this.totalPrice = totalPrice;
            return this;
        }

        public UpgradeRobotVoucherIssuedBuilder withUpgradeLevelCount(int upgradeLevelCount) {
            this.upgradeLevelCount = upgradeLevelCount;
            return this;
        }

        public UpgradeRobotVoucherIssuedBuilder withUpgradeType(String upgradeType) {
            this.upgradeType = upgradeType;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            UpgradeRobotVoucherPayload payload = new UpgradeRobotVoucherPayload(voucherId, gameId, playerId, totalPrice, upgradeLevelCount, upgradeType);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + gameId.toString() + "." + playerId.toString())
                    .createdAt(System.currentTimeMillis())
                    .build();

            return buildRecord(TOPIC_NAME, gameId.toString() + "." + playerId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing UpgradeRobotVoucherIssued ProducerRecords.
     * @return a new UpgradeRobotVoucherIssuedBuilder
     */
    public UpgradeRobotVoucherIssuedBuilder builder() {
        return new UpgradeRobotVoucherIssuedBuilder();
    }
}
