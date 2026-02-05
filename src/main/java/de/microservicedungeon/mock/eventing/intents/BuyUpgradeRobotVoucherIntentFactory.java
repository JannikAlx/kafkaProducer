package de.microservicedungeon.mock.eventing.intents;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class BuyUpgradeRobotVoucherIntentFactory extends AbstractEventFactory<BuyUpgradeRobotVoucherIntentFactory.BuyUpgradeRobotVoucherIntent> {

        private static final String TOPIC_NAME = "voucher.intents.v1";
        private static final String EVENT_TYPE = "voucher.upgrade-robot";
        private static final int SCHEMA_VERSION = 1;

        public BuyUpgradeRobotVoucherIntentFactory(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
            super(objectMapper, sequenceIdManager);
        }

        public record BuyUpgradeRobotVoucherIntent(
                @JsonProperty("gameId")
                @NotNull
                UUID gameId,
                @JsonProperty("robotId")
                @NotNull
                UUID robotId,
                @JsonProperty("upgradeLevelCount")
                @NotNull
                @Positive
                int upgradeLevelCount,
                @JsonProperty("upgradeType")
                @NotNull
                @NotBlank
                String upgradeType
        ) {}

        public ProducerRecord<String, byte[]> build(UUID playerId, UUID gameId, UUID robotId, int upgradeLevelCount, String upgradeType) {
                CommonHeaders headers = CommonHeaders
                        .builder()
                        .eventType(EVENT_TYPE)
                        .eventTypeVersion(SCHEMA_VERSION)
                        .createdAt(System.currentTimeMillis())
                        .build();
                String key = playerId.toString();
                BuyUpgradeRobotVoucherIntent payload =
                        new BuyUpgradeRobotVoucherIntent(gameId, robotId, upgradeLevelCount, upgradeType);

                return buildRecord(TOPIC_NAME, key, payload, headers);
        }
}
