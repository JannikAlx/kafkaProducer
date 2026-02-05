package de.microservicedungeon.mock.eventing.intents;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class BuyUpgradeRobotVoucherIntentFactory {

        private static final String TOPIC_NAME = "voucher.intents.v1";
        private static final String EVENT_TYPE = "voucher.upgrade-robot";
        private static final int SCHEMA_VERSION = 1;
        private final ObjectMapper objectMapper;

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

        public ProducerRecord<String, byte[]> build(UUID playerId, UUID gameId, UUID robotId, int upgradeLevelCount, String upgradeType) throws JsonProcessingException {
                CommonHeaders headers = CommonHeaders
                        .builder()
                        .eventType(EVENT_TYPE)
                        .eventTypeVersion(SCHEMA_VERSION)
                        .createdAt(System.currentTimeMillis())
                        .build();
                String key = playerId.toString();
                BuyUpgradeRobotVoucherIntent payload =
                        new BuyUpgradeRobotVoucherIntent(gameId, robotId, upgradeLevelCount, upgradeType);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC_NAME, key, objectMapper.writeValueAsBytes(payload));
                headers.toKafkaHeaders().forEach(
                        (header -> record.headers().add(header))
                );
                return record;
        }
}
