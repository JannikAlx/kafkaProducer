package de.microservicedungeon.mock.eventing.intents;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class BuyConstructRobotVoucherIntentFactory extends AbstractEventFactory<BuyConstructRobotVoucherIntentFactory.BuyConstructRobotVoucherIntent> {

        private static final String TOPIC_NAME="voucher.intents.v1";
        private static final String EVENT_TYPE="voucher.buy-robot";
        private static final int SCHEMA_VERSION= 1;

        public BuyConstructRobotVoucherIntentFactory(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
            super(objectMapper, sequenceIdManager);
        }

        public record BuyConstructRobotVoucherIntent(
                @JsonProperty("gameId")
                @NotNull
                UUID gameId,
                @JsonProperty("numberOfRobots")
                @NotNull
                @Positive
                int numberOfRobots
        ) {}

        public ProducerRecord<String, byte[]> build(UUID playerId, UUID gameId, int numberOfRobots) {
            CommonHeaders headers = CommonHeaders
                    .builder()
                    .eventType(EVENT_TYPE)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .createdAt(System.currentTimeMillis())
                    .build();
            String key = playerId.toString();
            BuyConstructRobotVoucherIntent payload =
                    new BuyConstructRobotVoucherIntent(gameId, numberOfRobots);

            return buildRecord(TOPIC_NAME, key, payload, headers);
        }
}
