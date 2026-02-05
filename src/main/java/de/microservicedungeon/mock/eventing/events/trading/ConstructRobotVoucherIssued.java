package de.microservicedungeon.mock.eventing.events.trading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
     * Fluent builder for creating ConstructRobotVoucherIssued ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class ConstructRobotVoucherIssuedBuilder {
        private UUID voucherId;
        private UUID gameId;
        private UUID playerId;
        private int totalPrice;
        private int numberOfRobots;

        public ConstructRobotVoucherIssuedBuilder forVoucher(UUID voucherId) {
            this.voucherId = voucherId;
            return this;
        }

        public ConstructRobotVoucherIssuedBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public ConstructRobotVoucherIssuedBuilder forPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public ConstructRobotVoucherIssuedBuilder withTotalPrice(int totalPrice) {
            this.totalPrice = totalPrice;
            return this;
        }

        public ConstructRobotVoucherIssuedBuilder withNumberOfRobots(int numberOfRobots) {
            this.numberOfRobots = numberOfRobots;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            ConstructRobotVoucherPayload payload = new ConstructRobotVoucherPayload(voucherId, gameId, playerId, totalPrice, numberOfRobots);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + gameId.toString() + "." + playerId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, gameId + "." + playerId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing ConstructRobotVoucherIssued ProducerRecords.
     * @return a new ConstructRobotVoucherIssuedBuilder
     */
    public ConstructRobotVoucherIssuedBuilder builder() {
        return new ConstructRobotVoucherIssuedBuilder();
    }
}
