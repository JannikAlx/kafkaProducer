package de.microservicedungeon.mock.eventing.events.trading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

/**
 * Event(factory) for voucher failure notifications.
 */
@Component
public class VoucherFailureEvent extends AbstractEventFactory<VoucherFailureEvent.VoucherFailurePayload> {
    private static final String TOPIC_NAME = "trading-service.dlt";
    private static final String AGGREGATE_NAME = "voucher";
    private static final String SCHEMA = "voucher-errors.v1";
    private static final int SCHEMA_VERSION = 1;

    public VoucherFailureEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record VoucherFailurePayload(
            @JsonProperty("gameId")
            @NotNull
            String gameId,
            @JsonProperty("playerId")
            @NotNull
            String playerId,
            @JsonProperty("errorCode")
            @NotNull
            int errorCode,
            @JsonProperty("exceptionName")
            @NotNull
            String exceptionName,
            @JsonProperty("errorMessage")
            @NotNull
            String errorMessage
    ) {}

    /**
     * Fluent builder for creating VoucherFailureEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class VoucherFailureEventBuilder {
        private String gameId;
        private String playerId;
        private int errorCode;
        private String exceptionName;
        private String errorMessage;

        public VoucherFailureEventBuilder inGame(String gameId) {
            this.gameId = gameId;
            return this;
        }

        public VoucherFailureEventBuilder forPlayer(String playerId) {
            this.playerId = playerId;
            return this;
        }

        public VoucherFailureEventBuilder withErrorCode(int errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public VoucherFailureEventBuilder withException(String exceptionName) {
            this.exceptionName = exceptionName;
            return this;
        }

        public VoucherFailureEventBuilder withErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            VoucherFailurePayload payload = new VoucherFailurePayload(gameId, playerId, errorCode, exceptionName, errorMessage);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + playerId)
                    .createdAt(System.currentTimeMillis())
                    .build();

            return buildRecord(TOPIC_NAME, playerId, payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing VoucherFailureEvent ProducerRecords.
     * @return a new VoucherFailureEventBuilder
     */
    public VoucherFailureEventBuilder builder() {
        return new VoucherFailureEventBuilder();
    }

}
