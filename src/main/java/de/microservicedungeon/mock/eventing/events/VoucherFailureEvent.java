package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

public class VoucherFailureEvent extends AbstractPublishableEvent<VoucherFailureEvent.VoucherFailurePayload> {
    private static final String TOPIC_NAME = "trading-service.dlt";
    private static final String AGGREGATE_NAME = "voucher";
    private static final String SCHEMA = "voucher-errors.v1";
    private static final int SCHEMA_VERSION = 1;

    protected VoucherFailureEvent(VoucherFailurePayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.playerId(), payload, jsonSerializationStrategy, sequenceIdManager);
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

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code VoucherFailureEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(VoucherFailurePayload payload) {
            VoucherFailureEvent event = new VoucherFailureEvent(payload, jsonSerializer, sequenceIdManager);
            return event.toProducerRecord();
        }
    }

}
