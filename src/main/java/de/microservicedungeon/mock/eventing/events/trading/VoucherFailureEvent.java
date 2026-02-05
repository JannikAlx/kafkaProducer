package de.microservicedungeon.mock.eventing.events.trading;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
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
     * Builds a new {@code VoucherFailureEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(VoucherFailurePayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.playerId())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.playerId(), payload, headers);
    }

}
