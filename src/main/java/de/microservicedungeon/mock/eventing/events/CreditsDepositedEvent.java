package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) indicates that credits were deposited into a players account. Contains information about how much and the resulting balance.
 */
@Component
public class CreditsDepositedEvent extends AbstractEventFactory<CreditsDepositedEvent.CreditsDepositedPayload> {

    private static final String TOPIC_NAME = "bl.bank-account.events.v1";
    private static final String AGGREGATE_NAME = "bank-account";
    private static final String SCHEMA = "credits-deposited";
    private static final int SCHEMA_VERSION = 1;

    public CreditsDepositedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record CreditsDepositedPayload(
            @JsonProperty("bankAccountId")
            @NotNull
            UUID bankAccountId,
            @JsonProperty("addition")
            @NotNull
            @Positive
            int addition,
            @JsonProperty("newBalance")
            @NotNull
            int newBalance
    ){}

    /**
     * Builds a new {@code CreditsDepositedEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(CreditsDepositedPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.bankAccountId().toString())
                .createdAt(System.currentTimeMillis()).build();

        return buildRecord(TOPIC_NAME, payload.bankAccountId().toString(), payload, headers);
    }
}
