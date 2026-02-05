package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) used to publish the whole state of the bank account via event carried state transfer. Should include all public information.
 */
@Component
public class BankAccountECSTEvent extends AbstractEventFactory<BankAccountECSTEvent.BankAccountECSTPayload> {
    private static final String TOPIC_NAME = "db.bank-account.ecst.v1";
    private static final String AGGREGATE_NAME = "bank-account";
    private static final String SCHEMA = "bank-account-ecst";
    private static final int SCHEMA_VERSION = 1;

    public BankAccountECSTEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record BankAccountECSTPayload(@JsonProperty("bankAccountId") @NotNull UUID bankAccountId,
                                         @JsonProperty("playerId") @NotNull UUID playerId,
                                         @JsonProperty("gameId") @NotNull UUID gameId,
                                         @JsonProperty("currentCredits") @NotNull int currentCredits) {
    }

    /**
     * Builds a new {@code BankAccountECSTEvent} ProducerRecord from a given payload.
     *
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(BankAccountECSTPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.bankAccountId().toString())
                .createdAt(System.currentTimeMillis()).build();

        return buildRecord(TOPIC_NAME, payload.bankAccountId().toString(), payload, headers);
    }

}