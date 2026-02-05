package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) to indicate that a new bank account has been opened for a player in a game. Includes identifying information and the current balance.
 */
@Component
public class BankAccountOpenedEvent extends AbstractEventFactory<BankAccountOpenedEvent.BankAccountOpenedPayload> {

    private static final String TOPIC_NAME = "bl.bank-account.events.v1";
    private static final String AGGREGATE_NAME = "bank-account";
    private static final String SCHEMA = "bank-account-opened";
    private static final int SCHEMA_VERSION = 1;

    public BankAccountOpenedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    @Builder(builderMethodName = "builder", setterPrefix = "with")
    public record BankAccountOpenedPayload(
            @JsonProperty("bankAccountId")
            @NotNull
            UUID bankAccountId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("balance")
            @NotNull
            int currentCredits
    ){}



    /**
     * Builds a new {@code BankAccountOpenedEvent} from a given dto.
     * @return a new {@code BankAccountOpenedEvent}
     */
    public ProducerRecord<String, byte[]> build(BankAccountOpenedPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.bankAccountId().toString())
                .createdAt(System.currentTimeMillis()).build();

        return buildRecord(TOPIC_NAME, payload.bankAccountId().toString(), payload, headers);
    }
}
