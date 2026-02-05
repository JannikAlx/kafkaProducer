package de.microservicedungeon.mock.eventing.events.trading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
     * Fluent builder for creating CreditsDepositedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class CreditsDepositedEventBuilder {
        private UUID bankAccountId;
        private int addition;
        private int newBalance;

        public CreditsDepositedEventBuilder forBankAccount(UUID bankAccountId) {
            this.bankAccountId = bankAccountId;
            return this;
        }

        public CreditsDepositedEventBuilder withAddition(int addition) {
            this.addition = addition;
            return this;
        }

        public CreditsDepositedEventBuilder withNewBalance(int newBalance) {
            this.newBalance = newBalance;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            CreditsDepositedPayload payload = new CreditsDepositedPayload(bankAccountId, addition, newBalance);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + bankAccountId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, bankAccountId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing CreditsDepositedEvent ProducerRecords.
     * @return a new CreditsDepositedEventBuilder
     */
    public CreditsDepositedEventBuilder builder() {
        return new CreditsDepositedEventBuilder();
    }
}
