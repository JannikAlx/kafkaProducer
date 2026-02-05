package de.microservicedungeon.mock.eventing.events.trading;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Indicates that funds have been withdrawn from a players account. Contains information about how much and the new balance.
 */
@Component
public class CreditsWithdrawnEvent extends AbstractEventFactory<CreditsWithdrawnEvent.CreditsWithdrawnPayload> {
    private static final String TOPIC_NAME = "bl.bank-account.events.v1";
    private static final String AGGREGATE_NAME = "bank-account";
    private static final String SCHEMA = "credits-withdrawn";
    private static final int SCHEMA_VERSION = 1;

    public CreditsWithdrawnEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record CreditsWithdrawnPayload(
            @JsonProperty("bankAccountId")
            @NotNull
            UUID bankAccountId,
            @JsonProperty("deduction")
            @NotNull
            @Positive
            int deduction,
            @JsonProperty("newBalance")
            @NotNull
            int newBalance
    ) {}

    /**
     * Fluent builder for creating CreditsWithdrawnEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class CreditsWithdrawnEventBuilder {
        private UUID bankAccountId;
        private int deduction;
        private int newBalance;

        public CreditsWithdrawnEventBuilder forBankAccount(UUID bankAccountId) {
            this.bankAccountId = bankAccountId;
            return this;
        }

        public CreditsWithdrawnEventBuilder withDeduction(int deduction) {
            this.deduction = deduction;
            return this;
        }

        public CreditsWithdrawnEventBuilder withNewBalance(int newBalance) {
            this.newBalance = newBalance;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            CreditsWithdrawnPayload payload = new CreditsWithdrawnPayload(bankAccountId, deduction, newBalance);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + bankAccountId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, bankAccountId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing CreditsWithdrawnEvent ProducerRecords.
     * @return a new CreditsWithdrawnEventBuilder
     */
    public CreditsWithdrawnEventBuilder builder() {
        return new CreditsWithdrawnEventBuilder();
    }
}
