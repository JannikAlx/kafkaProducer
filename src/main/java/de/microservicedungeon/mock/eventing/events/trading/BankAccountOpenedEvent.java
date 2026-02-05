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
     * Fluent builder for creating BankAccountOpenedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class BankAccountOpenedEventBuilder {
        private UUID bankAccountId;
        private UUID playerId;
        private UUID gameId;
        private int currentCredits;

        public BankAccountOpenedEventBuilder forBankAccount(UUID bankAccountId) {
            this.bankAccountId = bankAccountId;
            return this;
        }

        public BankAccountOpenedEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public BankAccountOpenedEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public BankAccountOpenedEventBuilder withBalance(int currentCredits) {
            this.currentCredits = currentCredits;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            BankAccountOpenedPayload payload = new BankAccountOpenedPayload(bankAccountId, playerId, gameId, currentCredits);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + bankAccountId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, bankAccountId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing BankAccountOpenedEvent ProducerRecords.
     * @return a new BankAccountOpenedEventBuilder
     */
    public BankAccountOpenedEventBuilder builder() {
        return new BankAccountOpenedEventBuilder();
    }
}
