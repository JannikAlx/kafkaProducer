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
     * Fluent builder for creating BankAccountECSTEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class BankAccountECSTEventBuilder {
        private UUID bankAccountId;
        private UUID playerId;
        private UUID gameId;
        private int currentCredits;

        public BankAccountECSTEventBuilder forBankAccount(UUID bankAccountId) {
            this.bankAccountId = bankAccountId;
            return this;
        }

        public BankAccountECSTEventBuilder withPlayer(UUID playerId) {
            this.playerId = playerId;
            return this;
        }

        public BankAccountECSTEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public BankAccountECSTEventBuilder withCurrentCredits(int currentCredits) {
            this.currentCredits = currentCredits;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            BankAccountECSTPayload payload = new BankAccountECSTPayload(bankAccountId, playerId, gameId, currentCredits);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + bankAccountId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, bankAccountId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing BankAccountECSTEvent ProducerRecords.
     * @return a new BankAccountECSTEventBuilder
     */
    public BankAccountECSTEventBuilder builder() {
        return new BankAccountECSTEventBuilder();
    }

}