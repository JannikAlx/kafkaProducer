package de.microservicedungeon.mock.eventing.events.game;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Event(factory) for game state transfer (ECST).
 */
@Component
public class GameStateTransferEvent extends AbstractEventFactory<GameStateTransferEvent.GameStateTransferPayload> {

    private static final String TOPIC_NAME = "db.game.ecst.v1";
    private static final String AGGREGATE_NAME = "game";
    private static final String SCHEMA = "game.ecst";
    private static final int SCHEMA_VERSION = 1;

    public GameStateTransferEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record GameStateTransferPayload(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("participants")
            @Valid
            List<@Valid ParticipantPayload> participants,
            @JsonProperty("participantLimit")
            @NotNull
            Integer participantLimit,
            @JsonProperty("state")
            @NotNull
            String state
    ) {}

    public record ParticipantPayload(
            @JsonProperty("participantId")
            @NotNull
            UUID participantId,
            @JsonProperty("playerId")
            @NotNull
            UUID playerId,
            @JsonProperty("joinedAt")
            @NotNull
            long joinedAt
    ) {}

    /**
     * Fluent builder for creating GameStateTransferEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class GameStateTransferEventBuilder {
        private UUID gameId;
        private List<ParticipantPayload> participants;
        private Integer participantLimit;
        private String state;

        public GameStateTransferEventBuilder forGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public GameStateTransferEventBuilder withParticipants(List<ParticipantPayload> participants) {
            this.participants = participants;
            return this;
        }

        public GameStateTransferEventBuilder withParticipantLimit(Integer participantLimit) {
            this.participantLimit = participantLimit;
            return this;
        }

        public GameStateTransferEventBuilder withState(String state) {
            this.state = state;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            GameStateTransferPayload payload = new GameStateTransferPayload(gameId, participants, participantLimit, state);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + gameId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, gameId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing GameStateTransferEvent ProducerRecords.
     * @return a new GameStateTransferEventBuilder
     */
    public GameStateTransferEventBuilder builder() {
        return new GameStateTransferEventBuilder();
    }
}
