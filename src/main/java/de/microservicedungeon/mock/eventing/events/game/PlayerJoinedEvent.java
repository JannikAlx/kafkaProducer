package de.microservicedungeon.mock.eventing.events.game;

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
 * Event(factory) for new player participants joining a game.
 */
@Component
public class PlayerJoinedEvent extends AbstractEventFactory<PlayerJoinedEvent.PlayerJoinedPayload> {

    private static final String TOPIC_NAME = "bl.game.events.v1";
    private static final String AGGREGATE_NAME = "game";
    private static final String SCHEMA = "game.player-joined";
    private static final int SCHEMA_VERSION = 1;

    public PlayerJoinedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record PlayerJoinedPayload(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("participant")
            @Valid
            @NotNull
            ParticipantPayload participant
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
     * Fluent builder for creating PlayerJoinedEvent payloads.
     */
    public static class PlayerJoinedEventBuilder {
        private UUID gameId;
        private ParticipantPayload participant;

        public static PlayerJoinedEventBuilder forGame(UUID gameId) {
            PlayerJoinedEventBuilder builder = new PlayerJoinedEventBuilder();
            builder.gameId = gameId;
            return builder;
        }

        public PlayerJoinedEventBuilder withPlayer(UUID playerId) {
            this.participant = new ParticipantPayload(UUID.randomUUID(), playerId, System.currentTimeMillis());
            return this;
        }

        public PlayerJoinedEventBuilder withParticipant(ParticipantPayload participant) {
            this.participant = participant;
            return this;
        }

        public PlayerJoinedPayload build() {
            return new PlayerJoinedPayload(gameId, participant);
        }
    }

    /**
     * Builds a new {@code PlayerJoinedEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(PlayerJoinedPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.gameId().toString())
                .createdAt(System.currentTimeMillis()).build();

        return buildRecord(TOPIC_NAME, payload.gameId().toString(), payload, headers);
    }
}
