package de.microservicedungeon.mock.eventing.events.game;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) for game state changes.
 */
@Component
public class GameStateChangedEvent extends AbstractEventFactory<GameStateChangedEvent.GameStateChangedPayload> {

    private static final String TOPIC_NAME = "bl.game.events.v1";
    private static final String AGGREGATE_NAME = "game";
    private static final String SCHEMA = "game.state-changed";
    private static final int SCHEMA_VERSION = 1;

    public GameStateChangedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record GameStateChangedPayload(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("state")
            @NotNull
            String state
    ) {}

    /**
     * Fluent builder for creating GameStateChangedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class GameStateChangedEventBuilder {
        private UUID gameId;
        private String state;

        public GameStateChangedEventBuilder forGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public GameStateChangedEventBuilder withState(String state) {
            this.state = state;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            GameStateChangedPayload payload = new GameStateChangedPayload(gameId, state);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + gameId.toString())
                    .createdAt(System.currentTimeMillis())
                    .build();

            return buildRecord(TOPIC_NAME, gameId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing GameStateChangedEvent ProducerRecords.
     * @return a new GameStateChangedEventBuilder
     */
    public GameStateChangedEventBuilder builder() {
        return new GameStateChangedEventBuilder();
    }
}
