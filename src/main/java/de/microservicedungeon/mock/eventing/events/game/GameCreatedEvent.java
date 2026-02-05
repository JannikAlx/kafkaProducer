package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) for game creation.
 */
@Component
public class GameCreatedEvent extends AbstractEventFactory<GameCreatedEvent.GameCreatedPayload> {

    private static final String TOPIC_NAME = "bl.game.events.v1";
    private static final String AGGREGATE_NAME = "game";
    private static final String SCHEMA = "game.created";
    private static final int SCHEMA_VERSION = 1;

    public GameCreatedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record GameCreatedPayload(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("participantLimit")
            @NotNull
            Integer participantLimit,
            @JsonProperty("state")
            @NotNull
            String state
    ) {}

    /**
     * Fluent builder for creating GameCreatedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class GameCreatedEventBuilder {
        private UUID gameId;
        private Integer participantLimit;
        private String state;

        public GameCreatedEventBuilder forGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public GameCreatedEventBuilder withParticipantLimit(int participantLimit) {
            this.participantLimit = participantLimit;
            return this;
        }

        public GameCreatedEventBuilder withState(String state) {
            this.state = state;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            GameCreatedPayload payload = new GameCreatedPayload(gameId, participantLimit, state);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + gameId.toString())
                    .createdAt(System.currentTimeMillis()).build();

            return buildRecord(TOPIC_NAME, gameId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing GameCreatedEvent ProducerRecords.
     * @return a new GameCreatedEventBuilder
     */
    public GameCreatedEventBuilder builder() {
        return new GameCreatedEventBuilder();
    }
}
