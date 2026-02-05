package de.microservicedungeon.mock.eventing.events.game;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
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
     * Builds a new GameStateChangedEvent ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(GameStateChangedPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.gameId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.gameId().toString(), payload, headers);
    }
}
