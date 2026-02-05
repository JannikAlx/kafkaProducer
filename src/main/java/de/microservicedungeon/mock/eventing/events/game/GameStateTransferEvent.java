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
     * Builds a new {@code GameStateTransferEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(GameStateTransferPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.gameId().toString())
                .createdAt(System.currentTimeMillis()).build();

        return buildRecord(TOPIC_NAME, payload.gameId().toString(), payload, headers);
    }
}
