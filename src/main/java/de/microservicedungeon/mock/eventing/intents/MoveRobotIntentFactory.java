package de.microservicedungeon.mock.eventing.intents;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.model.map.Direction;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class MoveRobotIntentFactory extends AbstractEventFactory<MoveRobotIntentFactory.MoveRobotIntent> {

    private static final String TOPIC_NAME = "robot.intents.v1";
    private static final String EVENT_TYPE = "robot.move";
    private static final int SCHEMA_VERSION = 1;

    public MoveRobotIntentFactory(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record MoveRobotIntent(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("robotId")
            @NotNull
            UUID robotId,
            @JsonProperty("direction")
            @NotNull
            String direction
    ) {}

    public ProducerRecord<String, byte[]> build(UUID playerId, UUID gameId, UUID robotId, Direction direction) {
        CommonHeaders headers = CommonHeaders
                .builder()
                .eventType(EVENT_TYPE)
                .eventTypeVersion(SCHEMA_VERSION)
                .createdAt(System.currentTimeMillis())
                .build();
        String key = playerId.toString();
        MoveRobotIntent payload = new MoveRobotIntent(gameId, robotId, direction.name());

        return buildRecord(TOPIC_NAME, key, payload, headers);
    }
}
