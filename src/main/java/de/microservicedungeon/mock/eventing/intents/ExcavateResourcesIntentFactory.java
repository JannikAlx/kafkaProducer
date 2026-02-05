package de.microservicedungeon.mock.eventing.intents;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ExcavateResourcesIntentFactory extends AbstractEventFactory<ExcavateResourcesIntentFactory.ExcavateResourcesIntent> {

    private static final String TOPIC_NAME = "robot.intents.v1";
    private static final String EVENT_TYPE = "excavate-resources";
    private static final int SCHEMA_VERSION = 1;

    public ExcavateResourcesIntentFactory(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record ExcavateResourcesIntent(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("robotId")
            @NotNull
            UUID robotId
    ) {}

    public ProducerRecord<String, byte[]> build(UUID playerId, UUID gameId, UUID robotId) {
        CommonHeaders headers = CommonHeaders
                .builder()
                .eventType(EVENT_TYPE)
                .eventTypeVersion(SCHEMA_VERSION)
                .createdAt(System.currentTimeMillis())
                .build();
        String key = playerId.toString();
        ExcavateResourcesIntent payload = new ExcavateResourcesIntent(gameId, robotId);

        return buildRecord(TOPIC_NAME, key, payload, headers);
    }
}
