package de.microservicedungeon.mock.eventing.intents;

import de.microservicedungeon.mock.eventing.CommonHeaders;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class SellResourcesIntentFactory {

    private static final String TOPIC_NAME = "robot.intents.v1";
    private static final String EVENT_TYPE = "sell-resources";
    private static final int SCHEMA_VERSION = 1;
    private final ObjectMapper objectMapper;

    public record SellResourcesIntent(
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
        SellResourcesIntent payload = new SellResourcesIntent(gameId, robotId);
        ProducerRecord<String, byte[]> record;
        try {
            record = new ProducerRecord<>(TOPIC_NAME, key, objectMapper.writeValueAsBytes(payload));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        headers.toKafkaHeaders().forEach(
                (header -> record.headers().add(header))
        );
        return record;
    }
}
