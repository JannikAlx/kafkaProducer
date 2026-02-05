package de.microservicedungeon.mock.eventing.events.map;

import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Event(factory) for mine ECST (Entity Current State Transfer).
 */
@Component
public class MineECSTEvent extends AbstractEventFactory<MineECSTEvent.MineECSTPayload> {

    private static final String TOPIC_NAME = "db.mine.ecst.v1";
    private static final String AGGREGATE_NAME = "mine";
    private static final String SCHEMA = "mine-ecst";
    private static final int SCHEMA_VERSION = 1;

    public MineECSTEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record MineECSTPayload(
            @JsonProperty("mineId")
            @NotNull
            UUID mineId,
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("resourceType")
            @NotNull
            String resourceType,
            @JsonProperty("resourceAmount")
            @NotNull
            @Min(0)
            Integer resourceAmount
    ) {}

    /**
     * Builds a new {@code MineECSTEvent} ProducerRecord from a given payload.
     * @param payload the event payload
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(MineECSTPayload payload) {
        CommonHeaders headers = CommonHeaders.builder()
                .eventType(SCHEMA)
                .eventTypeVersion(SCHEMA_VERSION)
                .entity(AGGREGATE_NAME + "." + payload.mineId().toString())
                .createdAt(System.currentTimeMillis())
                .build();

        return buildRecord(TOPIC_NAME, payload.mineId().toString(), payload, headers);
    }
}
