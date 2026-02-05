package de.microservicedungeon.mock.eventing.events;

import de.microservicedungeon.mock.eventing.AbstractPublishableEvent;
import de.microservicedungeon.mock.eventing.JsonSerializationStrategy;
import de.microservicedungeon.mock.state.SequenceIdManager;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Keyed ECST event publishing the mine's current state.
 */
public class MineECSTEvent extends AbstractPublishableEvent<MineECSTEvent.MineECSTPayload> {

    private static final String TOPIC_NAME = "db.mine.ecst.v1";
    private static final String AGGREGATE_NAME = "mine";
    private static final String SCHEMA = "mine-ecst";
    private static final int SCHEMA_VERSION = 1;

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

    protected MineECSTEvent(MineECSTPayload payload, JsonSerializationStrategy jsonSerializationStrategy, SequenceIdManager sequenceIdManager) {
        super(TOPIC_NAME, AGGREGATE_NAME, SCHEMA, SCHEMA_VERSION, payload.mineId().toString(), payload, jsonSerializationStrategy, sequenceIdManager);
    }

    @Component
    @AllArgsConstructor
    public static class Factory {
        private JsonSerializationStrategy jsonSerializer;
        private SequenceIdManager sequenceIdManager;

        /**
         * Builds a new {@code MineECSTEvent} ProducerRecord from a given payload.
         * @param payload the event payload
         * @return a ProducerRecord ready to be published to Kafka
         */
        public ProducerRecord<String, byte[]> build(MineECSTPayload payload) {
            MineECSTEvent event = new MineECSTEvent(payload, jsonSerializer, sequenceIdManager);
            return event.toProducerRecord();
        }
    }
}
