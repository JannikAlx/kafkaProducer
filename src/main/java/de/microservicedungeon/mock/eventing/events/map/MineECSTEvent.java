package de.microservicedungeon.mock.eventing.events.map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.CommonHeaders;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
     * Fluent builder for creating MineECSTEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class MineECSTEventBuilder {
        private UUID mineId;
        private UUID gameId;
        private String resourceType;
        private Integer resourceAmount;

        public MineECSTEventBuilder forMine(UUID mineId) {
            this.mineId = mineId;
            return this;
        }

        public MineECSTEventBuilder inGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public MineECSTEventBuilder withResourceType(String resourceType) {
            this.resourceType = resourceType;
            return this;
        }

        public MineECSTEventBuilder withResourceAmount(Integer resourceAmount) {
            this.resourceAmount = resourceAmount;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            MineECSTPayload payload = new MineECSTPayload(mineId, gameId, resourceType, resourceAmount);

            CommonHeaders headers = CommonHeaders.builder()
                    .eventType(SCHEMA)
                    .eventTypeVersion(SCHEMA_VERSION)
                    .entity(AGGREGATE_NAME + "." + mineId.toString())
                    .createdAt(System.currentTimeMillis())
                    .build();

            return buildRecord(TOPIC_NAME, mineId.toString(), payload, headers);
        }
    }

    /**
     * Creates a new builder instance for constructing MineECSTEvent ProducerRecords.
     * @return a new MineECSTEventBuilder
     */
    public MineECSTEventBuilder builder() {
        return new MineECSTEventBuilder();
    }
}
