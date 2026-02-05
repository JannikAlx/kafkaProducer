package de.microservicedungeon.mock.eventing.events.map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.microservicedungeon.mock.eventing.AbstractEventFactory;
import de.microservicedungeon.mock.eventing.commonheaders.CommonHeaders;
import de.microservicedungeon.mock.model.map.GameMap;
import de.microservicedungeon.mock.model.map.StarSystem;
import de.microservicedungeon.mock.state.SequenceIdManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * Event(factory) used to publish map initialization with all planets and resources.
 */
@Component
public class MapInitializedEvent extends AbstractEventFactory<MapInitializedEvent.MapInitializedPayload> {

    private static final String TOPIC_NAME = "bl.map.events.v1";
    private static final String AGGREGATE_NAME = "game";
    private static final String SCHEMA = "map-initialized";
    private static final int SCHEMA_VERSION = 1;

    public MapInitializedEvent(ObjectMapper objectMapper, SequenceIdManager sequenceIdManager) {
        super(objectMapper, sequenceIdManager);
    }

    public record MapInitializedPayload(
            @JsonProperty("gameId")
            @NotNull
            UUID gameId,
            @JsonProperty("mapData")
            @NotNull
            @Valid
            MapDataPayload mapData
    ) {}

    public record MapDataPayload(
            @JsonProperty("id")
            @NotNull
            UUID id,
            @JsonProperty("name")
            @NotNull
            String name,
            @JsonProperty("description")
            String description,
            @JsonProperty("topRight")
            @NotNull
            @Valid
            MapInitializedEvent.StarSystemPayload.CoordinatePayload topRight,
            @JsonProperty("planets")
            @NotNull
            @Valid
            List<@Valid StarSystemPayload> planets
    ) {}

    public record StarSystemPayload(
            @JsonProperty("coordinate")
            CoordinatePayload coordinate,
            @JsonProperty("gravity")
            int gravity,
            @JsonProperty("spaceStation")
            SpaceStation spaceStation,
            @JsonProperty("mine")
            Mine mine,
            @JsonProperty("blackHole")
            boolean blackHole,
            @JsonProperty("voidSystem")
            boolean voidSystem,
            @JsonProperty("type")
            String type
    ) {
        public record CoordinatePayload(
                @JsonProperty("x") int x,
                @JsonProperty("y") int y
        ) {}

        public record SpaceStation(
                @JsonProperty("allowsRobotSpawns") boolean allowsRobotSpawns
        ) {}

        public record Mine(
                @JsonProperty("id") String id,
                @JsonProperty("type") String type
        ) {}
    }

    /**
     * Maps a GameMap to MapInitializedPayload
     */
    private MapInitializedPayload mapToPayload(GameMap gameMap, UUID gameId) {
        MapDataPayload mapData = new MapDataPayload(
                gameMap.getId(),
                gameMap.getName(),
                gameMap.getDescription(),
                new StarSystemPayload.CoordinatePayload(gameMap.getTopRight().getX(), gameMap.getTopRight().getY()),
                gameMap.getAllStarSystems().stream()
                        .map(this::mapStarSystemToPayload)
                        .toList()
        );

        return new MapInitializedPayload(gameId, mapData);
    }

    /**
     * Maps a StarSystem to StarSystemPayload
     */
    private StarSystemPayload mapStarSystemToPayload(StarSystem starSystem) {
        return new StarSystemPayload(
                new StarSystemPayload.CoordinatePayload(starSystem.coordinate().getX(), starSystem.coordinate().getY()),
                starSystem.gravity() != null ? starSystem.gravity() : 0,
                starSystem.spaceStation() != null ?
                        new StarSystemPayload.SpaceStation(starSystem.spaceStation().allowsRobotSpawns()) : null,
                starSystem.mine() != null ?
                        new StarSystemPayload.Mine(starSystem.mine().id().toString(), starSystem.mine().type().name()) : null,
                starSystem.blackHole() != null ? starSystem.blackHole() : false,
                starSystem.voidSystem() != null ? starSystem.voidSystem() : false,
                starSystem.getType()
        );
    }

    /**
     * Fluent builder for creating MapInitializedEvent ProducerRecords.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public class MapInitializedEventBuilder {
        private UUID gameId;
        private GameMap gameMap;
        private MapDataPayload mapData;

        public MapInitializedEventBuilder forGame(UUID gameId) {
            this.gameId = gameId;
            return this;
        }

        public MapInitializedEventBuilder withGameMap(GameMap gameMap) {
            this.gameMap = gameMap;
            return this;
        }

        public MapInitializedEventBuilder withMapData(MapDataPayload mapData) {
            this.mapData = mapData;
            return this;
        }

        public ProducerRecord<String, byte[]> build() {
            MapInitializedPayload payload;
            if (gameMap != null) {
                payload = mapToPayload(gameMap, gameId);
            } else if (mapData != null) {
                payload = new MapInitializedPayload(gameId, mapData);
            } else {
                throw new IllegalStateException("Either gameMap or mapData must be provided");
            }

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
     * Creates a new builder instance for constructing MapInitializedEvent ProducerRecords.
     * @return a new MapInitializedEventBuilder
     */
    public MapInitializedEventBuilder builder() {
        return new MapInitializedEventBuilder();
    }

    /**
     * Builds a new {@code MapInitializedEvent} ProducerRecord from a given GameMap.
     * @param gameMap the game map to convert
     * @param gameId the game ID for the event
     * @return a ProducerRecord ready to be published to Kafka
     */
    public ProducerRecord<String, byte[]> build(GameMap gameMap, UUID gameId) {
        return builder().forGame(gameId).withGameMap(gameMap).build();
    }
}
