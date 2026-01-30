package com.example.kafka.service;

import com.example.kafka.model.*;
import com.example.kafka.state.GameState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Service responsible for initializing the game state and publishing
 * initialization events.
 * Creates the game, players, bank accounts, and map structure.
 */
@Service
public class GameInitializer {

    private static final Logger logger = LoggerFactory.getLogger(GameInitializer.class);

    private final GameState gameState;
    private final EventBuilder eventBuilder;
    private final KafkaEventPublisher kafkaPublisher;

    @Value("${game.simulation.player-count}")
    private int playerCount;

    @Value("${game.simulation.map-width}")
    private int mapWidth;

    @Value("${game.simulation.map-height}")
    private int mapHeight;

    public GameInitializer(GameState gameState, EventBuilder eventBuilder, KafkaEventPublisher kafkaPublisher) {
        this.gameState = gameState;
        this.eventBuilder = eventBuilder;
        this.kafkaPublisher = kafkaPublisher;
    }

    /**
     * Initialize the game and publish all initialization events.
     */
    public void initializeGame() {
        logger.info("Initializing game with {} players and {}x{} map", playerCount, mapWidth, mapHeight);

        // Initialize game state
        gameState.initialize(playerCount);

        // Publish game-created event
        publishGameCreatedEvent();

        // Publish player-joined events for each player
        for (UUID playerId : gameState.getPlayerIds()) {
            publishPlayerJoinedEvent(playerId);
        }

        // Publish bank-account-opened events for each player
        for (UUID playerId : gameState.getPlayerIds()) {
            publishBankAccountOpenedEvent(playerId);
        }

        // Initialize and publish map
        initializeAndPublishMap();

        logger.info("Game initialization complete. GameId: {}", gameState.getGameId());
    }

    /**
     * Publish game-created event.
     */
    private void publishGameCreatedEvent() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("gameId", gameState.getGameId().toString());
        payload.put("participantLimit", playerCount);
        payload.put("state", "CREATED");

        String event = eventBuilder.buildEvent(
                "bl.game.events.v1",
                "game.created",
                "game." + gameState.getGameId(),
                payload,
                gameState.getGameId().toString());

        kafkaPublisher.publish(event);
    }

    /**
     * Publish player-joined event for a specific player.
     */
    private void publishPlayerJoinedEvent(UUID playerId) {
        // Create participant object
        Map<String, Object> participant = new HashMap<>();
        participant.put("participantId", UUID.randomUUID().toString());
        participant.put("playerId", playerId.toString());
        participant.put("joinedAt",
                java.time.ZonedDateTime.now().format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        Map<String, Object> payload = new HashMap<>();
        payload.put("gameId", gameState.getGameId().toString());
        payload.put("participant", participant);

        String event = eventBuilder.buildEvent(
                "bl.game.events.v1",
                "game.player-joined",
                "game." + gameState.getGameId(),
                payload,
                gameState.getGameId().toString());

        kafkaPublisher.publish(event);
    }

    /**
     * Publish bank-account-opened event for a specific player.
     */
    private void publishBankAccountOpenedEvent(UUID playerId) {
        UUID bankAccountId = gameState.getBankAccountId(playerId);

        Map<String, Object> balance = new HashMap<>();
        balance.put("currentCredits", 1000); // Starting balance

        Map<String, Object> payload = new HashMap<>();
        payload.put("bankAccountId", bankAccountId.toString());
        payload.put("playerId", playerId.toString());
        payload.put("gameId", gameState.getGameId().toString());
        payload.put("balance", balance);

        String event = eventBuilder.buildEvent(
                "bl.bank-account.events.v1",
                "bank-account-opened",
                "bank-account." + bankAccountId,
                payload,
                bankAccountId.toString());

        kafkaPublisher.publish(event);
    }

    /**
     * Initialize the map and publish map-initialized event.
     */
    private void initializeAndPublishMap() {
        gameState.initializeMap(mapWidth, mapHeight);

        Random random = new Random();
        List<Map<String, Object>> tiles = new ArrayList<>();

        // Create tiles with a mix of planets, voids, and black holes
        for (int y = 0; y < mapHeight; y++) {
            for (int x = 0; x < mapWidth; x++) {
                Position pos = new Position(x, y);
                MapTile tile = createRandomTile(pos, random);
                gameState.setTile(x, y, tile);

                // Add tile to list for event
                tiles.add(createTileMap(tile));
            }
        }

        // Publish map-initialized event
        Map<String, Object> mapSize = new HashMap<>();
        mapSize.put("width", mapWidth);
        mapSize.put("height", mapHeight);

        Map<String, Object> payload = new HashMap<>();
        payload.put("gameId", gameState.getGameId().toString());
        payload.put("mapSize", mapSize);
        payload.put("planetCount", (int) tiles.stream().filter(t -> "planet".equals(t.get("type"))).count());

        String event = eventBuilder.buildEvent(
                "bl.map.events.v1",
                "map-initialized",
                "map." + gameState.getGameId(),
                payload,
                gameState.getGameId().toString());

        kafkaPublisher.publish(event);

        logger.info("Map initialized with {} tiles", tiles.size());
    }

    /**
     * Create a random tile with appropriate distribution.
     * ~60% planets, ~30% void, ~10% black holes
     */
    private MapTile createRandomTile(Position position, Random random) {
        double roll = random.nextDouble();

        if (roll < 0.60) {
            // Planet tile
            boolean hasSpaceStation = random.nextDouble() < 0.1; // 10% of planets have space stations
            boolean hasMines = random.nextDouble() < 0.7; // 70% of planets have mines

            MapTile.Builder builder = new MapTile.Builder(TileType.PLANET, position)
                    .gravity(random.nextInt(3) + 1); // Gravity 1-3

            if (hasSpaceStation) {
                builder.allowsRobotSpawns(true);
            }

            if (hasMines) {
                // Create 1-3 mines on this planet
                int mineCount = random.nextInt(3) + 1;
                List<UUID> mineIds = new ArrayList<>();

                for (int i = 0; i < mineCount; i++) {
                    UUID mineId = UUID.randomUUID();
                    mineIds.add(mineId);

                    // Initialize mine with random resources
                    int resourceAmount = random.nextInt(500) + 100; // 100-600 resources
                    gameState.getMineResources().put(mineId, resourceAmount);

                    // Publish mine ECST event
                    publishMineEcstEvent(mineId, resourceAmount);
                }

                builder.mineIds(mineIds);
            }

            return builder.build();

        } else if (roll < 0.90) {
            // Void tile
            return new MapTile.Builder(TileType.VOID, position).build();
        } else {
            // Black hole tile
            return new MapTile.Builder(TileType.BLACK_HOLE, position).build();
        }
    }

    /**
     * Publish mine ECST event.
     */
    private void publishMineEcstEvent(UUID mineId, int resourceAmount) {
        ResourceType resourceType = ResourceType.values()[new Random().nextInt(ResourceType.values().length)];

        Map<String, Object> payload = new HashMap<>();
        payload.put("mineId", mineId.toString());
        payload.put("gameId", gameState.getGameId().toString());
        payload.put("resourceType", resourceType.name());
        payload.put("resourceAmount", resourceAmount);

        String event = eventBuilder.buildEvent(
                "db.mine.ecst.v1",
                "mine-ecst",
                "mine." + mineId,
                payload,
                mineId.toString());

        kafkaPublisher.publish(event);
    }

    /**
     * Convert MapTile to a map structure for JSON serialization.
     */
    private Map<String, Object> createTileMap(MapTile tile) {
        Map<String, Object> tileMap = new HashMap<>();
        tileMap.put("type", tile.getType().name().toLowerCase());

        Map<String, Object> position = new HashMap<>();
        position.put("x", tile.getPosition().getX());
        position.put("y", tile.getPosition().getY());
        tileMap.put("position", position);

        if (tile.isPlanet()) {
            if (tile.getGravity() != null) {
                tileMap.put("gravity", tile.getGravity());
            }
            if (Boolean.TRUE.equals(tile.getAllowsRobotSpawns())) {
                Map<String, Object> spaceStation = new HashMap<>();
                spaceStation.put("allowsRobotSpawns", true);
                tileMap.put("spaceStation", spaceStation);
            }
            if (!tile.getMineIds().isEmpty()) {
                tileMap.put("mines", tile.getMineIds().stream().map(UUID::toString).toList());
            }
        }

        return tileMap;
    }
}
