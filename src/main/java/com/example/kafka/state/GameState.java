package com.example.kafka.state;

import com.example.kafka.model.MapTile;
import com.example.kafka.model.Position;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Centralized game state for the simulation.
 * Maintains information about the game, players, robots, and map.
 * Thread-safe for concurrent access.
 */
@Component
public class GameState {

    // Single game ID for all simulations
    private UUID gameId;

    // Player management
    private final List<UUID> playerIds = new ArrayList<>();
    private final Map<UUID, UUID> playerToBankAccount = new ConcurrentHashMap<>();

    // Robot management: playerId -> list of robotIds
    private final Map<UUID, List<UUID>> playerRobots = new ConcurrentHashMap<>();

    // Track destroyed robots to prevent further use
    private final Set<UUID> destroyedRobots = ConcurrentHashMap.newKeySet();

    // Robot positions: robotId -> Position
    private final Map<UUID, Position> robotPositions = new ConcurrentHashMap<>();

    // Map structure
    private MapTile[][] mapTiles; // [x][y]
    private int mapWidth;
    private int mapHeight;

    // Mine tracking: mineId -> resourceAmount remaining
    private final Map<UUID, Integer> mineResources = new ConcurrentHashMap<>();

    // Current player index for rotation
    private int currentPlayerIndex = 0;

    /**
     * Initialize the game with a new game ID and player count.
     *
     * @param playerCount number of players to create
     */
    public void initialize(int playerCount) {
        this.gameId = UUID.randomUUID();

        // Create players and their bank accounts
        for (int i = 0; i < playerCount; i++) {
            UUID playerId = UUID.randomUUID();
            UUID bankAccountId = UUID.randomUUID();

            playerIds.add(playerId);
            playerToBankAccount.put(playerId, bankAccountId);
            playerRobots.put(playerId, new ArrayList<>());
        }
    }

    /**
     * Initialize the map with given dimensions.
     *
     * @param width  map width
     * @param height map height
     */
    public void initializeMap(int width, int height) {
        this.mapWidth = width;
        this.mapHeight = height;
        this.mapTiles = new MapTile[width][height];
    }

    /**
     * Set a tile on the map.
     */
    public void setTile(int x, int y, MapTile tile) {
        if (x >= 0 && x < mapWidth && y >= 0 && y < mapHeight) {
            mapTiles[x][y] = tile;
        }
    }

    /**
     * Get a tile from the map.
     */
    public MapTile getTile(int x, int y) {
        if (x >= 0 && x < mapWidth && y >= 0 && y < mapHeight) {
            return mapTiles[x][y];
        }
        return null;
    }

    /**
     * Get a tile at a specific position.
     */
    public MapTile getTile(Position position) {
        return getTile(position.getX(), position.getY());
    }

    /**
     * Add a robot for a player.
     *
     * @param playerId player who owns the robot
     * @param robotId  the robot's ID
     * @param position initial position
     */
    public void addRobot(UUID playerId, UUID robotId, Position position) {
        playerRobots.computeIfAbsent(playerId, k -> new ArrayList<>()).add(robotId);
        robotPositions.put(robotId, position);
    }

    /**
     * Mark a robot as destroyed.
     */
    public void destroyRobot(UUID robotId) {
        destroyedRobots.add(robotId);
        robotPositions.remove(robotId);
    }

    /**
     * Check if a robot is destroyed.
     */
    public boolean isRobotDestroyed(UUID robotId) {
        return destroyedRobots.contains(robotId);
    }

    /**
     * Update robot position.
     */
    public void updateRobotPosition(UUID robotId, Position newPosition) {
        robotPositions.put(robotId, newPosition);
    }

    /**
     * Get the next player in rotation.
     */
    public UUID getNextPlayer() {
        UUID player = playerIds.get(currentPlayerIndex);
        currentPlayerIndex = (currentPlayerIndex + 1) % playerIds.size();
        return player;
    }

    /**
     * Get a random active robot for a player.
     * Returns null if player has no active robots.
     */
    public UUID getRandomActiveRobot(UUID playerId) {
        List<UUID> robots = playerRobots.get(playerId);
        if (robots == null || robots.isEmpty()) {
            return null;
        }

        // Filter out destroyed robots
        List<UUID> activeRobots = robots.stream()
                .filter(robotId -> !isRobotDestroyed(robotId))
                .toList();

        if (activeRobots.isEmpty()) {
            return null;
        }

        Random random = new Random();
        return activeRobots.get(random.nextInt(activeRobots.size()));
    }

    // Getters
    public UUID getGameId() {
        return gameId;
    }

    public List<UUID> getPlayerIds() {
        return new ArrayList<>(playerIds);
    }

    public UUID getBankAccountId(UUID playerId) {
        return playerToBankAccount.get(playerId);
    }

    public List<UUID> getPlayerRobots(UUID playerId) {
        return new ArrayList<>(playerRobots.getOrDefault(playerId, new ArrayList<>()));
    }

    public Position getRobotPosition(UUID robotId) {
        return robotPositions.get(robotId);
    }

    public int getMapWidth() {
        return mapWidth;
    }

    public int getMapHeight() {
        return mapHeight;
    }

    public MapTile[][] getMapTiles() {
        return mapTiles;
    }

    public Map<UUID, Integer> getMineResources() {
        return mineResources;
    }

    /**
     * Reduce mine resources by a given amount.
     * Returns the actual amount excavated (may be less if mine doesn't have
     * enough).
     */
    public int excavateMine(UUID mineId, int requestedAmount) {
        Integer current = mineResources.get(mineId);
        if (current == null || current <= 0) {
            return 0;
        }

        int actualAmount = Math.min(current, requestedAmount);
        mineResources.put(mineId, current - actualAmount);
        return actualAmount;
    }
}
