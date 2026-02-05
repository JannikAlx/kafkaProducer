package de.microservicedungeon.mock.state;

import de.microservicedungeon.mock.model.GameState;
import de.microservicedungeon.mock.model.Resource;
import de.microservicedungeon.mock.model.map.Coordinate;
import de.microservicedungeon.mock.model.map.GameMap;
import de.microservicedungeon.mock.model.trading.BankAccount;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class GameStateImpl implements GameState {
    private final ReadWriteLock gameStateLock = new ReentrantReadWriteLock();
    private final ConcurrentHashMap<UUID, Coordinate> robotPositions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Set<UUID>> playerRobots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, BankAccount> playerBankAccounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, List<Resource>> robotCargo = new ConcurrentHashMap<>();
    private final GameMap gameMap;

    private volatile UUID currentGameId;
    private final Set<UUID> currentPlayerIds = ConcurrentHashMap.newKeySet();


    /**
     * Creates a game with 10 players and starting money of 100000
     */
    public GameStateImpl(GameMap gameMap){
        gameStateLock.writeLock().lock();
        try {
            // Clear existing state
            reset();

            // Create a new game
            currentGameId = UUID.randomUUID();

            // Create 10 players with starting balance
            for (int i = 0; i < 10; i++) {
                UUID playerId = UUID.randomUUID();
                currentPlayerIds.add(playerId);
                playerBankAccounts.put(playerId, new BankAccount(UUID.randomUUID(), 10000));
            }
            this.gameMap = gameMap;
        } finally {
            gameStateLock.writeLock().unlock();
        }
    }


    @Override
    public List<UUID> currentPlayers() {
        return new ArrayList<>(currentPlayerIds);
    }

    @Override
    public UUID currentGame() {
        return currentGameId;
    }

    @Override
    public Map<UUID, Coordinate> getAllRobotPositions() {
        return new HashMap<>(robotPositions);
    }

    @Override
    public Coordinate getRobotPosition(UUID robotId) {
        return robotPositions.get(robotId);
    }

    @Override
    public void mergeRobotPositions(Map<UUID, Coordinate> robotPositions) {
        this.robotPositions.putAll(robotPositions);
    }

    @Override
    public void updateRobotPosition(UUID robotId, Coordinate coordinate) {
        if (coordinate != null) {
            robotPositions.put(robotId, coordinate);
        } else {
            robotPositions.remove(robotId);
        }
    }

    public void destroyRobot(UUID playerId, UUID robotId){
        robotPositions.remove(robotId);
        playerRobots.get(playerId).remove(robotId);
    }

    @Override
    public void addAllRobotsToPlayer(UUID playerId, Iterable<UUID> robotIds) {
        currentPlayerIds.add(playerId);
        Set<UUID> robots = playerRobots.computeIfAbsent(playerId, k -> ConcurrentHashMap.newKeySet());
        robotIds.forEach(robots::add);
    }

    @Override
    public void addRobotToPlayer(UUID playerId, UUID robotId) {
        playerRobots.computeIfAbsent(playerId, k -> ConcurrentHashMap.newKeySet()).add(robotId);
    }

    @Override
    public Set<UUID> getPlayerRobots(UUID playerId) {
        return playerRobots.getOrDefault(playerId, Collections.emptySet());
    }

    @Override
    public BankAccount getPlayerBankAccount(UUID playerId) {
        return playerBankAccounts.get(playerId);
    }

    @Override
    public void setPlayerBalance(UUID playerId, int balance) {
        playerBankAccounts.computeIfPresent(playerId, (k, existing) -> new BankAccount(existing.bankAccountId(), balance));
        playerBankAccounts.putIfAbsent(playerId, new BankAccount(UUID.randomUUID(), balance));
    }

    @Override
    public void addResourceToRobot(UUID robotId, Resource resource) {
        robotCargo.computeIfAbsent(robotId, k -> new ArrayList<>()).add(resource);
    }

    @Override
    public List<Resource> getRobotCargo(UUID robotId) {
        return robotCargo.getOrDefault(robotId, Collections.emptyList());
    }

    @Override
    public void clearRobotCargo(UUID robotId) {
        robotCargo.remove(robotId);
    }

    @Override
    public GameMap getMap() {
        return gameMap;
    }

    @Override
    public Coordinate getTopRight() {
        return gameMap.getTopRight();
    }

    public Coordinate getRandomRobotSpawn(){
        return gameMap.getRandomSpawnableStation();
    }

    @Override
    public void reset() {
        gameStateLock.writeLock().lock();
        try {
            currentGameId = null;
            currentPlayerIds.clear();
            robotPositions.clear();
            playerRobots.clear();
            playerBankAccounts.clear();
            robotCargo.clear();
        } finally {
            gameStateLock.writeLock().unlock();
        }
    }
    public void setCurrentGame(UUID gameId) {
        this.currentGameId = gameId;
    }
}
