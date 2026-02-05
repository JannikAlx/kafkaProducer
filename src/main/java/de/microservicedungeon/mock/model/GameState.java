package de.microservicedungeon.mock.model;

import de.microservicedungeon.mock.model.map.Coordinate;
import de.microservicedungeon.mock.model.map.GameMap;
import de.microservicedungeon.mock.model.trading.BankAccount;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface GameState {
    List<UUID> currentPlayers();
    UUID currentGame();
    Map<UUID, Coordinate> getAllRobotPositions();
    Coordinate getRobotPosition(UUID robotId);
    //Merge old positions with new ones, aka replace all values for all keys
    void mergeRobotPositions(Map<UUID, Coordinate> robotPositions);
    void updateRobotPosition(UUID robotId, Coordinate coordinate);
    void addAllRobotsToPlayer(UUID playerId, Iterable<UUID> robotIds);
    Set<UUID> getPlayerRobots(UUID playerId);

    BankAccount getPlayerBankAccount(UUID playerId);
    void setPlayerBalance(UUID bankAccountId, int balance);

    // Robot cargo management
    void addResourceToRobot(UUID robotId, Resource resource);
    List<Resource> getRobotCargo(UUID robotId);
    void clearRobotCargo(UUID robotId);

    GameMap getMap();

    void reset();

    Coordinate getRandomRobotSpawn();
}
