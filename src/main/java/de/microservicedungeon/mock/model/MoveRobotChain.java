package de.microservicedungeon.mock.model;

import de.microservicedungeon.mock.eventing.events.robot.RobotECSTEvent;
import de.microservicedungeon.mock.eventing.events.robot.RobotMovedEvent;
import de.microservicedungeon.mock.eventing.intents.MoveRobotIntentFactory;
import de.microservicedungeon.mock.model.map.Coordinate;
import de.microservicedungeon.mock.model.map.Direction;
import de.microservicedungeon.mock.model.map.GameMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class MoveRobotChain {

    private final MoveRobotIntentFactory moveRobotIntentFactory;
    private final RobotECSTEvent robotECSTEvent;
    private final RobotMovedEvent robotMovedEvent;
    private final Random random = new Random();
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    private final List<ProducerRecord<String, byte[]>> records = new LinkedList<>();

    public void executeForPlayer(UUID playerId, GameState gameState) {
        UUID currentGame = gameState.currentGame();
        Set<UUID> playerRobots = gameState.getPlayerRobots(playerId);

        if (playerRobots.isEmpty()) {
            log.info("Player {} has no robots to move", playerId);
            return;
        }

        GameMap gameMap = gameState.getMap();
        Map<UUID, Coordinate> robotMovements = new HashMap<>();

        for (UUID robotId : playerRobots) {
            Coordinate currentPosition = gameState.getRobotPosition(robotId);
            if (currentPosition == null) {
                log.warn("Robot {} position not found, skipping movement", robotId);
                continue;
            }

            // Try to find a valid direction to move
            Direction[] allDirections = Direction.values();
            List<Direction> validDirections = new ArrayList<>();

            for (Direction direction : allDirections) {
                if (gameMap.isDirectionReachable(direction, currentPosition)) {
                    validDirections.add(direction);
                }
            }

            if (validDirections.isEmpty()) {
                log.info("Robot {} at position {} has no valid moves", robotId, currentPosition);
                continue;
            }

            // Pick a random valid direction
            Direction chosenDirection = validDirections.get(random.nextInt(validDirections.size()));
            Coordinate newPosition = currentPosition.move(chosenDirection);

            // Create movement intent
            records.add(moveRobotIntentFactory.build(playerId, currentGame, robotId, chosenDirection));

            // Create robot ECST event (current state before move)
            records.add(robotECSTEvent.builder()
                    .forRobot(robotId)
                    .withPlayer(playerId)
                    .inGame(currentGame)
                    .atPosition(currentPosition.getX(), currentPosition.getY())
                    .withCargo(Collections.emptyList()) // No cargo for simplicity
                    .withHealth(100) // Full health
                    .build());

            // Create robot moved event
            records.add(robotMovedEvent.builder()
                    .forRobot(robotId)
                    .withPlayer(playerId)
                    .inGame(currentGame)
                    .fromPosition(new RobotMovedEvent.PositionPayload(currentPosition.getX(), currentPosition.getY()))
                    .toPosition(new RobotMovedEvent.PositionPayload(newPosition.getX(), newPosition.getY()))
                    .build());

            robotMovements.put(robotId, newPosition);
        }

        // Send all records to Kafka
        for (ProducerRecord<String, byte[]> record : records) {
            kafkaTemplate.send(record);
            log.info("Sent {} to topic {}", new String(record.headers().lastHeader("event_type").value()), record.topic());
        }

        // Update game state with new robot positions
        for (Map.Entry<UUID, Coordinate> movement : robotMovements.entrySet()) {
            gameState.updateRobotPosition(movement.getKey(), movement.getValue());
        }

        records.clear(); // Clear records for next execution
    }

    public void executeForGame(GameState gameState) {
        for (UUID player : gameState.currentPlayers()) {
            executeForPlayer(player, gameState);
        }
    }
}
