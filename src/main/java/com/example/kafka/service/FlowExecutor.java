package com.example.kafka.service;

import com.example.kafka.model.Direction;
import com.example.kafka.model.MapTile;
import com.example.kafka.model.Position;
import com.example.kafka.state.GameState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Executes different event flows with variations.
 * Handles buy-robot, mining, selling, and movement flows.
 */
@Service
public class FlowExecutor {

    private static final Logger logger = LoggerFactory.getLogger(FlowExecutor.class);

    private final GameState gameState;
    private final EventBuilder eventBuilder;
    private final KafkaEventPublisher kafkaPublisher;
    private final Random random = new Random();

    @Value("${game.simulation.blackhole-destruction-chance}")
    private double blackholeDestructionChance;

    public FlowExecutor(GameState gameState, EventBuilder eventBuilder, KafkaEventPublisher kafkaPublisher) {
        this.gameState = gameState;
        this.eventBuilder = eventBuilder;
        this.kafkaPublisher = kafkaPublisher;
    }

    /**
     * Execute buy-robot flow for a player.
     * Variation: Buy 1-3 robots at once.
     */
    public void executeBuyRobotFlow(UUID playerId) {
        int robotCount = random.nextInt(3) + 1; // 1-3 robots
        logger.info("Player {} buying {} robot(s)", playerId, robotCount);

        UUID bankAccountId = gameState.getBankAccountId(playerId);
        int costPerRobot = 100;
        int totalCost = robotCount * costPerRobot;

        // Step 1: buy-robot intent
        Map<String, Object> intentPayload = new HashMap<>();
        intentPayload.put("gameId", gameState.getGameId().toString());
        intentPayload.put("amountOfRobots", robotCount);

        String intent = eventBuilder.buildIntent(
                "voucher.intents.v1",
                "buy-robot",
                intentPayload,
                playerId.toString());
        kafkaPublisher.publish(intent);

        // Step 2: credits-withdrawn event
        Map<String, Object> withdrawPayload = new HashMap<>();
        withdrawPayload.put("bankAccountId", bankAccountId.toString());
        withdrawPayload.put("deduction", totalCost);
        withdrawPayload.put("newBalance", 1000 - totalCost); // Simplified balance tracking

        String withdrawEvent = eventBuilder.buildEvent(
                "bl.bank-account.events.v1",
                "credits-withdrawn",
                "bank-account." + bankAccountId,
                withdrawPayload,
                bankAccountId.toString());
        kafkaPublisher.publish(withdrawEvent);

        // Step 3 & 4: For each robot, issue voucher and construct robot
        for (int i = 0; i < robotCount; i++) {
            UUID voucherId = UUID.randomUUID();
            UUID robotId = UUID.randomUUID();

            // Step 3: construct-robot-voucher-issued
            Map<String, Object> voucherPayload = new HashMap<>();
            voucherPayload.put("voucherId", voucherId.toString());
            voucherPayload.put("gameId", gameState.getGameId().toString());
            voucherPayload.put("playerId", playerId.toString());
            voucherPayload.put("totalPrice", costPerRobot);
            voucherPayload.put("numberOfRobots", 1);

            String voucherEvent = eventBuilder.buildEvent(
                    "bl.voucher.events.v1",
                    "construct-robot-voucher-issued",
                    "voucher." + voucherId,
                    voucherPayload,
                    gameState.getGameId() + "." + playerId);
            kafkaPublisher.publish(voucherEvent);

            // Step 4: robot-constructed
            Position spawnPosition = findSpawnPosition();
            gameState.addRobot(playerId, robotId, spawnPosition);

            Map<String, Object> constructPayload = new HashMap<>();
            constructPayload.put("robotId", robotId.toString());
            constructPayload.put("playerId", playerId.toString());
            constructPayload.put("gameId", gameState.getGameId().toString());
            constructPayload.put("voucherId", voucherId.toString());

            Map<String, Object> posMap = new HashMap<>();
            posMap.put("x", spawnPosition.getX());
            posMap.put("y", spawnPosition.getY());
            constructPayload.put("position", posMap);

            String constructEvent = eventBuilder.buildEvent(
                    "bl.robot.events.v1",
                    "robot-constructed",
                    "robot." + robotId,
                    constructPayload,
                    gameState.getGameId() + "." + playerId);
            kafkaPublisher.publish(constructEvent);
        }
    }

    /**
     * Execute movement flow for a player's robot.
     * Variation: Move in random directions, may hit blackhole.
     */
    public void executeMovementFlow(UUID playerId) {
        UUID robotId = gameState.getRandomActiveRobot(playerId);
        if (robotId == null) {
            logger.debug("Player {} has no active robots to move", playerId);
            return;
        }

        Position currentPos = gameState.getRobotPosition(robotId);
        if (currentPos == null) {
            return;
        }

        // Try random directions until we find a valid move
        List<Direction> directions = Arrays.asList(Direction.values());
        Collections.shuffle(directions);

        for (Direction direction : directions) {
            Position newPos = currentPos.move(direction);

            if (!newPos.isWithinBounds(gameState.getMapWidth(), gameState.getMapHeight())) {
                continue;
            }

            MapTile targetTile = gameState.getTile(newPos);
            if (targetTile == null || targetTile.isVoid()) {
                continue; // Can't move to void
            }

            // Step 1: move-robot intent
            Map<String, Object> intentPayload = new HashMap<>();
            intentPayload.put("gameId", gameState.getGameId().toString());
            intentPayload.put("robotId", robotId.toString());
            intentPayload.put("direction", direction.name());

            String intent = eventBuilder.buildIntent(
                    "robot.intents.v1",
                    "move-robot",
                    intentPayload,
                    playerId.toString());
            kafkaPublisher.publish(intent);

            // Step 2: robot-moved event
            Map<String, Object> movedPayload = new HashMap<>();
            movedPayload.put("robotId", robotId.toString());
            movedPayload.put("playerId", playerId.toString());
            movedPayload.put("gameId", gameState.getGameId().toString());

            Map<String, Object> fromPosMap = new HashMap<>();
            fromPosMap.put("x", currentPos.getX());
            fromPosMap.put("y", currentPos.getY());
            movedPayload.put("fromPosition", fromPosMap);

            Map<String, Object> toPosMap = new HashMap<>();
            toPosMap.put("x", newPos.getX());
            toPosMap.put("y", newPos.getY());
            movedPayload.put("toPosition", toPosMap);

            String movedEvent = eventBuilder.buildEvent(
                    "bl.robot.events.v1",
                    "robot-moved",
                    "robot." + robotId,
                    movedPayload,
                    robotId.toString());
            kafkaPublisher.publish(movedEvent);

            gameState.updateRobotPosition(robotId, newPos);

            // Step 3 (maybe): robot-destroyed if blackhole
            if (targetTile.isBlackHole() && random.nextDouble() < blackholeDestructionChance) {
                logger.info("Robot {} destroyed by black hole!", robotId);

                Map<String, Object> destroyedPayload = new HashMap<>();
                destroyedPayload.put("robotId", robotId.toString());
                destroyedPayload.put("playerId", playerId.toString());
                destroyedPayload.put("gameId", gameState.getGameId().toString());
                destroyedPayload.put("position", toPosMap);
                destroyedPayload.put("reason", "BLACK_HOLE");

                String destroyedEvent = eventBuilder.buildEvent(
                        "bl.robot.events.v1",
                        "robot-destroyed",
                        "robot." + robotId,
                        destroyedPayload,
                        robotId.toString());
                kafkaPublisher.publish(destroyedEvent);

                gameState.destroyRobot(robotId);
            }

            return; // Successfully moved
        }

        logger.debug("Could not find valid move for robot {}", robotId);
    }

    /**
     * Execute mining flow for a player's robot.
     */
    public void executeMiningFlow(UUID playerId) {
        UUID robotId = gameState.getRandomActiveRobot(playerId);
        if (robotId == null) {
            logger.debug("Player {} has no active robots to mine", playerId);
            return;
        }

        Position robotPos = gameState.getRobotPosition(robotId);
        if (robotPos == null) {
            return;
        }

        MapTile tile = gameState.getTile(robotPos);
        if (tile == null || !tile.isPlanet() || tile.getMineIds().isEmpty()) {
            logger.debug("Robot {} not on a planet with mines", robotId);
            return;
        }

        // Pick a random mine
        UUID mineId = tile.getMineIds().get(random.nextInt(tile.getMineIds().size()));
        int requestedAmount = random.nextInt(50) + 10; // 10-60 resources

        // Step 1: excavate-resources intent
        Map<String, Object> intentPayload = new HashMap<>();
        intentPayload.put("gameId", gameState.getGameId().toString());
        intentPayload.put("mineId", mineId.toString());
        intentPayload.put("amount", requestedAmount);

        String intent = eventBuilder.buildIntent(
                "robot.intents.v1",
                "excavate-resources",
                intentPayload,
                playerId.toString());
        kafkaPublisher.publish(intent);

        // Step 2: resource-excavated event
        int actualAmount = gameState.excavateMine(mineId, requestedAmount);
        if (actualAmount == 0) {
            logger.debug("Mine {} is empty", mineId);
            return;
        }

        Integer remainingAmount = gameState.getMineResources().get(mineId);

        Map<String, Object> excavatedPayload = new HashMap<>();
        excavatedPayload.put("planetId", UUID.randomUUID().toString()); // Simplified
        excavatedPayload.put("gameId", gameState.getGameId().toString());
        excavatedPayload.put("robotId", robotId.toString());
        excavatedPayload.put("resourceType", getRandomResourceType());
        excavatedPayload.put("amount", actualAmount);
        excavatedPayload.put("remainingAmount", remainingAmount != null ? remainingAmount : 0);

        String excavatedEvent = eventBuilder.buildEvent(
                "bl.map.events.v1",
                "resource-excavated",
                "planet." + UUID.randomUUID(), // Simplified
                excavatedPayload,
                UUID.randomUUID().toString() // Simplified
        );
        kafkaPublisher.publish(excavatedEvent);

        // Step 3: resource-picked-up event
        Map<String, Object> pickedUpPayload = new HashMap<>();
        pickedUpPayload.put("robotId", robotId.toString());
        pickedUpPayload.put("playerId", playerId.toString());
        pickedUpPayload.put("gameId", gameState.getGameId().toString());
        pickedUpPayload.put("planetId", UUID.randomUUID().toString()); // Simplified
        pickedUpPayload.put("resourceType", getRandomResourceType());
        pickedUpPayload.put("amount", actualAmount);

        String pickedUpEvent = eventBuilder.buildEvent(
                "bl.robot.events.v1",
                "resource-picked-up",
                "robot." + robotId,
                pickedUpPayload,
                robotId.toString());
        kafkaPublisher.publish(pickedUpEvent);
    }

    /**
     * Execute selling flow for a player's robot.
     */
    public void executeSellingFlow(UUID playerId) {
        UUID robotId = gameState.getRandomActiveRobot(playerId);
        if (robotId == null) {
            logger.debug("Player {} has no active robots to sell resources", playerId);
            return;
        }

        UUID bankAccountId = gameState.getBankAccountId(playerId);
        int resourceAmount = random.nextInt(50) + 10;
        int creditValue = resourceAmount * 5; // 5 credits per resource

        // Step 1: sell-resources intent
        Map<String, Object> intentPayload = new HashMap<>();
        intentPayload.put("gameId", gameState.getGameId().toString());
        intentPayload.put("robotId", robotId.toString());

        String intent = eventBuilder.buildIntent(
                "robot.intents.v1",
                "sell-resources",
                intentPayload,
                playerId.toString());
        kafkaPublisher.publish(intent);

        // Step 2: robot-delivered-resources event (no price info)
        Map<String, Object> deliveredPayload = new HashMap<>();
        deliveredPayload.put("robotId", robotId.toString());
        deliveredPayload.put("playerId", playerId.toString());
        deliveredPayload.put("gameId", gameState.getGameId().toString());
        deliveredPayload.put("bankAccountId", bankAccountId.toString());
        deliveredPayload.put("resourceType", getRandomResourceType());
        deliveredPayload.put("amount", resourceAmount);

        String deliveredEvent = eventBuilder.buildEvent(
                "bl.robot.events.v1",
                "robot-delivered-resources",
                "robot." + robotId,
                deliveredPayload,
                robotId.toString());
        kafkaPublisher.publish(deliveredEvent);

        // Step 3: credits-deposited event
        Map<String, Object> depositPayload = new HashMap<>();
        depositPayload.put("bankAccountId", bankAccountId.toString());
        depositPayload.put("addition", creditValue);
        depositPayload.put("newBalance", 1000 + creditValue); // Simplified balance tracking

        String depositEvent = eventBuilder.buildEvent(
                "bl.bank-account.events.v1",
                "credits-deposited",
                "bank-account." + bankAccountId,
                depositPayload,
                bankAccountId.toString());
        kafkaPublisher.publish(depositEvent);
    }

    /**
     * Find a spawn position (planet with space station, or any planet).
     */
    private Position findSpawnPosition() {
        // Try to find a space station first
        for (int y = 0; y < gameState.getMapHeight(); y++) {
            for (int x = 0; x < gameState.getMapWidth(); x++) {
                MapTile tile = gameState.getTile(x, y);
                if (tile != null && tile.isPlanet() && Boolean.TRUE.equals(tile.getAllowsRobotSpawns())) {
                    return new Position(x, y);
                }
            }
        }

        // Fallback: find any planet
        for (int y = 0; y < gameState.getMapHeight(); y++) {
            for (int x = 0; x < gameState.getMapWidth(); x++) {
                MapTile tile = gameState.getTile(x, y);
                if (tile != null && tile.isPlanet()) {
                    return new Position(x, y);
                }
            }
        }

        // Last resort: position (0,0)
        return new Position(0, 0);
    }

    /**
     * Get a random resource type name.
     */
    private String getRandomResourceType() {
        String[] types = { "BIO_MATTER", "CRYO_GAS", "DARK_MATTER", "ION_DUST", "PLASMA_CORES" };
        return types[random.nextInt(types.length)];
    }
}
