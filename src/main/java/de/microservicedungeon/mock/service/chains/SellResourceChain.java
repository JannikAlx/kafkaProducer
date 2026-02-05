package de.microservicedungeon.mock.service.chains;

import de.microservicedungeon.mock.eventing.events.robot.RobotDeliveredResourcesEvent;
import de.microservicedungeon.mock.eventing.events.robot.RobotECSTEvent;
import de.microservicedungeon.mock.eventing.events.trading.BankAccountECSTEvent;
import de.microservicedungeon.mock.eventing.events.trading.CreditsDepositedEvent;
import de.microservicedungeon.mock.eventing.intents.SellResourcesIntentFactory;
import de.microservicedungeon.mock.model.GameState;
import de.microservicedungeon.mock.model.Resource;
import de.microservicedungeon.mock.model.map.Coordinate;
import de.microservicedungeon.mock.model.trading.BankAccount;
import de.microservicedungeon.mock.service.ResourceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SellResourceChain {

    private final SellResourcesIntentFactory intentFactory;
    private final RobotDeliveredResourcesEvent robotDeliveredResourcesEvent;
    private final RobotECSTEvent robotECSTEvent;
    private final CreditsDepositedEvent creditsDepositedEvent;
    private final BankAccountECSTEvent bankAccountECSTEvent;
    private final ResourceService resourceService;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final Random random = new Random();

    public void executeForPlayer(UUID playerId, GameState gameState) {
        UUID currentGame = gameState.currentGame();
        Set<UUID> playerRobotIds = gameState.getPlayerRobots(playerId);
        BankAccount bankAccount = gameState.getPlayerBankAccount(playerId);

        // Skip if player has no robots
        if (playerRobotIds.isEmpty()) {
            log.info("Player {} has no robots, skipping selling", playerId);
            return;
        }

        // Find a robot that has resources to sell
        List<UUID> robotsWithCargo = playerRobotIds.stream()
                .filter(robotId -> !gameState.getRobotCargo(robotId).isEmpty())
                .toList();

        if (robotsWithCargo.isEmpty()) {
            log.info("Player {} has no robots with resources to sell", playerId);
            return;
        }

        // Pick a random robot with cargo
        UUID selectedRobotId = robotsWithCargo.get(random.nextInt(robotsWithCargo.size()));
        List<Resource> robotCargo = new ArrayList<>(gameState.getRobotCargo(selectedRobotId));

        // Pick a random resource type from the cargo
        if (robotCargo.isEmpty()) {
            log.info("Robot {} has no cargo to sell", selectedRobotId);
            return;
        }

        Resource selectedResource = robotCargo.get(random.nextInt(robotCargo.size()));
        int amountToSell = selectedResource.amount();

        // Calculate credits to be deposited
        int pricePerUnit = resourceService.getPriceForResource(selectedResource);
        int previousBalance = bankAccount.balance();
        int totalCredits = amountToSell * pricePerUnit;
        int newBalance = previousBalance + totalCredits;

        // Remove the sold resource from robot cargo
        robotCargo.remove(selectedResource);
        gameState.clearRobotCargo(selectedRobotId);
        robotCargo.forEach(resource -> gameState.addResourceToRobot(selectedRobotId, resource));

        // Update player balance in game state
        gameState.setPlayerBalance(playerId, newBalance);

        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();

        try {
            // 1. Sell Resources Intent
            records.add(intentFactory.build(playerId, currentGame, selectedRobotId));

            // 2. Robot Delivered Resources Event
            records.add(robotDeliveredResourcesEvent.builder()
                    .forRobot(selectedRobotId)
                    .withPlayer(playerId)
                    .inGame(currentGame)
                    .toBankAccount(bankAccount.bankAccountId())
                    .withResourceType(selectedResource.type().name())
                    .withAmount(amountToSell)
                    .build());

            // 3. Robot ECST Event (updated cargo)
            Coordinate robotPosition = gameState.getRobotPosition(selectedRobotId);
            if (robotPosition == null) {
                robotPosition = gameState.getRandomRobotSpawn();
            }

            List<RobotECSTEvent.CargoPayload> cargoPayload = robotCargo.stream()
                    .map(resource -> new RobotECSTEvent.CargoPayload(resource.type().name(), resource.amount()))
                    .collect(Collectors.toList());

            records.add(robotECSTEvent.builder()
                    .forRobot(selectedRobotId)
                    .withPlayer(playerId)
                    .inGame(currentGame)
                    .atPosition(robotPosition.getX(), robotPosition.getY())
                    .withCargo(cargoPayload)
                    .withHealth(100) // Assume full health for now
                    .build());

            // 4. Credits Deposited Event
            records.add(creditsDepositedEvent.builder()
                    .forBankAccount(bankAccount.bankAccountId())
                    .withAddition(totalCredits)
                    .withNewBalance(newBalance)
                    .build());

            // 5. Bank Account ECST Event
            records.add(bankAccountECSTEvent.builder()
                    .forBankAccount(bankAccount.bankAccountId())
                    .withPlayer(playerId)
                    .inGame(currentGame)
                    .withCurrentCredits(newBalance)
                    .build());

            // Publish all events
            for (ProducerRecord<String, byte[]> record : records) {
                kafkaTemplate.send(record);
                log.info("Sent {} to topic {}", extractEventType(record), record.topic());
            }

            log.info("Player {} robot {} sold {} {} for {} credits (Balance Change: {} to {})",
                    playerId, selectedRobotId, amountToSell, selectedResource.type(), totalCredits, previousBalance , newBalance);

        } catch (Exception e) {
            log.error("Failed to execute sell resource chain for player {}", playerId, e);
        }
    }

    public void executeForGame(GameState gameState) {
        List<UUID> players = gameState.currentPlayers();
        log.info("Starting sell resource chain for {} players", players.size());

        for (UUID playerId : players) {
            executeForPlayer(playerId, gameState);
        }

        log.info("Completed sell resource chain for all players");
    }

    private String extractEventType(ProducerRecord<String, byte[]> record) {
        return record.headers().lastHeader("event_type") != null ?
                new String(record.headers().lastHeader("event_type").value()) : "unknown";
    }
}
