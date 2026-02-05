package de.microservicedungeon.mock.model;

import de.microservicedungeon.mock.eventing.events.map.MineECSTEvent;
import de.microservicedungeon.mock.eventing.events.map.ResourceExcavatedEvent;
import de.microservicedungeon.mock.eventing.events.robot.ResourcePickedUpEvent;
import de.microservicedungeon.mock.eventing.intents.ExcavateResourcesIntentFactory;
import de.microservicedungeon.mock.model.map.Mine;
import de.microservicedungeon.mock.model.map.StarSystem;
import de.microservicedungeon.mock.service.ResourceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExcavateResourceChain {

    private final ExcavateResourcesIntentFactory intentFactory;
    private final MineECSTEvent mineECSTEvent;
    private final ResourceExcavatedEvent resourceExcavatedEvent;
    private final ResourcePickedUpEvent resourcePickedUpEvent;
    private final ResourceService resourceService;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final Random random = new Random();

    public void executeForPlayer(UUID playerId, GameState gameState) {
        UUID currentGame = gameState.currentGame();
        Set<UUID> playerRobotIds = gameState.getPlayerRobots(playerId);

        // Skip if player has no robots
        if (playerRobotIds.isEmpty()) {
            log.info("Player {} has no robots, skipping excavation", playerId);
            return;
        }

        // Get a random robot from the player
        List<UUID> robotsList = new ArrayList<>(playerRobotIds);
        UUID selectedRobotId = robotsList.get(random.nextInt(robotsList.size()));

        // Get a random mine from the map
        Mine randomMine = getRandomMine(gameState);
        if (randomMine == null) {
            log.warn("No mines available on the map, skipping excavation");
            return;
        }

        // Generate random excavation amount (1-50 resources)
        int excavatedAmount = random.nextInt(50) + 1;
        int remainingMineAmount = random.nextInt(100) + excavatedAmount; // Ensure mine has enough

        // Create the event chain
        List<ProducerRecord<String, byte[]>> records = new ArrayList<>();

        // 1. Excavate Resources Intent
        records.add(intentFactory.build(playerId, currentGame, selectedRobotId));

        // 2. Mine ECST Event (update mine state)
        records.add(mineECSTEvent.builder()
                .forMine(randomMine.id())
                .inGame(currentGame)
                .withResourceType(randomMine.type().name())
                .withResourceAmount(remainingMineAmount - excavatedAmount)
                .build());

        // 3. Resource Excavated Event
        records.add(resourceExcavatedEvent.builder()
                .forPlanet(randomMine.id()) // Using mineId as planetId for this mock
                .inGame(currentGame)
                .byRobot(selectedRobotId)
                .withPlayer(playerId)
                .withResourceType(randomMine.type().name())
                .withAmount(excavatedAmount)
                .withRemainingAmount(remainingMineAmount - excavatedAmount)
                .build());

        // 4. Resource Picked Up Event (robot picks up the excavated resources)
        records.add(resourcePickedUpEvent.builder()
                .forRobot(selectedRobotId)
                .withPlayer(playerId)
                .inGame(currentGame)
                .fromPlanet(randomMine.id()) // Using mineId as planetId for this mock
                .withResourceType(randomMine.type().name())
                .withAmount(excavatedAmount)
                .build());

        // Publish all events
        for (ProducerRecord<String, byte[]> record : records) {
            kafkaTemplate.send(record);
            log.info("Sent {} to topic {}", getEventType(record), record.topic());
        }

        // Update game state - add resources to robot cargo
        Resource excavatedResource = new Resource(excavatedAmount, randomMine.type());
        gameState.addResourceToRobot(selectedRobotId, excavatedResource);

        log.info("Player {} robot {} excavated {} {} from mine {}",
                playerId, selectedRobotId, excavatedAmount, randomMine.type(), randomMine.id());
    }

    public void executeForGame(GameState gameState) {
        for (UUID player : gameState.currentPlayers()) {
            executeForPlayer(player, gameState);
        }
    }

    private Mine getRandomMine(GameState gameState) {
        List<Mine> availableMines = new ArrayList<>();

        // Collect all mines from the map
        for (StarSystem[] row : gameState.getMap().getStarSystems()) {
            for (StarSystem starSystem : row) {
                if (starSystem.mine() != null) {
                    availableMines.add(starSystem.mine());
                }
            }
        }

        if (availableMines.isEmpty()) {
            return null;
        }

        return availableMines.get(random.nextInt(availableMines.size()));
    }

    private String getEventType(ProducerRecord<String, byte[]> record) {
        return record.headers().lastHeader("event_type") != null
                ? new String(record.headers().lastHeader("event_type").value())
                : "unknown";
    }
}
