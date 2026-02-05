package de.microservicedungeon.mock.service.chains;

import de.microservicedungeon.mock.eventing.events.robot.RobotConstructedEvent;
import de.microservicedungeon.mock.eventing.events.trading.BankAccountECSTEvent;
import de.microservicedungeon.mock.eventing.events.trading.ConstructRobotVoucherIssued;
import de.microservicedungeon.mock.eventing.events.trading.CreditsWithdrawnEvent;
import de.microservicedungeon.mock.eventing.intents.BuyConstructRobotVoucherIntentFactory;
import de.microservicedungeon.mock.model.Fixtures;
import de.microservicedungeon.mock.model.GameState;
import de.microservicedungeon.mock.model.map.Coordinate;
import de.microservicedungeon.mock.model.trading.BankAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class BuyRobotChain {

    private final BuyConstructRobotVoucherIntentFactory intentFactory;
    private final BankAccountECSTEvent bankAccountECSTEvent;
    private final CreditsWithdrawnEvent creditsWithdrawnEvent;
    private final ConstructRobotVoucherIssued constructRobotVoucherIssued;
    private final RobotConstructedEvent robotConstructedEvent;
    private final Random random = new Random();
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    private final List<ProducerRecord<String, byte[]>> records = new LinkedList<>();

    public void executeForPlayer(UUID playerId, GameState gameState) {
        UUID currentGame = gameState.currentGame();
        BankAccount bankAccount = gameState.getPlayerBankAccount(playerId);
        int robotsBought = random.nextInt(5) + 1;
        int deductedCredits = robotsBought * Fixtures.ROBOT_PRICE;
        if (deductedCredits > bankAccount.balance()){
            log.info("Player {} ran out of money", playerId);
            return;
        }
        int finalCredits = bankAccount.balance() - deductedCredits;
        records.add(intentFactory.build(playerId, currentGame, robotsBought));
        records.add(bankAccountECSTEvent.builder()
                .forBankAccount(bankAccount.bankAccountId())
                .withCurrentCredits(bankAccount.balance())
                .withPlayer(playerId)
                .inGame(currentGame)
                .build());

        records.add(creditsWithdrawnEvent.builder()
                .forBankAccount(bankAccount.bankAccountId())
                .withNewBalance(finalCredits)
                .withDeduction(deductedCredits)
                .build());

        records.add(constructRobotVoucherIssued.builder()
                .forPlayer(playerId)
                .inGame(currentGame)
                .forVoucher(UUID.randomUUID())
                .withNumberOfRobots(robotsBought)
                .withTotalPrice(deductedCredits)
                .build());

        Map<UUID, Coordinate> newRobotPositions = new HashMap<>();
        for (int i = 0; i < robotsBought; i++) {
            UUID robotId = UUID.randomUUID();
            Coordinate newCoordinate = gameState.getRandomRobotSpawn();
            records.add(robotConstructedEvent.builder()
                    .inGame(currentGame)
                    .forRobot(robotId)
                    .withPlayer(playerId)
                    .atPosition(newCoordinate.getX(), newCoordinate.getY())
                    .build());
            newRobotPositions.put(robotId, newCoordinate);
        }

        for (ProducerRecord<String, byte[]> record : records) {
            kafkaTemplate.send(record);
            //log.info("Sent {} to topic {}", new String(record.headers().lastHeader("event_type").value()), record.topic());
        }

        gameState.mergeRobotPositions(newRobotPositions);
        gameState.addAllRobotsToPlayer(playerId, newRobotPositions.keySet());
        gameState.setPlayerBalance(playerId, finalCredits);
    }

    public void executeForGame(GameState gameState) {
        for (UUID player : gameState.currentPlayers()) {
            executeForPlayer(player, gameState);
        }
    }
}
