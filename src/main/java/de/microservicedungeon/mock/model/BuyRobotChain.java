package de.microservicedungeon.mock.model;

import de.microservicedungeon.mock.eventing.events.BankAccountECSTEvent;
import de.microservicedungeon.mock.eventing.events.ConstructRobotVoucherIssued;
import de.microservicedungeon.mock.eventing.events.CreditsWithdrawnEvent;
import de.microservicedungeon.mock.eventing.events.RobotConstructedEvent;
import de.microservicedungeon.mock.eventing.intents.BuyConstructRobotVoucherIntentFactory;
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
        int robotsBought = random.nextInt(10) + 1;
        int deductedCredits = robotsBought * Fixtures.ROBOT_PRICE;
        int finalCredits = bankAccount.balance() - deductedCredits;
        records.add(intentFactory.build(playerId, currentGame, robotsBought));
        records.add(bankAccountECSTEvent.build(new BankAccountECSTEvent.BankAccountECSTPayload(bankAccount.bankAccountId(), playerId, currentGame, finalCredits)));
        records.add(creditsWithdrawnEvent.build(new CreditsWithdrawnEvent.CreditsWithdrawnPayload(bankAccount.bankAccountId(), deductedCredits, finalCredits)));
        records.add(constructRobotVoucherIssued.build(new ConstructRobotVoucherIssued.ConstructRobotVoucherPayload(UUID.randomUUID(), currentGame, playerId, deductedCredits, robotsBought)));

        Map<UUID, Coordinate> newRobotPositions = new HashMap<>();
        for (int i = 0; i < robotsBought; i++) {
            UUID robotId = UUID.randomUUID();
            Coordinate newCoordinate = gameState.getRandomRobotSpawn();
            records.add(robotConstructedEvent.build(
                    new RobotConstructedEvent.RobotConstructedPayload(
                            robotId, playerId, currentGame, new RobotConstructedEvent.PositionPayload(newCoordinate.getX(), newCoordinate.getY())
                    )
            ));
            newRobotPositions.put(robotId, newCoordinate);
        }

        for (ProducerRecord<String, byte[]> record : records) {
            kafkaTemplate.send(record);
            log.info("Sent {} to topic {}", new String(record.headers().lastHeader("event_type").value()), record.topic());
        }

        gameState.mergeRobotPositions(newRobotPositions);
        gameState.addAllRobotsToPlayer(playerId, newRobotPositions.keySet());
        gameState.setPlayerBalance(bankAccount.bankAccountId(), finalCredits);
    }

    public void executeForGame(GameState gameState){
        for (UUID player: gameState.currentPlayers()){
            executeForPlayer(player, gameState);
        }
    }
}
