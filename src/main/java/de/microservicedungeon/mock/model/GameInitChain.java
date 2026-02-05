package de.microservicedungeon.mock.model;

import de.microservicedungeon.mock.eventing.events.game.GameCreatedEvent;
import de.microservicedungeon.mock.eventing.events.game.GameStateChangedEvent;
import de.microservicedungeon.mock.eventing.events.game.GameStateTransferEvent;
import de.microservicedungeon.mock.eventing.events.game.PlayerJoinedEvent;
import de.microservicedungeon.mock.eventing.events.map.MapInitializedEvent;
import de.microservicedungeon.mock.eventing.events.trading.BankAccountECSTEvent;
import de.microservicedungeon.mock.eventing.events.trading.BankAccountOpenedEvent;
import de.microservicedungeon.mock.model.map.GameMap;
import de.microservicedungeon.mock.model.trading.BankAccount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class GameInitChain {

    private static final int PARTICIPANT_LIMIT_MULTIPLIER = 4;
    private static final String GAME_STATE_CREATED = "CREATED";
    private static final String GAME_STATE_STARTED = "STARTED";

    private final GameCreatedEvent gameCreatedEvent;
    private final GameStateTransferEvent gameStateTransferEvent;
    private final PlayerJoinedEvent playerJoinedEvent;
    private final GameStateChangedEvent gameStateChangedEvent;
    private final BankAccountOpenedEvent bankAccountOpenedEvent;
    private final BankAccountECSTEvent bankAccountECSTEvent;
    private final MapInitializedEvent mapInitializedEvent;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    /**
     * Mocks the creation of a game in events. Does not modify any gameState.
     * <p>
     * Executes the following event chain:
     * 1. Game creation - publishes GameCreatedEvent and initial GameStateTransferEvent
     * 2. Player joining - for each player, publishes PlayerJoinedEvent and updated GameStateTransferEvent
     * 3. Bank account setup - for each player, publishes BankAccountOpenedEvent and BankAccountECSTEvent
     * 4. Map initialization - publishes MapInitializedEvent with the game map
     * 5. Game start - publishes GameStateChangedEvent (STARTED) and final GameStateTransferEvent
     * <p>
     * All events are sent to Kafka topics to simulate the complete game initialization flow.
     *
     * @param gameState the existing game state to generate events for
     */
    public void executeForExistingGame(GameState gameState) {
        UUID gameId = gameState.currentGame();
        List<UUID> players = gameState.currentPlayers();
        //Create player payloads beforehand, as time joined has to be the same
        List<GameStateTransferEvent.ParticipantPayload> participantPayloads = createParticipantPayloads(players);
        int participantLimit = calculateParticipantLimit(players.size());

        log.info("Starting game initialization chain for game {} with {} players", gameId, players.size());

        List<ProducerRecord<String, byte[]>> records = new LinkedList<>();

        // 1. Game creation events
        addGameCreationEvents(records, gameId, participantLimit);

        // 2. Player joining events - for each player
        addPlayerJoinEvents(records, gameId, players, participantPayloads, participantLimit);

        // 3. Bank account events - for each player
        addBankAccountEvents(records, gameId, players, gameState);

        // 4. Map initialization events
        addMapInitializationEvents(records, gameId, gameState.getMap());

        // 5. Game start events
        addGameStartEvents(records, gameId, participantPayloads, participantLimit);

        // Publish all events in the exact order they were added
        publishAllEvents(records);

        log.info("Completed game initialization chain for game {} - {} events published", gameId, records.size());
    }

    /**
     * Calculate participant limit based on player count.
     */
    private int calculateParticipantLimit(int playerCount) {
        return playerCount * PARTICIPANT_LIMIT_MULTIPLIER;
    }

    /**
     * Create participant payloads for all players with unique participant IDs.
     */
    private List<GameStateTransferEvent.ParticipantPayload> createParticipantPayloads(List<UUID> players) {
        long currentTime = System.currentTimeMillis();
        return players.stream()
                .map(playerId -> new GameStateTransferEvent.ParticipantPayload(
                        UUID.randomUUID(),
                        playerId,
                        currentTime))
                .toList();
    }

    /**
     * Add game creation events to the records list.
     */
    private void addGameCreationEvents(List<ProducerRecord<String, byte[]>> records, UUID gameId, int participantLimit) {
        log.debug("Adding game creation events for game {}", gameId);

        records.add(gameCreatedEvent.builder()
                        .forGame(gameId)
                        .withParticipantLimit(participantLimit)
                        .withState(GAME_STATE_CREATED)
                        .build()
        );

        records.add(gameStateTransferEvent.builder()
                        .forGame(gameId)
                        .withParticipantLimit(participantLimit)
                        .withParticipants(List.of())
                        .withState(GAME_STATE_CREATED)
                .build());
    }

    /**
     * Add player join events to the records list.
     */
    private void addPlayerJoinEvents(List<ProducerRecord<String, byte[]>> records,
                                   UUID gameId,
                                   List<UUID> players,
                                   List<GameStateTransferEvent.ParticipantPayload> participantPayloads,
                                   int participantLimit) {
        log.debug("Adding player join events for {} players in game {}", players.size(), gameId);

        for (UUID playerId : players) {
            records.add(playerJoinedEvent.builder()
                            .forGame(gameId)
                            .withPlayer(playerId)
                            .build());

            records.add(gameStateTransferEvent.builder()
                    .forGame(gameId)
                    .withParticipants(participantPayloads)
                    .withState(GAME_STATE_CREATED)
                    .withParticipantLimit(participantLimit)
                    .build());
        }
    }

    /**
     * Add bank account events to the records list.
     */
    private void addBankAccountEvents(List<ProducerRecord<String, byte[]>> records,
                                    UUID gameId,
                                    List<UUID> players,
                                    GameState gameState) {
        log.debug("Adding bank account events for {} players in game {}", players.size(), gameId);

        players.forEach(playerId -> {
            BankAccount bankAccount = gameState.getPlayerBankAccount(playerId);

            records.add(bankAccountOpenedEvent.builder()
                            .forBankAccount(bankAccount.bankAccountId())
                            .withPlayer(playerId)
                            .inGame(gameId)
                            .withBalance(bankAccount.balance())
                            .build());

            records.add(bankAccountECSTEvent.builder()
                    .forBankAccount(bankAccount.bankAccountId())
                    .inGame(gameId)
                    .withCurrentCredits(bankAccount.balance())
                    .withPlayer(playerId)
                    .build());
        });
    }

    /**
     * Add map initialization events to the records list.
     */
    private void addMapInitializationEvents(List<ProducerRecord<String, byte[]>> records, UUID gameId, GameMap gameMap) {
        log.debug("Adding map initialization events for game {}", gameId);

        records.add(mapInitializedEvent.build(gameMap, gameId));
    }

    /**
     * Add game start events to the records list.
     */
    private void addGameStartEvents(List<ProducerRecord<String, byte[]>> records,
                                  UUID gameId,
                                  List<GameStateTransferEvent.ParticipantPayload> participantPayloads,
                                  int participantLimit) {
        log.debug("Adding game start events for game {}", gameId);

        records.add(gameStateChangedEvent.builder()
                .forGame(gameId)
                .withState(GAME_STATE_STARTED)
                .build());

        records.add(gameStateTransferEvent.builder()
                        .forGame(gameId)
                        .withState(GAME_STATE_STARTED)
                        .withParticipantLimit(participantLimit)
                        .withParticipants(participantPayloads)
                .build());
    }

    /**
     * Publish all events in the exact order they appear in the records list.
     */
    private void publishAllEvents(List<ProducerRecord<String, byte[]>> records) {
        for (ProducerRecord<String, byte[]> record : records) {
            kafkaTemplate.send(record);
            log.info("Sent {} to topic {}",
                    new String(record.headers().lastHeader("event_type").value()),
                    record.topic());
        }

    }
}
