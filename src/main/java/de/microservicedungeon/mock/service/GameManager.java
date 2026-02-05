package de.microservicedungeon.mock.service;

import de.microservicedungeon.mock.service.chains.BuyRobotChain;
import de.microservicedungeon.mock.service.chains.ExcavateResourceChain;
import de.microservicedungeon.mock.service.chains.GameInitChain;
import de.microservicedungeon.mock.model.GameState;
import de.microservicedungeon.mock.service.chains.MoveRobotChain;
import de.microservicedungeon.mock.service.chains.SellResourceChain;
import de.microservicedungeon.mock.model.map.GameMap;
import de.microservicedungeon.mock.state.GameStateImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class GameManager {
    private final BuyRobotChain buyRobotChain;
    private final GameInitChain gameInitChain;
    private final MoveRobotChain moveRobotChain;
    private final ExcavateResourceChain excavateResourceChain;
    private final SellResourceChain sellResourceChain;
    private final Random random = new Random();

    private String customMapPath = null; // Custom map path if provided

    public void setCustomMapPath(String customMapPath) {
        this.customMapPath = customMapPath;
        log.info("Custom map path set to: {}", customMapPath);
    }

    public void simulateGame(boolean loadTest, long publishDelayMs) throws Exception {
        GameState gameState = init();

        gameInitChain.executeForExistingGame(gameState);
        buyRobotChain.executeForGame(gameState);
        int i = 0;
        while (i < 500){
            for (UUID player: gameState.currentPlayers()){
                if (random.nextInt(3) == 1){
                    moveRobotChain.executeForPlayer(player,gameState);
                }
                if (random.nextInt(3) == 1){
                    excavateResourceChain.executeForPlayer(player, gameState);
                }
                if (random.nextInt(5)==1){
                    sellResourceChain.executeForPlayer(player, gameState);
                }
                if (random.nextInt(6) == 1){
                    buyRobotChain.executeForPlayer(player, gameState);
                }
                Thread.sleep(publishDelayMs);
            }
            if (!loadTest){
                i++;
            }
        }
    }

    public void runOnce(long delay) throws Exception{
        GameState gameState = init();

        gameInitChain.executeForExistingGame(gameState);
        Thread.sleep(delay);
        buyRobotChain.executeForGame(gameState);
        Thread.sleep(delay);

        // Execute robot movements after buying robots
        moveRobotChain.executeForGame(gameState);
        Thread.sleep(delay);

        // Execute resource excavation after robots are positioned
        excavateResourceChain.executeForGame(gameState);
        Thread.sleep(delay);

        // Execute resource selling after excavation
        sellResourceChain.executeForGame(gameState);
    }

    public void executeScenario(String string, int times, long publishDelayMs) throws InterruptedException, IOException {
        GameState gameState = init();

        for (int i = 0; i<times; i++) {
            switch (string) {
                case "create" -> {
                    gameState.reInit();
                    gameInitChain.executeForExistingGame(gameState);
                }
                case "buy" -> buyRobotChain.executeForGame(gameState);
                case "move" -> {
                    buyRobotChain.executeForGame(gameState);
                    moveRobotChain.executeForGame(gameState);
                }
                case "excavate" -> {
                    buyRobotChain.executeForGame(gameState);
                    excavateResourceChain.executeForGame(gameState);
                }
                case "sell" -> {
                    buyRobotChain.executeForGame(gameState);
                    excavateResourceChain.executeForGame(gameState);
                    sellResourceChain.executeForGame(gameState);
                }
                default -> {
                    log.error("Unknown scenario. available scenarios are: create, buy, move, excavate, sell");
                    Thread.sleep(publishDelayMs);
                }
            }
        }
    }

    private GameState init() throws IOException {
        GameMap gameMap = MapConverter.convertAsciiMapToGameMap(customMapPath);
        return new GameStateImpl(gameMap);
    }
}
