package de.microservicedungeon.mock.service;

import de.microservicedungeon.mock.model.BuyRobotChain;
import de.microservicedungeon.mock.model.GameInitChain;
import de.microservicedungeon.mock.model.GameState;
import de.microservicedungeon.mock.model.map.GameMap;
import de.microservicedungeon.mock.state.GameStateImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class GameManager implements CommandLineRunner {
    private final BuyRobotChain buyRobotChain;
    private final GameInitChain gameInitChain;

    @Value("${game.simulation.publish-delay-ms}")
    private long publishDelayMs;

    @Override
    public void run(String... args) throws Exception {

        GameMap gameMap = MapConverter.convertAsciiMapToGameMap("input/map.ascii");
        GameState gameState = new GameStateImpl(gameMap);

        gameInitChain.executeForExistingGame(gameState);

        buyRobotChain.executeForGame(gameState);
    }
}
