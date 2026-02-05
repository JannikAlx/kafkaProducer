package de.microservicedungeon.mock.service;

import de.microservicedungeon.mock.model.BuyRobotChain;
import de.microservicedungeon.mock.model.ExcavateResourceChain;
import de.microservicedungeon.mock.model.GameInitChain;
import de.microservicedungeon.mock.model.GameState;
import de.microservicedungeon.mock.model.MoveRobotChain;
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
    private final MoveRobotChain moveRobotChain;
    private final ExcavateResourceChain excavateResourceChain;

    @Value("${game.simulation.publish-delay-ms}")
    private long publishDelayMs;

    @Override
    public void run(String... args) throws Exception {

        GameMap gameMap = MapConverter.convertAsciiMapToGameMap("input/map.ascii");
        GameState gameState = new GameStateImpl(gameMap);

        gameInitChain.executeForExistingGame(gameState);

        buyRobotChain.executeForGame(gameState);

        // Execute robot movements after buying robots
        moveRobotChain.executeForGame(gameState);

        // Execute resource excavation after robots are positioned
        excavateResourceChain.executeForGame(gameState);
    }
}
