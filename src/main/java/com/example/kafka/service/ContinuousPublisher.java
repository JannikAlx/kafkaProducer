package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Main orchestrator for continuous event publishing.
 * Initializes the game and continuously executes event flows in sequence with
 * variations.
 */
@Service
public class ContinuousPublisher implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ContinuousPublisher.class);

    private final GameInitializer gameInitializer;
    private final FlowExecutor flowExecutor;
    private final com.example.kafka.state.GameState gameState;

    @Value("${game.simulation.publish-delay-ms}")
    private long publishDelayMs;

    public ContinuousPublisher(GameInitializer gameInitializer,
            FlowExecutor flowExecutor,
            com.example.kafka.state.GameState gameState) {
        this.gameInitializer = gameInitializer;
        this.flowExecutor = flowExecutor;
        this.gameState = gameState;
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Starting continuous event publisher...");

        // Initialize game and publish initialization events
        gameInitializer.initializeGame();

        logger.info("Game initialized. Starting continuous flow execution...");
        logger.info("Press Ctrl+C to stop the simulation.");

        // Continuous publishing loop
        int iteration = 0;
        while (true) {
            iteration++;

            // Get next player in rotation
            UUID currentPlayer = gameState.getNextPlayer();

            logger.info("=== Iteration {} - Player {} ===", iteration, currentPlayer);

            try {
                // Execute flows in order with variations

                // 1. Buy robots (occasionally, not every iteration)
                if (iteration % 5 == 1) { // Every 5th iteration starting from 1
                    flowExecutor.executeBuyRobotFlow(currentPlayer);
                    sleep();
                }

                // 2. Move robots (frequently)
                int moveCount = (iteration % 3) + 1; // 1-3 moves per iteration
                for (int i = 0; i < moveCount; i++) {
                    flowExecutor.executeMovementFlow(currentPlayer);
                    sleep();
                }

                // 3. Mine resources (regularly)
                if (iteration % 2 == 0) { // Every other iteration
                    flowExecutor.executeMiningFlow(currentPlayer);
                    sleep();
                }

                // 4. Sell resources (occasionally)
                if (iteration % 4 == 0) { // Every 4th iteration
                    flowExecutor.executeSellingFlow(currentPlayer);
                    sleep();
                }

            } catch (Exception e) {
                logger.error("Error during flow execution for player {}", currentPlayer, e);
                // Continue with next iteration despite errors
            }

            // Small delay between iterations for readability
            Thread.sleep(publishDelayMs);
        }
    }

    /**
     * Small sleep between events within a flow for readability.
     */
    private void sleep() {
        try {
            Thread.sleep(publishDelayMs / 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
