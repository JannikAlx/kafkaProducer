package de.microservicedungeon.mock;

import de.microservicedungeon.mock.service.GameManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.Scanner;

@Slf4j
@Service
@RequiredArgsConstructor
public class GameCliService implements CommandLineRunner {

    private final GameManager gameManager;
    private final Scanner scanner = new Scanner(System.in);
    private int publishDelayMs = 10; // Default value
    private String customMapPath = null; // Custom map file path

    @Override
    public void run(String... args) {
        parseArguments(args);
        printWelcome();
        boolean running = true;

        while (running) {
            printMainMenu();
            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1" -> handleSimulateGame();
                case "2" -> handleExecuteScenario();
                case "3" -> handleRunOnce();
                case "h", "help" -> printHelp();
                case "q", "quit", "exit" -> running = false;
                default -> System.out.println("Invalid choice. Type 'h' for help or 'q' to quit.");
            }
        }

        System.out.println("Goodbye!");
    }

    private void parseArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals("--delay") && i + 1 < args.length) {
                try {
                    publishDelayMs = Integer.parseInt(args[i + 1]);
                    log.info("Publish delay set to {}ms via CLI argument", publishDelayMs);
                    i++; // Skip next argument since we consumed it
                } catch (NumberFormatException e) {
                    System.err.println("Invalid delay value: " + args[i + 1] + ". Using default: " + publishDelayMs + "ms");
                }
            } else if (arg.equals("--map") && i + 1 < args.length) {
                customMapPath = args[i + 1];
                log.info("Custom map path set to: {}", customMapPath);
                i++; // Skip next argument since we consumed it
            }
        }
        if (publishDelayMs < 0) {
            System.err.println("Delay cannot be negative. Using default: 10ms");
            publishDelayMs = 10;
        }

        // Set custom map path in GameManager
        if (customMapPath != null) {
            gameManager.setCustomMapPath(customMapPath);
        }
    }

    private void printWelcome() {
        System.out.println("=".repeat(60));
        System.out.println("    Microservice Dungeon Game Simulator");
        System.out.println("=".repeat(60));
        System.out.println("Configuration: Publish delay = " + publishDelayMs + "ms");
        System.out.println("Map: " + (customMapPath != null ? customMapPath : "bundled map.ascii"));
        if (publishDelayMs < 5) {
            System.out.println("⚠️  WARNING: Very low delay may cause heavy resource usage");
        }
        System.out.println();
    }

    private void printMainMenu() {
        System.out.println("\n--- Main Menu ---");
        System.out.println("1. Simulate Game (continuous/load test mode)");
        System.out.println("2. Execute Scenario (run specific scenarios multiple times)");
        System.out.println("3. Run Once (execute complete game flow once)");
        System.out.println("h. Help");
        System.out.println("q. Quit");
        System.out.print("\nEnter your choice: ");
    }

    private void handleSimulateGame() {
        System.out.println("\n--- Simulate Game ---");
        System.out.println("This will run a continuous game simulation with random events.");
        System.out.println("Choose mode:");
        System.out.println("1. Normal mode (500 iterations)");
        System.out.println("2. Load test mode (infinite loop)");
        System.out.print("Enter choice (1-2): ");

        String choice = scanner.nextLine().trim();
        boolean loadTest = "2".equals(choice);

        System.out.println(loadTest ? "Starting load test mode..." : "Starting normal simulation...");
        System.out.println("Press Ctrl+C to stop.");

        try {
            gameManager.simulateGame(loadTest, publishDelayMs);
        } catch (Exception e) {
            log.error("Error during game simulation", e);
            System.out.println("Error occurred during simulation: " + e.getMessage());
        }
    }

    private void handleExecuteScenario() {
        System.out.println("\n--- Execute Scenario ---");
        System.out.println("Available scenarios:");
        System.out.println("1. create  - Game initialization");
        System.out.println("2. buy     - Robot purchasing");
        System.out.println("3. move    - Robot movement");
        System.out.println("4. excavate - Resource excavation");
        System.out.println("5. sell    - Resource selling");
        System.out.print("Enter scenario name: ");

        String scenario = scanner.nextLine().trim().toLowerCase();

        if (!isValidScenario(scenario)) {
            System.out.println("Invalid scenario. Valid options: create, buy, move, excavate, sell");
            return;
        }

        System.out.print("How many times to execute? ");
        try {
            int times = Integer.parseInt(scanner.nextLine().trim());
            if (times <= 0) {
                System.out.println("Number must be positive.");
                return;
            }

            System.out.printf("Executing '%s' scenario %d time(s)...\n", scenario, times);
            gameManager.executeScenario(scenario, times, publishDelayMs);
            System.out.println("Scenario execution completed.");
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format.");
        } catch (Exception e) {
            log.error("Error during scenario execution", e);
            System.out.println("Error occurred: " + e.getMessage());
        }
    }

    private void handleRunOnce() {
        System.out.println("\n--- Run Once ---");
        System.out.println("This will execute the complete game flow once:");
        System.out.println("1. Initialize game");
        System.out.println("2. Buy robots");
        System.out.println("3. Move robots");
        System.out.println("4. Excavate resources");
        System.out.println("5. Sell resources");
        System.out.println();
        System.out.print("Continue? (y/N): ");

        String confirm = scanner.nextLine().trim().toLowerCase();
        if ("y".equals(confirm) || "yes".equals(confirm)) {
            try {
                gameManager.runOnce(publishDelayMs);
                System.out.println("Game flow completed successfully.");
            } catch (Exception e) {
                log.error("Error during single run", e);
                System.out.println("Error occurred: " + e.getMessage());
            }
        } else {
            System.out.println("Operation cancelled.");
        }
    }

    private void printHelp() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("                          HELP");
        System.out.println("=".repeat(60));
        System.out.println();

        System.out.println("USAGE:");
        System.out.println("  java -jar app.jar [OPTIONS]");
        System.out.println();
        System.out.println("OPTIONS:");
        System.out.println("  --delay <ms>     Set publish delay in milliseconds (default: 10)");
        System.out.println("  --map <path>     Use custom map file instead of bundled map.ascii");
        System.out.println();
        System.out.println("EXAMPLES:");
        System.out.println("java -jar kafka-producer-1.0.0.jar --delay 100");
        System.out.println("java -jar kafka-producer-1.0.0.jar --map /path/to/custom.ascii");
        System.out.println("java -jar kafka-producer-1.0.0.jar --delay 50 --map ./custom-map.ascii");
        System.out.println();

        System.out.println("GAME MODES:");
        System.out.println();

        System.out.println("1. SIMULATE GAME");
        System.out.println("   Purpose: Continuous game simulation with random player actions");
        System.out.println("   Normal Mode: Runs 500 iterations with random events");
        System.out.println("   Load Test Mode: Runs indefinitely for performance testing");
        System.out.println("   Events: Random mix of robot movement, excavation, selling, buying");
        System.out.println();

        System.out.println("2. EXECUTE SCENARIO");
        System.out.println("   Purpose: Run specific game scenarios multiple times");
        System.out.println("   Scenarios:");
        System.out.println("   - create: Game initialization and setup");
        System.out.println("   - buy: Robot purchasing chain");
        System.out.println("   - move: Robot movement chain");
        System.out.println("   - excavate: Resource excavation chain");
        System.out.println("   - sell: Resource selling chain");
        System.out.println();

        System.out.println("3. RUN ONCE");
        System.out.println("   Purpose: Execute complete game flow once");
        System.out.println("   Flow: init → buy robots → move → excavate → sell");
        System.out.println("   Use: Testing complete workflow or demonstration");
        System.out.println();

        System.out.println("CONFIGURATION:");
        System.out.println("   Current publish delay: " + publishDelayMs + "ms");
        System.out.println("   Lower values = higher throughput, more resource usage");
        System.out.println();

        System.out.println("NAVIGATION:");
        System.out.println("   h, help - Show this help page");
        System.out.println("   q, quit, exit - Exit the application");
        System.out.println("=".repeat(60));
    }

    private boolean isValidScenario(String scenario) {
        return scenario.equals("create") || scenario.equals("buy") ||
                scenario.equals("move") || scenario.equals("excavate") ||
                scenario.equals("sell");
    }
}
