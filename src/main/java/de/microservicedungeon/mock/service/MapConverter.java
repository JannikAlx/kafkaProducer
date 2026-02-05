package de.microservicedungeon.mock.service;

import de.microservicedungeon.mock.model.ResourceType;
import de.microservicedungeon.mock.model.map.*;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Maps have to follow a specific format.
 */
@Slf4j
public class MapConverter {

    /**
     * Converts ASCII map to GameMap, supporting both custom file paths and bundled resources.
     * @param customMapPath Optional custom path to map file. If null, uses bundled map.ascii
     * @return GameMap converted from ASCII representation
     * @throws IOException if map cannot be loaded
     */
    public static GameMap convertAsciiMapToGameMap(String customMapPath) throws IOException {
        List<String> lines;

        if (customMapPath != null && !customMapPath.trim().isEmpty()) {
            // Try to load from custom file path
            Path customPath = Paths.get(customMapPath);
            if (Files.exists(customPath)) {
                log.info("Loading map from custom path: {}", customMapPath);
                lines = Files.readAllLines(customPath);
            } else {
                log.warn("Custom map file not found at: {}. Falling back to bundled map.", customMapPath);
                lines = loadBundledMap();
            }
        } else {
            // Load from bundled resource
            log.info("Loading bundled map from resources");
            lines = loadBundledMap();
        }

        return processMapLines(lines);
    }

    /**
     * Legacy method for backward compatibility
     */
    public static GameMap convertAsciiMapToGameMap() throws IOException {
        return convertAsciiMapToGameMap(null);
    }

    /**
     * Loads the bundled map.ascii from resources
     */
    private static List<String> loadBundledMap() throws IOException {
        try (InputStream inputStream = MapConverter.class.getClassLoader().getResourceAsStream("map.ascii")) {
            if (inputStream == null) {
                throw new IOException("Bundled map.ascii not found in resources");
            }

            List<String> lines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
            return lines;
        }
    }

    /**
     * Processes the map lines and converts them to GameMap
     */
    private static GameMap processMapLines(List<String> lines) throws IOException {
        // Remove header and footer lines with coordinates
        List<String> mapLines = lines.subList(1, lines.size() - 1);

        // Calculate dimensions
        int height = mapLines.size();
        // Calculate width based on the first line
        String sampleLine = mapLines.getFirst().replaceAll("\\b\\d+\\b", "");
        int width = sampleLine.trim().split("\\s+").length;

        // Initialize 2D array
        StarSystem[][] starSystems = new StarSystem[width][height];

        List<Coordinate> spawnableStations = new ArrayList<>();

        for (int y = 0; y < mapLines.size(); y++) {
            log.debug("Raw y: {}", y);
            String line = mapLines.get(y);
            String[] tiles = line.trim().split("\\s+");
            // Skip the row number at start and end
            for (int x = 0; x < tiles.length; x++) {
                log.debug("Raw x: {}", x);
                char tile = tiles[x].charAt(0);

                // Convert ASCII y-coordinate to game coordinate (flip y-axis)
                int gameY = mapLines.size() - 1 - y;
                int gameX = x - 1; // Adjust for row number

                StarSystem starSystem = createStarSystem(tile, gameX, gameY);
                if (starSystem != null) { // Check if that character was actually parsable
                    log.debug("Setting x: {}, y: {} to {}", gameX, gameY, starSystem.getType());
                    starSystems[gameX][gameY] = starSystem; // Direct array assignment
                    if (starSystem.spaceStation()!= null && starSystem.spaceStation().allowsRobotSpawns()){
                        spawnableStations.add(starSystem.coordinate());
                    }
                }
            }
        }

        return new GameMap(
                UUID.randomUUID(),
                "Converted ASCII Map",
                "Map converted from ASCII representation",
                new Coordinate(width - 1, height - 1),
                starSystems,
                spawnableStations.toArray(Coordinate[]::new)
        );
    }

    private static StarSystem createStarSystem(char tile, int x, int y) {
        Coordinate coordinate = new Coordinate(x, y);
        Integer gravity = 1; // Default gravity

        return switch (tile) {
            case 'S' -> new StarSystem(
                    coordinate,
                    gravity,
                    new SpaceStation(true), // Spawnable stations
                    null,
                    false,
                    false
            );
            case 'N' -> new StarSystem(
                    coordinate,
                    gravity,
                    new SpaceStation(false), // Neutral stations (no spawning)
                    null,
                    false,
                    false
            );
            case 'B' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    new Mine(UUID.randomUUID(), ResourceType.BIO_MATTER),
                    false,
                    false
            );
            case 'C' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    new Mine(UUID.randomUUID(), ResourceType.CRYO_GAS),
                    false,
                    false
            );
            case 'D' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    new Mine(UUID.randomUUID(), ResourceType.DARK_MATTER),
                    false,
                    false
            );
            case 'I' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    new Mine(UUID.randomUUID(), ResourceType.ION_DUST),
                    false,
                    false
            );
            case 'P' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    new Mine(UUID.randomUUID(), ResourceType.PLASMA_CORES),
                    false,
                    false
            );
            case 'X' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    null,
                    true, // Black hole
                    false
            );
            case '█' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    null,
                    false,
                    true // Void system (intraversible)
            );
            case '.' -> new StarSystem(
                    coordinate,
                    gravity,
                    null,
                    null,
                    false,
                    false // Empty traversible system
            );
            default -> null; // Skip unknown tiles
        };
    }
}
