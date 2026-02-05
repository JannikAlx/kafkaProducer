package de.microservicedungeon.mock.model.map;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Getter
@AllArgsConstructor
public class GameMap{
    private final UUID id;
    private final String name;
    private final String description;
    private final Coordinate topRight;
    private final StarSystem[][] starSystems;
    private final Coordinate[] spawnableSpaceStations;
    private final Random random = new Random();
    // Add utility methods for convenient access
    public StarSystem getStarSystem(int x, int y) {
        if (x >= 0 && x < getWidth() && y >= 0 && y < getHeight()) {
            return starSystems[x][y];
        }
        return null;
    }

    public StarSystem getStarSystem(Coordinate coordinate) {
        return getStarSystem(coordinate.getX(), coordinate.getY());
    }

    public int getWidth() {
        return topRight.getX() + 1;
    }

    public int getHeight() {
        return topRight.getY() + 1;
    }

    // For backward compatibility and iteration needs
    public List<StarSystem> getAllStarSystems() {
        List<StarSystem> allSystems = new ArrayList<>();
        for (int y = 0; y < getHeight(); y++) {
            for (int x = 0; x < getWidth(); x++) {
                StarSystem system = starSystems[x][y];
                if (system != null) {
                    allSystems.add(system);
                }
            }
        }
        return allSystems;
    }

    public boolean isDirectionReachable(Direction direction, Coordinate startingCoordinate){
        var dest = startingCoordinate.move(direction);
        // Check boundaries first
        if (dest.getX() < 0 || dest.getX() >= getWidth() || dest.getY() < 0 || dest.getY() >= getHeight()) {
            return false;
        }
        // Check if destination system exists and is not a void system (void systems are intraversable)
        StarSystem destSystem = starSystems[dest.getX()][dest.getY()];
        return destSystem != null && !destSystem.voidSystem();
    }

    public Coordinate getRandomSpawnableStation(){
        return spawnableSpaceStations[random.nextInt(spawnableSpaceStations.length)];
    }
}
