package de.microservicedungeon.mock.model.map;

import lombok.Getter;

import java.util.Objects;

/**
 * Represents a position on the game map with x,y coordinates.
 * Provides utility methods for calculating new positions based on direction.
 */
@Getter
public class Coordinate {
    private final int x;
    private final int y;

    public Coordinate(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /**
     * Calculate a new position by moving in the specified direction.
     *
     * @param direction the compass direction to move
     * @return a new Position object representing the destination
     */
    public Coordinate move(Direction direction) {
        return switch (direction) {
            case N -> new Coordinate(x, y - 1);
            case NE -> new Coordinate(x + 1, y - 1);
            case E -> new Coordinate(x + 1, y);
            case SE -> new Coordinate(x + 1, y + 1);
            case S -> new Coordinate(x, y + 1);
            case SW -> new Coordinate(x - 1, y + 1);
            case W -> new Coordinate(x - 1, y);
            case NW -> new Coordinate(x - 1, y - 1);
        };
    }

    /**
     * Check if this position is within the bounds of a map.
     *
     * @param width  map width
     * @param height map height
     * @return true if position is valid, false otherwise
     */
    public boolean isWithinBounds(int width, int height) {
        return x >= 0 && x < width && y >= 0 && y < height;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Coordinate coordinate = (Coordinate) o;
        return x == coordinate.x && y == coordinate.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
