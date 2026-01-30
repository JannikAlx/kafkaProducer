package com.example.kafka.model;

import java.util.Objects;

/**
 * Represents a position on the game map with x,y coordinates.
 * Provides utility methods for calculating new positions based on direction.
 */
public class Position {
    private final int x;
    private final int y;

    public Position(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /**
     * Calculate a new position by moving in the specified direction.
     *
     * @param direction the compass direction to move
     * @return a new Position object representing the destination
     */
    public Position move(Direction direction) {
        return switch (direction) {
            case N -> new Position(x, y - 1);
            case NE -> new Position(x + 1, y - 1);
            case E -> new Position(x + 1, y);
            case SE -> new Position(x + 1, y + 1);
            case S -> new Position(x, y + 1);
            case SW -> new Position(x - 1, y + 1);
            case W -> new Position(x - 1, y);
            case NW -> new Position(x - 1, y - 1);
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

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Position position = (Position) o;
        return x == position.x && y == position.y;
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
