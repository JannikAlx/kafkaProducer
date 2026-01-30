package com.example.kafka.model;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Represents a tile on the game map.
 * Each tile has a type, position, and may contain additional properties
 * depending on the tile type (e.g., mines for planets, gravity, space
 * stations).
 */
public class MapTile {
    private final TileType type;
    private final Position position;
    private final Integer gravity; // Only for planets
    private final Boolean allowsRobotSpawns; // Only for planets with space stations
    private final List<UUID> mineIds; // Only for planets with mines

    private MapTile(Builder builder) {
        this.type = builder.type;
        this.position = builder.position;
        this.gravity = builder.gravity;
        this.allowsRobotSpawns = builder.allowsRobotSpawns;
        this.mineIds = builder.mineIds != null ? new ArrayList<>(builder.mineIds) : new ArrayList<>();
    }

    public TileType getType() {
        return type;
    }

    public Position getPosition() {
        return position;
    }

    public Integer getGravity() {
        return gravity;
    }

    public Boolean getAllowsRobotSpawns() {
        return allowsRobotSpawns;
    }

    public List<UUID> getMineIds() {
        return new ArrayList<>(mineIds);
    }

    public boolean isPlanet() {
        return type == TileType.PLANET;
    }

    public boolean isVoid() {
        return type == TileType.VOID;
    }

    public boolean isBlackHole() {
        return type == TileType.BLACK_HOLE;
    }

    /**
     * Builder for creating MapTile instances with different configurations.
     */
    public static class Builder {
        private final TileType type;
        private final Position position;
        private Integer gravity;
        private Boolean allowsRobotSpawns;
        private List<UUID> mineIds;

        public Builder(TileType type, Position position) {
            this.type = type;
            this.position = position;
        }

        public Builder gravity(int gravity) {
            this.gravity = gravity;
            return this;
        }

        public Builder allowsRobotSpawns(boolean allowsRobotSpawns) {
            this.allowsRobotSpawns = allowsRobotSpawns;
            return this;
        }

        public Builder mineIds(List<UUID> mineIds) {
            this.mineIds = mineIds;
            return this;
        }

        public MapTile build() {
            return new MapTile(this);
        }
    }
}
