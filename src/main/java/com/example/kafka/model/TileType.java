package com.example.kafka.model;

/**
 * Types of tiles that can exist on the game map.
 */
public enum TileType {
    /**
     * Planet tile - can contain mines and allows robot spawning at space stations
     */
    PLANET,

    /**
     * Void tile - empty space, robots cannot move here
     */
    VOID,

    /**
     * Black hole tile - dangerous, may destroy robots that move here
     */
    BLACK_HOLE
}
