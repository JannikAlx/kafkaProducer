package de.microservicedungeon.mock.model.map;

import de.microservicedungeon.mock.model.ResourceType;

import java.util.UUID;

public record Mine(
        UUID id,
        ResourceType type
) {}
