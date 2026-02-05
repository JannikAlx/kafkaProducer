package de.microservicedungeon.mock.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

@Slf4j
class MapConverterTest {

    @Test
    void convertAsciiMapToGameMap() throws IOException {
        // Test with bundled map (no custom path)
        var result = MapConverter.convertAsciiMapToGameMap();
        result.getAllStarSystems().forEach((it) -> {
            log.info("Coordinate: {}, type: {}", it.coordinate().toString(), it.getType());
        });

        Assertions.assertEquals(18, result.getStarSystems().length);
        Assertions.assertEquals(18*18,result.getAllStarSystems().size());
    }

    @Test
    void convertCustomMapToGameMap() throws IOException {
        // Test fallback when custom path doesn't exist
        var result = MapConverter.convertAsciiMapToGameMap("/nonexistent/path.ascii");

        // Should fall back to bundled map and still work
        Assertions.assertEquals(18, result.getStarSystems().length);
        Assertions.assertEquals(18*18,result.getAllStarSystems().size());
    }

    @Test
    void convertBundledMapExplicitly() throws IOException {
        // Test explicit null parameter
        var result = MapConverter.convertAsciiMapToGameMap(null);

        Assertions.assertEquals(18, result.getStarSystems().length);
        Assertions.assertEquals(18*18,result.getAllStarSystems().size());
    }
}