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
        MapConverter mapConverter = new MapConverter();
        var result = mapConverter.convertAsciiMapToGameMap("input/map.ascii");
        result.getAllStarSystems().forEach((it) -> {
            log.info("Coordinate: {}, type: {}", it.coordinate().toString(), it.getType());
        });

        Assertions.assertEquals(18, result.getStarSystems().length);
        Assertions.assertEquals(18*18,result.getAllStarSystems().size());
    }

    @Test
    void convertAsciiMapToGameMap2() throws IOException {
        MapConverter mapConverter = new MapConverter();
        var result = mapConverter.convertAsciiMapToGameMap("input/map2.ascii");
        result.getAllStarSystems().forEach((it) -> {
            log.info("Coordinate: {}, type: {}", it.coordinate().toString(), it.getType());
        });

        Assertions.assertEquals(12, result.getStarSystems().length);
        Assertions.assertEquals(12*12,result.getAllStarSystems().size());
    }


    @Test
    void convertAsciiMapToJson() throws IOException {
        MapConverter mapConverter = new MapConverter();
        var result = mapConverter.convertAsciiMapToGameMap("input/map.ascii");
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter writer = objectMapper.writer();
        log.info(writer.writeValueAsString(result));
    }
}