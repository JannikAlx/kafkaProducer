package de.microservicedungeon.mock.eventing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

/**
 * A JSON {@code Strategy} for object serialization.
 *
 * @since 20.08.2025
 * @author Daniel Köllgen, Jannik Alexander
 */
@Component
@Validated
public class JsonSerializationStrategy {

    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    /**
     * Serializes the given object.
     *
     * @param json a JSON serializable object. Must not be null.
     * @return the serialized object as an array of bytes
     * @throws RuntimeException on serialization failure
     */
    public <T> byte[] serialize(@NotNull T json) {
        try {
            return MAPPER.writeValueAsBytes(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
