package de.microservicedungeon.mock.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Jackson ObjectMapper.
 * Provides a properly configured ObjectMapper bean for JSON
 * serialization/deserialization.
 */
@Configuration
public class JacksonConfig {

    /**
     * Create and configure an ObjectMapper bean.
     * Registers JavaTimeModule for proper Java 8 date/time handling.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
