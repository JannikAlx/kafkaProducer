package de.microservicedungeon.mock.config;

import org.springframework.context.annotation.Configuration;

/**
 * Kafka configuration.
 *
 * Spring Boot auto-configures ProducerFactory and KafkaTemplate based on
 * `spring.kafka.*` properties.
 */
@Configuration
public class KafkaProducerConfig {
    // Intentionally empty: rely on Spring Boot's Kafka auto-configuration.
}
