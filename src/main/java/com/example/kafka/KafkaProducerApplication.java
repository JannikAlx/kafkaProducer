package com.example.kafka;

import com.example.kafka.service.JsonFileReaderService;
import com.example.kafka.service.JsonFileReaderService.JsonFileData;
import com.example.kafka.service.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class KafkaProducerApplication implements CommandLineRunner {

    private final JsonFileReaderService jsonFileReaderService;
    private final KafkaProducerService kafkaProducerService;

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("Starting Kafka Producer Application...");

        // Parse repeat count from CLI arguments
        int repeatCount = 1; // default
        if (args.length > 0) {
            try {
                repeatCount = Integer.parseInt(args[0]);
                log.info("Repeat count set to: {}", repeatCount);
            } catch (NumberFormatException e) {
                log.warn("Invalid repeat count argument '{}', using default: 1", args[0]);
            }
        }

        // Read JSON files from input directory
        List<JsonFileData> jsonFiles = jsonFileReaderService.readJsonFiles();

        if (jsonFiles.isEmpty()) {
            log.warn("No JSON files found to process");
            return;
        }

        log.info("Processing {} JSON files, {} times each", jsonFiles.size(), repeatCount);

        // Send each file to Kafka, repeated the specified number of times
        int totalSuccessCount = 0;
        int totalMessageCount = 0;

        for (JsonFileData fileData : jsonFiles) {
            // Use filename (without extension) as the key
            String key = fileData.fileName().replaceFirst("[.][^.]+$", "");

            // Get initial sequence_id
            Long currentSequenceId = fileData.sequenceId();

            // Repeat sending the message
            for (int i = 0; i < repeatCount; i++) {
                totalMessageCount++;

                // Send message with headers
                if (kafkaProducerService.sendMessageWithHeaders(
                        key,
                        fileData.topic(),
                        fileData.payloadBytes(),
                        fileData.eventType(),
                        currentSequenceId)) {
                    totalSuccessCount++;
                }

                // Increment sequence_id for next iteration (if it exists)
                if (currentSequenceId != null) {
                    currentSequenceId++;
                }
            }
        }

        log.info("Completed processing. Successfully sent {}/{} messages to Kafka",
                totalSuccessCount, totalMessageCount);
    }
}
