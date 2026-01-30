package com.example.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@Service
public class JsonFileReaderService {

    @Value("${app.input-directory}")
    private String inputDirectory;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public List<JsonFileData> readJsonFiles() {
        List<JsonFileData> fileDataList = new ArrayList<>();
        Path dirPath = Paths.get(inputDirectory);

        if (!Files.exists(dirPath)) {
            log.warn("Input directory does not exist: {}", inputDirectory);
            return fileDataList;
        }

        if (!Files.isDirectory(dirPath)) {
            log.error("Input path is not a directory: {}", inputDirectory);
            return fileDataList;
        }

        try (Stream<Path> paths = Files.walk(dirPath, 1)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".json"))
                    .forEach(path -> {
                        try {
                            byte[] content = Files.readAllBytes(path);
                            String fileName = path.getFileName().toString();

                            // Parse JSON to extract headers and payload
                            JsonNode rootNode = objectMapper.readTree(content);
                            JsonNode headersNode = rootNode.get("headers");
                            JsonNode payloadNode = rootNode.get("payload");
                            JsonNode topicNode = rootNode.get("topic");

                            if (headersNode == null || payloadNode == null || topicNode == null) {
                                log.error("File {} missing 'headers', 'payload' or 'topic' field", fileName);
                                return;
                            }

                            // Extract event_type
                            String eventType = headersNode.has("event_type")
                                    ? headersNode.get("event_type").asText()
                                    : null;

                            // Extract sequence_id (optional)
                            Long sequenceId = headersNode.has("sequence_id")
                                    ? headersNode.get("sequence_id").asLong()
                                    : null;

                            String topic = topicNode.asText();

                            // Convert payload to bytes
                            byte[] payloadBytes = objectMapper.writeValueAsBytes(payloadNode);

                            fileDataList.add(new JsonFileData(fileName, eventType, sequenceId, payloadBytes, topic));
                            log.info("Read file: {} - event_type: {}, sequence_id: {}, payload: {} bytes",
                                    fileName, eventType, sequenceId, payloadBytes.length);
                        } catch (IOException e) {
                            log.error("Error reading file: {}", path, e);
                        }
                    });
        } catch (IOException e) {
            log.error("Error walking directory: {}", inputDirectory, e);
        }

        log.info("Total JSON files read: {}", fileDataList.size());
        return fileDataList;
    }

    public record JsonFileData(String fileName, String eventType, Long sequenceId, byte[] payloadBytes, String topic) {
    }
}
