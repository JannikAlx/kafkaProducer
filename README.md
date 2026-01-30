# Kafka JSON Producer

A Spring Boot application that reads JSON files and publishes them to Apache Kafka using the spring-kafka producer API.

## Features

- Reads JSON files from a configurable directory
- Parses JSON structure with `topic`, `headers` and `payload` fields
- Publishes payload to Kafka as byte arrays
- Uses topic from JSON Structure
- Transfers headers to Kafka record headers:
  - `event_type`: String value from input file
  - `sequence_id`: Long value serialized as 8 bytes, auto-incremented on repeated sends
- Uses String serializer for keys and ByteArraySerializer for values
- Supports repeating message publication via CLI argument
- Configurable Kafka broker, and input directory

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- Apache Kafka running (default: `localhost:9092`)

## Configuration

Edit `src/main/resources/application.yml` to configure:

```yaml
kafka:
  bootstrap-servers: localhost:9092  # Kafka broker address
  topic: json-events                 # Default topic name

app:
  input-directory: ./input           # Directory containing JSON files
  topic-name: ${kafka.topic}         # Topic name (configurable)
```

## JSON File Format

Input JSON files must follow this structure:

```json
{
  "headers": {
    "event_type": "robot.delivered-resource",
    "sequence_id": 1
  },
  "payload": {
    "gameId": "5f3c2b1a-9d8e-4c7b-a6f1-2d3e4f5a6b7c",
    "playerId": "a1b2c3d4-5678-4abc-9def-0123456789ab",
    "resourceType": "DARK_MATTER",
    "amount": 42
  }
}
```

- **headers.event_type**: (Required) Event type identifier
- **headers.sequence_id**: (Optional) Sequence number, will be incremented on repeated sends
- **payload**: (Required) The actual data to send to Kafka

## Building the Application

```bash
mvn clean package
```

## Running the Application

### Basic Usage (Send each file once)

```bash
mvn spring-boot:run
```

Or with the JAR:

```bash
java -jar target/kafka-producer-1.0.0.jar
```

### With Repeat Count

To send each file multiple times, pass the repeat count as an argument:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=5
```

Or with the JAR:

```bash
java -jar target/kafka-producer-1.0.0.jar 5
```

This will send each JSON file 5 times, incrementing the `sequence_id` header for each send.

## How It Works

1. **File Reading**: The application scans the configured input directory for `.json` files
2. **Parsing**: Each file is parsed to extract:
   - `event_type` from headers
   - `sequence_id` from headers (if present)
   - `payload` object (converted to bytes)
3. **Key Generation**: The filename (without extension) is used as the Kafka message key
4. **Header Transfer**: 
   - `event_type` is added as a string header
   - `sequence_id` is serialized as 8 bytes and added as a header
5. **Publishing**: The payload is sent to Kafka with the headers
6. **Repetition**: If a repeat count is specified, the message is sent multiple times with `sequence_id` incrementing by 1 each time

## Example

Place `example.json` in the `./input` directory and run:

```bash
java -jar target/kafka-producer-1.0.0.jar 3
```

This will:
- Read `example.json`
- Send the payload 3 times to Kafka
- First send: `sequence_id = 1`
- Second send: `sequence_id = 2`
- Third send: `sequence_id = 3`

## Kafka Record Structure

Each Kafka record will have:
- **Key**: String (filename without extension)
- **Value**: byte[] (JSON payload serialized to bytes)
- **Headers**:
  - `event_type`: byte[] (UTF-8 encoded string)
  - `sequence_id`: byte[] (8 bytes, long value in big-endian format)

## Troubleshooting

### No files found
- Ensure JSON files are in the configured `input-directory`
- Check that files have `.json` extension

### Connection refused
- Verify Kafka is running on the configured `bootstrap-servers`
- Check network connectivity to Kafka broker

### Missing headers or payload
- Verify JSON files follow the required structure
- Check application logs for parsing errors

## Logging

The application logs:
- Files being read and their metadata
- Kafka send operations (success/failure)
- Partition and offset information for each message
- Overall processing statistics

Log level can be adjusted in `application.yml`.
