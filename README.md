# Kafka Producer with Bundled Map Configuration

## Overview

This project now supports both bundled and custom map configurations:

1. **Bundled Map**: The `map.ascii` file is packaged within the JAR and used as the default
2. **Custom Map**: You can specify a custom map file path using the `--map` CLI argument

## Packaging the Map

The `map.ascii` file is located in `src/main/resources/map.ascii` and will be automatically included in the JAR when you build the project.

## Building the JAR

```bash
# Using Maven wrapper (recommended)
./mvnw clean package

# Using installed Maven
mvn clean package
```

This creates a self-contained JAR file in the `target/` directory.

## Running the Application

### Using Bundled Map (Default)
```bash
java -jar target/kafka-producer-1.0.0.jar
```

### Using Custom Map
```bash
java -jar target/kafka-producer-1.0.0.jar --map /path/to/your/custom.ascii
java -jar target/kafka-producer-1.0.0.jar --map ./custom-map.ascii
```

### With Additional Options
```bash
# Custom delay and custom map
java -jar target/kafka-producer-1.0.0.jar --delay 100 --map ./custom-map.ascii

# Custom delay with bundled map
java -jar target/kafka-producer-1.0.0.jar --delay 50
```

## Map File Format

The map file should follow this ASCII format:
- Header and footer with coordinates
- Each row numbered on both sides
- Supported tile types:
  - `S` - Spawnable space stations
  - `N` - Neutral space stations (no spawning)
  - `B` - Bio matter mines
  - `C` - Cryo gas mines
  - `D` - Dark matter mines
  - `I` - Ion dust mines
  - `P` - Plasma core mines
  - `X` - Black holes
  - `█` - Void systems (intraversible)
  - `.` - Empty traversible systems

## Implementation Details

### MapConverter Changes
- Added support for loading from classpath resources
- Fallback mechanism: tries custom path first, then bundled resource
- Error handling for missing files

### GameManager Changes
- Added `setCustomMapPath()` method
- Updated to use new MapConverter API

### GameCliService Changes
- Added `--map` CLI argument parsing
- Updated help text and welcome message
- Proper configuration display

## File Structure
```
src/main/resources/
├── application.yml
└── map.ascii          # Bundled map file

target/
└── kafka-producer-1.0.0.jar  # Self-contained JAR
```

## Testing the Configuration

1. **Test bundled map**: Run without `--map` argument
2. **Test custom map**: Create a custom ASCII file and use `--map` argument
3. **Test fallback**: Use `--map` with non-existent file (should fall back to bundled)
