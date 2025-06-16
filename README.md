# Cryptocurrency Price Streaming ETL Pipeline

A real-time Extract-Transform-Load (ETL) pipeline for cryptocurrency price streaming using Apache Kafka, Apache Spark, and MongoDB. This project streams BTC/USDT price data from Binance WebSocket API, processes it using Spark Streaming, and stores results in MongoDB.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- **Docker Desktop** v20.10.13 or higher
- **Java 17** (required for Spark)
- **Python 3.12**

## Architecture

```
Binance WebSocket API → Kafka → Spark Streaming → MongoDB
                                    ↓
                          Moving Averages & Z-Score
```

The pipeline consists of:
- **Extract**: Real-time price streaming from Binance WebSocket API
- **Transform**: Moving averages and Z-score calculations using Spark Streaming
- **Load**: Data persistence to MongoDB collections

## Project Structure

```
22120384/
├── docker-compose.yml          # Docker services configuration
├── Pipfile                     # Python dependencies
├── Pipfile.lock               # Locked dependencies
├── README.md                  # This file
├── .gitignore                 # Git ignore rules
├── docs/
│   └── Case Study.pdf         # Project documentation
└── src/
    ├── .env                   # Environment variables
    ├── Extract/
    │   ├── 22120384.py        # Extract entry point
    │   └── code/
    │       ├── __init__.py
    │       └── extract.py     # Price extraction from Binance
    ├── Transform/
    │   ├── 22120384_moving.py # Moving averages entry point
    │   ├── 22120384_zscore.py # Z-score entry point
    │   └── code/
    │       ├── __init__.py
    │       ├── transform_moving.py  # Moving averages implementation
    │       └── transform_zscore.py  # Z-score implementation
    └── Load/
        ├── 22120384.py        # Load entry point
        └── code/
            ├── __init__.py
            └── load.py        # Data loading to MongoDB
```

## Installation

### 1. Java 17 Installation

#### Linux (Ubuntu/Debian)
```bash
# Update package index and install OpenJDK 17
sudo apt update && sudo apt install -y openjdk-17-jdk

# Configure environment variables
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Verify installation
java -version
echo $JAVA_HOME
```

#### Windows
1. Download OpenJDK 17
2. Run the installer and follow the setup wizard
3. Add Java to your system PATH:
   - Open System Properties → Advanced → Environment Variables
   - Add `JAVA_HOME` pointing to your Java installation directory
   - Add `%JAVA_HOME%\bin` to your PATH variable.
4. Verify installation by opening Command Prompt and running:
   ```cmd
   java -version
   echo %JAVA_HOME%
   ```

### 2. Python Environment Setup

```bash
# Install pipenv
pip install pipenv

# Install project dependencies
pipenv install

# Activate virtual environment
pipenv shell
```

### 3. Docker Services

```bash
# Start all services (Kafka, Spark, MongoDB)
docker compose up -d

# Verify services are running
docker compose ps
```

### 4. Kafka Topics Creation

Create the required Kafka topics:

```bash
# Create btc-price topic
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic btc-price \
  --partitions 1 \
  --replication-factor 1

# Create btc-price-moving topic
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic btc-price-moving \
  --partitions 1 \
  --replication-factor 1

# Create btc-price-zscore topic
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic btc-price-zscore \
  --partitions 1 \
  --replication-factor 1

# Verify topics are created
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

## Configuration

### Project Source Configuration

**If you obtained this project from OneDrive**: The `.env` file and `docs` folder are already included, so no additional configuration is needed.

**If you cloned this project from GitHub**: You need to configure the `src/.env` file as follows:

```bash
# Create the .env file
touch src/.env
```

Add the following configuration to `src/.env`:

```env
# =====================================
#             Extract Stage
# =====================================

# Binance Configuration
BINANCE_WS_BASE_URL=wss://stream.binance.com:9443/ws/  # Binance WebSocket base URL
BINANCE_SYMBOL=BTCUSDT                                 # Trading pair symbol
BINANCE_STREAM_TYPE=@trade                             # Stream type for real-time trades

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092                 # Kafka broker address
KAFKA_TOPIC_BTC_PRICE=btc-price                       # Topic for raw price data
KAFKA_CLIENT_ID_EXTRACT=btc-price-extract             # Client ID for extract stage

# Extract Timing Configuration
EXTRACT_RESOLUTION_MS=100                              # Data extraction interval in milliseconds

# =====================================
#             Transform Stage
# =====================================

# Kafka Configuration
KAFKA_TOPIC_BTC_PRICE_MOVING=btc-price-moving         # Topic for moving averages
KAFKA_CLIENT_ID_TRANSFORM_MOVING=btc-price-transform-moving  # Client ID for moving avg transform
KAFKA_TOPIC_BTC_PRICE_ZSCORE=btc-price-zscore         # Topic for Z-score calculations
KAFKA_CLIENT_ID_TRANSFORM_ZSCORE=btc-price-transform-zscore  # Client ID for Z-score transform
KAFKA_CONNECTOR=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0  # Spark Kafka connector

# Spark Configuration
SPARK_MASTER_URL=local[*]                              # Spark master URL (local mode)
SPARK_MOVING_APP_NAME=BTCPriceMovingStatistics         # App name for moving averages
SPARK_ZSCORE_APP_NAME=BTCPriceZScoreCalculation        # App name for Z-score calculation
SPARK_LOG_LEVEL=ERROR                                  # Spark logging level
SPARK_TIMEZONE=UTC                                     # Timezone for Spark operations
SPARK_HADOOP_FS_DEFAULT=file:///                       # Hadoop filesystem default
SPARK_STATEFUL_OPERATOR_CHECK=false                    # Stateful operator checkpoint
SPARK_WATERMARK_DELAY=10 seconds                       # Watermark delay for late data
SPARK_MAX_OFFSETS_PER_TRIGGER=100                      # Max offsets per trigger

# =====================================
#             Load Stage
# =====================================

# MongoDB Configuration
MONGO_URI=mongodb://admin:password123@localhost:27017  # MongoDB connection URI
MONGO_DB=cryptocurrency_prices                         # MongoDB database name
MONGO_COLLECTION_PREFIX=btc-price-zscore-             # Collection name prefix
MONGO_CONNECTOR=org.mongodb.spark:mongo-spark-connector_2.13:10.4.0  # MongoDB Spark connector

# Kafka Configuration  
KAFKA_CLIENT_ID_LOAD=btc-price-load                   # Client ID for load stage

# Spark Configuration
SPARK_LOAD_APP_NAME=BTCPriceLoadToMongoDB             # App name for load operations
```

### Key Configuration Sections:

- **Binance WebSocket**: Stream configuration for BTC/USDT real-time data
- **Kafka**: Bootstrap servers, topic names, and client IDs for different stages
- **Timing**: Extract resolution (default: 100ms for high-frequency data)
- **MongoDB**: Connection strings and authentication credentials

## Usage

### Prerequisites for Each Run
For each component, open a new terminal and run:
```bash
pipenv shell
cd src
```

### 1. Extract Stage
```bash
python3 Extract/code/extract.py
```
This streams real-time BTC/USDT prices from Binance WebSocket and publishes to Kafka topic `btc-price`.

### 2. Transform Stage
Run both transformation processes in separate terminals:

**Moving Averages:**
```bash
python3 Transform/22120384_moving.py
```

**Z-Score Calculations:**
```bash
python3 Transform/22120384_zscore.py
```

### 3. Load Stage
```bash
python3 Load/22120384.py
```
This consumes processed data from Kafka and stores it in MongoDB.

## Monitoring

### Kafka Topic Monitoring

View messages in real-time:

```bash
# Monitor raw prices
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic btc-price \
  --from-beginning \
  --max-messages 10

# Monitor moving averages
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic btc-price-moving \
  --from-beginning \
  --max-messages 10

# Monitor Z-scores
docker exec -it kafka-broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic btc-zscore \
  --from-beginning \
  --max-messages 10
```

### MongoDB Monitoring

Connect to MongoDB to view stored data:
```bash
docker exec -it mongodb mongosh -u admin -p password123
```

## Troubleshooting

### Common Issues

1. **Java Version Conflicts**: Ensure Java 17 is installed and `JAVA_HOME` is properly set
2. **Docker Services Not Starting**: Check if ports 9092, 8080, 27017 are available
3. **Kafka Topics Not Created**: Topics are auto-created when first message is published
4. **WebSocket Connection Issues**: Check internet connectivity and Binance API status

### Log Monitoring

```bash
# View Docker service logs
docker compose logs -f kafka-broker
docker compose logs -f spark-master
docker compose logs -f mongodb
```
