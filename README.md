# Lab 04: Spark Streaming with Kafka, MongoDB and PySpark

This project implements a real-time ETL pipeline for cryptocurrency price analysis using:

* **Kafka 4.x (KRaft mode)**
* **PySpark**
* **MongoDB**
* **confluent_kafka** for high-performance Kafka client
* All packaged with **Docker + Docker Compose**

---

## Project Structure

```
.
├── docker-compose.yml          # Docker services configuration
├── Dockerfile.pyspark         # Docker image for Spark
├── README.md                  # Guide
├── docs/
│   └── Report.pdf            # Project report
└── src/                      # Main source code
    ├── .env                  # Environment variables
    ├── reset.sh             # Docker reset script
    ├── Extract/             # Extract stage
    │   ├── 22120384.py     # Extract executable file
    │   └── code/
    │       ├── __init__.py
    │       └── extract.py   # WebSocket extraction logic
    ├── Transform/           # Transform stage
    │   ├── 22120384_moving.py    # Moving executable file
    │   ├── 22120384_zscore.py    # Z-Score executable file
    │   └── code/
    │       ├── __init__.py
    │       ├── transform_moving.py  # Moving statistics
    │       └── transform_zscore.py  # Z-Score
    └── Load/                # Load stage
        ├── 22120384.py     # Load executable file
        └── code/
            ├── __init__.py
            └── load.py      # MongoDB loading logic
```

---

## Prerequisites

Make sure you have the following installed:

| Tool           | Version           | Check command                                          |
| -------------- | ----------------- | ------------------------------------------------------ |
| Docker         | >= 20.x           | `docker --version`                                     |
| Docker Compose | v2.x or legacy v1 | `docker compose version` or `docker-compose --version` |

> If you have Docker Desktop installed, prefer using `docker compose` (no hyphen).
> `docker-compose` (with hyphen) is deprecated.

---

## 1. Build Docker Images

From the root of the project (where `docker-compose.yml` is located), run:

### For **new** Docker Compose (v2+):

```bash
docker compose build
```

### For **legacy** Docker Compose (v1.x):

```bash
docker-compose build
```

---

## 2. Start All Services

This will start Kafka (in KRaft mode), MongoDB, and your Spark container:

```bash
docker compose up -d
```

> Use `docker-compose up -d` if you're using the legacy version.

---

## 3. Check Containers

Make sure all containers are running:

```bash
docker ps
```

You should see:

* `kafka`
* `mongodb`
* `spark`

---

## 4. Access Spark Container & Run ETL Pipeline

To open a shell inside your Spark container:

```bash
docker exec -it spark bash
```

### Run Extract (Data Collection)

```bash
# Inside Spark container
cd /app/Extract
python3 22120384.py
```

**Function:** Collects cryptocurrency price data from Binance WebSocket and sends to Kafka topic `crypto_prices`.

### Run Transform (Data Processing)

#### Transform Moving Statistics:
```bash
# Inside Spark container
cd /app/Transform
spark-submit 22120384_moving.py
```

**Function:** Calculates moving average and standard deviation with sliding windows (30s, 1m, 5m, 15m, 30m, 1h).

#### Transform Z-Score:
```bash
# Inside Spark container
cd /app/Transform
spark-submit 22120384_zscore.py
```

**Function:** Calculates Z-Score based on moving statistics to detect price anomalies.

### Run Load (Data Storage)

```bash
# Inside Spark container
cd /app/Load
spark-submit 22120384.py
```

**Function:** Stores Z-Score data into MongoDB with upsert operations.

---

## 5. Run Complete Pipeline

To run the entire ETL pipeline, open **4 separate terminals**:

### Terminal 1 - Extract:
```bash
docker exec -it spark bash
cd /app/Extract
python3 22120384.py
```

### Terminal 2 - Transform Moving:
```bash
docker exec -it spark bash
cd /app/Transform
spark-submit 22120384_moving.py
```

### Terminal 3 - Transform Z-Score:
```bash
docker exec -it spark bash
cd /app/Transform
spark-submit 22120384_zscore.py
```

### Terminal 4 - Load:
```bash
docker exec -it spark bash
cd /app/Load
spark-submit 22120384.py
```

---

## Check MongoDB

You can connect to MongoDB directly:

```bash
docker exec -it mongodb mongosh
```

Or use **MongoDB Compass**:

* Host: `localhost`
* Port: `27017`

### Check data:
```javascript
// Inside mongosh
use crypto_analytics
db.zscore_data.find().limit(5).pretty()
```

---

## Reset Data

To reset all data and start fresh:

```bash
# Inside Spark container
cd /app
bash reset.sh
```

---

## Notes & Troubleshooting

* Make sure you're in the **project root folder** when running build commands.

* If you get this error:

  ```
  COPY ./src /app: "/src": not found
  ```

  ➔ It means you're running `docker compose` inside `src/`. Go one level up and try again.

* You can ignore this warning safely:

  ```
  WARN[0000] ... the attribute `version` is obsolete ...
  ```

* If Kafka topics haven't been created, the pipeline will automatically create them.

---

## Kafka Topics

The pipeline uses the following Kafka topics:

* `btc-price` - Raw price data from Extract
* `btc-price-moving` - Moving statistics from Transform Moving
* `btc-price-zscore` - Z-Score data from Transform Z-Score
