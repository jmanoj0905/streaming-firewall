# Streaming Firewall – Kafka + Spark Structured Streaming

A real-time **streaming firewall** built using **Apache Kafka** and **Apache Spark Structured Streaming**.
The system validates incoming event logs, enforces schema and rule-based checks, and routes traffic into **clean** and **quarantine** streams for downstream consumption and monitoring.

This project focuses on **stream processing and data quality enforcement**, not batch analytics.

---

## Architecture

### High-level Flow

1. A Python producer generates both valid and malicious events.
2. Events are published to Kafka via the `firewall-logs` topic.
3. Spark Structured Streaming consumes the stream and applies firewall validation rules.
4. Events are split and routed:

   * **Valid events** → `firewall-clean`
   * **Invalid events** → `firewall-quarantine`
5. A lightweight UI consumer visualizes both streams in real time.

### Architecture Diagram

[https://raw.githubusercontent.com/jmanoj0905/streaming-firewall/main/architecture.png](https://raw.githubusercontent.com/jmanoj0905/streaming-firewall/main/architecture.png)

---

## Project Structure

```
streaming-firewall/
├── consumer/
│   ├── consumer.py        # Spark streaming firewall (core logic)
│   └── ui_consumer.py     # Streamlit UI for clean vs quarantine
├── producer/
│   └── producer.py        # Event producer (good + malicious)
├── docker-compose.yml     # Kafka + Zookeeper + Spark cluster
└── event_format.txt       # Event schema contract
```

---

## Firewall Rules

An event is **quarantined** if **any** of the following conditions fail:

* `event_id` is missing or null
* `level` is not one of: `INFO`, `WARN`, `ERROR`
* `timestamp` is not valid ISO-8601
* `source` is not a known service

Each rejected event is tagged with:

* `is_valid = false`
* `reject_reason` indicating the violated rule

---

## Kafka Topics

| Topic                 | Purpose                      |
| --------------------- | ---------------------------- |
| `firewall-logs`       | Raw incoming events          |
| `firewall-clean`      | Validated clean events       |
| `firewall-quarantine` | Rejected or malicious events |

---

## How to Run

### 1. Start Infrastructure

```bash
docker compose up -d
```

### 2. Create Kafka Topics

```bash
docker exec -it kafka kafka-topics \
  --create --topic firewall-logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics \
  --create --topic firewall-clean \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics \
  --create --topic firewall-quarantine \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 3. Run Spark Firewall

```bash
docker exec -it spark-master bash
```

Inside the container:

```bash
spark-submit /opt/spark/work/consumer/consumer.py
```

### 4. Run Producer (Host Machine)

```bash
python3 producer/producer.py
```

### 5. Run UI (Host Machine)

```bash
streamlit run consumer/ui_consumer.py
```

