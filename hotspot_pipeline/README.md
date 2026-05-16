# Hotspot Gateway Analytics Pipeline

This repository provides a Linux-ready hotspot gateway pipeline that captures connected device activity, bandwidth and DNS logs, and streams structured events into Kafka. A Databricks-style Structured Streaming layer ingests raw Kafka events into Bronze, Silver, and Gold tables for analytics and anomaly detection.

## Key components

- `collectors/connection_collector.py` - monitors neighbor/ARP state and emits connect/disconnect events
- `collectors/bandwidth_collector.py` - samples interface usage and generates bandwidth event summaries
- `collectors/dns_collector.py` - captures DNS queries with tshark and streams domain activity
- `producer/anomaly_detector.py` - consumes Kafka events and writes anomaly alerts
- `databricks/pipeline.py` - PySpark Bronze/Silver/Gold pipeline for stream processing
- `tests/` - pytest coverage for schema validation, parsing, transformations, and anomaly logic

## Run locally

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set Kafka environment variables if needed:
   ```bash
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export KAFKA_TOPIC_CONNECTION=connection_events
   export KAFKA_TOPIC_BANDWIDTH=bandwidth_events
   export KAFKA_TOPIC_DNS=dns_activity_events
   export KAFKA_TOPIC_ANOMALY=anomaly_events
   ```

3. Start collectors:
   ```bash
   python collectors/connection_collector.py
   python collectors/bandwidth_collector.py
   python collectors/dns_collector.py
   ```

4. Start anomaly detector:
   ```bash
   python producer/anomaly_detector.py
   ```

5. Start the bronze streaming ingestion:
   ```bash
   python databricks/pipeline.py bronze --output-dir spark_output --checkpoint-dir spark_checkpoints
   ```

6. Start the silver pipeline:
   ```bash
   python databricks/pipeline.py silver --output-dir spark_output --checkpoint-dir spark_checkpoints
   ```

7. Generate Gold analytics tables:
   ```bash
   python databricks/pipeline.py gold --output-dir spark_output --checkpoint-dir spark_checkpoints
   ```

## Testing

Run full unit tests:
```bash
pytest
```

## Notes

- The current collector implementations are designed for Linux hotspots, but they can run on a Windows host if equivalent capture tools are installed and environment variables are updated.
- All Kafka events are validated against JSON schemas before they are emitted.
- The Databricks pipeline is implemented with PySpark Structured Streaming semantics and writes Bronze/Silver/Gold parquet tables.
