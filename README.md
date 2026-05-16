Real-time-hospot-monitoring

A real-time network monitoring and streaming pipeline built using Kafka, Python, Linux networking tools, and Databricks Structured Streaming.

Project Overview

This project captures internet usage activity from connected devices and streams the events into Kafka for real-time analytics processing.

The pipeline collects:

Connection events
Disconnection events
Bandwidth usage logs
DNS/domain activity logs
Anomaly detection events

All events are transformed into structured JSON format and streamed through Kafka topics hosted on Aiven Cloud.

Databricks Structured Streaming consumes the Kafka streams and stores the processed data into Delta Lake Bronze tables for further analytics and monitoring.

Architecture
Connected Devices
        ↓
Windows Network Environment
        ↓
Python Kafka Producers
        ↓
Kafka Topics (Aiven Cloud)
        ↓
Databricks Structured Stre

echnologies Used
Python
Apache Kafka
PySpark
Databricks
Delta Lake
Windows Networking Tools
GitHub
CI/CD

Kafka Topics

The following Kafka topics are used in the project:

connection_events
bandwidth_events
dns_activity_events
anomaly_events

Features
Real-time event streaming
Secure Kafka SSL authentication
Structured JSON event generation
Databricks streaming integration
Delta Lake Bronze storage
Fault-tolerant checkpointing
Scalable streaming architecture

Project Structure
Real-time-hospot-monitoring/
│
├── producers/
│   ├── connection_producer.py
│   ├── bandwidth_producer.py
│   ├── dns_producer.py
│   └── anomaly_producer.py
│
├── databricks/
│   └── kafka_stream.py
│
├── .github/
│   └── workflows/
│
├── requirements.txt
├── README.md
└── .gitignore

Databricks Streaming

The streaming pipeline performs:

Kafka topic subscription
Real-time stream ingestion
Event transformation
Memory sink streaming
Delta Bronze layer storage

Security

Kafka communication is secured using:

SASL_SSL
PEM certificates
Client authentication
SSL truststore configuration

Future Enhancements
Silver and Gold Delta layers
Real-time dashboards
Alerting system
ML-based anomaly detection
Automated CI/CD deploymentaming
        ↓
Delta Lake Bronze Layer
