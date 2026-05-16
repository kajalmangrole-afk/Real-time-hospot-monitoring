import os
import time
from collections import defaultdict
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from producer.kafka_client import  get_producer, TOPICS
from schemas import now_iso
from validators import validate_event


def main():
    producer = get_producer()
    topics = [TOPICS["connection"], TOPICS["bandwidth"], TOPICS["dns"]]
    consumer = get_consumer(topics, group_id="hotspot-anomaly-group")

    dns_threshold = int(os.getenv("ANOMALY_DNS_THRESHOLD", "100"))
    bandwidth_threshold = int(os.getenv("ANOMALY_BANDWIDTH_THRESHOLD", "104857600"))
    device_counts = defaultdict(int)

    print("Anomaly Detector started...")

    for record in consumer:
        event = record.value
        event_type = event.get("event_type")
        device_ip = event.get("device_ip")

        if event_type == "DNS_QUERY" and device_ip:
            device_counts[device_ip] += 1
            if device_counts[device_ip] > dns_threshold:
                anomaly = {
                    "event_type": "ANOMALY_ALERT",
                    "device_ip": device_ip,
                    "alert_type": "DNS_VOLUME_SPIKE",
                    "description": f"High DNS query volume for {device_ip}",
                    "source_event": event,
                    "timestamp": now_iso()
                }
                validate_event(anomaly)
                producer.send(TOPICS["anomaly"], value=anomaly)
                producer.flush()
                print("Anomaly event sent:", anomaly)

        if event_type == "BANDWIDTH_USAGE" and device_ip:
            if event.get("bytes_sent", 0) > bandwidth_threshold or event.get("bytes_received", 0) > bandwidth_threshold:
                anomaly = {
                    "event_type": "ANOMALY_ALERT",
                    "device_ip": device_ip,
                    "alert_type": "BANDWIDTH_SPIKE",
                    "description": f"High bandwidth usage for {device_ip}",
                    "source_event": event,
                    "timestamp": now_iso()
                }
                validate_event(anomaly)
                producer.send(TOPICS["anomaly"], value=anomaly)
                producer.flush()
                print("Anomaly event sent:", anomaly)


if __name__ == "__main__":
    main()
