import json
from kafka import KafkaConsumer

# Kafka config (Aiven)
BOOTSTRAP_SERVER = "kafka-d44d5dc-mangrolekaju-90fb.d.aivencloud.com:26653"
TOPIC = "kafka-hospot"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVER,
    security_protocol="SSL",
    ssl_cafile="ca.pem",
    ssl_certfile="service.cert",
    ssl_keyfile="service.key",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# Store request count per device
device_count = {}

# Threshold for anomaly detection
THRESHOLD = 10

print("Anomaly Detector Started...")

for message in consumer:
    event = message.value

    device_ip = event.get("device_ip")
    domain = event.get("domain")

    if device_ip:
        device_count[device_ip] = device_count.get(device_ip, 0) + 1

        count = device_count[device_ip]

        print(f"Device: {device_ip} | Count: {count} | Domain: {domain}")

        # ---------------- ANOMALY RULE ----------------
        if count > THRESHOLD:
            print("ANOMALY DETECTED!")
            print({
                "device_ip": device_ip,
                "message": "High DNS activity detected",
                "count": count
            })