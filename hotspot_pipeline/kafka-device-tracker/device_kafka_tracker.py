from kafka import KafkaProducer
import json
import subprocess
import time
from datetime import datetime
import threading

# Kafka Producer
BOOTSTRAP_SERVERS = "localhost:9092"
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Device Tracker Started (bootstrap_servers={BOOTSTRAP_SERVERS})...")



# Get connected devices (IP + MAC)
def get_devices():
    result = subprocess.run(["ip", "neigh"], capture_output=True, text=True)
    lines = result.stdout.splitlines()

    devices = []

    for line in lines:
        parts = line.split()

        if len(parts) >= 4:
            devices.append(
                {
                    "ip": parts[0],
                    "mac": parts[4],
                    "state": parts[5] if len(parts) > 5 else "UNKNOWN",
                }
            )

    return devices

# Stream device list

def stream_devices():
    seen = set()

    while True:
        devices = get_devices()

        for d in devices:
            key = f"{d['ip']}-{d['mac']}"

            if key not in seen:
                seen.add(key)

                event = {
                    "event_type": "device_seen",
                    "timestamp": datetime.utcnow().isoformat(),
                    "ip": d["ip"],
                    "mac": d["mac"],
                    "state": d["state"],
                }

                producer.send("device-events", event)
                print("Sent DEVICE:", event)

        time.sleep(5)

# Stream network traffic

def stream_traffic():
    process = subprocess.Popen(
        ["tcpdump", "-i", "eth0", "-l", "-n"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    for line in process.stdout:
        event = {
            "event_type": "network_activity",
            "timestamp": datetime.utcnow().isoformat(),
            "raw_log": line.strip(),
        }

        producer.send("device-events", event)
        print("Sent TRAFFIC:", event)


# Run both threads

t1 = threading.Thread(target=stream_devices)
t2 = threading.Thread(target=stream_traffic)

t1.start()
t2.start()

t1.join()
t2.join()

