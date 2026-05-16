import os
import re
import subprocess
import time
import uuid
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from producer.kafka_client import  get_producer, TOPICS
from schemas import now_iso
from validators import validate_event


def parse_neighbor_table(output):
    devices = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue

        if "lladdr" in line:
            parts = line.split()
            try:
                ip = parts[0]
                mac = parts[parts.index("lladdr") + 1]
                devices[ip] = mac
            except (ValueError, IndexError):
                continue

        elif re.search(r"\d+\.\d+\.\d+\.\d+", line) and re.search(r"[0-9a-f]{2}-[0-9a-f]{2}-[0-9a-f]{2}-[0-9a-f]{2}-[0-9a-f]{2}-[0-9a-f]{2}", line, re.I):
            parts = line.split()
            ip = parts[0]
            mac = parts[1].replace("-", ":")
            devices[ip] = mac
    return devices


def build_connection_event(ip, mac, action):
    return {
        "event_type": "DEVICE_CONNECTED" if action == "connected" else "DEVICE_DISCONNECTED",
        "device_ip": ip,
        "device_mac": mac,
        "event_action": action,
        "session_id": f"{mac}-{uuid.uuid4().hex}",
        "timestamp": now_iso()
    }


def get_neighbor_state():
    if os.name == "posix":
        output = subprocess.check_output(["ip", "neigh"], text=True)
    else:
        output = subprocess.check_output("arp -a", shell=True, text=True)
    return parse_neighbor_table(output)


def main():
    producer = get_producer()
    interval = int(os.getenv("CONNECTION_COLLECTOR_INTERVAL", "5"))
    known_devices = {}

    print("Connection Collector started...")

    while True:
        try:
            current_devices = get_neighbor_state()

            for ip, mac in current_devices.items():
                if ip not in known_devices:
                    event = build_connection_event(ip, mac, "connected")
                    validate_event(event)
                    producer.send(TOPICS["connection"], value=event)
                    producer.flush()
                    print("Connected:", event)

            for ip, mac in list(known_devices.items()):
                if ip not in current_devices:
                    event = build_connection_event(ip, mac, "disconnected")
                    validate_event(event)
                    producer.send(TOPICS["connection"], value=event)
                    producer.flush()
                    print("Disconnected:", event)

            known_devices = current_devices
            time.sleep(interval)

        except Exception as exc:
            print("Connection Collector error:", exc)
            time.sleep(interval)


if __name__ == "__main__":
    main()
