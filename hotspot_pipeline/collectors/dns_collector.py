import os
import subprocess
import time
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from producer.kafka_client import get_consumer, get_producer, TOPICS
from schemas import now_iso
from validators import validate_event


def parse_tshark_line(line):
    parts = line.strip().split("\t")
    if len(parts) < 2:
        raise ValueError("Invalid tshark line")
    return parts[0].strip(), parts[1].strip()


def main():
    producer = get_producer()
    interface = os.getenv("DNS_INTERFACE", "wlan0")
    tshark_path = os.getenv("TSHARK_PATH", "tshark")
    command = [
        tshark_path,
        "-i", interface,
        "-Y", "dns.qry.name",
        "-T", "fields",
        "-e", "ip.src",
        "-e", "dns.qry.name"
    ]

    print("DNS Collector started...")

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
    for line in process.stdout:
        try:
            ip, domain = parse_tshark_line(line)
            event = {
                "event_type": "DNS_QUERY",
                "device_ip": ip,
                "domain": domain,
                "query_type": "A",
                "timestamp": now_iso()
            }
            validate_event(event)
            producer.send(TOPICS["dns"], value=event)
            producer.flush()
            print("DNS event sent:", event)
        except Exception as exc:
            print("DNS Collector parse error:", exc)


if __name__ == "__main__":
    main()
