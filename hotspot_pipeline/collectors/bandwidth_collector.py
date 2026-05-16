# import os
# import re
# import shutil
# import subprocess
# import time
# import psutil
# import sys
# sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# from producer.kafka_client import  get_producer, TOPICS
# from schemas import now_iso
# from validators import validate_event


# def parse_conntrack_table(output):
#     events = []
#     for line in output.splitlines():
#         if "src=" not in line:
#             continue
#         src_match = re.search(r"src=([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", line)
#         orig_bytes = re.search(r"orig_bytes=(\d+)", line)
#         reply_bytes = re.search(r"reply_bytes=(\d+)", line)
#         if src_match and orig_bytes and reply_bytes:
#             events.append({
#                 "device_ip": src_match.group(1),
#                 "bytes_sent": int(orig_bytes.group(1)),
#                 "bytes_received": int(reply_bytes.group(1)),
#                 "packets_sent": 0,
#                 "packets_received": 0,
#                 "interface": os.getenv("HOTSPOT_INTERFACE", "wlan0")
#             })
#     return events


# def collect_interface_stats(interface, previous_counters=None):
#     counters = psutil.net_io_counters(pernic=True)
#     if interface not in counters:
#         return None, previous_counters

#     current = counters[interface]
#     if previous_counters is None or interface not in previous_counters:
#         snapshot = {
#             "bytes_sent": current.bytes_sent,
#             "bytes_recv": current.bytes_recv,
#             "packets_sent": current.packets_sent,
#             "packets_recv": current.packets_recv
#         }
#         return None, {interface: snapshot}

#     prev = previous_counters[interface]
#     event = {
#         "device_ip": "hotspot_gateway",
#         "event_type": "BANDWIDTH_USAGE",
#         "bytes_sent": max(0, current.bytes_sent - prev["bytes_sent"]),
#         "bytes_received": max(0, current.bytes_recv - prev["bytes_recv"]),
#         "packets_sent": max(0, current.packets_sent - prev["packets_sent"]),
#         "packets_received": max(0, current.packets_recv - prev["packets_recv"]),
#         "interface": interface,
#         "timestamp": now_iso()
#     }
#     return event, {interface: {
#         "bytes_sent": current.bytes_sent,
#         "bytes_recv": current.bytes_recv,
#         "packets_sent": current.packets_sent,
#         "packets_recv": current.packets_recv
#     }}


# def main():
#     producer = get_producer()
#     interface = os.getenv("HOTSPOT_INTERFACE", "wlan0")
#     previous_counters = None
#     interval = int(os.getenv("BANDWIDTH_COLLECTOR_INTERVAL", "5"))

#     print("Bandwidth Collector started...")

#     while True:
#         try:
#             event, previous_counters = collect_interface_stats(interface, previous_counters)
#             if event:
#                 validate_event(event)
#                 producer.send(TOPICS["bandwidth"], value=event)
#                 producer.flush()
#                 print("Bandwidth event sent:", event)

#             conntrack_path = shutil.which("conntrack")
#             if conntrack_path:
#                 output = subprocess.check_output([conntrack_path, "-L"], text=True)
#                 flow_events = parse_conntrack_table(output)
#                 for item in flow_events:
#                     event = {
#                         "event_type": "BANDWIDTH_USAGE",
#                         "device_ip": item["device_ip"],
#                         "bytes_sent": item["bytes_sent"],
#                         "bytes_received": item["bytes_received"],
#                         "packets_sent": item["packets_sent"],
#                         "packets_received": item["packets_received"],
#                         "interface": item["interface"],
#                         "timestamp": now_iso()
#                     }
#                     validate_event(event)
#                     producer.send(TOPICS["bandwidth"], value=event)
#                     producer.flush()
#                     print("Per-device bandwidth event sent:", event)

#             time.sleep(interval)
#         except Exception as exc:
#             print("Bandwidth Collector error:", exc)
#             time.sleep(interval)


# if __name__ == "__main__":
#     main()




import os
import re
import shutil
import subprocess
import time
import psutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from producer.kafka_client import get_producer, TOPICS
from schemas import now_iso
from validators import validate_event


# ---------------- PARSE CONNTRACK ----------------
def parse_conntrack_table(output):

    events = []

    for line in output.splitlines():

        if "src=" not in line:
            continue

        src_match = re.search(r"src=([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", line)

        orig_bytes = re.search(r"orig_bytes=(\d+)", line)

        reply_bytes = re.search(r"reply_bytes=(\d+)", line)

        if src_match and orig_bytes and reply_bytes:

            events.append({
                "device_ip": src_match.group(1),
                "bytes_sent": int(orig_bytes.group(1)),
                "bytes_received": int(reply_bytes.group(1)),
                "packets_sent": 0,
                "packets_received": 0,
                "interface": "Wi-Fi"
            })

    return events


# ---------------- INTERFACE STATS ----------------
def collect_interface_stats(interface, previous_counters=None):

    counters = psutil.net_io_counters(pernic=True)

    print("Available interfaces:", counters.keys())

    if interface not in counters:

        print(f"Interface not found: {interface}")

        return None, previous_counters

    current = counters[interface]

    if previous_counters is None:

        snapshot = {
            "bytes_sent": current.bytes_sent,
            "bytes_recv": current.bytes_recv,
            "packets_sent": current.packets_sent,
            "packets_recv": current.packets_recv
        }

        return None, {interface: snapshot}

    prev = previous_counters[interface]

    event = {
        "device_ip": "hotspot_gateway",
        "event_type": "BANDWIDTH_USAGE",
        "bytes_sent": max(0, current.bytes_sent - prev["bytes_sent"]),
        "bytes_received": max(0, current.bytes_recv - prev["bytes_recv"]),
        "packets_sent": max(0, current.packets_sent - prev["packets_sent"]),
        "packets_received": max(0, current.packets_recv - prev["packets_recv"]),
        "interface": interface,
        "timestamp": now_iso()
    }

    snapshot = {
        "bytes_sent": current.bytes_sent,
        "bytes_recv": current.bytes_recv,
        "packets_sent": current.packets_sent,
        "packets_recv": current.packets_recv
    }

    return event, {interface: snapshot}


# ---------------- MAIN ----------------
def main():

    producer = get_producer()

    # WINDOWS INTERFACE
    interface = "Wi-Fi"

    previous_counters = None

    interval = 5

    print("Bandwidth Collector started...")

    while True:

        try:

            event, previous_counters = collect_interface_stats(
                interface,
                previous_counters
            )

            if event:

                validate_event(event)

                producer.send(TOPICS["bandwidth"], value=event)

                producer.flush()

                print("Bandwidth event sent:", event)

            # ---------------- CONNTRACK ----------------
            conntrack_path = shutil.which("conntrack")

            if conntrack_path:

                output = subprocess.check_output(
                    [conntrack_path, "-L"],
                    text=True
                )

                flow_events = parse_conntrack_table(output)

                for item in flow_events:

                    flow_event = {
                        "event_type": "BANDWIDTH_USAGE",
                        "device_ip": item["device_ip"],
                        "bytes_sent": item["bytes_sent"],
                        "bytes_received": item["bytes_received"],
                        "packets_sent": item["packets_sent"],
                        "packets_received": item["packets_received"],
                        "interface": item["interface"],
                        "timestamp": now_iso()
                    }

                    validate_event(flow_event)

                    producer.send(
                        TOPICS["bandwidth"],
                        value=flow_event
                    )

                    producer.flush()

                    print("Per-device bandwidth event sent:", flow_event)

            else:

                print("conntrack not installed (OK on Windows)")

            time.sleep(interval)

        except Exception as exc:

            print("Bandwidth Collector error:", exc)

            time.sleep(interval)


# ---------------- ENTRY ----------------
if __name__ == "__main__":

    main()