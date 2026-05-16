# import os
# import sys
# import subprocess
# import json
# import time
# import shutil

# SCRIPT_DIR = os.path.dirname(__file__)
# ROOT_DIR = os.path.dirname(SCRIPT_DIR)
# if ROOT_DIR not in sys.path:
#     sys.path.insert(0, ROOT_DIR)

# from kafka_client import get_producer
# # from c import DNS_TOPIC, DNS_INTERFACE, TSHARK_PATH

# TOPIC_NAME = os.getenv("KAFKA_TOPIC_DNS", DNS_TOPIC)


# def resolve_tshark_path():
#     if os.path.isabs(TSHARK_PATH) and os.path.exists(TSHARK_PATH):
#         return TSHARK_PATH

#     resolved = shutil.which(TSHARK_PATH)
#     if resolved:
#         return resolved

#     raise FileNotFoundError(
#         f"tshark executable not found at {TSHARK_PATH}. Set TSHARK_PATH or install tshark."
#     )


# def get_dns_interface():
#     env_interface = os.getenv("DNS_INTERFACE")
#     if env_interface:
#         return env_interface

#     if os.name == "nt":
#         return "6"

#     return DNS_INTERFACE


# def main():
#     producer = get_producer()
#     tshark = resolve_tshark_path()
#     interface = get_dns_interface()
#     print(f"Producer started... publishing DNS events to topic={TOPIC_NAME}")
#     print(f"Using tshark={tshark} interface={interface}")

#     try:
#         process = subprocess.Popen(
#             [
#                 tshark,
#                 "-l",
#                 "-i", interface,
#                 "-Y", "dns.qry.name",
#                 "-T", "fields",
#                 "-e", "ip.src",
#                 "-e", "dns.qry.name"
#             ],
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             text=True,
#             bufsize=1
#         )
#     except FileNotFoundError as exc:
#         print("Error starting tshark:", exc)
#         return

#     for line in process.stdout:
#         try:
#             parts = line.strip().split("\t")
#             if len(parts) < 2:
#                 continue
#             ip, domain = parts[0].strip(), parts[1].strip()
#             if not ip or not domain:
#                 continue

#             event = {
#                 "device_ip": ip,
#                 "domain": domain,
#                 "timestamp": time.time(),
#                 "event_type": "DNS_QUERY"
#             }
#             producer.send(TOPIC_NAME, value=event)
#             producer.flush()
#             print("Sent:", event)
#         except Exception as exc:
#             print("Error processing line:", exc, "line=", line.strip())

#     process.wait()
#     if process.returncode != 0:
#         stderr = process.stderr.read().strip()
#         print(f"tshark exited with code={process.returncode}")
#         if stderr:
#             print("tshark stderr:", stderr)


# if __name__ == "__main__":
#     main()

import os
import sys
import subprocess
import time
import shutil

SCRIPT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from kafka_client import get_producer, TOPICS

# ---------------- CONFIG ----------------
DNS_INTERFACE = "6"

TSHARK_PATH = r"F:\Wireshark\tshark.exe"

TOPIC_NAME = os.getenv("KAFKA_TOPIC_DNS", TOPICS["dns"])


# ---------------- RESOLVE TSHARK ----------------
def resolve_tshark_path():

    if os.path.isabs(TSHARK_PATH) and os.path.exists(TSHARK_PATH):
        return TSHARK_PATH

    resolved = shutil.which(TSHARK_PATH)

    if resolved:
        return resolved

    raise FileNotFoundError(
        f"tshark executable not found at {TSHARK_PATH}"
    )


# ---------------- DNS INTERFACE ----------------
def get_dns_interface():

    env_interface = os.getenv("DNS_INTERFACE")

    if env_interface:
        return env_interface

    if os.name == "nt":
        return "6"

    return DNS_INTERFACE


# ---------------- MAIN ----------------
def main():

    producer = get_producer()

    tshark = resolve_tshark_path()

    interface = get_dns_interface()

    print(f"Producer started... publishing DNS events to topic={TOPIC_NAME}")
    print(f"Using tshark={tshark} interface={interface}")

    try:

        process = subprocess.Popen(
            [
                tshark,
                "-l",
                "-i", interface,
                "-Y", "dns.qry.name",
                "-T", "fields",
                "-e", "ip.src",
                "-e", "dns.qry.name"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

    except FileNotFoundError as exc:

        print("Error starting tshark:", exc)

        return

    # ---------------- STREAM LOOP ----------------
    for line in process.stdout:

        try:

            parts = line.strip().split("\t")

            if len(parts) < 2:
                continue

            ip = parts[0].strip()
            domain = parts[1].strip()

            if not ip or not domain:
                continue

            event = {
                "device_ip": ip,
                "domain": domain,
                "timestamp": time.time(),
                "event_type": "DNS_QUERY"
            }

            # SEND TO KAFKA
            future = producer.send(TOPIC_NAME, value=event)

            producer.flush()

            metadata = future.get(timeout=20)

            print("Sent:", event)
            print("Topic:", metadata.topic)

        except Exception as exc:

            print("Error processing line:", exc)

    process.wait()

    if process.returncode != 0:

        stderr = process.stderr.read().strip()

        print(f"tshark exited with code={process.returncode}")

        if stderr:
            print("tshark stderr:", stderr)


# ---------------- ENTRY ----------------
if __name__ == "__main__":

    main()