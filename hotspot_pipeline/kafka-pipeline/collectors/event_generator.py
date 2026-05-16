import subprocess
from datetime import datetime

# DEVICE EVENTS (IP + MAC)
def get_device_events():
    result = subprocess.run(["ip", "neigh"], capture_output=True, text=True)

    events = []

    for line in result.stdout.splitlines():
        parts = line.split()

        if len(parts) >= 4:
            events.append({
                "event_type": "DEVICE_DISCOVERY",
                "device_ip": parts[0],
                "device_mac": parts[4],
                "state": parts[5] if len(parts) > 5 else "UNKNOWN",
                "timestamp": datetime.utcnow().isoformat()
            })

    return events


# DNS / TRAFFIC EVENTS
def get_dns_events():
    result = subprocess.run(
        ["tcpdump", "-i", "eth0", "-l", "port", "53"],
        capture_output=True,
        text=True
    )

    events = []

    for line in result.stdout.splitlines():
        events.append({
            "event_type": "DNS_ACTIVITY",
            "raw": line,
            "timestamp": datetime.utcnow().isoformat()
        })

    return events