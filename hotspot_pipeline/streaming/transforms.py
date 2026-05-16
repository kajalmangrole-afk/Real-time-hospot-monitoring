from datetime import datetime
from collections import defaultdict


def normalize_timestamp(timestamp):
    if timestamp is None:
        return None
    if isinstance(timestamp, (int, float)):
        return datetime.utcfromtimestamp(timestamp)
    if isinstance(timestamp, str):
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def calculate_session_duration(events):
    sorted_events = sorted(events, key=lambda event: normalize_timestamp(event["timestamp"]))
    duration_by_device = defaultdict(int)
    active_sessions = {}

    for event in sorted_events:
        ip = event.get("device_ip")
        action = event.get("event_action")
        ts = normalize_timestamp(event.get("timestamp"))
        if ip is None or ts is None:
            continue

        if action == "connected":
            active_sessions[ip] = ts
        elif action == "disconnected" and ip in active_sessions:
            duration = (ts - active_sessions[ip]).total_seconds()
            if duration > 0:
                duration_by_device[ip] += int(duration)
            active_sessions.pop(ip, None)

    return dict(duration_by_device)


def top_domains(dns_events, top_n=10):
    counts = defaultdict(int)
    for event in dns_events:
        domain = event.get("domain")
        if domain:
            counts[domain.lower()] += 1
    return sorted(counts.items(), key=lambda item: item[1], reverse=True)[:top_n]


def detect_dns_spikes(events, threshold=50):
    counts = defaultdict(int)
    alerts = []
    for event in events:
        ip = event.get("device_ip")
        if not ip:
            continue
        counts[ip] += 1
        if counts[ip] == threshold + 1:
            alerts.append({"device_ip": ip, "count": counts[ip]})
    return alerts


def aggregate_bandwidth(events):
    totals = defaultdict(lambda: {"bytes_sent": 0, "bytes_received": 0, "packets_sent": 0, "packets_received": 0})
    for event in events:
        ip = event.get("device_ip", "hotspot_gateway")
        totals[ip]["bytes_sent"] += event.get("bytes_sent", 0)
        totals[ip]["bytes_received"] += event.get("bytes_received", 0)
        totals[ip]["packets_sent"] += event.get("packets_sent", 0)
        totals[ip]["packets_received"] += event.get("packets_received", 0)
    return {ip: stats for ip, stats in totals.items()}
