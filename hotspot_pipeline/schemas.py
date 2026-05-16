from datetime import datetime

CONNECTION_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_type": {"type": "string", "enum": ["DEVICE_CONNECTED", "DEVICE_DISCONNECTED"]},
        "device_ip": {"type": "string"},
        "device_mac": {"type": "string"},
        "event_action": {"type": "string", "enum": ["connected", "disconnected"]},
        "session_id": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"}
    },
    "required": ["event_type", "device_ip", "device_mac", "event_action", "timestamp", "session_id"],
    "additionalProperties": False
}

BANDWIDTH_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_type": {"type": "string", "enum": ["BANDWIDTH_USAGE"]},
        "device_ip": {"type": "string"},
        "bytes_sent": {"type": "integer", "minimum": 0},
        "bytes_received": {"type": "integer", "minimum": 0},
        "packets_sent": {"type": "integer", "minimum": 0},
        "packets_received": {"type": "integer", "minimum": 0},
        "interface": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"}
    },
    "required": ["event_type", "device_ip", "bytes_sent", "bytes_received", "packets_sent", "packets_received", "interface", "timestamp"],
    "additionalProperties": False
}

DNS_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_type": {"type": "string", "enum": ["DNS_QUERY"]},
        "device_ip": {"type": "string"},
        "domain": {"type": "string"},
        "query_type": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"}
    },
    "required": ["event_type", "device_ip", "domain", "query_type", "timestamp"],
    "additionalProperties": False
}

ANOMALY_EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "event_type": {"type": "string", "enum": ["ANOMALY_ALERT"]},
        "device_ip": {"type": "string"},
        "alert_type": {"type": "string"},
        "description": {"type": "string"},
        "source_event": {"type": "object"},
        "timestamp": {"type": "string", "format": "date-time"}
    },
    "required": ["event_type", "device_ip", "alert_type", "description", "timestamp"],
    "additionalProperties": False
}

EVENT_SCHEMAS = {
    "DEVICE_CONNECTED": CONNECTION_EVENT_SCHEMA,
    "DEVICE_DISCONNECTED": CONNECTION_EVENT_SCHEMA,
    "BANDWIDTH_USAGE": BANDWIDTH_EVENT_SCHEMA,
    "DNS_QUERY": DNS_EVENT_SCHEMA,
    "ANOMALY_ALERT": ANOMALY_EVENT_SCHEMA
}


def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
