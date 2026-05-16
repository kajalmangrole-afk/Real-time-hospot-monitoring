import pytest
from validators import validate_event, safe_validate

VALID_CONNECTION_EVENT = {
    "event_type": "DEVICE_CONNECTED",
    "device_ip": "192.168.12.5",
    "device_mac": "A8:6B:AD:63:F6:1D",
    "event_action": "connected",
    "session_id": "session-12345",
    "timestamp": "2026-05-11T17:10:00Z"
}

INVALID_CONNECTION_EVENT = {
    "event_type": "DEVICE_CONNECTED",
    "device_ip": "192.168.12.5",
    "event_action": "connected",
    "timestamp": "2026-05-11T17:10:00Z"
}


def test_valid_connection_event():
    assert validate_event(VALID_CONNECTION_EVENT)


def test_invalid_connection_event():
    valid, error = safe_validate(INVALID_CONNECTION_EVENT)
    assert not valid
    assert "device_mac" in error
