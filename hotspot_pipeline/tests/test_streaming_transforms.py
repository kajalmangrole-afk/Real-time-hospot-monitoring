from streaming.transforms import calculate_session_duration, top_domains, aggregate_bandwidth


def test_calculate_session_duration():
    events = [
        {"device_ip": "192.168.12.5", "event_action": "connected", "timestamp": "2026-05-11T17:10:00Z"},
        {"device_ip": "192.168.12.5", "event_action": "disconnected", "timestamp": "2026-05-11T18:10:00Z"}
    ]
    duration = calculate_session_duration(events)
    assert duration["192.168.12.5"] == 3600


def test_top_domains():
    events = [
        {"domain": "example.com"},
        {"domain": "example.com"},
        {"domain": "github.com"}
    ]
    top = top_domains(events, top_n=2)
    assert top[0][0] == "example.com"
    assert top[0][1] == 2


def test_aggregate_bandwidth():
    events = [
        {"device_ip": "192.168.12.5", "bytes_sent": 100, "bytes_received": 200, "packets_sent": 1, "packets_received": 2},
        {"device_ip": "192.168.12.5", "bytes_sent": 300, "bytes_received": 400, "packets_sent": 3, "packets_received": 4}
    ]
    totals = aggregate_bandwidth(events)
    assert totals["192.168.12.5"]["bytes_sent"] == 400
    assert totals["192.168.12.5"]["bytes_received"] == 600
