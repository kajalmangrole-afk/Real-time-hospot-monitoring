from streaming.transforms import detect_dns_spikes


def test_detect_dns_spikes():
    events = []
    for i in range(52):
        events.append({"device_ip": "192.168.12.5", "domain": f"site{i}.com"})

    alerts = detect_dns_spikes(events, threshold=50)
    assert len(alerts) == 1
    assert alerts[0]["device_ip"] == "192.168.12.5"
    assert alerts[0]["count"] == 51
