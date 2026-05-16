import pytest
from collectors import connection_collector, dns_collector


def test_parse_arp_line_linux():
    sample = "192.168.12.5 dev wlan0 lladdr A8:6B:AD:63:F6:1D REACHABLE"
    devices = connection_collector.parse_neighbor_table(sample)
    assert devices["192.168.12.5"] == "A8:6B:AD:63:F6:1D"


def test_parse_tshark_dns_line():
    sample = "192.168.12.5\tyoutube.com"
    ip, domain = dns_collector.parse_tshark_line(sample)
    assert ip == "192.168.12.5"
    assert domain == "youtube.com"


def test_invalid_tshark_dns_line():
    with pytest.raises(ValueError):
        dns_collector.parse_tshark_line("invalid line")
