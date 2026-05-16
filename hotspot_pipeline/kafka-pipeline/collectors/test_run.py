import os, sys

# Ensure project root is on sys.path when running this file directly
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from event_generator import get_device_events, get_dns_events

print("DEVICE EVENTS:")
print(get_device_events())

print("\nDNS EVENTS:")
print(get_dns_events())