import json
from datetime import datetime
import os

OUTPUT_FILE = "output/events.json"

def create_event(event_type, data):
    return {
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

def print_event(event):
    print(json.dumps(event, indent=2))

def save_event(event):
    os.makedirs("output", exist_ok=True)

    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")