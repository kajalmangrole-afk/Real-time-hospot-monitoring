import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COLLECTOR_SCRIPTS = [
    os.path.join(BASE_DIR, "collectors", "connection_collector.py"),
    os.path.join(BASE_DIR, "collectors", "bandwidth_collector.py"),
    os.path.join(BASE_DIR, "collectors", "dns_collector.py"),
    os.path.join(BASE_DIR, "collectors", "anomaly_detector.py")
]

if __name__ == "__main__":
    processes = []
    for script in COLLECTOR_SCRIPTS:
        if os.path.exists(script):
            processes.append(subprocess.Popen([sys.executable, script]))
    print(f"Started {len(processes)} hotspot pipeline processes.")
