import socket
import json
import time
from rina_utils import allocate_flow, send_data

class Flow:
    def __init__(self, apn, host, port):
        self.apn = apn
        self.host = host
        self.port = port
        self.flow_id = None
        self.connected = False
        self.reuse_threshold = 20
        self.packets_sent = 0  # Initialize counter


    def allocate(self, retries=3):
        for attempt in range(retries):
            try:
                self.flow_id = allocate_flow(self.host, self.port, self.apn)
                if self.flow_id:
                    self.connected = True
                    return
            except Exception as e:
                print(f"Allocation attempt {attempt+1} failed: {e}")
                time.sleep(0.5 * (attempt + 1))
        self.connected = False 

    def teardown(self):
        try:
            with socket.socket() as s:
                s.connect((self.host, self.port))
                s.send(json.dumps({
                    "type": "teardown",
                    "flow_id": self.flow_id,
                    "apn": self.apn
                }).encode())
        except Exception as e:
            print(f"Teardown error: {e}")
        finally:
            self.flow_id = None
            self.connected = False
            self.packets_sent = 0
            
    def send(self, payload_size=1024, retries=5):
        for attempt in range(retries):
            try:
                if not self.connected:
                    self.allocate()
                latency = send_data(self.host, self.port, self.flow_id, self.apn, payload_size)
                if latency:
                    self.packets_sent += 1  # Increment on success
                    if self.packets_sent >= self.reuse_threshold:
                        self.teardown()  # Teardown only after threshold
                    return latency
            except Exception as e:
                delay = 0.2 * (2 ** attempt)  # Exponential backoff: 0.2s, 0.4s, 0.8s...
                print(f"Attempt {attempt+1} failed. Retrying in {delay:.1f}s...")
                time.sleep(delay)
        return None