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

    def allocate(self):
        self.flow_id = allocate_flow(self.host, self.port, self.apn)
        self.connected = bool(self.flow_id)

    def teardown(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((self.host, self.port))
                s.send(json.dumps({
                    "type": "teardown",
                    "flow_id": self.flow_id,
                    "apn": self.apn
                }).encode())
        except Exception as e:
            print(f"Teardown failed: {e}")
        finally:
            self.flow_id = None
            self.connected = False

    def send(self, payload_size=1024):
        if not self.connected:
            print(f"Flow not active for {self.apn}, re-allocating...")
            self.allocate()
        return send_data(self.host, self.port, self.flow_id, self.apn, payload_size)
