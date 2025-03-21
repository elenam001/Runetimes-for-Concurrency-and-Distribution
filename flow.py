import logging
import socket
import json
import struct
import time
import protocol
from rina_utils import allocate_flow, send_data

class Flow:
    def __init__(self, apn, host, port):
        self.apn = apn
        self.host = host
        self.port = port
        self.flow_id = None
        self.connected = False
        self.reuse_threshold = 20
        self.packets_sent = 0 

    def allocate(self, retries=5):
        for attempt in range(retries):
            try:
                self.flow_id = allocate_flow(self.host, self.port, self.apn)
                if self.flow_id:
                    # Create a new socket for verification
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(5)  # 5-second timeout
                        s.connect((self.host, self.port))
                        # Send test packet
                        test_payload = protocol.pack_message(
                            flow_id=self.flow_id, 
                            payload=b"TEST_PACKET"
                        )
                        s.sendall(test_payload)
                        # Wait for ACK
                        ack = s.recv(1024)
                        if not ack:
                            raise ValueError("Flow verification failed")
                        self.connected = True
                        return
            except Exception as e:
                logging.error(f"Allocation attempt {attempt+1} failed: {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
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
        self._send_heartbeat()
        for attempt in range(retries):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 10))  # Graceful close
                    s.settimeout(10)
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
    
    def _send_heartbeat(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                s.send(protocol.pack_message(
                    flow_id="HEARTBEAT",
                    payload=b""  # Explicit empty payload
                ))
        except:
            pass