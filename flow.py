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
        for attempt in range(retries):
            try:
                if not self.connected:
                    self.allocate()
                latency = send_data(self.host, self.port, self.flow_id, self.apn, payload_size)
                if latency:
                    self.packets_sent += 1
                    return latency
            except Exception as e:
                logging.warning(f"Packet transmission failed: {e}")
            time.sleep(0.01 * (2 ** attempt))  # Exponential backoff
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