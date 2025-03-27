import logging
import socket
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
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(5)
                    s.connect((self.host, self.port))
                    
                    # Send binary allocation request
                    request = protocol.pack_message(
                        flow_id="FLOW_REQ",
                        payload=f"REQ:{self.apn}".encode()
                    )
                    s.sendall(request)
                    
                    # Get response
                    response = s.recv(1024)
                    _, self.flow_id, _ = protocol.unpack_message(response)
                    
                    # Verify flow
                    test_payload = protocol.pack_message(
                        flow_id=self.flow_id,
                        payload=b"TEST_PACKET"
                    )
                    s.sendall(test_payload)
                    ack = s.recv(1024)
                    self.connected = True
                    return
            except Exception as e:
                logging.error(f"Allocation attempt {attempt+1} failed: {str(e)}")
                time.sleep(2 ** attempt)

    def teardown(self):
        try:
            with socket.socket() as s:
                s.connect((self.host, self.port))
                teardown_msg = protocol.pack_message(
                    flow_id=self.flow_id,
                    payload=f"TEARDOWN:{self.flow_id}".encode()
                )
                s.sendall(teardown_msg)
                # Wait for ACK
                ack = s.recv(1024)
        except Exception as e:
            logging.error(f"Teardown error: {e}")
        finally:
            self.flow_id = None
            self.connected = False
            
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