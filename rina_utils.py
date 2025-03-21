import logging
import socket
import os
import protocol
import time

def allocate_flow(host, port, dest_apn):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(15)  # Increased timeout
            s.connect((host, port))
            
            # Send request
            request = protocol.pack_message(
                flow_id="FLOW_REQ",  # Required argument
                payload=f"REQ:{dest_apn}".encode()  # Required argument
            )            
            s.sendall(request)
            
            # Read full response
            response = s.recv(1024)
            _, flow_id, _ = protocol.unpack_message(response)
            return flow_id
    except Exception as e:
        logging.error(f"Flow allocation error: {str(e)}")
        return None
'''
def allocate_flow(host, port, dest_apn):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(10)
            s.connect((host, port))
            request = protocol.pack_message(
                flow_id="FLOW_REQ",
                payload=f"REQ:{dest_apn}".encode()  # Ensure this matches server expectations
            )
            s.sendall(request)
            response = s.recv(1024)
            _, flow_id, _ = protocol.unpack_message(response)
            return flow_id
    except Exception as e:
        logging.error(f"Flow allocation error: {str(e)}")
        return None
'''

def send_data(host, port, flow_id, dest_apn, payload_size=1024):
    start = time.time()
    try:
        payload = os.urandom(payload_size)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((host, port))
            start = time.time()
            # Send via binary protocol
            packed = protocol.pack_message(flow_id=flow_id, payload=payload)
            s.sendall(packed)
            # Wait for ACK
            ack_data = s.recv(4096)
            _, ack_flow_id, ack_payload = protocol.unpack_message(ack_data)
            logging.debug(f"send_data took {time.time() - start:.3f}s")
            return time.time() - start
    except Exception as e:
        print(f"Data transfer failed: {str(e)}")
        logging.debug(f"send_data took {time.time() - start:.3f}s")
        return None

