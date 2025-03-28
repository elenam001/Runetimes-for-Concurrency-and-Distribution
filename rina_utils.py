from concurrent.futures import ThreadPoolExecutor
import logging
import socket
import os
import statistics
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

def send_data(host, port, flow_id, dest_apn, payload_size=1024, parallel_connections=4):
    """Send data using multiple parallel connections with thread pool"""
    def _send_single():
        """Helper function to send a single packet"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((host, port))
                payload = os.urandom(payload_size)
                start = time.time()
                
                # Pack and send
                packed = protocol.pack_message(
                    flow_id=flow_id, 
                    payload=payload
                )
                s.sendall(packed)
                
                # Wait for ACK
                ack_data = s.recv(4096)
                protocol.unpack_message(ack_data)
                
                return time.time() - start
                
        except Exception as e:
            logging.debug(f"Single packet failed: {str(e)}")
            return None

    start_time = time.time()
    latencies = []
    
    try:
        with ThreadPoolExecutor(max_workers=parallel_connections) as executor:
            # Launch parallel transmissions
            futures = [executor.submit(_send_single) 
                      for _ in range(parallel_connections)]
            
            # Collect results
            for future in futures:
                latency = future.result()
                if latency:
                    latencies.append(latency)
                    
    except Exception as e:
        logging.error(f"Parallel send failed: {str(e)}")
        return None

    total_time = time.time() - start_time
    
    if latencies:
        avg_latency = statistics.mean(latencies)
        logging.debug(f"Sent {len(latencies)}/{parallel_connections} packets in {total_time:.3f}s")
        return avg_latency
        
    return None

