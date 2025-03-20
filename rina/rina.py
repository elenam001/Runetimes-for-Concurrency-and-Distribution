import os
import socket
import statistics
import sys
import json
import logging
import time
import argparse
import protocol
import naming_registry
import dif

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

# ----------------------------
# Client Simulation
# ----------------------------
def client_simulation(src_apn, dest_apn, dest_host, dest_port, payload_size=1024, num_transfers=10):
    # Allocate flow using JSON (compatibility)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as alloc_sock:
        alloc_sock.connect((dest_host, dest_port))
        alloc_sock.send(json.dumps({"type": "flow_allocation_request", "src_apn": src_apn, "dest_apn": dest_apn}).encode())
        resp = alloc_sock.recv(4096)
        try:
            flow_id = json.loads(resp)["flow_id"]
        except Exception as e:
            logging.error("Failed to extract flow_id from response: %s", e)
            return

    # Data transfer using binary protocol
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as data_sock:
        data_sock.connect((dest_host, dest_port))
        latencies = []
        
        for _ in range(num_transfers):
            payload = os.urandom(payload_size)
            start = time.time()
            
            # Pack message using binary protocol
            data_sock.sendall(protocol.pack_message(
                flow_id=flow_id,
                payload=payload
            ))
            
            # Wait for acknowledgment
            ack_data = data_sock.recv(4096)
            _, ack_flow_id, ack_payload = protocol.unpack_message(ack_data)
            latencies.append(time.time() - start)
    
    return statistics.mean(latencies)


# ----------------------------
# Main 
# ----------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="RINA Node Simulation")
    parser.add_argument('--port', type=int, default=10000, help="Port number for the DIF server")
    parser.add_argument('--node', type=str, default="NodeA", help="Name of the node/DIF")
    parser.add_argument('--apn', type=str, default="APN_A", help="Application Process Name for the local IPCP")
    parser.add_argument('--dest_port', type=int, help="Destination node's port for client simulation")
    parser.add_argument('--dest_apn', type=str, help="Destination APN for client simulation")
    parser.add_argument('--simulate_loss', action='store_true', help="Enable packet loss simulation")
    parser.add_argument('--tcp_host', type=str, help="TCP server host")
    parser.add_argument('--tcp_port', type=int, help="TCP server port")
    parser.add_argument('--server', action='store_true', help="Run in server mode")
    args = parser.parse_args()

    if args.server:
        # SERVER MODE
        dif = dif.DIF(port=args.port, node_name=args.node)
        dif.register_ipcp(args.apn)
        try:
            logging.info(f"Active APNs: {list(naming_registry.registry.keys())}")
            dif.run() 
        except KeyboardInterrupt:
            dif.shutdown(None, None)
    else:
        # CLIENT MODE
        if not (args.dest_port or args.tcp_host):
            logging.error("Specify either --dest_port/--dest_apn or --tcp_host/--tcp_port")
            sys.exit(1)
            
        client_simulation(
            src_apn=args.apn,
            dest_apn=args.dest_apn or "APN_TCP",
            dest_host='localhost',
            dest_port=args.dest_port or 10000,
            payload_size=1024,  
            num_transfers=10,
        )


# Server mode (Terminal 1) python rina.py --server --port 10002 --node NodeA --apn APN_A

# Client mode (Terminal 2) python rina.py --dest_port 10000 --dest_apn APN_TCP --apn APN_A