import os
import signal
import socket
import statistics
import sys
import json
import logging
import threading
import time
import argparse
import protocol

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

class NamingRegistry:
    def __init__(self):
        self.registry = {}
    
    def register(self, apn, ipcp):
        self.registry[apn] = ipcp
        logging.info("Registered APN '%s' with IPCP instance.", apn)
    
    def resolve(self, apn):
        return self.registry.get(apn, None)

naming_registry = NamingRegistry()

class DIF:
    def __init__(self, host='localhost', port=10000, node_name="Node"):
        self.host = host
        self.port = port
        self.node_name = node_name
        self.ipcps = {}
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)        
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.server_socket.bind((self.host, self.port))
        except OSError as e:
            logging.error(f"Failed to bind port {self.port}: {e}")
            raise
        self.server_socket.listen(5)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # Enable keepalive
        self.server_socket.settimeout(600)
        logging.info("%s: DIF started on %s:%d", self.node_name, self.host, self.port)
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def register_ipcp(self, apn):
        ipcp = IPCP(apn, self)
        self.ipcps[apn] = ipcp
        return ipcp
    
    def run(self):
        while True:
            try:
                client_sock, addr = self.server_socket.accept()
                logging.info("%s: Connection from %s", self.node_name, addr)
                t = threading.Thread(target=self.handle_client, args=(client_sock, addr))
                t.start()
            except socket.timeout:
                logging.debug("Server socket accept() timeout")
            except Exception as e:
                logging.error(f"Server error: {str(e)}")
    
    def handle_client(self, client_sock, addr):
        buffer = b""
        try:
            while True:
                try:
                    # Receive data with error handling
                    raw_data = client_sock.recv(4096)
                    if not raw_data:
                        break  # Connection closed gracefully
                    buffer += raw_data

                    # Process complete binary messages
                    while len(buffer) >= protocol.HEADER_SIZE + protocol.FLOW_ID_LENGTH:
                        timestamp, flow_id, payload = protocol.unpack_message(buffer)
                        
                        if payload is None:  # Incomplete message
                            break

                        try:
                            # Handle heartbeats first
                            if flow_id == "HEARTBEAT":
                                client_sock.sendall(protocol.pack_message(
                                    flow_id="HEARTBEAT_ACK", 
                                    payload=b""
                                ))
                                consumed = protocol.HEADER_SIZE + protocol.FLOW_ID_LENGTH + len(payload)
                                buffer = buffer[consumed:]
                                continue

                            # Extract destination APN safely
                            if payload.startswith(b"REQ:"):
                                try:
                                    dest_apn = payload.split(b":", 1)[1].decode().strip()
                                except (IndexError, UnicodeDecodeError) as e:
                                    logging.error(f"Invalid REQ format: {str(e)}")
                                    consumed = protocol.HEADER_SIZE + protocol.FLOW_ID_LENGTH + len(payload)
                                    buffer = buffer[consumed:]
                                    continue
                            else:
                                # Extract APN from flow_id format: "APN-flow-timestamp"
                                try:
                                    dest_apn = flow_id.split("-")[0]
                                except IndexError:
                                    logging.error(f"Malformed flow ID: {flow_id}")
                                    consumed = protocol.HEADER_SIZE + protocol.FLOW_ID_LENGTH + len(payload)
                                    buffer = buffer[consumed:]
                                    continue

                            # Resolve and handle IPCP
                            ipcp = naming_registry.resolve(dest_apn)
                            if ipcp:
                                ipcp.handle_binary_message(timestamp, flow_id, payload, client_sock)
                            else:
                                logging.error(f"No IPCP found for APN: {dest_apn}")

                            # Update buffer after processing
                            consumed = protocol.HEADER_SIZE + protocol.FLOW_ID_LENGTH + len(payload)
                            buffer = buffer[consumed:]

                        except Exception as e:
                            logging.error(f"Message processing failed: {str(e)}")
                            break  # Prevent infinite loop on corrupt messages

                    # Process JSON messages only if buffer is empty after binary processing
                    if not buffer:
                        try:
                            message = json.loads(raw_data.decode())
                            if not isinstance(message, dict):
                                raise ValueError("Message is not a JSON object")
                            if 'type' not in message or 'dest_apn' not in message:
                                raise ValueError("Missing required fields in JSON message")
                                
                            if message.get('type') == 'flow_allocation_request':
                                dest_apn = message['dest_apn']
                                ipcp = naming_registry.resolve(dest_apn)
                                if ipcp:
                                    new_flow_id = f"{dest_apn}-flow-{time.time()}"
                                    ipcp.flows[new_flow_id] = (client_sock.getpeername(), time.time())
                                    response = json.dumps({"flow_id": new_flow_id}).encode()
                                    client_sock.sendall(response)
                                    logging.info(f"Allocated flow {new_flow_id} (JSON)")

                        except (UnicodeDecodeError, json.JSONDecodeError):
                            pass  # Skip non-JSON data
                        except ValueError as e:
                            logging.warning(f"Invalid JSON message: {str(e)}")

                except (ConnectionResetError, BrokenPipeError):
                    logging.info("Client disconnected abruptly")
                    break
                except socket.timeout:
                    logging.debug("Socket timeout")
                    break

        except Exception as e:
            logging.error(f"Connection error: {str(e)}")
        finally:
            try:
                client_sock.close()
            except Exception as e:
                logging.debug(f"Error closing socket: {str(e)}")
            
    def shutdown(self, signum, frame):
        self.server_socket.close()
        logging.info("Server socket closed")
    
    def send_to_tcp_server(self, tcp_host, tcp_port, message):
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.connect((tcp_host, tcp_port))
            tcp_sock.sendall(message.encode())
            tcp_sock.close()
            logging.info("%s: Sent message to TCP server %s:%d", self.node_name, tcp_host, tcp_port)
        except Exception as e:
            logging.error("%s: Error sending message to TCP server %s:%d: %s", self.node_name, tcp_host, tcp_port, e)


class IPCP:
    def __init__(self, apn, dif):
        self.apn = apn
        self.dif = dif
        self.flows = {}
        naming_registry.register(apn, self)
    
    def handle_message(self, message, client_sock, client_addr):
        try:
            timestamp, flow_id, payload = protocol.unpack_message(message)
            if payload == b"FLOW_REQ":
                new_flow_id = f"{self.apn}-flow-{time.time()}"
                response = protocol.pack_message(
                    flow_id=new_flow_id,
                    payload=b"FLOW_OK"
                )
                client_sock.sendall(response)
                self.flows[new_flow_id] = (client_addr, time.time())
        except:
            message = json.loads(message.decode())
            msg_type = message.get("type")
            if msg_type == "flow_allocation_request":
                flow_id = f"{self.apn}-flow-{time.time()}"
                response = json.dumps({"flow_id": flow_id}).encode()
                client_sock.sendall(response)
            elif msg_type == "data_transfer":
                flow_id = message.get("flow_id")  # Ensure this line exists!
                response = json.dumps({"type": "ack", "flow_id": flow_id}).encode()
                client_sock.sendall(response)


    def handle_binary_message(self, timestamp, flow_id, payload, sock):
        if flow_id == "HEARTBEAT":
            sock.sendall(protocol.pack_message(flow_id="HEARTBEAT_ACK", payload=b""))
            return
        if payload == b"TEST_PACKET":
            sock.sendall(protocol.pack_message(flow_id=flow_id, payload=b"ACK"))
            return
        if payload.startswith(b"REQ:"):
            # Flow allocation logic
            new_flow_id = f"{self.apn}-flow-{time.time()}"
            response = protocol.pack_message(flow_id=new_flow_id, payload=b"ACK")
            sock.sendall(response)
            self.flows[new_flow_id] = (sock.getpeername(), time.time())
        elif payload == b"TEARDOWN":
            if flow_id in self.flows:
                del self.flows[flow_id]
                sock.sendall(protocol.pack_message(flow_id=flow_id, payload=b"TEARDOWN_ACK"))
        else:
            response = protocol.pack_message(flow_id=flow_id, payload=b"ACK")
            sock.sendall(response)


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
        dif = DIF(port=args.port, node_name=args.node)
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