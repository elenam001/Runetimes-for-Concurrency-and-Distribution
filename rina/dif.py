
# ----------------------------
# DIF (Distributed Inter-Process Communication Facility)
# ----------------------------
import json
import logging
import signal
import socket
import threading
import time
import protocol
import naming_registry


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
        logging.info("%s: DIF started on %s:%d", self.node_name, self.host, self.port)
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def register_ipcp(self, apn):
        ipcp = ipcp.IPCP(apn, self)
        self.ipcps[apn] = ipcp
        return ipcp
    
    def run(self):
        while True:
            client_sock, addr = self.server_socket.accept()
            logging.info("%s: Connection from %s", self.node_name, addr)
            t = threading.Thread(target=self.handle_client, args=(client_sock, addr))
            t.start()
    
    def handle_client(self, client_sock, addr):
        try:
            while True:
                raw_data = client_sock.recv(4096)
                if not raw_data:
                    break
                try:
                    timestamp, flow_id, payload = protocol.unpack_message(raw_data)
                    logging.info(f"Received data for flow {flow_id}")
                    if payload.startswith(b"REQ:"):
                        dest_apn = payload.split(b":")[1].decode().strip()
                    else:
                        dest_apn = flow_id.split("-")[0]
                    ipcp = naming_registry.resolve(dest_apn)
                    if ipcp:
                        ipcp.handle_binary_message(timestamp, flow_id, payload, client_sock)
                    else:
                        logging.error(f"No IPCP found for APN: {dest_apn}")

                    continue  # Skip JSON fallback
                except Exception as e:
                    logging.debug(f"Binary parse failed: {e}")

                # JSON fallback (for flow allocation)
                try:
                    message = json.loads(raw_data.decode())
                    if message.get('type') == 'flow_allocation_request':
                        dest_apn = message.get('dest_apn')
                        ipcp = naming_registry.resolve(dest_apn)
                        if ipcp:
                            new_flow_id = f"{dest_apn}-flow-{time.time()}"
                            response = json.dumps({"flow_id": new_flow_id}).encode()
                            client_sock.sendall(response)
                            logging.info(f"Allocated flow {new_flow_id} (JSON)")
                except Exception as e:
                    logging.error(f"Message handling failed: {e}")

        except Exception as e:
            logging.error(f"Connection error: {e}")
        finally:
            client_sock.close()
            
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
