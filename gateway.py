import socket
import threading
import json
import logging
import time
from rina import IPCP, DIF

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

dummy_dif = DIF(port=9999, node_name="GatewayInternalDIF")  # Dummy port just to hold IPCP
dummy_ipcp = IPCP("APN_TCP", dummy_dif)

class RINA_TCP_Gateway:
    def __init__(
        self, 
        rina_host='localhost', 
        rina_port=10000,
        tcp_host='localhost', 
        tcp_port=11000, 
        gateway_tcp_port=12000  
    ):
        self.rina_host = rina_host
        self.rina_port = rina_port
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.gateway_tcp_port = gateway_tcp_port  # Port for gateway's own TCP server
        
        # APN to external TCP server mappings
        self.apn_tcp_mappings = {"APN_TCP": (tcp_host, tcp_port)}
        self.flow_mappings = {}  # RINA flow ↔ TCP connection
        
        # RINA listener (for RINA nodes)
        self.rina_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.rina_socket.bind((self.rina_host, self.rina_port))
        self.rina_socket.listen(5)
        
        # TCP listener (for external TCP clients)
        self.gateway_tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.gateway_tcp_socket.bind((self.tcp_host, self.gateway_tcp_port))  # Use new port
        self.gateway_tcp_socket.listen(5)

    def start(self):
        threading.Thread(target=self.handle_rina_connections, daemon=True).start()
        threading.Thread(target=self.handle_tcp_connections, daemon=True).start()
        logging.info(f"Gateway started. RINA @ {self.rina_port}, TCP @ {self.gateway_tcp_port}")
    
    def handle_rina_connections(self):
        while True:
            client_sock, addr = self.rina_socket.accept()
            logging.info("RINA connection from %s", addr)
            threading.Thread(target=self.handle_rina_client, args=(client_sock, addr), daemon=True).start()
    
    def handle_tcp_connections(self):
        while True:
            client_sock, addr = self.gateway_tcp_socket.accept()  # Use new socket
            logging.info("TCP connection from %s", addr)
            threading.Thread(target=self.handle_tcp_client, args=(client_sock, addr), daemon=True).start()
    
    def handle_rina_client(self, client_sock, addr):
        try:
            data = client_sock.recv(4096)
            if data:
                message = json.loads(data.decode())
                if message['type'] == 'flow_allocation_request':
                    tcp_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    tcp_conn.connect((self.tcp_host, self.tcp_port))
                    flow_id = f"rina-flow-{addr[1]}"
                    self.flow_mappings[flow_id] = tcp_conn
                    response = {"type": "flow_allocation_response", "flow_id": flow_id}
                    client_sock.send(json.dumps(response).encode())
                elif message['type'] == 'data_transfer':
                    dest_apn = message.get('dest_apn')
                    if dest_apn in self.apn_tcp_mappings:
                        tcp_host, tcp_port = self.apn_tcp_mappings[dest_apn]
                        # Send raw payload to TCP
                        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        tcp_sock.connect((tcp_host, tcp_port))
                        tcp_sock.sendall(message['payload'].encode())
                        tcp_sock.close()

                        ack_response = {
                            "type": "ack",
                            "flow_id": message.get("flow_id"),
                            "recv_time": time.time()
                        }
                        client_sock.send(json.dumps(ack_response).encode())

#                elif message['type'] == 'data_transfer':
#                    flow_id = message['flow_id']
#                    if flow_id in self.flow_mappings:
#                        self.flow_mappings[flow_id].sendall(json.dumps(message).encode())

        except Exception as e:
            logging.error("Error handling RINA client: %s", e)
        finally:
            client_sock.close()
    
    def handle_tcp_client(self, client_sock, addr):
        try:
            while True:
                data = client_sock.recv(4096)
                rina_msg = {
                    "type": "data_transfer",
                    "src_apn": "TCP_GATEWAY",
                    "dest_apn": "APN_A",  # Default destination
                    "payload": data.decode()
                }
                self.send_to_rina(json.dumps(rina_msg))
                if not data:
                    break
                for flow_id, conn in self.flow_mappings.items():
                    if conn == client_sock:
                        rina_response = {"type": "data_transfer", "flow_id": flow_id, "payload": data.decode()}
                        self.send_to_rina(json.dumps(rina_response))
                        break
        except Exception as e:
            logging.error("Error handling TCP client: %s", e)
        finally:
            client_sock.close()
    
    def send_to_rina(self, message):
        rina_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rina_sock.connect((self.rina_host, self.rina_port))
        rina_sock.sendall(message.encode())
        rina_sock.close()

if __name__ == "__main__":
    gateway = RINA_TCP_Gateway()
    gateway.start()
    while True:
        pass  # Keep the script running
