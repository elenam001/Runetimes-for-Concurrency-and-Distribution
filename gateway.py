import socket
import threading
import json
import logging
import time
from rina import ipcp, dif, protocol

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

class RINA_TCP_Gateway:
    def __init__(
        self, 
        rina_host='localhost', 
        rina_port=10000,
        tcp_host='localhost', 
        tcp_port=11000, 
        gateway_tcp_port=12000
    ):
        self.dif = dif.DIF(host=rina_host, port=rina_port, node_name="GatewayDIF")
        self.ipcp = ipcp.IPCP("APN_TCP", self.dif)  
        self.dif.register_ipcp("APN_TCP") 
        self.apn_mappings = {"APN_A": (tcp_host, tcp_port)} 
        self.rina_host = rina_host
        self.rina_port = rina_port
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.gateway_tcp_port = gateway_tcp_port

        # Connection mappings
        self.flow_map = {}          
        self.tcp_flow_map = {}      
        self.apn_mappings = {"APN_TCP": (tcp_host, tcp_port)}

        # Initialize servers
        self.rina_socket = self._setup_server(rina_host, rina_port)
        self.tcp_socket = self._setup_server(tcp_host, gateway_tcp_port)

    def _setup_server(self, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(5)
        return sock

    def start(self):
        threading.Thread(target=self._accept_rina_connections, daemon=True).start()
        threading.Thread(target=self._accept_tcp_connections, daemon=True).start()
        logging.info(f"Gateway active - RINA:{self.rina_port}, TCP:{self.gateway_tcp_port}")

    def _accept_rina_connections(self):
        while True:
            client_sock, addr = self.rina_socket.accept()
            threading.Thread(target=self._handle_rina_client, args=(client_sock,)).start()

    def _accept_tcp_connections(self):
        while True:
            client_sock, addr = self.tcp_socket.accept()
            threading.Thread(target=self._handle_tcp_client, args=(client_sock,)).start()

    def _handle_rina_client(self, rina_sock):
        try:
            while True:
                data = rina_sock.recv(4096)
                if not data:
                    break

                # Try binary protocol first
                try:
                    timestamp, flow_id, payload = protocol.unpack_message(data)
                    if flow_id in self.flow_map:
                        self.flow_map[flow_id].sendall(payload)
                except:
                    # Fallback to JSON handling
                    message = json.loads(data.decode())
                    if message['type'] == 'flow_allocation_response':
                        flow_id = message['flow_id']
                        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        tcp_sock.connect(self.apn_mappings["APN_TCP"])
                        self.flow_map[flow_id] = tcp_sock
                        self.tcp_flow_map[tcp_sock] = flow_id

        except Exception as e:
            logging.error(f"RINA handler error: {e}")
        finally:
            self._cleanup_rina(rina_sock)

    def _handle_tcp_client(self, tcp_sock):
        try:
            # Allocate RINA flow for new TCP connection
            flow_id = self._allocate_rina_flow()
            if not flow_id:
                raise Exception("Flow allocation failed")

            while True:
                data = tcp_sock.recv(4096)
                if not data:
                    break

                # Send via binary protocol
                packed = protocol.pack_message(flow_id=flow_id, payload=data)
                rina_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                rina_sock.connect((self.rina_host, self.rina_port))
                rina_sock.sendall(packed)
                rina_sock.close()

        except Exception as e:
            logging.error(f"TCP handler error: {e}")
        finally:
            self._cleanup_tcp(tcp_sock)

    def _allocate_rina_flow(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.rina_host, self.rina_port))
                s.send(json.dumps({
                    "type": "flow_allocation_request",
                    "dest_apn": "APN_TCP"
                }).encode())
                response = json.loads(s.recv(4096))
                return response.get('flow_id')
        except Exception as e:
            logging.error(f"Flow allocation failed: {e}")
            return None

    def _cleanup_rina(self, sock):
        sock.close()
        flow_id = self.tcp_flow_map.get(sock)
        if flow_id:
            del self.flow_map[flow_id]
            del self.tcp_flow_map[sock]

    def _cleanup_tcp(self, sock):
        sock.close()
        flow_id = self.tcp_flow_map.get(sock)
        if flow_id:
            del self.flow_map[flow_id]
            del self.tcp_flow_map[sock]

if __name__ == "__main__":
    gateway = RINA_TCP_Gateway()
    gateway.start()
    while True:
        time.sleep(1)