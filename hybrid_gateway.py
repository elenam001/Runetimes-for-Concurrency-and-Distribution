import socket
import threading
import logging
import time
import rina
import protocol

class RinaTcpGateway:
    def __init__(self, rina_port, tcp_mappings):
        self.rina_port = rina_port
        self.tcp_mappings = tcp_mappings # {"APN_TCP": ("127.0.0.1", 8080)}
        self.rina_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.rina_socket.bind(("localhost", rina_port))
        self.rina_socket.listen(5)
        self.flows = {} # {flow_id: tcp_socket}

    def run(self):
        while True:
            client_socket, addr = self.rina_socket.accept()
            threading.Thread(target=self.handle_rina_connection, args=(client_socket, addr)).start()

    def handle_rina_connection(self, rina_socket, addr):
        try:
            while True:
                data = rina_socket.recv(4096)
                if not data:
                    break
                timestamp, flow_id, payload = protocol.unpack_message(data)

                if payload.startswith(b"REQ:"):
                    dest_apn = payload.split(b":")[1].decode()
                    if dest_apn in self.tcp_mappings:
                        tcp_host, tcp_port = self.tcp_mappings[dest_apn]
                        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        tcp_socket.connect((tcp_host, tcp_port))
                        new_flow_id = f"{dest_apn}-flow-{time.time()}"
                        self.flows[new_flow_id] = tcp_socket
                        response = protocol.pack_message(flow_id=new_flow_id, payload=b"ACK")
                        rina_socket.sendall(response)
                elif flow_id in self.flows:
                    tcp_socket = self.flows[flow_id]
                    tcp_socket.sendall(payload)
                    tcp_response = tcp_socket.recv(4096)
                    response = protocol.pack_message(flow_id=flow_id, payload=tcp_response)
                    rina_socket.sendall(response)
        except Exception as e:
            logging.error(f"Gateway error: {e}")
        finally:
            rina_socket.close()

if __name__ == "__main__":
    gateway = RinaTcpGateway(10001, {"APN_TCP": ("localhost", 8080)})
    gateway.run()