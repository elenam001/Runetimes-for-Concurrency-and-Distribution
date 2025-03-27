import socket
import time

# Placeholder for RINA IPC connection function
class RINA_IPC:
    def __init__(self, apn):
        self.apn = apn
        print(f"[RINA] Connected to APN: {apn}")

    def send(self, data):
        print(f"[RINA] Sending: {data.decode()}")
        time.sleep(0.1)  # Simulating RINA delay
        return b"ACK from RINA"

    def close(self):
        print("[RINA] Connection closed")

# TCP Configuration
TCP_HOST = "localhost"
TCP_PORT = 11000

# RINA Configuration
RINA_APN = "APN_A"

# Start TCP Server
tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_sock.bind((TCP_HOST, TCP_PORT))
tcp_sock.listen(1)

print(f"[GATEWAY] Listening on {TCP_HOST}:{TCP_PORT}...")

# Simulate RINA connection
rina_sock = RINA_IPC(RINA_APN)

while True:
    conn, addr = tcp_sock.accept()
    print(f"[GATEWAY] TCP Connection from {addr}")

    data = conn.recv(1024)
    if not data:
        break

    print(f"[TCP -> RINA] Forwarding: {data.decode()}")
    
    # Send to RINA
    rina_response = rina_sock.send(data)
    
    # Send back response to TCP client
    conn.sendall(rina_response)

conn.close()
rina_sock.close()
