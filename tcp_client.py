import socket
import threading
import sys

#Wait for incoming data from server
#.decode is used to turn the message in bytes to a string
def receive(socket, signal):
    while signal:
        try:
            data = socket.recv(32)
            print(str(data.decode("utf-8")))
        except:
            print("You have been disconnected from the server")
            signal = False
            break

# Get host, port, and optional RINA node details
host = input("Host: ")
port = int(input("Port: "))
rina_host = input("RINA Host (leave blank if not using RINA): ")
rina_port = input("RINA Port (leave blank if not using RINA): ")

# Attempt connection to server
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
except:
    print("Could not make a connection to the server")
    input("Press enter to quit")
    sys.exit(0)

# Create new thread to wait for data
receiveThread = threading.Thread(target=receive, args=(sock, True))
receiveThread.start()

# Send data to server or RINA node
while True:
    message = input()
    if rina_host and rina_port:
        try:
            rina_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            rina_sock.connect((rina_host, int(rina_port)))
            rina_sock.sendall(message.encode())
            rina_sock.close()
            print("Sent message to RINA node")
        except:
            print("Could not send message to RINA node")
    else:
        sock.sendall(str.encode(message))