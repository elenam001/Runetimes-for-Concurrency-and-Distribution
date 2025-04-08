import socket


class DIF:
    def __init__(self, host='localhost', port=10000, node_name="Node"):
        self.host = host
        self.port = port
        self.node_name = node_name
        self.connections = []
        self.total_connections = 0
        self.ipcps = []
        
    def naming(self, name):
        """Set the name of the DIF node."""
        self.node_name = name
        
    def addressing(self, address):
        """Set the address of the DIF node."""
        self.host = address
        
    def QoS(self, qos):
        """Set the Quality of Service for the DIF."""
        self.qos = qos
        
    def enrollment(self, enrollment_info):
        """Enroll the DIF node with the given information."""
        self.enrollment_info = enrollment_info
        # Implement enrollment logic here
        
    def flow_allocation(self, flow_info):
        """Allocate a flow for the DIF."""
        self.flow_info = flow_info
        # Implement flow allocation logic here
        
    async def run(self):
        """Run the DIF node."""
        # Create a socket for the DIF
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        
        print(f"DIF node {self.node_name} running on {self.host}:{self.port}")
        
        while True:
            conn, addr = self.sock.accept()
            print(f"New connection from {addr}")
            self.connections.append(conn)
            self.total_connections += 1
            
            # Handle IPCP enrollment and flow allocation here
        
class IPCP:
    def __init__(self):
        pass