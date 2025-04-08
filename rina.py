import os
import signal
import socket
import statistics
import struct
import sys
import logging
import threading
import time
import argparse
from flow import Flow
import protocol
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

class NamingRegistry:
    def __init__(self):
        self.registry = {}

    def register(self, apn, ipcp):
        if ':' in apn:
            raise ValueError("APN cannot contain ':'")
        self.registry[apn] = ipcp

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
        self.server_socket.listen(10)  # Increased from 10
        self.server_socket.settimeout(None)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        logging.info("%s: DIF started on %s:%d", self.node_name, self.host, self.port)

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def register_ipcp(self, apn):
        ipcp = IPCP(apn, self)
        self.ipcps[apn] = ipcp
        return ipcp

    async def run(self):  # Make the run method an async function
        loop = asyncio.get_running_loop()
        self.server_socket.setblocking(False)  # Set the socket to non-blocking mode
        logging.info("%s: Async DIF running...", self.node_name)
        while True:
            try:
                client_sock, addr = await loop.sock_accept(self.server_socket)  # Asynchronously accept connections
                logging.info("%s: Connection from %s", self.node_name, addr)
                asyncio.create_task(self.handle_client(client_sock, addr))  # Create a task for each client
            except OSError as e:
                if e.errno == socket.EBADF:  # Handle the case where the socket might be closed
                    logging.debug("Server socket closed.")
                    break
                else:
                    logging.error(f"Error during accept: {e}")
                    break
            except Exception as e:
                logging.error(f"Server error: {str(e)}")
                break

    async def handle_client(self, client_sock, addr):
        loop = asyncio.get_running_loop()
        try:
            buffer = b''
            while True:
                # Read header
                header_data = await loop.sock_recv(client_sock, protocol.HEADER_SIZE - len(buffer))
                if not header_data:
                    break
                buffer += header_data
                if len(buffer) < protocol.HEADER_SIZE:
                    continue
                
                # Unpack header
                timestamp, payload_size = struct.unpack(protocol.HEADER_FORMAT, buffer[:protocol.HEADER_SIZE])
                buffer = buffer[protocol.HEADER_SIZE:]
                
                # Read flow ID
                flow_id_data = await loop.sock_recv(client_sock, protocol.FLOW_ID_LENGTH - len(buffer))
                buffer += flow_id_data
                if len(buffer) < protocol.FLOW_ID_LENGTH:
                    continue
                flow_id = buffer[:protocol.FLOW_ID_LENGTH].decode().rstrip('\0')
                buffer = buffer[protocol.FLOW_ID_LENGTH:]
                
                # Read payload
                payload_data = await loop.sock_recv(client_sock, payload_size - len(buffer))
                buffer += payload_data
                if len(buffer) < payload_size:
                    continue
                payload = buffer[:payload_size]
                buffer = buffer[payload_size:]
                
                # Process message
                if flow_id == "HEARTBEAT":
                    response = protocol.pack_message(flow_id="HEARTBEAT", payload=b"ACK")
                    await loop.sock_sendall(client_sock, response)
                    continue

                # Flow allocation request
                if payload.startswith(b"REQ:"):
                    dest_apn = payload.split(b":")[1].decode().strip()
                    ipcp = naming_registry.resolve(dest_apn)
                    if ipcp:
                        new_flow_id = f"{dest_apn}:flow:{time.time()}"
                        response = protocol.pack_message(
                            flow_id=new_flow_id,
                            payload=b"ACK"
                        )
                        await loop.sock_sendall(client_sock, response)  # Asynchronously send data
                        ipcp.flows[new_flow_id] = (client_sock.getpeername(), time.time())
                    else:
                        logging.error(f"No IPCP found for APN: {dest_apn}")

                # Teardown request
                elif payload.startswith(b"TEARDOWN:"):
                    try:
                        # Split on bytes delimiter
                        _, flow_id_part = payload.split(b":", 1)  # <-- Fix here
                        flow_id_to_delete = flow_id_part.decode()
                        apn = flow_id_to_delete.split(":", 1)[0]
                        ipcp = naming_registry.resolve(apn)
                        
                        if ipcp and flow_id_to_delete in ipcp.flows:
                            del ipcp.flows[flow_id_to_delete]
                            response = protocol.pack_message(flow_id=flow_id_to_delete, payload=b"TEARDOWN_ACK")
                            await loop.sock_sendall(client_sock, response)
                        else:
                            logging.error(f"Teardown failed: Invalid flow {flow_id_to_delete} for APN {apn}")
                            
                    except ValueError as e:
                        logging.error(f"Invalid teardown payload format: {e}")
                    except Exception as e:
                        logging.error(f"Teardown processing error: {e}")

                # Regular data packet
                else:
                    response = protocol.pack_message(flow_id=flow_id, payload=b"ACK")
                    await loop.sock_sendall(client_sock, response)  # Asynchronously send data

        except Exception as e:
            logging.error(f"Connection error: {e}", exc_info=True)
        finally:
            try:
                client_sock.close()
            except:
                pass

    def shutdown(self, signum, frame):
        logging.info("Shutting down DIF...")
        try:
            self.server_socket.close()
            logging.info("Server socket closed")
        except Exception as e:
            logging.error(f"Error closing server socket: {e}")

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

    def handle_incoming_data(self, timestamp, flow_id, payload, sock):
        if payload.startswith(b"REQ:"):
            dest_apn = payload.split(b":")[1].decode().strip()
            new_flow_id = f"{dest_apn}:flow:{time.time()}"  # <-- Changed "-" to ":"
            response = protocol.pack_message(flow_id=new_flow_id, payload=b"ACK")
            sock.sendall(response)
            self.flows[new_flow_id] = (sock.getpeername(), time.time())
        elif payload.startswith(b"TEARDOWN:"):
            try:
                try:
                    _, flow_id_part = payload.split(b":", 1)  # Split only once
                    flow_id_to_delete = flow_id_part.decode()
                except ValueError:
                    logging.error(f"Invalid teardown payload format: {payload.decode()}")
                    return
                flow_id_to_delete = flow_id_to_delete.decode()
                if flow_id_to_delete in self.flows:
                    del self.flows[flow_id_to_delete]
                    sock.sendall(protocol.pack_message(flow_id=flow_id_to_delete, payload=b"TEARDOWN_ACK"))
                else:
                    logging.error(f"Teardown failed: Invalid flow {flow_id_to_delete} for APN {self.apn}")
            except Exception as e:
                logging.error(f"Teardown processing error in IPCP: {e}")
        else:
            response = protocol.pack_message(flow_id=flow_id, payload=b"ACK")
            sock.sendall(response)


# ----------------------------
# Client Simulation
# ----------------------------
async def async_client_simulation(src_apn, dest_apn, dest_host, dest_port, payload_size=1024, num_transfers=10):
    flow = Flow(dest_apn, dest_host, dest_port)
    flow.retry_base_delay = 0.2
    try:
        await flow.allocate()
        if not flow.connected:
            logging.error("Failed to allocate flow")
            return None

        latencies = []
        for _ in range(num_transfers):
            start = time.time()
            await flow.send(payload_size=4096, parallel=16)
            latencies.append(time.time() - start)

        await flow.teardown()
        return statistics.mean(latencies) if latencies else None
    except Exception as e:
        logging.error(f"Client simulation error: {e}")
        if flow.connected:
            await flow.teardown()
        return None


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
            asyncio.run(dif.run())  # Run the asyncio event loop
        except KeyboardInterrupt:
            dif.shutdown(None, None)
    else:
        # CLIENT MODE
        if not (args.dest_port or args.tcp_host):
            logging.error("Specify either --dest_port/--dest_apn or --tcp_host/--tcp_port")
            sys.exit(1)

        async def main():
            latency = await async_client_simulation(
                src_apn=args.apn,
                dest_apn=args.dest_apn or "APN_TCP",
                dest_host='localhost',
                dest_port=args.dest_port or 10000,
                payload_size=1024,
                num_transfers=args.num_transfers if hasattr(args, 'num_transfers') and args.num_transfers else 10,
            )
            if latency is not None:
                logging.info(f"Average latency: {latency * 1000:.2f} ms")

        asyncio.run(main())

# Server mode (Terminal 1) python rina.py --server --port 10000 --node NodeA --apn APN_A

# Client mode (Terminal 2) python rina.py --dest_port 10000 --dest_apn APN_TCP --apn APN_A