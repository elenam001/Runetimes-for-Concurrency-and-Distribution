import socket
import time
import json
import logging
import statistics
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def test_connection(host, port, label, results, timeout=2):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        start_conn = time.time()
        sock.connect((host, port))
        conn_time = time.time() - start_conn
        
        message = json.dumps({"type": "ping", "timestamp": time.time()})
        start = time.time()
        sock.sendall(message.encode())
        response = sock.recv(4096)
        latency = time.time() - start
        
        if response:
            results[label].append((conn_time, latency))
            logging.info(f"{label} success | Conn: {conn_time:.4f}s | Latency: {latency:.4f}s")
        else:
            logging.warning(f"{label} empty response")
    except Exception as e:
        logging.error(f"{label} error: {str(e)}")
    finally:
        sock.close()

def test_hybrid_network(gateway_host, gateway_tcp_port, gateway_rina_port, num_packets=100):
    results = {"Hybrid": []}
    
    for _ in range(num_packets):
        # Send via Gateway TCP → RINA → TCP Server
        start = time.time()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((gateway_host, gateway_tcp_port))
            s.sendall(b"test")
            s.recv(1024)  # Wait for echo
            results["Hybrid"].append(time.time() - start)
    
    # Calculate statistics
    print(f"Hybrid Avg Latency: {statistics.mean(results['Hybrid']):.4f}s")

if __name__ == "__main__":
    test_hybrid_network(
        tcp_host="localhost",
        tcp_port=11000,  # TCP server port
        rina_host="localhost",
        rina_port=10000   # Gateway RINA port
    )