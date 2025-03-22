import socket
import time
import statistics
import argparse

def measure_latency(target_host, target_port, num_packets=10, payload_size=1024):
    latencies = []
    errors = 0
    for _ in range(num_packets):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((target_host, target_port))
                start_time = time.time()
                sock.sendall(b'X' * payload_size)  # Send 1KB
                response = sock.recv(1024)
                if response:
                    latencies.append(time.time() - start_time)
        except Exception:
            errors += 1
    return latencies, errors

def measure_throughput(target_host, target_port, duration=5):
    data = b'X' * 1024  # 1KB chunk
    total_sent = 0
    start_time = time.time()
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(2)
            sock.connect((target_host, target_port))
            while time.time() - start_time < duration:
                sent = sock.send(data)
                total_sent += sent
    except Exception:
        pass
    
    elapsed = time.time() - start_time
    return total_sent / elapsed if elapsed > 0 else 0

def main():
    parser = argparse.ArgumentParser(description="TCP Network Performance Test")
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--packets", type=int, default=10)
    parser.add_argument("--duration", type=int, default=5)
    args = parser.parse_args()

    print("Testing TCP Network...")
    latencies, errors = measure_latency(args.host, args.port, args.packets)
    throughput = measure_throughput(args.host, args.port, args.duration)

    if latencies:
        avg_latency = statistics.mean(latencies)
        jitter = statistics.stdev(latencies) if len(latencies) > 1 else 0
    else:
        avg_latency = jitter = 0

    packet_loss = (errors / args.packets) * 100
    avg_rtt = avg_latency * 2  # Symmetric assumption

    print(f"Average Latency: {avg_latency:.4f} sec")
    print(f"Jitter: {jitter:.4f} sec")
    print(f"Packet Loss: {packet_loss:.2f}%")
    print(f"Throughput: {throughput / 1024:.2f} KB/s")
    print(f"Average RTT: {avg_rtt:.4f} sec")

if __name__ == "__main__":
    main()