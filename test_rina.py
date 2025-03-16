import time
import socket
import json
import statistics
import os
import argparse
from datetime import datetime

import protocol

# In allocate_flow() function
def allocate_flow(host, port, dest_apn):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((host, port))
            
            # Send binary flow request
            request = protocol.pack_message(
                flow_id="FLOW_REQ",
                payload=dest_apn.encode()
            )
            s.sendall(request)
            
            # Receive response
            response = s.recv(1024)
            if len(response) < protocol.HEADER_SIZE:
                raise ValueError("Invalid response length")
                
            _, flow_id, _ = protocol.unpack_message(response)
            return flow_id
            
    except Exception as e:
        print(f"Flow allocation failed: {str(e)}")
        return None

def send_data(host, port, flow_id, dest_apn, payload_size=1024):
    """Send data over existing flow and return latency"""
    try:
        payload = os.urandom(payload_size)  # Generate random bytes
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((host, port))
            start = time.time()
            s.send(json.dumps({
                "type": "data_transfer",
                "flow_id": flow_id,
                "dest_apn": dest_apn,
                "payload": payload.hex()  # Convert bytes to hex string
            }).encode())
            response = s.recv(4096)
            return time.time() - start
    except Exception as e:
        print(f"Data transfer failed: {str(e)}")
        return None

def run_test(host, port, dest_apn, num_packets=100, payload_kb=1):
    """Comprehensive RINA performance test"""
    results = {
        "latencies": [],
        "payload_size": payload_kb * 1024,
        "successes": 0,
        "failures": 0
    }
    
    # Allocate flow once
    flow_id = allocate_flow(host, port, dest_apn)
    if not flow_id:
        print("Flow allocation failed, aborting test")
        return results

    # Test loop
    for _ in range(num_packets):
        latency = send_data(host, port, flow_id, dest_apn, results["payload_size"])
        if latency:
            results["latencies"].append(latency)
            results["successes"] += 1
        else:
            results["failures"] += 1
        time.sleep(0.01)  # Short cooldown

    # Calculate metrics
    if results["successes"] > 0:
        results.update({
            "avg_latency": statistics.mean(results["latencies"]),
            "jitter": statistics.stdev(results["latencies"]) if len(results["latencies"]) > 1 else 0,
            "throughput": (results["successes"] * results["payload_size"]) / sum(results["latencies"])
        })
    
    return results

def main():
    parser = argparse.ArgumentParser(description="RINA Performance Test")
    parser.add_argument("--host", default="localhost", help="RINA node host")
    parser.add_argument("--port", type=int, default=10002, help="RINA node port")
    parser.add_argument("--apn", default="APN_A", help="Destination APN")
    parser.add_argument("--packets", type=int, default=100, help="Number of packets to send")
    parser.add_argument("--size", type=int, default=1, choices=[1, 10, 100, 1000],
                      help="Payload size in KB (1, 10, 100, 1000)")
    parser.add_argument("--emulate", type=str, help="Network conditions (format: latency_ms,loss_percent,bandwidth_mbps)")
    
    args = parser.parse_args()

    print(f"\n{' RINA Performance Test ':=^80}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Target: {args.host}:{args.port} | APN: {args.apn}")
    print(f"Payload: {args.size}KB | Packets: {args.packets}")
    
    if args.emulate:
        print(f"\nNetwork Emulation: {args.emulate}")
        # Here you would add code to apply network emulation rules
        # This requires root privileges and platform-specific tools
        
    results = run_test(
        host=args.host,
        port=args.port,
        dest_apn=args.apn,
        num_packets=args.packets,
        payload_kb=args.size
    )

    print("\nResults:")
    print(f"  Successful transmissions: {results['successes']}/{args.packets}")
    print(f"  Packet loss: {(results['failures']/args.packets)*100:.2f}%")
    
    if results['successes'] > 0:
        print(f"  Average latency: {results['avg_latency']*1000:.2f}ms")
        print(f"  Jitter: {results['jitter']*1000:.2f}ms")
        print(f"  Throughput: {results['throughput']/1024:.2f} KB/s")
    else:
        print("  No successful transmissions to calculate metrics")

    print("\n" + "="*80)

if __name__ == "__main__":
    main()
    
    
# Basic test with 1KB payloads python test_rina.py --host localhost --port 10002 --apn APN_A --packets 100 --size 1

# Large payload test (100KB) python test_rina.py --size 100 --packets 50

# With network emulation parameters (requires separate setup) python test_rina.py --emulate "50,1,10"  # 50ms latency, 1% loss, 10Mbps