import time
import statistics
import time
import argparse
from datetime import datetime
from rina_src import flow

def run_test(host, port, dest_apn, num_packets=100, payload_kb=1, teardown_interval=10):
    flow = flow.Flow(dest_apn, host, port)
    flow.allocate()

    results = {
        "latencies": [],
        "payload_size": payload_kb * 1024,
        "successes": 0,
        "failures": 0
    }

    for i in range(num_packets):
        if i > 0 and i % teardown_interval == 0:
            print(f"\n[Flow Teardown at packet {i}]")
            flow.teardown()
            time.sleep(0.05)
            flow.allocate()

        latency = flow.send(payload_size=results["payload_size"])
        if latency:
            results["latencies"].append(latency)
            results["successes"] += 1
        else:
            results["failures"] += 1
        time.sleep(0.01)

    if results["successes"] > 0:
        results.update({
            "avg_latency": statistics.mean(results["latencies"]),
            "jitter": statistics.stdev(results["latencies"]) if len(results["latencies"]) > 1 else 0,
            "throughput": (results["successes"] * results["payload_size"]) / sum(results["latencies"])
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="RINA Performance Test")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=10002)
    parser.add_argument("--apn", default="APN_A")
    parser.add_argument("--packets", type=int, default=100)
    parser.add_argument("--size", type=int, default=1, choices=[1, 10, 100, 1000])
    parser.add_argument("--emulate", type=str, help="Format: latency_ms,loss%,bandwidth")
    parser.add_argument("--teardown-interval", type=int, default=10)

    args = parser.parse_args()

    print(f"\n{' RINA Performance Test ':=^80}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Target: {args.host}:{args.port} | APN: {args.apn}")
    print(f"Payload: {args.size}KB | Packets: {args.packets}")

    if args.emulate:
        print(f"Network Emulation: {args.emulate}")

    results = run_test(
        host=args.host,
        port=args.port,
        dest_apn=args.apn,
        num_packets=args.packets,
        payload_kb=args.size,
        teardown_interval=args.teardown_interval
    )

    print("\nResults:")
    print(f"  Success: {results['successes']}/{args.packets}")
    print(f"  Packet loss: {(results['failures']/args.packets)*100:.2f}%")

    if results["successes"]:
        print(f"  Avg latency: {results['avg_latency']*1000:.2f}ms")
        print(f"  Jitter: {results['jitter']*1000:.2f}ms")
        print(f"  Throughput: {results['throughput']/1024:.2f} KB/s")
    else:
        print("  No successful transmissions")

    print("="*80)


if __name__ == "__main__":
    main()

    
# Basic test with 1KB payloads python test_rina.py --host localhost --port 10002 --apn APN_A --packets 100 --size 1

# Large payload test (100KB) python test_rina.py --size 100 --packets 50

# With network emulation parameters (requires separate setup) python test_rina.py --emulate "50,1,10"  # 50ms latency, 1% loss, 10Mbps