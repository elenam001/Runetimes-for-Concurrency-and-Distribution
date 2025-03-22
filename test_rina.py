import time
import statistics
import argparse
import csv
import os
from datetime import datetime
from flow import Flow


def run_test(host, port, dest_apn, num_packets=100, payload_kb=1, teardown_interval=10, warmup_packets=0, retries=3):
    flow = Flow(dest_apn, host, port)
    flow.allocate()

    results = {
        "latencies": [],
        "payload_size": payload_kb * 1024,
        "successes": 0,
        "failures": 0,
        "retries": 0,
        "setup_times": []
    }

    # Warmup phase (optional)
    for _ in range(warmup_packets):
        flow.send(payload_size=results["payload_size"])
        time.sleep(0.01)

    for i in range(num_packets):
        if teardown_interval > 0 and i > 0 and i % teardown_interval == 0:
            print(f"\n[Flow Teardown at packet {i}]")
            flow.teardown()
            t0 = time.time()
            flow.allocate()
            setup_time = time.time() - t0
            results["setup_times"].append(setup_time)
            time.sleep(0.05)

        success = False
        for attempt in range(retries):
            latency = flow.send(payload_size=results["payload_size"])
            if latency:
                results["latencies"].append(latency)
                results["successes"] += 1
                success = True
                break
            else:
                results["retries"] += 1
                time.sleep(0.02)

        if not success:
            results["failures"] += 1

        time.sleep(0.01)

    if results["successes"] > 0:
        total_time = sum(results["latencies"])
        results.update({
            "avg_latency": statistics.mean(results["latencies"]),
            "jitter": statistics.stdev(results["latencies"]) if len(results["latencies"]) > 1 else 0,
            "throughput": (results["successes"] * results["payload_size"]) / total_time,
            "avg_setup_time": statistics.mean(results["setup_times"]) if results["setup_times"] else 0,
            "avg_rtt" : statistics.mean(results["latencies"]) * 2
        })

    return results


def write_csv(results, label="RINA", filename="results.csv"):
    header = ["Label", "Timestamp", "Success", "TotalPackets", "PacketLoss(%)", "Retries", "AvgLatency(ms)", "Jitter(ms)", "Throughput(KB/s)", "AvgSetupTime(s)"]
    row = [
        label,
        datetime.now().isoformat(),
        results.get("successes", 0),
        results.get("successes", 0) + results.get("failures", 0),
        round((results.get("failures", 0) / (results.get("successes", 0) + results.get("failures", 0))) * 100),
        results.get("retries", 0),
        round(results.get("avg_latency", 0) * 1000, 3),
        round(results.get("jitter", 0) * 1000, 3),
        round(results.get("throughput", 0) / 1024, 2),
        round(results.get("avg_setup_time", 0), 3)
    ]

    file_exists = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="RINA Performance Test")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=10002)
    parser.add_argument("--apn", default="APN_A")
    parser.add_argument("--packets", type=int, default=100)
    parser.add_argument("--size", type=int, default=1, choices=[1, 10, 100, 1000])
    parser.add_argument("--emulate", type=str, help="Format: latency_ms,loss%,bandwidth")
    parser.add_argument("--teardown-interval", type=int, default=10)
    parser.add_argument("--warmup-packets", type=int, default=0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--csv", default="results.csv")
    parser.add_argument("--label", default="RINA")

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
        teardown_interval=args.teardown_interval,
        warmup_packets=args.warmup_packets,
        retries=args.retries
    )

    print("\nResults:")
    print(f"  Success: {results['successes']}/{args.packets}")
    print(f"  Packet loss: {(results['failures']/args.packets)*100:.2f}%")
    print(f"  Retries: {results['retries']}")

    if results["successes"]:
        print(f"  Avg latency: {results['avg_latency']*1000:.2f}ms")
        print(f"  Jitter: {results['jitter']*1000:.2f}ms")
        print(f"  Throughput: {results['throughput']/1024:.2f} KB/s")
        print(f"  Avg flow setup time: {results['avg_setup_time']:.3f} sec")
    else:
        print("  No successful transmissions")

    write_csv(results, label=args.label, filename=args.csv)
    print("="*80)


if __name__ == "__main__":
    main()
