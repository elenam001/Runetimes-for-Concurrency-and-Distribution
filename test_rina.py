import logging
import time
import statistics
import argparse
import csv
import os
from datetime import datetime
from flow import Flow
import multiprocessing


def run_test(host, port, dest_apn, num_packets=100, payload_kb=1, teardown_interval=10, warmup_packets=0, retries=3):
    start_time = time.time()  
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
            try:
                latency = flow.send(payload_size=results["payload_size"])
                if latency:
                    results["latencies"].append(latency)
                    results["successes"] += 1
                    success = True
                    break
                else:
                    results["retries"] += 1
                    time.sleep(0.02)
            except Exception as e:
                logging.error(f"Error sending packet {i}, attempt {attempt + 1}: {e}")
                results["failures"] += 1
                time.sleep(0.02)

        if not success:
            results["failures"] += 1

        time.sleep(0.01)
        
    end_time = time.time()
    total_wall_time = end_time - start_time

    if results["successes"] > 0:
        total_time = sum(results["latencies"])
        results.update({
            "avg_latency": statistics.mean(results["latencies"]),
            "jitter": statistics.stdev(results["latencies"]) if len(results["latencies"]) > 1 else 0,
            "throughput": (results["successes"] * results["payload_size"]) / total_wall_time,
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
    parser.add_argument("--processes", type=int, default=1, help="Number of parallel processes") # Added argument

    args = parser.parse_args()

    print(f"\n{' RINA Performance Test ':=^80}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Target: {args.host}:{args.port} | APN: {args.apn}")
    print(f"Payload: {args.size}KB | Packets: {args.packets}")

    if args.emulate:
        print(f"Network Emulation: {args.emulate}")

    test_args = (
        args.host,
        args.port,
        args.apn,
        args.packets,
        args.size,
        args.teardown_interval,
        args.warmup_packets,
        args.retries,
    )

    if args.processes > 1:
        with multiprocessing.Pool(processes=args.processes) as pool:
            results = pool.starmap(run_test, [test_args] * args.processes)

        # Aggregate results from multiple processes
        aggregated_results = {
            "latencies": [],
            "payload_size": args.size * 1024,
            "successes": 0,
            "failures": 0,
            "retries": 0,
            "setup_times": [],
        }

        for result in results:
            aggregated_results["latencies"].extend(result["latencies"])
            aggregated_results["successes"] += result["successes"]
            aggregated_results["failures"] += result["failures"]
            aggregated_results["retries"] += result["retries"]
            aggregated_results["setup_times"].extend(result["setup_times"])

        if aggregated_results["successes"] > 0:
            aggregated_results.update({
                "avg_latency": statistics.mean(aggregated_results["latencies"]),
                "jitter": statistics.stdev(aggregated_results["latencies"]) if len(aggregated_results["latencies"]) > 1 else 0,
                "throughput": (aggregated_results["successes"] * aggregated_results["payload_size"]) / (time.time() - datetime.fromisoformat(datetime.now().isoformat()).timestamp()),
                "avg_setup_time": statistics.mean(aggregated_results["setup_times"]) if aggregated_results["setup_times"] else 0,
                "avg_rtt" : statistics.mean(aggregated_results["latencies"]) * 2
            })
    else:
        aggregated_results = run_test(*test_args)

    print("\nResults:")
    print(f"  Success: {aggregated_results['successes']}/{args.packets * args.processes}")
    print(f"  Packet loss: {(aggregated_results['failures'] / (args.packets * args.processes)) * 100:.2f}%")
    print(f"  Retries: {aggregated_results['retries']}")

    if aggregated_results["successes"]:
        print(f"  Avg latency: {aggregated_results['avg_latency'] * 1000:.2f}ms")
        print(f"  Jitter: {aggregated_results['jitter'] * 1000:.2f}ms")
        print(f"  Throughput: {aggregated_results['throughput'] / 1024:.2f} KB/s")
        print(f"  Avg flow setup time: {aggregated_results['avg_setup_time']:.3f} sec")
    else:
        print("  No successful transmissions")

    write_csv(aggregated_results, label=args.label, filename=args.csv)
    print("=" * 80)

if __name__ == "__main__":
    main()

