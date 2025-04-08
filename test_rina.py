from concurrent.futures import process
import logging
import sys
import time
import statistics
import argparse
import csv
import os
from datetime import datetime
from flow import Flow
import asyncio
import protocol
import psutil

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

async def async_run_test(host, port, dest_apn, num_packets=100, payload_kb=1, teardown_interval=10, warmup_packets=0, retries=3):
    start_time = time.time()
    flow = Flow(dest_apn, host, port)
    flow.retry_base_delay = 0.2
    await flow.allocate()
    if not flow.connected:
        logging.error("Failed to allocate flow for testing.")
        return {
            "latencies": [],
            "payload_size": payload_kb * 1024,
            "successes": 0,
            "failures": num_packets,
            "retries": 0,
            "setup_times": [],
        }

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
        await flow.send(payload_size=results["payload_size"])
        await asyncio.sleep(0.01)

    for i in range(num_packets):
        if teardown_interval > 0 and i > 0 and i % teardown_interval == 0:
            print(f"\n[Flow Teardown at packet {i}]")
            await flow.teardown()
            t0 = time.time()
            await flow.allocate()
            setup_time = time.time() - t0
            results["setup_times"].append(setup_time)
            await asyncio.sleep(0.05)

        success = False
        for attempt in range(retries):
            #await asyncio.sleep(min(0.2 * (2 ** attempt), 2.0))
            try:
                latency = await flow.send(payload_size=results["payload_size"])
                if latency:
                    results["latencies"].append(latency)
                    results["successes"] += 1
                    success = True
                    break
                else:
                    results["retries"] += 1
                    await asyncio.sleep(min(0.1 * (2 ** attempt), 1.0)) 
            except Exception as e:
                logging.error(f"Error sending packet {i}, attempt {attempt + 1}: {e}")
                results["failures"] += 1
                await asyncio.sleep(min(0.1 * (2 ** attempt), 1.0))

        if not success:
            results["failures"] += 1

        await asyncio.sleep(0.01)

    end_time = time.time()
    total_wall_time = end_time - start_time

    if results["successes"] > 0:
        total_bytes = results["successes"] * (
            results["payload_size"] + 
            protocol.HEADER_SIZE + 
            protocol.FLOW_ID_LENGTH
        )
        results.update({
            "avg_latency": statistics.mean(results["latencies"]),
            "jitter": statistics.stdev(results["latencies"]) if len(results["latencies"]) > 1 else 0,
            "throughput": total_bytes / total_wall_time,  # In bytes/sec
            "avg_setup_time": statistics.mean(results["setup_times"]) if results["setup_times"] else 0,
            "avg_rtt": statistics.mean(results["latencies"]) * 2,
            "memory_usage": psutil.Process().memory_info().rss
        })
    elif not results["setup_times"]:
        results["avg_setup_time"] = 0

    return results


def write_csv(results, label="RINA", filename="results.csv"):
    header = ["Label", "Timestamp", "Success", "TotalPackets", "PacketLoss(%)", "Retries", "AvgLatency(ms)", "Jitter(ms)", "Throughput(KiB/s)", "AvgSetupTime(s)"]
    row = [
        label,
        datetime.now().isoformat(),
        results.get("successes", 0),
        results.get("successes", 0) + results.get("failures", 0),
        round((results.get("failures", 0) / (results.get("successes", 0) + results.get("failures", 0))) * 100) if (results.get("successes", 0) + results.get("failures", 0)) > 0 else 0,
        results.get("retries", 0),
        round(results.get("avg_latency", 0) * 1000, 3) if results.get("avg_latency") is not None else 0,
        round(results.get("jitter", 0) * 1000, 3) if results.get("jitter") is not None else 0,
        round(results.get("throughput", 0) / 1024, 2),
        round(results.get("avg_setup_time", 0), 3) if results.get("avg_setup_time") is not None else 0
    ]

    file_exists = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


async def main():
    parser = argparse.ArgumentParser(description="RINA Performance Test")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=10000)
    parser.add_argument("--apn", default="APN_A")
    parser.add_argument("--packets", type=int, default=100)
    parser.add_argument("--size", type=int, default=1, choices=[1, 10, 100, 1000])
    parser.add_argument("--emulate", type=str, help="Format: latency_ms,loss%,bandwidth")
    parser.add_argument("--teardown-interval", type=int, default=10)
    parser.add_argument("--warmup-packets", type=int, default=0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--csv", default="results.csv")
    parser.add_argument("--label", default="RINA")
    parser.add_argument("--processes", type=int, default=1, help="Number of parallel processes")

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

    start_overall_time = time.time()

    if args.processes > 1:
        async def run_test_in_process(args):
            return await async_run_test(*args)

        async def parallel_tests():
            tasks = [run_test_in_process(test_args) for _ in range(args.processes)]
            return await asyncio.gather(*tasks)

        results = await parallel_tests()

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

        end_overall_time = time.time()
        total_wall_time = end_overall_time - start_overall_time

        if aggregated_results["successes"] > 0 and total_wall_time > 0:
            total_bytes = aggregated_results["successes"] * (
                aggregated_results["payload_size"] + 
                protocol.HEADER_SIZE + 
                protocol.FLOW_ID_LENGTH
            )
            aggregated_results.update({
                "avg_latency": statistics.mean(aggregated_results["latencies"]),
                "jitter": statistics.stdev(aggregated_results["latencies"]) if len(aggregated_results["latencies"]) > 1 else 0,
                "throughput": total_bytes / total_wall_time,
                "avg_setup_time": statistics.mean(aggregated_results["setup_times"]) if aggregated_results["setup_times"] else 0,
                "avg_rtt" : statistics.mean(aggregated_results["latencies"]) * 2,
                "memory_usage": psutil.Process().memory_info().rss
            })
        elif not aggregated_results["setup_times"]:
            aggregated_results["avg_setup_time"] = 0

    else:
        aggregated_results = await async_run_test(*test_args)
        end_overall_time = time.time()
        total_wall_time = end_overall_time - start_overall_time
        if aggregated_results["successes"] > 0 and total_wall_time > 0:
            total_bytes = aggregated_results["successes"] * (
                aggregated_results["payload_size"] + 
                protocol.HEADER_SIZE + 
                protocol.FLOW_ID_LENGTH
            )
            aggregated_results["throughput"] = total_bytes / total_wall_time
        else:
            aggregated_results["throughput"] = 0


    print("\nResults:")
    print(f"Success: {aggregated_results['successes']}/{args.packets * args.processes}")
    print(f"Packet loss: {(aggregated_results['failures'] / (args.packets * args.processes)) * 100:.2f}%" if (args.packets * args.processes) > 0 else "N/A")
    print(f"Retries: {aggregated_results['retries']}")

    if aggregated_results["successes"]:
        print(f"Avg latency: {aggregated_results['avg_latency'] * 1000:.2f}ms")
        print(f"Jitter: {aggregated_results['jitter'] * 1000:.2f}ms")
        print(f"Throughput: {aggregated_results['throughput'] / 1024:.2f} KiB/s ({aggregated_results['throughput'] * 8 / (1024*1024):.2f} Mbps)")
        print(f"Avg flow setup time: {aggregated_results['avg_setup_time']:.3f} sec")
        print(f"avg_rtt: {aggregated_results['avg_rtt']:.3f} sec")
        print(f"Memory usage: {aggregated_results['memory_usage'] / (1024*1024):.2f} MB")
    else:
        print("No successful transmissions")

    write_csv(aggregated_results, label=args.label, filename=args.csv)
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
