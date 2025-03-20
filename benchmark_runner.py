import subprocess
import argparse
import csv
from datetime import datetime
import os
import re

def parse_rina_output(output):
    result = {
        "Label": "RINA",
        "Success": None,
        "PacketLoss(%)": None,
        "Retries": None,
        "AvgLatency(ms)": None,
        "Jitter(ms)": None,
        "Throughput(KB/s)": None,
        "AvgSetupTime(s)": None
    }
    for line in output.splitlines():
        if "Success:" in line:
            result["Success"] = int(re.findall(r"\d+", line)[0])
        elif "Packet loss:" in line:
            result["PacketLoss(%)"] = float(re.findall(r"\d+\.\d+", line)[0])
        elif "Retries:" in line:
            result["Retries"] = int(re.findall(r"\d+", line)[0])
        elif "Avg latency:" in line:
            result["AvgLatency(ms)"] = float(re.findall(r"\d+\.\d+", line)[0])
        elif "Jitter:" in line:
            result["Jitter(ms)"] = float(re.findall(r"\d+\.\d+", line)[0])
        elif "Throughput:" in line:
            result["Throughput(KB/s)"] = float(re.findall(r"\d+\.\d+", line)[0])
        elif "Avg flow setup time:" in line:
            result["AvgSetupTime(s)"] = float(re.findall(r"\d+\.\d+", line)[0])
    return result

def parse_tcp_output(output):
    result = {
        "Label": "TCP",
        "Success": None,
        "PacketLoss(%)": None,
        "Retries": 0,
        "AvgLatency(ms)": None,
        "Jitter(ms)": None,
        "Throughput(KB/s)": None,
        "AvgSetupTime(s)": 0
    }
    for line in output.splitlines():
        if "Latency" in line:
            result["AvgLatency(ms)"] = float(re.findall(r"\d+\.\d+", line)[0]) * 1000
        elif "Jitter" in line:
            result["Jitter(ms)"] = float(re.findall(r"\d+\.\d+", line)[0]) * 1000
        elif "Packet Loss" in line:
            result["PacketLoss(%)"] = float(re.findall(r"\d+\.\d+", line)[0])
        elif "Throughput" in line:
            result["Throughput(KB/s)"] = float(re.findall(r"\d+\.\d+", line)[0])
    return result

def write_csv(results, filename):
    header = ["Label", "Timestamp", "Success", "PacketLoss(%)", "Retries", "AvgLatency(ms)", "Jitter(ms)", "Throughput(KB/s)", "AvgSetupTime(s)"]
    file_exists = os.path.isfile(filename)
    with open(filename, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            writer.writeheader()
        for result in results:
            result["Timestamp"] = datetime.now().isoformat()
            writer.writerow(result)

def main():
    parser = argparse.ArgumentParser(description="Benchmark Runner for RINA vs TCP")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--tcp-port", type=int, default=11000)
    parser.add_argument("--rina-port", type=int, default=10000)
    parser.add_argument("--apn", default="APN_A")
    parser.add_argument("--packets", type=int, default=100)
    parser.add_argument("--size", type=int, default=1)
    parser.add_argument("--teardown-interval", type=int, default=0)
    parser.add_argument("--csv", default="comparison_results.csv")

    args = parser.parse_args()

    print("Running RINA benchmark...")
    rina_cmd = ["python", "test_rina.py",
                "--host", args.host,
                "--port", str(args.rina_port),
                "--apn", args.apn,
                "--packets", str(args.packets),
                "--size", str(args.size),
                "--teardown-interval", str(args.teardown_interval)]
    rina_output = subprocess.check_output(rina_cmd, text=True)
    print(rina_output)
    rina_results = parse_rina_output(rina_output)

    print("Running TCP benchmark...")
    tcp_cmd = ["python", "test_tcp.py",
               "--host", args.host,
               "--port", str(args.tcp_port),
               "--packets", str(args.packets)]
    tcp_output = subprocess.check_output(tcp_cmd, text=True)
    print(tcp_output)
    tcp_results = parse_tcp_output(tcp_output)

    write_csv([rina_results, tcp_results], filename=args.csv)
    print(f"Results written to {args.csv}")

if __name__ == "__main__":
    main()
