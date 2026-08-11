import re
import sys

def extract_throughput_results(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    results = []
    current_bm = ""
    current_mode = ""
    current_params = ""
    in_throughput = False

    for i, line in enumerate(lines):
        line_s = line.strip()

        # Detect benchmark name
        m = re.match(r'# Benchmark:\s+.*\.([^.]+)$', line_s)
        if m:
            current_bm = m.group(1)
            continue

        # Detect benchmark mode
        m = re.match(r'# Benchmark mode:\s+(.+)', line_s)
        if m:
            current_mode = m.group(1).strip()
            in_throughput = "Throughput" in current_mode
            continue

        # Detect parameters
        m = re.match(r'# Parameters:\s+\(([^)]+)\)', line_s)
        if m:
            current_params = m.group(1).strip()
            continue

        # Detect result (only for throughput)
        if in_throughput and re.match(r'^Result\s+"[^"]+":', line_s):
            if i + 1 < len(lines):
                score_line = lines[i + 1].strip()
                sm = re.match(r'([\d.]+)\s+.*?\s+(ops/s|usecs|msecs|s/op)', score_line)
                if sm:
                    score = float(sm.group(1))
                    unit = sm.group(2)
                    results.append({
                        'benchmark': current_bm,
                        'params': current_params,
                        'score': score,
                        'unit': unit
                    })

    return results

# Process all files
import glob, os

base_dir = r'D:\learn\project\StreamMQ\benchmark-results'

# Serialization
ser_files = sorted(glob.glob(os.path.join(base_dir, 'serialization-*.txt')))
# Use the v3 file which has complete results
ser_file = os.path.join(base_dir, 'serialization-20260808-193921.txt')
if os.path.exists(ser_file):
    ser_results = extract_throughput_results(ser_file)
    print("=" * 90)
    print("SERIALIZATION BENCHMARK RESULTS (Throughput)")
    print("=" * 90)
    print(f"{'Benchmark':<30} {'Params':<20} {'Score':>15} {'Unit':<10}")
    print("-" * 90)
    for r in ser_results:
        print(f"{r['benchmark']:<30} {r['params']:<20} {r['score']:>15,.1f} {r['unit']:<10}")

# Send
send_files = sorted(glob.glob(os.path.join(base_dir, 'send-*.txt')))
if send_files:
    send_file = send_files[-1]
    send_results = extract_throughput_results(send_file)
    print(f"\n{'=' * 90}")
    print(f"SEND BENCHMARK RESULTS (Throughput) - {os.path.basename(send_file)}")
    print("=" * 90)
    print(f"{'Benchmark':<30} {'Params':<20} {'Score':>15} {'Unit':<10}")
    print("-" * 90)
    for r in send_results:
        print(f"{r['benchmark']:<30} {r['params']:<20} {r['score']:>15,.1f} {r['unit']:<10}")

# Consume
cons_files = sorted(glob.glob(os.path.join(base_dir, 'consume-*.txt')))
if cons_files:
    cons_file = cons_files[-1]
    cons_results = extract_throughput_results(cons_file)
    print(f"\n{'=' * 90}")
    print(f"CONSUME BENCHMARK RESULTS (Throughput) - {os.path.basename(cons_file)}")
    print("=" * 90)
    print(f"{'Benchmark':<30} {'Params':<20} {'Score':>15} {'Unit':<10}")
    print("-" * 90)
    for r in cons_results:
        print(f"{r['benchmark']:<30} {r['params']:<20} {r['score']:>15,.1f} {r['unit']:<10}")
else:
    print("\nNo consume benchmark results yet.")