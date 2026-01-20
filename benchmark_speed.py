import socket
import json
import time
import threading
import random
import string
from concurrent.futures import ThreadPoolExecutor
import os

HOST = "localhost"
PORT = int(os.getenv("KV_SERVER_PORT", 8000))
NUM_REQUESTS = 10000
CONCURRENCY = 50

def get_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    return s

def send_request(sock, command):
    sock.sendall((json.dumps(command) + "\n").encode('utf-8'))
    data = sock.recv(4096)
    return json.loads(data.decode('utf-8'))

def worker_write(num_ops, start_index):
    client = get_client()
    for i in range(num_ops):
        key = f"bench_key_{start_index + i}"
        value = "x" * 100 # 100 byte value
        send_request(client, {"command": "SET", "key": key, "value": value})
    client.close()

def worker_read(num_ops, start_index):
    client = get_client()
    for i in range(num_ops):
        key = f"bench_key_{start_index + i}"
        send_request(client, {"command": "GET", "key": key})
    client.close()

def run_benchmark(name, worker_func):
    print(f"--- Starting {name} Benchmark ---")
    print(f"Requests: {NUM_REQUESTS}, Concurrency: {CONCURRENCY}")
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        ops_per_worker = NUM_REQUESTS // CONCURRENCY
        futures = []
        for i in range(CONCURRENCY):
            futures.append(executor.submit(worker_func, ops_per_worker, i * ops_per_worker))
        
        for f in futures:
            f.result()
            
    end_time = time.time()
    duration = end_time - start_time
    ops_per_sec = NUM_REQUESTS / duration
    
    print(f"Completed in {duration:.2f} seconds")
    print(f"Throughput: {ops_per_sec:.2f} ops/sec")
    print("-" * 30 + "\n")

if __name__ == "__main__":
    # Ensure we have enough file descriptors for threads if needed
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (max(soft, 4096), hard))
    except ImportError:
        pass

    print(f"Targeting Server at {HOST}:{PORT}")
    
    # 1. Benchmark Writes
    run_benchmark("WRITE", worker_write)
    
    # 2. Benchmark Reads
    run_benchmark("READ", worker_read)
