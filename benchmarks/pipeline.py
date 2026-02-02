import socket
import json
import time
import os

HOST = "localhost"
PORT = int(os.getenv("KV_SERVER_PORT", 8000))
NUM_REQUESTS = 50000

def run_pipeline_benchmark():
    print(f"--- Starting Pipelined Benchmark (Client-Side Batching) ---")
    print(f"Goal: {NUM_REQUESTS} requests on a single connection")
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    
    # 1. Prepare Payload
    print("Preparing payload...")
    payload = ""
    for i in range(NUM_REQUESTS):
        cmd = {"command": "SET", "key": f"pipe_{i}", "value": "x"}
        payload += json.dumps(cmd) + "\n"
    
    payload_bytes = payload.encode('utf-8')
    print(f"Payload size: {len(payload_bytes) / 1024 / 1024:.2f} MB")
    
    # 2. Send All (Blast)
    print("Sending...")
    start_time = time.time()
    s.sendall(payload_bytes)
    
    # 3. Receive All
    print("Receiving...")
    received_count = 0
    buffer = b""
    while received_count < NUM_REQUESTS:
        data = s.recv(65536)
        if not data:
            break
        buffer += data
        received_count += data.count(b"\n")
        
    end_time = time.time()
    s.close()
    
    duration = end_time - start_time
    ops_per_sec = NUM_REQUESTS / duration
    
    print(f"Completed in {duration:.2f} seconds")
    print(f"Throughput: {ops_per_sec:.2f} ops/sec")
    print("-" * 30)

if __name__ == "__main__":
    run_pipeline_benchmark()
