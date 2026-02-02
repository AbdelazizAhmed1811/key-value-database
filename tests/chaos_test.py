import subprocess
import time
import socket
import json
import os
import signal
import random
import sys
import threading
from typing import Dict, List, Any

# Configuration
SERVER_SCRIPT = "src/server.py"
DB_FILE = "chaos_test.json"
PORT = 9000
HOST = "localhost"
NUM_WRITES = 5000 # Increased for better chance of catching race conditions

# Global state
server_process = None
stop_writing = False
acked_writes: Dict[str, str] = {}
ambiguous_writes: Dict[str, str] = {}

def start_server():
    """Start the TCP server in a subprocess."""
    env = os.environ.copy()
    env["KV_SERVER_PORT"] = str(PORT)
    env["KV_STORE_FILE"] = DB_FILE
    env["PYTHONPATH"] = os.getcwd() # Fix import error
    # Use unbuffered output for python
    process = subprocess.Popen(
        [sys.executable, "-u", SERVER_SCRIPT],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid 
    )
    time.sleep(1) # Wait for startup
    return process

def kill_server(process):
    """Kill the server process immediately (SIGKILL)."""
    if process:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            process.wait()
        except ProcessLookupError:
            pass

def send_command(command):
    """Send a command to the server."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            s.connect((HOST, PORT))
            s.sendall((json.dumps(command) + "\n").encode('utf-8'))
            data = s.recv(4096)
            return json.loads(data.decode('utf-8'))
    except Exception:
        return None

def writer_thread_func():
    global stop_writing
    print(f"Writer thread started. Aiming for {NUM_WRITES} writes...")
    
    for i in range(NUM_WRITES):
        if stop_writing:
            break
            
        key = f"key_{i}"
        value = f"value_{i}"
        
        # We record the attempt before sending, but move to acked only on success
        try:
            resp = send_command({"command": "SET", "key": key, "value": value})
            
            if resp and resp.get("status") == "success":
                acked_writes[key] = value
            else:
                # If we didn't get a success response, it might have been written or not.
                ambiguous_writes[key] = value
                # If we get an error/None, it likely means server crashed or is crashing
                # We continue trying until the main thread tells us to stop (or we just fail repeatedly)
        except Exception:
            ambiguous_writes[key] = value

    print(f"Writer thread finished. Acked: {len(acked_writes)}, Ambiguous: {len(ambiguous_writes)}")

def killer_thread_func():
    global stop_writing, server_process
    
    # Wait a random amount of time before killing
    sleep_time = random.uniform(0.5, 3.0)
    print(f"Killer thread waiting {sleep_time:.2f}s...")
    time.sleep(sleep_time)
    
    print("!!! KILLING SERVER !!!")
    kill_server(server_process)
    stop_writing = True

def run_chaos_test():
    global server_process
    print(f"--- Starting Enhanced Durability Chaos Test ---")
    
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    
    # 1. Start Server
    print("Starting server...")
    server_process = start_server()
    
    # 2. Start Threads
    writer = threading.Thread(target=writer_thread_func)
    killer = threading.Thread(target=killer_thread_func)
    
    writer.start()
    killer.start()
    
    # 3. Wait for completion
    killer.join()
    writer.join()
    
    print("Test phase complete. Verifying...")
    
    # 4. Restart Server
    print("Restarting server...")
    server_process = start_server()
    
    # 5. Verify Data
    missing_acked = []
    mismatched_acked = []
    recovered_ambiguous = []
    
    # Check Acked Writes (MUST be present)
    print(f"Verifying {len(acked_writes)} acknowledged writes...")
    for key, expected_value in acked_writes.items():
        resp = send_command({"command": "GET", "key": key})
        
        if not resp or resp.get("status") != "success":
            missing_acked.append(key)
        elif resp.get("result") != expected_value:
            mismatched_acked.append(key)

    # Check Ambiguous Writes (MIGHT be present)
    print(f"Checking {len(ambiguous_writes)} ambiguous writes...")
    for key, expected_value in ambiguous_writes.items():
        resp = send_command({"command": "GET", "key": key})
        if resp and resp.get("status") == "success" and resp.get("result") == expected_value:
            recovered_ambiguous.append(key)
            
    # 6. Report
    print("\n--- Results ---")
    print(f"Total Acked Writes: {len(acked_writes)}")
    print(f"Missing Acked: {len(missing_acked)}")
    print(f"Mismatched Acked: {len(mismatched_acked)}")
    print(f"Ambiguous Writes Recovered: {len(recovered_ambiguous)} / {len(ambiguous_writes)}")
    
    durability_score = 100.0
    if len(acked_writes) > 0:
        durability_score = ((len(acked_writes) - len(missing_acked) - len(mismatched_acked)) / len(acked_writes)) * 100.0
        
    print(f"Durability Score (Acked): {durability_score:.2f}%")
    
    # Cleanup
    kill_server(server_process)
    
    # Print server output for debugging
    stdout, stderr = server_process.communicate()
    print("\n--- Server Output ---")
    if stdout: print(stdout.decode('utf-8'))
    if stderr: print(stderr.decode('utf-8'))
    
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    if durability_score == 100.0 and len(acked_writes) > 0:
        print("TEST PASSED: Perfect Durability for Acknowledged Writes")
        sys.exit(0)
    elif len(acked_writes) == 0:
        print("TEST INCONCLUSIVE: 0 Acknowledged Writes (Server failed?)")
        sys.exit(1)
    else:
        print("TEST FAILED: Data Loss Detected")
        sys.exit(1)

if __name__ == "__main__":
    run_chaos_test()
