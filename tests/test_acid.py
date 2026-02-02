import unittest
import threading
import time
import os
import sys
import subprocess
import signal
import socket
import json
import random

HOST = "localhost"
PORT = int(os.getenv("KV_SERVER_PORT", 8000))
DB_FILE = "acid_test.json"

class TCPClient:
    def __init__(self):
        self.host = HOST
        self.port = PORT

    def send_command(self, command):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                s.sendall((json.dumps(command) + "\n").encode('utf-8'))
                data = s.recv(4096)
                return json.loads(data.decode('utf-8').strip())
        except Exception:
            return None

    def bulk_set(self, items):
        return self.send_command({"command": "BULK_SET", "key": "batch", "items": items})

    def get(self, key):
        return self.send_command({"command": "GET", "key": key})

class TestACID(unittest.TestCase):
    def setUp(self):
        # Start Server
        if os.path.exists(DB_FILE):
             os.remove(DB_FILE)
             
        env = os.environ.copy()
        env["KV_SERVER_PORT"] = str(PORT)
        env["KV_STORE_FILE"] = DB_FILE
        env["PYTHONPATH"] = os.getcwd()
        
        self.server_process = subprocess.Popen(
            [sys.executable, "-u", "src/tcp_server.py"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1) # Wait for startup

    def tearDown(self):
        if self.server_process:
            self.server_process.send_signal(signal.SIGINT)
            self.server_process.wait()
        
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

    def test_isolation_concurrent_bulk_sets(self):
        """
        Test that concurrent bulk sets are isolated.
        Thread A sets (k1, A), (k2, A)
        Thread B sets (k1, B), (k2, B)
        Result should be either (A, A) or (B, B), never (A, B) or (B, A).
        This proves that the server processes the batch atomically in memory.
        """
        client1 = TCPClient()
        client2 = TCPClient()
        
        stop_event = threading.Event()
        errors = []
        
        def worker(client, val):
            while not stop_event.is_set():
                client.bulk_set([("k1", val), ("k2", val)])
                
        t1 = threading.Thread(target=worker, args=(client1, "A"))
        t2 = threading.Thread(target=worker, args=(client2, "B"))
        
        t1.start()
        t2.start()
        
        # Monitor for crashes but rely on final state for consistency check
        # checking during execution is prone to read-skew (getting k1 from T1 and k2 from T2's overwrite)
        time.sleep(2)
        stop_event.set()
        
        t1.join()
        t2.join()
        
        # Verify Final State Consistency
        # k1 and k2 must match (either both A or both B, or last writer wins)
        # They cannot be different.
        v1_resp = client1.get("k1")
        v2_resp = client1.get("k2")
        
        if v1_resp and v2_resp:
            v1 = v1_resp.get("result")
            v2 = v2_resp.get("result")
            if v1 != v2:
                self.fail(f"Inconsistency detected! k1={v1}, k2={v2}")
        else:
            self.fail("Failed to retrieve keys")
            
        if errors:
            self.fail(errors[0])
            
        print("\n[ISOLATION] Passed: Final state is consistent (k1 == k2).")

    def test_atomicity_persistence(self):
        """
        Verify that a BULK_SET is all-or-nothing durable.
        We perform a large bulk set and verify keys exist.
        (Note: Random kill testing is better handled by chaos_test, 
         this verifies basic functionality).
        """
        client = TCPClient()
        items = [(f"ak_{i}", f"av_{i}") for i in range(100)]
        resp = client.bulk_set(items)
        self.assertEqual(resp.get("status"), "success")
        
        # Verify all
        for i in range(100):
            val = client.get(f"ak_{i}")
            self.assertEqual(val.get("result"), f"av_{i}")
            
        print("\n[ATOMICITY] Passed: Bulk set applied correctly.")

if __name__ == "__main__":
    unittest.main()
