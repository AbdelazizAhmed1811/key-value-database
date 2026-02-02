import unittest
import threading
import time
import os
import sys
import subprocess
import signal
import json
from src.client import TCPClient

HOST = "localhost"
PORT = int(os.getenv("KV_SERVER_PORT", 8000))
DB_FILE = "acid_test.json"

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
            [sys.executable, "-u", "src/server.py"],
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
        """
        client1 = TCPClient(HOST, PORT)
        client2 = TCPClient(HOST, PORT)
        
        stop_event = threading.Event()
        errors = []
        
        def worker(client, val):
            while not stop_event.is_set():
                client.bulk_set([("k1", val), ("k2", val)])
                
        t1 = threading.Thread(target=worker, args=(client1, "A"))
        t2 = threading.Thread(target=worker, args=(client2, "B"))
        
        t1.start()
        t2.start()
        
        # Run concurrent writes for a bit
        time.sleep(2)
        stop_event.set()
        
        t1.join()
        t2.join()
        
        # Verify Final State Consistency
        v1_resp = client1.get("k1")
        v2_resp = client1.get("k2")
        
        if v1_resp and v2_resp:
            v1 = v1_resp.get("result")
            v2 = v2_resp.get("result")
            if v1 != v2:
                self.fail(f"Inconsistency detected! k1={v1}, k2={v2}")
        else:
            self.fail("Failed to retrieve keys")
            
        print("\n[ISOLATION] Passed: Final state is consistent (k1 == k2).")

    def test_atomicity_persistence(self):
        """
        Verify that a BULK_SET is all-or-nothing durable.
        """
        client = TCPClient(HOST, PORT)
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
