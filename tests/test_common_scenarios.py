import unittest
import time
import os
import sys
import subprocess
import signal
import json
from src.client import TCPClient

HOST = "localhost"
PORT = int(os.getenv("KV_SERVER_PORT", 8000))
DB_FILE = "common_test.json"

class TestCommonScenarios(unittest.TestCase):
    def setUp(self):
        # Start Server
        if os.path.exists(DB_FILE):
             os.remove(DB_FILE)
        self.start_server()
        self.client = TCPClient(HOST, PORT)

    def start_server(self):
        # Force kill any lingering server
        subprocess.run(["fuser", "-k", f"{PORT}/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1) # Wait for release

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
        time.sleep(1)

    def stop_server(self):
        if self.server_process:
            self.server_process.send_signal(signal.SIGTERM)
            self.server_process.wait()

    def tearDown(self):
        self.stop_server()
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

    def test_basic_ops(self):
        """Test Set, Get, Delete, Update"""
        # Set then Get
        self.client.set("k1", "v1")
        resp = self.client.get("k1")
        self.assertEqual(resp.get("result"), "v1")
        
        # Set then Set (Update)
        self.client.set("k1", "v2")
        resp = self.client.get("k1")
        self.assertEqual(resp.get("result"), "v2")
        
        # Set then Delete then Get
        self.client.delete("k1")
        resp = self.client.get("k1")
        self.assertEqual(resp.get("message"), "Key not found")

    def test_get_nonexistent(self):
        """Get without setting"""
        resp = self.client.get("unknown")
        self.assertEqual(resp.get("message"), "Key not found")

    def test_persistence_graceful(self):
        """Set then exit (gracefully) then Get"""
        self.client.set("persist", "true")
        
        # Graceful Exit
        self.stop_server()
        time.sleep(0.5)
        
        # Restart
        self.start_server()
        time.sleep(1)
        
        # Verify
        resp = self.client.get("persist")
        self.assertEqual(resp.get("result"), "true")

if __name__ == "__main__":
    unittest.main()
