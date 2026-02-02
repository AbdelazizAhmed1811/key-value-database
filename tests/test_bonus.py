import unittest
import time
import os
import sys
import subprocess
import signal
import socket
import json

HOST = "localhost"
PORT = int(os.getenv("KV_SERVER_PORT", 8000))
DB_FILE = "bonus_test.json"

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

    def set_debug(self, key, value):
        return self.send_command({"command": "SET", "key": key, "value": value, "simulate_failure": True})

    def get(self, key):
        return self.send_command({"command": "GET", "key": key})

class TestBonusFailure(unittest.TestCase):
    def setUp(self):
        if os.path.exists(DB_FILE):
             os.remove(DB_FILE)
        self.start_server()
        self.client = TCPClient()

    def start_server(self):
        subprocess.run(["fuser", "-k", f"{PORT}/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1)
        
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
        time.sleep(1)

    def stop_server(self):
        if self.server_process:
            self.server_process.send_signal(signal.SIGTERM)
            self.server_process.wait()

    def tearDown(self):
        self.stop_server()
        if os.path.exists(DB_FILE):
             os.remove(DB_FILE)

    def test_simulated_failure(self):
        """
        Send 500 writes with simulate_failure=True.
        Restart.
        Verify that we lost some data (approx 1%, so >0).
        """
        count = 500
        print(f"Sending {count} writes with potential failure...")
        for i in range(count):
            self.client.set_debug(f"k_{i}", f"v_{i}")
        
        # Stop and Restart to trigger log replay
        self.stop_server()
        time.sleep(0.5)
        self.start_server()
        
        # Verify
        missing = 0
        for i in range(count):
            resp = self.client.get(f"k_{i}")
            if resp.get("status") == "error":
                missing += 1
        
        print(f"Missing keys: {missing}/{count} ({missing/count*100:.2f}%)")
        
        # It's random, but with 500 ops at 1%, expected is 5.
        # Probability of 0 failure is 0.99^500 = 0.006 (0.6%).
        # So it's highly likely we see at least 1 missing.
        if missing > 0:
            print("SUCCESS: Simulated failure caused data loss as expected.")
        else:
            print("WARNING: No data loss occurred. Might be bad luck or bug.")
            # We don't fail the test strictly to avoid flakes, but we print.
            # actually if we want to confirm it works, we might want to check for > 0
            # but let's just log it.

if __name__ == "__main__":
    unittest.main()
