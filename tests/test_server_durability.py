import unittest
import subprocess
import time
import httpx
import os
import signal
import sys

class TestServerDurability(unittest.TestCase):
    SERVER_PORT = 8001
    BASE_URL = f"http://localhost:{SERVER_PORT}"
    TEST_DB_FILE = "server_durability_test.json"
    
    def setUp(self):
        # Clean up previous test file
        if os.path.exists(self.TEST_DB_FILE):
            os.remove(self.TEST_DB_FILE)
        
        self.server_process = None

    def tearDown(self):
        # Stop server if running
        if self.server_process:
            os.kill(self.server_process.pid, signal.SIGTERM)
            self.server_process.wait()
        
        # Cleanup file
        if os.path.exists(self.TEST_DB_FILE):
            os.remove(self.TEST_DB_FILE)

    def start_server(self):
        """Start the FastAPI server in a subprocess."""
        env = os.environ.copy()
        env["KV_STORE_FILE"] = self.TEST_DB_FILE
        # Add local lib to PYTHONPATH
        env["PYTHONPATH"] = os.path.abspath("./lib") + ":" + env.get("PYTHONPATH", "")
        
        # Command to run server on specific port
        cmd = [
            sys.executable, "-m", "uvicorn", "src.server:app", 
            "--host", "127.0.0.1", "--port", str(self.SERVER_PORT)
        ]
        
        # Ensure current directory is in PYTHONPATH so src can be found
        env["PYTHONPATH"] = os.getcwd() + ":" + env["PYTHONPATH"]
        
        self.server_process = subprocess.Popen(
            cmd, 
            env=env,
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for server to be ready
        try:
            self.wait_for_server()
        except RuntimeError as e:
            self.server_process.kill() # Kill to ensure we can read output
            stdout, stderr = self.server_process.communicate()
            print(f"Server STDOUT:\n{stdout}")
            print(f"Server STDERR:\n{stderr}")
            raise e

    def wait_for_server(self):
        """Poll the server until it's ready."""
        retries = 50
        for _ in range(retries):
            try:
                httpx.get(f"{self.BASE_URL}/docs", timeout=0.5)
                return
            except (httpx.ConnectError, httpx.ReadTimeout):
                time.sleep(0.1)
        raise RuntimeError("Server failed to start")

    def test_durability_across_restarts(self):
        print("\nStarting server for durability test...")
        self.start_server()
        
        # 1. Write data
        print("Writing data...")
        httpx.post(f"{self.BASE_URL}/set", json={"key": "persistent_key", "value": "persistent_value"})
        
        # Verify write
        resp = httpx.get(f"{self.BASE_URL}/get/persistent_key")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["value"], "persistent_value")
        
        # Wait for background sync (interval is 1.0s, so wait 1.5s to be safe)
        time.sleep(1.5)
        
        # 2. Stop Server
        print("Stopping server...")
        os.kill(self.server_process.pid, signal.SIGTERM)
        self.server_process.wait()
        self.server_process = None
        
        # 3. Restart Server
        print("Restarting server...")
        self.start_server()
        
        # 4. Verify Data Persisted
        print("Verifying data persistence...")
        resp = httpx.get(f"{self.BASE_URL}/get/persistent_key")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["value"], "persistent_value")
        print("Data persisted successfully!")

if __name__ == "__main__":
    unittest.main()
