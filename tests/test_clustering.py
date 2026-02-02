import unittest
import subprocess
import time
import os
import sys
import signal
import shutil
from src.client import TCPClient

class TestClustering(unittest.TestCase):
    def setUp(self):
        # Cleanup
        for p in [8000, 8001, 8002]:
             subprocess.run(["fuser", "-k", f"{p}/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for f in ["kv_store_node1.json", "kv_store_node2.json", "kv_store_node3.json"]:
            if os.path.exists(f): os.remove(f)

        # Start 3 Nodes
        # Access PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        
        self.procs = []
        peers_map = {
            "node1": "127.0.0.1:8001,127.0.0.1:8002",
            "node2": "127.0.0.1:8000,127.0.0.1:8002",
            "node3": "127.0.0.1:8000,127.0.0.1:8001"
        }
        ports_map = {"node1": 8000, "node2": 8001, "node3": 8002}
        
        print("\nStarting Cluster...")
        for node_id in ["node1", "node2", "node3"]:
            cmd = [
                sys.executable, "-u", "src/server.py",
                "--id", node_id,
                "--port", str(ports_map[node_id]),
                "--peers", peers_map[node_id]
            ]
            # Redirect stdout to avoid clogging test runner, or keep for debug
            # For now keep to pipe to debug if needed
            p = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.procs.append(p)
            
        time.sleep(5) # Wait for election

    def tearDown(self):
        print("\nTearing down cluster...")
        for i, p in enumerate(self.procs):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    p.kill()
            
            # Print Logs
            out, err = p.communicate()
            if out: print(f"--- Node {i} STDOUT ---\n{out.decode('utf-8')}")
            if err: print(f"--- Node {i} STDERR ---\n{err.decode('utf-8')}")
            
        for p in [8000, 8001, 8002]:
             subprocess.run(["fuser", "-k", f"{p}/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        for f in ["kv_store_node1.json", "kv_store_node2.json", "kv_store_node3.json"]:
            if os.path.exists(f): os.remove(f)

    def test_replication_and_failover(self):
        """
        1. Connect to any node.
        2. Write Key.
        3. Verify success (Client follows redirect).
        4. Kill Leader.
        5. Wait for Election.
        6. Read Key from New Leader.
        """
        client = TCPClient("127.0.0.1", 8000)
        
        # 1. Write
        print("Writing 'cluster_key'...")
        resp = client.set("cluster_key", "replicated_val")
        print(f"Write Resp: {resp}")
        self.assertEqual(resp.get("status"), "success")
        
        # Identify Leader
        leader_host = client.host
        leader_port = client.port
        print(f"Current Leader seems to be: {leader_host}:{leader_port}")
        
        # 2. Verify Read
        resp = client.get("cluster_key")
        self.assertEqual(resp.get("result"), "replicated_val")
        
        # Wait for replication to complete across all nodes
        time.sleep(1)
        
        # 3. Kill Leader
        print(f"Killing Leader on port {leader_port}...")
        
        # Find the process for this port
        target_proc = None
        idx_to_remove = -1
        
        ports_map = [8000, 8001, 8002]
        for i, p in enumerate(self.procs):
            if ports_map[i] == leader_port:
                target_proc = p
                idx_to_remove = i
                break
        
        if target_proc:
            target_proc.terminate()
            target_proc.wait()
            self.procs.pop(idx_to_remove)
        else:
            self.fail("Could not find leader process")
            
        print("Waiting 5s for Re-Election...")
        time.sleep(5)
        
        # 4. Read from (hopefully) new leader
        # Client should auto-redirect if we connect to a surviving node
        # We need to manually point client to a survivor if the one it holds is dead
        # The client code updates self.host/port. If that one is dead, it fails.
        # So we should reset client to a known alive node.
        
        alive_port = 8001 if leader_port == 8000 else 8000
        print(f"Connecting to survivor at {alive_port}...")
        client = TCPClient("localhost", alive_port)
        
        resp = client.get("cluster_key")
        print(f"Read Post-Failover Resp: {resp}")
        
        if resp.get("status") == "error" and "redirect" in resp:
             # Manual redirect if client didn't handle it (it should though)
             # But client handles it in send_command loop.
             pass
             
        self.assertEqual(resp.get("result"), "replicated_val", "Data lost after failover!")
        print("SUCCESS: Data persisted and replicated to new leader.")

if __name__ == "__main__":
    unittest.main()
