"""
Tests for indexing and search functionality.
"""

import unittest
import subprocess
import time
import os
import sys
from src.client import TCPClient
from src.kv_store import KeyValueStore


class TestIndexingUnit(unittest.TestCase):
    """Unit tests for indexing module."""

    def setUp(self):
        self.db_file = "test_index.log"
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
        self.db = KeyValueStore(self.db_file)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.db_file):
            os.remove(self.db_file)

    def test_value_index(self):
        """Test secondary value index."""
        self.db.create_index("category")
        
        self.db.set("doc1", {"title": "Apple", "category": "fruit"})
        self.db.set("doc2", {"title": "Banana", "category": "fruit"})
        self.db.set("doc3", {"title": "Carrot", "category": "vegetable"})
        
        results = self.db.query_index("category", "fruit")
        self.assertEqual(set(results), {"doc1", "doc2"})
        
        results = self.db.query_index("category", "vegetable")
        self.assertEqual(results, ["doc3"])

    def test_full_text_search(self):
        """Test BM25 full-text search."""
        self.db.set("doc1", "The quick brown fox jumps over the lazy dog")
        self.db.set("doc2", "A quick brown dog runs in the park")
        self.db.set("doc3", "The lazy cat sleeps all day")
        
        results = self.db.search("quick brown", top_k=10)
        self.assertTrue(len(results) >= 2)
        
        # First two results should contain both "quick" and "brown"
        top_keys = [r[0] for r in results[:2]]
        self.assertIn("doc1", top_keys)
        self.assertIn("doc2", top_keys)

    def test_semantic_search(self):
        """Test TF-IDF semantic search."""
        self.db.set("doc1", "machine learning artificial intelligence")
        self.db.set("doc2", "deep learning neural networks")
        self.db.set("doc3", "cooking recipes and food")
        
        results = self.db.semantic_search("artificial intelligence machine learning", top_k=3)
        self.assertTrue(len(results) > 0)
        
        # doc1 should be most similar
        self.assertEqual(results[0][0], "doc1")

    def test_index_updates_on_delete(self):
        """Test that indexes are updated on delete."""
        self.db.set("doc1", "hello world")
        
        results = self.db.search("hello")
        self.assertEqual(len(results), 1)
        
        self.db.delete("doc1")
        
        results = self.db.search("hello")
        self.assertEqual(len(results), 0)


class TestIndexingIntegration(unittest.TestCase):
    """Integration tests via TCP client."""

    @classmethod
    def setUpClass(cls):
        # Start server
        subprocess.run(["fuser", "-k", "8888/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)
        
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        
        cls.server_proc = subprocess.Popen(
            [sys.executable, "-u", "src/server.py", "--port", "8888", "--id", "test_idx"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)

    @classmethod
    def tearDownClass(cls):
        cls.server_proc.terminate()
        cls.server_proc.wait()
        subprocess.run(["fuser", "-k", "8888/tcp"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if os.path.exists("kv_store_test_idx.json"):
            os.remove("kv_store_test_idx.json")

    def test_search_via_client(self):
        """Test SEARCH command via TCP."""
        client = TCPClient("localhost", 8888)
        
        # Add some documents
        client.set("search_doc1", "Python programming language")
        client.set("search_doc2", "Python is great for data science")
        client.set("search_doc3", "JavaScript for web development")
        
        # Search
        resp = client.send_command({"command": "SEARCH", "query": "Python programming", "top_k": 5})
        self.assertEqual(resp.get("status"), "success")
        results = resp.get("result", [])
        self.assertTrue(len(results) > 0)

    def test_semantic_search_via_client(self):
        """Test SEMANTIC_SEARCH command via TCP."""
        client = TCPClient("localhost", 8888)
        
        resp = client.send_command({"command": "SEMANTIC_SEARCH", "query": "data science", "top_k": 5})
        self.assertEqual(resp.get("status"), "success")


if __name__ == "__main__":
    unittest.main()
