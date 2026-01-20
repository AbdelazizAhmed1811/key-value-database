import unittest
import os
import time
import json
from src.kv_store import KeyValueStore, KeyNotFoundError

class TestKeyValueStoreWAL(unittest.TestCase):
    TEST_DB_FILE = "test_kv_store.log"

    def setUp(self):
        if os.path.exists(self.TEST_DB_FILE):
            os.remove(self.TEST_DB_FILE)
        self.kv = KeyValueStore(self.TEST_DB_FILE, sync_interval=0.1)

    def tearDown(self):
        self.kv.close()
        if os.path.exists(self.TEST_DB_FILE):
            os.remove(self.TEST_DB_FILE)
        if os.path.exists(f"{self.TEST_DB_FILE}.tmp"):
            os.remove(f"{self.TEST_DB_FILE}.tmp")

    def test_set_and_get(self):
        self.kv.set("name", "Zizo")
        self.assertEqual(self.kv.get("name"), "Zizo")

    def test_persistence_wal(self):
        self.kv.set("key1", "value1")
        self.kv.set("key2", "value2")
        self.kv.delete("key1")
        
        # Force sync before closing
        self.kv.close()
        
        new_kv = KeyValueStore(self.TEST_DB_FILE)
        self.assertIsNone(new_kv.get("key1"))
        self.assertEqual(new_kv.get("key2"), "value2")
        new_kv.close()

    def test_compaction(self):
        self.kv.set("key", "v1")
        self.kv.set("key", "v2")
        self.kv.compact()
        
        self.assertEqual(self.kv.get("key"), "v2")
        
        with open(self.TEST_DB_FILE, 'r') as f:
            lines = len(f.readlines())
        self.assertEqual(lines, 1)

    def test_background_sync(self):
        """Test that data is eventually synced to disk."""
        self.kv.set("async_key", "async_val")
        
        # Immediately check file size (might be 0 or small if not flushed)
        # Wait for sync interval + buffer
        time.sleep(0.2)
        
        with open(self.TEST_DB_FILE, 'r') as f:
            content = f.read()
        
        self.assertIn("async_key", content)

if __name__ == '__main__':
    unittest.main()
