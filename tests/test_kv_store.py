import unittest
import os
import json
from src.kv_store import KeyValueStore, KeyNotFoundError

class TestKeyValueStoreWAL(unittest.TestCase):
    TEST_DB_FILE = "test_kv_store.log"

    def setUp(self):
        """Set up a fresh database for each test."""
        if os.path.exists(self.TEST_DB_FILE):
            os.remove(self.TEST_DB_FILE)
        self.kv = KeyValueStore(self.TEST_DB_FILE)

    def tearDown(self):
        """Clean up the test database file."""
        if os.path.exists(self.TEST_DB_FILE):
            os.remove(self.TEST_DB_FILE)
        if os.path.exists(f"{self.TEST_DB_FILE}.tmp"):
            os.remove(f"{self.TEST_DB_FILE}.tmp")

    def test_set_and_get(self):
        """Test setting a value and retrieving it."""
        self.kv.set("name", "Zizo")
        self.assertEqual(self.kv.get("name"), "Zizo")

    def test_persistence_wal(self):
        """Test that data persists via WAL replay."""
        self.kv.set("key1", "value1")
        self.kv.set("key2", "value2")
        self.kv.delete("key1")
        
        # Create new instance to trigger replay
        new_kv = KeyValueStore(self.TEST_DB_FILE)
        self.assertIsNone(new_kv.get("key1"))
        self.assertEqual(new_kv.get("key2"), "value2")

    def test_compaction(self):
        """Test that compaction reduces log size and preserves data."""
        # 1. Generate some log churn
        self.kv.set("key", "v1")
        self.kv.set("key", "v2")
        self.kv.set("key", "v3")
        self.kv.delete("key")
        self.kv.set("key", "final_value")
        
        # Check log size before compaction (should be > 1 entry)
        with open(self.TEST_DB_FILE, 'r') as f:
            lines_before = len(f.readlines())
        self.assertGreater(lines_before, 1)
        
        # 2. Compact
        self.kv.compact()
        
        # 3. Verify Data
        self.assertEqual(self.kv.get("key"), "final_value")
        
        # 4. Verify Log Size (should be exactly 1 entry now)
        with open(self.TEST_DB_FILE, 'r') as f:
            lines_after = len(f.readlines())
        self.assertEqual(lines_after, 1)
        
        # 5. Verify Persistence after compaction
        new_kv = KeyValueStore(self.TEST_DB_FILE)
        self.assertEqual(new_kv.get("key"), "final_value")

if __name__ == '__main__':
    unittest.main()
