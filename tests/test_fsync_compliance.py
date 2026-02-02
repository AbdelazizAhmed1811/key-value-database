import unittest
from unittest.mock import patch, MagicMock
import os
import asyncio
import tempfile
import shutil
from src.kv_store import KeyValueStore
from src.tcp_server import BatchWALWriter

class TestPowerSafety(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_file = os.path.join(self.test_dir, "power_test.log")
        self.db = KeyValueStore(self.db_file)
        
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir)

    def test_batch_triggers_fsync(self):
        """
        Verify that BatchWALWriter actually calls os.fsync().
        This confirms protection against Power Off (which requires fsync),
        distinguishing it from simple App Crash protection (which only needs flush).
        """
        
        # Mock os.fsync to count calls
        with patch('os.fsync') as mock_fsync:
            # We also need to let the real write happen to the file object 
            # so serialization works, but we only care about the fsync call.
            
            async def run_test():
                batcher = BatchWALWriter(self.db)
                await batcher.start()
                
                # Submit a write
                await batcher.submit({"op": "SET", "key": "k1", "value": "v1"})
                
                # Stop batcher to ensure processing completes
                await batcher.stop()
                
                return mock_fsync.call_count

            count = asyncio.run(run_test())
            
            print(f"\n[Power Safety] os.fsync called {count} times.")
            self.assertGreater(count, 0, "FATAL: Data NOT safe against Power Off! os.fsync() was never called.")

if __name__ == "__main__":
    unittest.main()
