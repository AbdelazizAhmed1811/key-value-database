import json
import os
import threading
import time
from typing import Any, Dict, Optional

class KeyNotFoundError(Exception):
    """Raised when a key is not found in the database."""
    pass

class KeyValueStore:
    """
    A persistent key-value store using a Transaction Log (WAL).
    Operations are appended to the log file.
    Uses Periodic Sync (Group Commit) for high performance.
    """

    def __init__(self, filename: str = "kv_store.log", sync_interval: float = 1.0):
        """
        Initialize the KeyValueStore.

        Args:
            filename: The name of the log file to use for persistence.
            sync_interval: Time in seconds between disk syncs (fsync).
        """
        self.filename = filename
        self.sync_interval = sync_interval
        self.store: Dict[str, Any] = {}
        self._replay_log()
        
        # Open file in append mode and keep it open
        self.log_file = open(self.filename, 'a')
        
        # Start background sync thread
        self.running = True
        self.sync_thread = threading.Thread(target=self._background_sync, daemon=True)
        self.sync_thread.start()

    def _replay_log(self) -> None:
        """Replay the log file to reconstruct the in-memory state."""
        if not os.path.exists(self.filename):
            return

        try:
            with open(self.filename, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        op = entry.get("op")
                        key = entry.get("key")
                        
                        if op == "SET":
                            self.store[key] = entry.get("value")
                        elif op == "DELETE":
                            if key in self.store:
                                del self.store[key]
                        elif op == "INCR":
                            amount = entry.get("amount", 1)
                            current_value = self.store.get(key, 0)
                            if isinstance(current_value, int):
                                self.store[key] = current_value + amount
                    except json.JSONDecodeError:
                        continue
        except IOError:
            pass

    def _background_sync(self) -> None:
        """Periodically fsync the log file to disk."""
        while self.running:
            time.sleep(self.sync_interval)
            try:
                if self.log_file and not self.log_file.closed:
                    self.log_file.flush()
                    os.fsync(self.log_file.fileno())
            except ValueError:
                pass # File might be closed during compaction

    def _append_log(self, entry: Dict[str, Any]) -> None:
        """
        Append an entry to the log file.
        Writes to OS buffer immediately, fsync handled by background thread.
        """
        self.log_file.write(json.dumps(entry) + "\n")
        self.log_file.flush()
        # We flush to OS buffer immediately to survive application crashes (SIGKILL).
        # fsync is still handled by background thread to batch disk I/O for performance.

    def set(self, key: str, value: Any) -> bool:
        """Create or update a key with a value."""
        self.store[key] = value
        self._append_log({"op": "SET", "key": key, "value": value})
        return True

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by its key."""
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        """Remove a key from the store."""
        if key not in self.store:
            raise KeyNotFoundError(f"Key '{key}' not found.")
        
        del self.store[key]
        self._append_log({"op": "DELETE", "key": key})
        return True

    def incr(self, key: str, amount: int = 1) -> int:
        """Increment the integer value of a key by the given amount."""
        current_value = self.store.get(key, 0)
        
        if not isinstance(current_value, int):
            raise ValueError(f"Key '{key}' cannot be incremented: value is not an integer.")
            
        new_value = current_value + amount
        self.store[key] = new_value
        self._append_log({"op": "INCR", "key": key, "amount": amount})
        return new_value

    def compact(self) -> None:
        """Compact the log file."""
        # Close current file to ensure all writes are flushed
        self.log_file.flush()
        os.fsync(self.log_file.fileno())
        self.log_file.close()

        temp_filename = f"{self.filename}.tmp"
        try:
            with open(temp_filename, 'w') as f:
                for key, value in self.store.items():
                    entry = {"op": "SET", "key": key, "value": value}
                    f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(temp_filename, self.filename)
            
            # Re-open the new file
            self.log_file = open(self.filename, 'a')
        except Exception as e:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            # Try to re-open original file if compaction failed
            if self.log_file.closed:
                 self.log_file = open(self.filename, 'a')
            raise e

    def close(self):
        """Clean shutdown."""
        self.running = False
        if self.sync_thread.is_alive():
            self.sync_thread.join(timeout=1.0)
        
        if self.log_file and not self.log_file.closed:
            self.log_file.flush()
            os.fsync(self.log_file.fileno())
            self.log_file.close()
