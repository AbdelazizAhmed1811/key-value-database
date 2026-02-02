import json
import os
import threading
import random
from typing import Any, Dict, Optional, List, Tuple
from src.indexing import IndexManager

class KeyNotFoundError(Exception):
    """Raised when a key is not found in the database."""
    pass

class KeyValueStore:
    """
    A persistent key-value store using a Transaction Log (WAL).
    Operations are appended to the log file.
    Uses Group Commit (handled externally) for high performance.
    Thread-safe memory operations.
    """

    def __init__(self, filename: str = "kv_store.log"):
        """
        Initialize the KeyValueStore.

        Args:
            filename: The name of the log file to use for persistence.
        """
        self.filename = filename
        self.store: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.index_manager = IndexManager()
        
        self.log_file = open(self.filename, 'a')
        self._replay_log()

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
                            value = self.store.get(key, 0)
                            if isinstance(value, int):
                                self.store[key] = value + amount
                    except json.JSONDecodeError:
                        continue
            # Rebuild indexes
            for key, value in self.store.items():
                self.index_manager.on_set(key, value)
        except IOError:
            pass

    def _append_log(self, entry: Dict[str, Any], simulate_failure: bool = False) -> None:
        """
        Append an entry to the log file.
        Writes to OS buffer immediately.
        simulate_failure: If True, 1% chance to skip writing (simulate data loss).
        """
        if simulate_failure and random.random() < 0.01:
            return

        self.log_file.write(json.dumps(entry) + "\n")
        self.log_file.flush()

    def write_batch(self, entries: list[Dict[str, Any]], simulate_failure: bool = False) -> None:
        """
        Write a batch of entries to the log and fsync.
        simulate_failure: If True, 1% chance to skip writing.
        """
        if simulate_failure and random.random() < 0.01:
            return

        if not entries:
            return
        
        payload = "".join(json.dumps(entry) + "\n" for entry in entries)
        self.log_file.write(payload)
        self.log_file.flush()
        # fsync to ensure durability against power loss
        os.fsync(self.log_file.fileno())

    def set(self, key: str, value: Any, sync: bool = True, simulate_failure: bool = False) -> Optional[Dict[str, Any]]:
        """Create or update a key with a value."""
        with self.lock:
            old_value = self.store.get(key)
            self.store[key] = value
            self.index_manager.on_set(key, value, old_value)
            entry = {"op": "SET", "key": key, "value": value}
        
        if sync:
            self._append_log(entry, simulate_failure=simulate_failure)
            return None
        return entry

    def bulk_set(self, items: List[Tuple[str, Any]]) -> Optional[List[Dict[str, Any]]]:
        """
        Atomically set multiple keys. 
        Returns list of entries to be persisted if sync=False.
        """
        entries = []
        with self.lock:
            for key, value in items:
                old_value = self.store.get(key)
                self.store[key] = value
                self.index_manager.on_set(key, value, old_value)
                entries.append({"op": "SET", "key": key, "value": value})
        
        return entries

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value by its key."""
        with self.lock:
            return self.store.get(key)

    def delete(self, key: str, sync: bool = True) -> Optional[Dict[str, Any]]:
        """Remove a key from the store."""
        with self.lock:
            if key not in self.store:
                raise KeyNotFoundError(f"Key '{key}' not found.")
            
            value = self.store[key]
            del self.store[key]
            self.index_manager.on_delete(key, value)
            entry = {"op": "DELETE", "key": key}
        
        if sync:
            self._append_log(entry)
            return None
        return entry

    def incr(self, key: str, amount: int = 1, sync: bool = True) -> Tuple[int, Optional[Dict[str, Any]]]:
        """Increment the integer value of a key by the given amount."""
        with self.lock:
            current_value = self.store.get(key, 0)
            
            if not isinstance(current_value, int):
                raise ValueError(f"Key '{key}' cannot be incremented: value is not an integer.")
                
            new_value = current_value + amount
            self.store[key] = new_value
            entry = {"op": "INCR", "key": key, "amount": amount}
            
        if sync:
            self._append_log(entry)
            return new_value, None
        return new_value, entry

    def compact(self) -> None:
        """Compact the log file."""
        # Close current file to ensure all writes are flushed
        self.log_file.flush()
        os.fsync(self.log_file.fileno())
        self.log_file.close()

        temp_filename = f"{self.filename}.tmp"
        try:
            with open(temp_filename, 'w') as f:
                with self.lock: # Lock during snapshotting
                    snapshot = self.store.copy()
                
                for key, value in snapshot.items():
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
        if self.log_file and not self.log_file.closed:
            self.log_file.flush()
            os.fsync(self.log_file.fileno())
            self.log_file.close()

    # ==================== Indexing & Search ====================

    def create_index(self, field: str) -> None:
        """Create a secondary index on a field."""
        with self.lock:
            self.index_manager.create_value_index(field)
            # Rebuild index from existing data
            for key, value in self.store.items():
                self.index_manager.value_indexes[field].add(key, value)

    def query_index(self, field: str, value: Any) -> List[str]:
        """Query a secondary index for keys where field == value."""
        return self.index_manager.query_value_index(field, value)

    def search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Full-text search using BM25."""
        return self.index_manager.search(query, top_k)

    def semantic_search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Semantic similarity search using TF-IDF cosine similarity."""
        return self.index_manager.semantic_search(query, top_k)

