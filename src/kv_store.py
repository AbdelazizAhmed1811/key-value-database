import json
import os
from typing import Any, Dict, Optional

class KeyNotFoundError(Exception):
    """Raised when a key is not found in the database."""
    pass

class KeyValueStore:
    """
    A persistent key-value store using a Transaction Log (WAL).
    Operations are appended to the log file.
    """

    def __init__(self, filename: str = "kv_store.log"):
        """
        Initialize the KeyValueStore.

        Args:
            filename: The name of the log file to use for persistence.
        """
        self.filename = filename
        self.store: Dict[str, Any] = {}
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
                    except json.JSONDecodeError:
                        # Skip corrupt lines (or handle more gracefully)
                        continue
        except IOError:
            pass

    def _append_log(self, entry: Dict[str, Any]) -> None:
        """
        Append an entry to the log file atomically.
        
        Args:
            entry: The operation dictionary to append.
        """
        with open(self.filename, 'a') as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def set(self, key: str, value: Any) -> bool:
        """
        Create or update a key with a value.

        Args:
            key: The key to set.
            value: The value to associate with the key.

        Returns:
            True to acknowledge the operation was successful.
        """
        self.store[key] = value
        self._append_log({"op": "SET", "key": key, "value": value})
        return True

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value by its key.

        Args:
            key: The key to retrieve.

        Returns:
            The value associated with the key, or None if not found.
        """
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        """
        Remove a key from the store.

        Args:
            key: The key to remove.

        Returns:
            True to acknowledge the operation was successful.

        Raises:
            KeyNotFoundError: If the key does not exist.
        """
        if key not in self.store:
            raise KeyNotFoundError(f"Key '{key}' not found.")
        
        del self.store[key]
        self._append_log({"op": "DELETE", "key": key})
        return True

    def compact(self) -> None:
        """
        Compact the log file by rewriting it with the current in-memory state.
        This removes obsolete entries (e.g., overwritten updates, deletions).
        """
        temp_filename = f"{self.filename}.tmp"
        try:
            with open(temp_filename, 'w') as f:
                for key, value in self.store.items():
                    entry = {"op": "SET", "key": key, "value": value}
                    f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic replacement
            os.replace(temp_filename, self.filename)
        except Exception as e:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            raise e
