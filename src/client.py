import socket
import json
import sys
import os
from typing import Any, Dict, List, Tuple

class TCPClient:
    def __init__(self, host: str = "localhost", port: int = 8000):
        self.host = host
        self.port = port

    def send_command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """Send a JSON command to the server and receive the response."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                s.sendall((json.dumps(command) + "\n").encode('utf-8'))
                
                # Read response (simple implementation assuming single line response)
                data = s.recv(4096)
                if not data:
                    return {"status": "error", "message": "Connection closed by server"}
                response_str = data.decode('utf-8').strip()
                return json.loads(response_str)
        except ConnectionRefusedError:
            print("Error: Could not connect to server. Is it running?")
            return {"status": "error", "message": "Connection refused"}
        except Exception as e:
            print(f"Error: {e}")
            return {"status": "error", "message": str(e)}

    def set(self, key: str, value: Any) -> Dict[str, Any]:
        return self.send_command({"command": "SET", "key": key, "value": value})

    def get(self, key: str) -> Dict[str, Any]:
        return self.send_command({"command": "GET", "key": key})

    def delete(self, key: str) -> Dict[str, Any]:
        return self.send_command({"command": "DELETE", "key": key})
    
    def incr(self, key: str, amount: int = 1) -> Dict[str, Any]:
        return self.send_command({"command": "INCR", "key": key, "amount": amount})

    def bulk_set(self, items: List[Tuple[str, Any]]) -> Dict[str, Any]:
        """items: List of (key, value) tuples"""
        return self.send_command({"command": "BULK_SET", "key": "batch", "items": items})
    
    def set_debug(self, key: str, value: Any) -> Dict[str, Any]:
        """Set with simulated failure flag (Bonus Feature)."""
        return self.send_command({"command": "SET", "key": key, "value": value, "simulate_failure": True})
