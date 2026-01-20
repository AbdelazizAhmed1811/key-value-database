import socket
import json
import sys
import time
from typing import Any, Dict, Optional

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
                response_str = data.decode('utf-8').strip()
                return json.loads(response_str)
        except ConnectionRefusedError:
            print("Error: Could not connect to server. Is it running?")
            sys.exit(1)
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

def main():
    print("--- Key-Value Database TCP Client Demo ---")
    client = TCPClient()

    # 1. SET Operation
    print("\n[SET] Setting 'user' to 'Alice'...")
    resp = client.set("user", "Alice")
    print(f"Response: {resp}")

    print("[SET] Setting 'score' to 100...")
    resp = client.set("score", 100)
    print(f"Response: {resp}")

    # 2. GET Operation
    print(f"\n[GET] user...")
    resp = client.get("user")
    print(f"Response: {resp}")

    print(f"[GET] score...")
    resp = client.get("score")
    print(f"Response: {resp}")

    # 3. INCR Operation
    print(f"\n[INCR] Incrementing 'score' by 10...")
    resp = client.incr("score", 10)
    print(f"Response: {resp}")
    
    print(f"[GET] score after incr...")
    resp = client.get("score")
    print(f"Response: {resp}")

    # 4. DELETE Operation
    print("\n[DELETE] Deleting 'score'...")
    resp = client.delete("score")
    print(f"Response: {resp}")

    # Verify Deletion
    print(f"[GET] score after delete...")
    resp = client.get("score")
    print(f"Response: {resp}")

    print("\n--- Demo Complete ---")

if __name__ == "__main__":
    main()
