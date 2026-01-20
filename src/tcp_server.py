import socket
import json
import threading
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict
from src.kv_store import KeyValueStore, KeyNotFoundError

class TCPServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8000, max_workers: int = 10, db_file: str = "kv_store.json"):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.db = KeyValueStore(db_file)
        self.running = True
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def start(self):
        """Start the TCP server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()
            print(f"TCP Server listening on {self.host}:{self.port} with {self.max_workers} workers")

            try:
                while self.running:
                    conn, addr = s.accept()
                    self.thread_pool.submit(self.handle_client, conn, addr)
            except KeyboardInterrupt:
                print("\nServer stopping...")
            finally:
                self.stop()

    def stop(self):
        """Stop the server and close resources."""
        self.running = False
        self.thread_pool.shutdown(wait=True)
        self.db.close()

    def handle_client(self, conn: socket.socket, addr: Any):
        """Handle a client connection."""
        # print(f"Connected by {addr}") # Commented out for performance in load testing
        with conn:
            buffer = ""
            while True:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    buffer += data.decode('utf-8')
                    
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        if not line.strip():
                            continue
                        
                        response = self.process_command(line)
                        conn.sendall((json.dumps(response) + "\n").encode('utf-8'))
                        
                except ConnectionResetError:
                    break
                except Exception as e:
                    print(f"Error handling client {addr}: {e}")
                    break

    def process_command(self, command_str: str) -> Dict[str, Any]:
        """Process a single JSON command."""
        try:
            command = json.loads(command_str)
            cmd_type = command.get("command")
            key = command.get("key")

            if not cmd_type or not key:
                return {"status": "error", "message": "Missing 'command' or 'key'"}

            if cmd_type == "SET":
                value = command.get("value")
                self.db.set(key, value)
                return {"status": "success", "result": "OK"}
            
            elif cmd_type == "GET":
                value = self.db.get(key)
                if value is None:
                    return {"status": "error", "message": "Key not found"}
                return {"status": "success", "result": value}
            
            elif cmd_type == "DELETE":
                try:
                    self.db.delete(key)
                    return {"status": "success", "result": "OK"}
                except KeyNotFoundError:
                    return {"status": "error", "message": "Key not found"}
            
            elif cmd_type == "INCR":
                amount = command.get("amount", 1)
                try:
                    new_value = self.db.incr(key, amount)
                    return {"status": "success", "result": new_value}
                except ValueError as e:
                    return {"status": "error", "message": str(e)}

            else:
                return {"status": "error", "message": f"Unknown command: {cmd_type}"}

        except json.JSONDecodeError:
            return {"status": "error", "message": "Invalid JSON"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    port = int(os.getenv("KV_SERVER_PORT", 8000))
    db_file = os.getenv("KV_STORE_FILE", "kv_store.json")
    max_workers = int(os.getenv("KV_MAX_WORKERS", 10))
    
    server = TCPServer(port=port, db_file=db_file, max_workers=max_workers)
    server.start()
