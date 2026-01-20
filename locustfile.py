from locust import User, task, between, events
import socket
import json
import random
import string
import time

class TCPClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_command(self, command_name, command_data):
        start_time = time.time()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.host, self.port))
                s.sendall((json.dumps(command_data) + "\n").encode('utf-8'))
                
                data = s.recv(4096)
                response = json.loads(data.decode('utf-8'))
                
            total_time = int((time.time() - start_time) * 1000)
            
            if response.get("status") == "success":
                events.request.fire(
                    request_type="TCP",
                    name=command_name,
                    response_time=total_time,
                    response_length=len(data),
                    exception=None,
                )
            else:
                events.request.fire(
                    request_type="TCP",
                    name=command_name,
                    response_time=total_time,
                    response_length=len(data),
                    exception=Exception(response.get("message")),
                )
                
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="TCP",
                name=command_name,
                response_time=total_time,
                response_length=0,
                exception=e,
            )

class KeyValueUser(User):
    wait_time = between(0.1, 0.5)
    host = "localhost"
    port = 8000

    def on_start(self):
        self.client = TCPClient(self.host, self.port)
        self.keys = [f"key_{i}" for i in range(100)]
        self.int_keys = [f"int_key_{i}" for i in range(20)]
        
        # Pre-populate integer keys
        for key in self.int_keys:
            self.client.send_command("SET", {"command": "SET", "key": key, "value": 0})

    @task(3)
    def set_key(self):
        key = random.choice(self.keys)
        value = ''.join(random.choices(string.ascii_letters, k=10))
        self.client.send_command("SET", {"command": "SET", "key": key, "value": value})

    @task(5)
    def get_key(self):
        key = random.choice(self.keys)
        self.client.send_command("GET", {"command": "GET", "key": key})

    @task(1)
    def delete_key(self):
        key = random.choice(self.keys)
        self.client.send_command("DELETE", {"command": "DELETE", "key": key})

    @task(2)
    def incr_key(self):
        key = random.choice(self.int_keys)
        self.client.send_command("INCR", {"command": "INCR", "key": key, "amount": 1})
