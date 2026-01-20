from locust import HttpUser, task, between
import random
import string

class KeyValueUser(HttpUser):
    wait_time = between(0.1, 0.5)

    def on_start(self):
        """Generate a pool of keys to use."""
        self.keys = [f"key_{i}" for i in range(100)]

    @task(3)
    def set_key(self):
        """Heavily weighted task: Set a random key."""
        key = random.choice(self.keys)
        value = ''.join(random.choices(string.ascii_letters, k=10))
        self.client.post("/set", json={"key": key, "value": value})

    @task(5)
    def get_key(self):
        """Most frequent task: Get a random key."""
        key = random.choice(self.keys)
        self.client.get(f"/get/{key}")

    @task(1)
    def delete_key(self):
        """Less frequent task: Delete a random key."""
        key = random.choice(self.keys)
        self.client.delete(f"/delete/{key}")
