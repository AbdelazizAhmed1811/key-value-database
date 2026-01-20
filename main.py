import httpx
import time
import sys

BASE_URL = "http://localhost:8000"

def main():
    print("--- Key-Value Database Client Demo ---")
    print(f"Connecting to server at {BASE_URL}")

    try:
        # 1. SET Operation
        print("\n[SET] Setting 'user' to 'Alice'...")
        resp = httpx.post(f"{BASE_URL}/set", json={"key": "user", "value": "Alice"})
        print(f"Response: {resp.status_code} - {resp.json()}")

        print("[SET] Setting 'score' to 100...")
        resp = httpx.post(f"{BASE_URL}/set", json={"key": "score", "value": 100})
        print(f"Response: {resp.status_code} - {resp.json()}")

        # 2. GET Operation
        print(f"\n[GET] user...")
        resp = httpx.get(f"{BASE_URL}/get/user")
        print(f"Response: {resp.status_code} - {resp.json()}")

        print(f"[GET] score...")
        resp = httpx.get(f"{BASE_URL}/get/score")
        print(f"Response: {resp.status_code} - {resp.json()}")

        print(f"[GET] missing_key...")
        resp = httpx.get(f"{BASE_URL}/get/missing_key")
        print(f"Response: {resp.status_code} - {resp.json()}")

        # 3. DELETE Operation
        print("\n[DELETE] Deleting 'score'...")
        resp = httpx.delete(f"{BASE_URL}/delete/score")
        print(f"Response: {resp.status_code} - {resp.json()}")

        # Verify Deletion
        print(f"[GET] score after delete...")
        resp = httpx.get(f"{BASE_URL}/get/score")
        print(f"Response: {resp.status_code} - {resp.json()}")

    except httpx.ConnectError:
        print("\nError: Could not connect to the server.")
        print("Make sure the server is running: python3 src/server.py")
        sys.exit(1)

    print("\n--- Demo Complete ---")

if __name__ == "__main__":
    main()
