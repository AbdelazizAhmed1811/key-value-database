from src.client import TCPClient
import time

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

    # 2. BULK SET (New)
    print("\n[BULK SET] Setting multiple keys...")
    bulk_data = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")]
    resp = client.bulk_set(bulk_data)
    print(f"Response: {resp}")
    
    print("[GET] k1...")
    print(f"Response: {client.get('k1')}")

    # 3. GET Operation
    print(f"\n[GET] user...")
    resp = client.get("user")
    print(f"Response: {resp}")

    print(f"[GET] score...")
    resp = client.get("score")
    print(f"Response: {resp}")

    # 4. INCR Operation
    print(f"\n[INCR] Incrementing 'score' by 10...")
    resp = client.incr("score", 10)
    print(f"Response: {resp}")
    
    print(f"[GET] score after incr...")
    resp = client.get("score")
    print(f"Response: {resp}")

    # 5. DELETE Operation
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
