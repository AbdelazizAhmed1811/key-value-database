from fastapi.testclient import TestClient
import unittest
import os
from src.server import app, db

client = TestClient(app)

class TestServer(unittest.TestCase):
    TEST_DB_FILE = "kv_store.json"

    def setUp(self):
        # Ensure clean state
        # We are using the same file as the server "kv_store.json" for simplicity in this test setup
        # In a real app, we might want to override the db dependency or file path
        pass

    def tearDown(self):
        # Clean up keys used in tests
        try:
            db.delete("test_api_key")
        except:
            pass

    def test_set_and_get(self):
        # Test SET
        response = client.post("/set", json={"key": "test_api_key", "value": "api_value"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "success")

        # Test GET
        response = client.get("/get/test_api_key")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["value"], "api_value")

    def test_get_not_found(self):
        response = client.get("/get/non_existent_key")
        self.assertEqual(response.status_code, 404)

    def test_delete(self):
        # Setup
        client.post("/set", json={"key": "test_api_key", "value": "to_delete"})
        
        # Test DELETE
        response = client.delete("/delete/test_api_key")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "success")

        # Verify deletion
        response = client.get("/get/test_api_key")
        self.assertEqual(response.status_code, 404)

    def test_delete_not_found(self):
        response = client.delete("/delete/non_existent_key")
        self.assertEqual(response.status_code, 404)

if __name__ == "__main__":
    unittest.main()
