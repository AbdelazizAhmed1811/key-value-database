# Running the Project

This guide explains how to run the Key-Value Database server, client, tests, and benchmarks.

## Prerequisites
- Python 3.13
- Dependencies installed in `./lib` (included in the project)

## 1. Start the Server
The server must be running for all other operations.
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
python3.13 -m uvicorn src.server:app --host 0.0.0.0 --port 8000
```
*Keep this terminal open.*

## 2. Run the Client Demo
In a new terminal:
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
python3.13 main.py
```

## 3. Run Automated Tests
This runs unit tests, integration tests, and durability tests.
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
python3.13 -m unittest discover tests
```

## 4. Run Load Testing (Benchmark)
We use **Locust** for load testing.
1. Ensure the server is running (Step 1).
2. Run Locust:
   ```bash
   export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
   python3.13 -m locust -f locustfile.py --host http://localhost:8000
   ```
3. Open your browser at `http://localhost:8089` to start the test.

## 5. Manual Testing with cURL
**Set a Value:**
```bash
curl -X POST "http://localhost:8000/set" -H "Content-Type: application/json" -d '{"key": "foo", "value": "bar"}'
```

**Get a Value:**
```bash
curl "http://localhost:8000/get/foo"
```

**Delete a Value:**
```bash
curl -X DELETE "http://localhost:8000/delete/foo"
```
