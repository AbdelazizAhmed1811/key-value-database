# Simple Key-Value Database

A simple, persistent key-value store implemented in Python.

## Features
- **Set**: Create or update a key-value pair.
- **Get**: Retrieve a value by key.
- **Delete**: Remove a key-value pair.
- **Persistence**: Data is saved to `kv_store.json` and persists across runs.
- **Durability**: Verified by automated tests.

## Usage

### Prerequisites
- Python 3.x
- Dependencies installed in `./lib` (if using the provided setup)

### 1. Start the Server
You need to start the server process first. Make sure to include the local library path:
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
python3 -m uvicorn src.server:app --host 0.0.0.0 --port 8000
```
The server will start at `http://0.0.0.0:8000`.

### 2. Run the Client Demo
In a separate terminal, run the client script to interact with the server:
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
python3 main.py
```

### 3. Manual Testing (curl)
You can also test manually using `curl`:

**Set a value:**
```bash
curl -X POST "http://localhost:8000/set" \
     -H "Content-Type: application/json" \
     -d '{"key": "mykey", "value": "myvalue"}'
```

**Get a value:**
```bash
curl "http://localhost:8000/get/mykey"
```

**Delete a value:**
```bash
curl -X DELETE "http://localhost:8000/delete/mykey"
```

## Running Tests
To run the automated tests (including server durability):
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/lib
python3 -m unittest discover tests
```
