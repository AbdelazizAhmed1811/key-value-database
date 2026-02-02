# Key-Value Database with Raft Clustering

A high-performance, distributed key-value store with Raft consensus, full-text search, and semantic similarity search.

## Features

### Core Operations
- **SET** / **GET** / **DELETE** - Standard key-value operations
- **INCR** - Atomic increment
- **BULK_SET** - Atomic batch writes

### Distributed Clustering (Raft)
- Leader election with automatic failover
- Log replication across nodes
- Linearizable consistency
- Automatic client redirect to leader

### Indexing & Search
- **Value Index** - Secondary indexes for field lookups
- **Full-Text Search** - BM25 ranking algorithm
- **Semantic Search** - TF-IDF cosine similarity

### Durability
- Write-Ahead Log (WAL) persistence
- Group commit for high throughput
- fsync for crash safety

---

## Quick Start

### Single Node
```bash
export PYTHONPATH=$(pwd)
python3.13 src/server.py --port 8000 --id node1
```

### 3-Node Cluster
```bash
# Terminal 1
python3.13 src/server.py --id node1 --port 8000 --peers 127.0.0.1:8001,127.0.0.1:8002

# Terminal 2
python3.13 src/server.py --id node2 --port 8001 --peers 127.0.0.1:8000,127.0.0.1:8002

# Terminal 3
python3.13 src/server.py --id node3 --port 8002 --peers 127.0.0.1:8000,127.0.0.1:8001
```

---

## Client Usage

### Python Client
```python
from src.client import TCPClient

client = TCPClient("localhost", 8000)
client.set("name", "Alice")
print(client.get("name"))  # {'status': 'success', 'result': 'Alice'}
client.delete("name")
```

### TCP Protocol (JSON)
```bash
# Connect via netcat
echo '{"command": "SET", "key": "foo", "value": "bar"}' | nc localhost 8000
echo '{"command": "GET", "key": "foo"}' | nc localhost 8000
```

---

## Search Commands

### Full-Text Search (BM25)
```python
client.send_command({"command": "SEARCH", "query": "machine learning", "top_k": 10})
# Returns: [{"key": "doc1", "score": 2.45}, ...]
```

### Semantic Search (TF-IDF)
```python
client.send_command({"command": "SEMANTIC_SEARCH", "query": "AI algorithms", "top_k": 10})
# Returns: [{"key": "doc1", "similarity": 0.87}, ...]
```

### Secondary Index
```python
client.send_command({"command": "CREATE_INDEX", "field": "category"})
client.send_command({"command": "QUERY_INDEX", "field": "category", "value": "fruit"})
# Returns: ["doc1", "doc2"]
```

---

## API Reference

| Command | Parameters | Description |
|---------|------------|-------------|
| `SET` | key, value | Set a key-value pair |
| `GET` | key | Get value by key |
| `DELETE` | key | Delete a key |
| `INCR` | key, amount | Increment integer value |
| `BULK_SET` | items | Atomic batch set |
| `SEARCH` | query, top_k | Full-text search |
| `SEMANTIC_SEARCH` | query, top_k | Semantic similarity |
| `CREATE_INDEX` | field | Create secondary index |
| `QUERY_INDEX` | field, value | Query secondary index |

---

## Running Tests

```bash
export PYTHONPATH=$(pwd)

# All tests
python3.13 -m pytest tests/ -v

# Specific test
python3.13 -m pytest tests/test_clustering.py -v
```

### Verification Script
```bash
./run_verification.sh
```

---

## Benchmarks

```bash
# Start server
python3.13 src/server.py --port 8000 --id bench &
sleep 2

# Speed benchmark (50k ops, 500 concurrent)
python3.13 benchmarks/speed.py

# Pipeline benchmark (50k pipelined writes)
python3.13 benchmarks/pipeline.py
```

**Typical Results:**
- WRITE: ~8,500 ops/sec
- READ: ~17,000 ops/sec

---

## Project Structure

```
├── src/
│   ├── server.py      # Async TCP server with Raft
│   ├── client.py      # TCP client with retry logic
│   ├── kv_store.py    # Persistence layer (WAL)
│   ├── raft.py        # Raft consensus module
│   └── indexing.py    # Search indexes
├── tests/
│   ├── test_clustering.py
│   ├── test_indexing.py
│   ├── test_acid.py
│   └── ...
├── benchmarks/
│   ├── speed.py
│   └── pipeline.py
└── run_verification.sh
```
