#!/bin/bash
set -e
export PYTHONPATH=$(pwd)

echo ">>> 1. Environment Cleanup"
pkill -f "python3.13 src/server.py" || true
pkill -f "python3.13 benchmarks/speed.py" || true
fuser -k 8000/tcp || true
fuser -k 9000/tcp || true
rm -f *.log *.json

echo ">>> 2. Project Structure Verification"
find src tests benchmarks examples -maxdepth 2

echo ">>> 3. Running Unit Tests"
python3.13 -m unittest discover tests -p "test_*.py"

echo ">>> 4. Running Chaos Test (Durability)"
python3.13 tests/chaos_test.py

echo ">>> 5. Running Benchmarks"
nohup python3.13 src/server.py > server_bench.log 2>&1 &
SERVER_PID=$!
echo "Server started (PID $SERVER_PID). Waiting 3s..."
sleep 3

echo "--> Benchmark: Pipeline"
python3.13 benchmarks/pipeline.py

echo "--> Benchmark: Speed (Concurrent)"
python3.13 benchmarks/speed.py

echo ">>> 6. Teardown"
kill $SERVER_PID
echo "All verifications passed."
