import asyncio
import json
import os
import signal
import sys
import argparse
from typing import Any, Dict, List, Optional
from src.kv_store import KeyValueStore, KeyNotFoundError
from src.raft import RaftNode

class BatchWALWriter:
    def __init__(self, db: KeyValueStore, batch_interval: float = 0.001):
        self.db = db
        self.queue: asyncio.Queue = asyncio.Queue()
        self.batch_interval = batch_interval
        self.running = True
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self._process_batches())

    async def stop(self):
        self.running = False
        await self.queue.put((None, None))
        if self.task:
            await self.task

    async def submit(self, entry: Any, simulate_failure: bool = False) -> None:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self.queue.put((entry, future, simulate_failure))
        await future

    async def _process_batches(self):
        loop = asyncio.get_running_loop()
        while self.running:
            item = await self.queue.get()
            entry = item[0]
            future = item[1]
            simulate_failure = item[2] if len(item) > 2 else False

            if entry is None:
                break
            
            if isinstance(entry, list):
                batch_entries = entry
            else:
                batch_entries = [entry]
            
            futures = [future]
            batch_simulate_failure = simulate_failure
            
            while not self.queue.empty():
                try:
                    item = self.queue.get_nowait()
                    entry = item[0]
                    future = item[1]
                    s_fail = item[2] if len(item) > 2 else False
                    
                    if s_fail:
                        batch_simulate_failure = True

                    if isinstance(entry, list):
                        batch_entries.extend(entry)
                    else:
                        batch_entries.append(entry)
                    futures.append(future)
                    if len(batch_entries) >= 2000:
                        break
                except asyncio.QueueEmpty:
                    break
            
            if batch_entries:
                try:
                    await loop.run_in_executor(None, lambda: self.db.write_batch(batch_entries, simulate_failure=batch_simulate_failure))
                    for f in futures:
                        if not f.done():
                            f.set_result(None)
                except Exception as e:
                    print(f"Batch write failed: {e}")
                    for f in futures:
                        if not f.done():
                            f.set_exception(e)

class AsyncTCPServer:
    def __init__(self, host: str, port: int, node_id: str, peers: List[str], db_file: str = "kv_store.json"):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.db = KeyValueStore(db_file)
        self.batcher = BatchWALWriter(self.db)
        self.running = True
        
        # Raft
        self_address = f"{host}:{port}" if host != "0.0.0.0" else f"127.0.0.1:{port}"
        self.raft = RaftNode(node_id, peers, self, self_address=self_address)
        
    async def send_rpc(self, peer: str, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send JSON RPC to a peer."""
        try:
            async with asyncio.timeout(0.5):
                host, port = peer.split(":")
                reader, writer = await asyncio.open_connection(host, int(port))
                
                writer.write(json.dumps(msg).encode('utf-8') + b"\n")
                await writer.drain()
                
                data = await reader.read(65536)
                writer.close()
                await writer.wait_closed()
                
                if data:
                    return json.loads(data.decode('utf-8'))
                return None
        except Exception as e:
            return None

    async def apply_to_fsm(self, command: Dict[str, Any]):
        cmd_type = command.get("command")
        key = command.get("key")
        try:
            if cmd_type == "SET":
                value = command.get("value")
                simulate_failure = command.get("simulate_failure", False)
                entry = self.db.set(key, value, sync=False)
                if entry:
                    await self.batcher.submit(entry, simulate_failure=simulate_failure)
            elif cmd_type == "DELETE":
                try:
                    entry = self.db.delete(key, sync=False)
                    if entry:
                        await self.batcher.submit(entry)
                except KeyNotFoundError:
                    pass
            elif cmd_type == "INCR":
                amount = command.get("amount", 1)
                try:
                    new_val, entry = self.db.incr(key, amount, sync=False)
                    if entry:
                        await self.batcher.submit(entry)
                except ValueError:
                    pass
            elif cmd_type == "BULK_SET":
                items = command.get("items")
                simulate_failure = command.get("simulate_failure", False)
                entries = self.db.bulk_set(items)
                if entries:
                    await self.batcher.submit(entries, simulate_failure=simulate_failure)
        except Exception as e:
            print(f"FSM Apply Error: {e}")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                buffer = data.decode('utf-8')
                for line in buffer.splitlines():
                    if not line: continue
                    response = await self.process_message(line)
                    writer.write(json.dumps(response).encode('utf-8') + b"\n")
                await writer.drain()
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def process_message(self, msg_str: str) -> Dict[str, Any]:
        try:
            msg = json.loads(msg_str)
        except json.JSONDecodeError:
            return {"status": "error", "message": "Invalid JSON"}
        if "type" in msg and msg["type"] in ["RequestVote", "AppendEntries"]:
            return await self.raft.handle_rpc(msg)
        return await self.process_client_command(msg)

    async def process_client_command(self, command: Dict[str, Any]) -> Dict[str, Any]:
        cmd_type = command.get("command")
        key = command.get("key")

        # Read-only commands can be handled by any node
        if cmd_type == "GET":
            if self.raft.role != "LEADER":
                leader = self.raft.leader_id
                return {"status": "error", "message": "Not Leader", "redirect": leader}
            value = self.db.get(key)
            if value is None:
                return {"status": "error", "message": "Key not found"}
            return {"status": "success", "result": value}

        # Search commands (read-only)
        if cmd_type == "SEARCH":
            query = command.get("query", "")
            top_k = command.get("top_k", 10)
            results = self.db.search(query, top_k)
            return {"status": "success", "result": [{"key": k, "score": s} for k, s in results]}

        if cmd_type == "SEMANTIC_SEARCH":
            query = command.get("query", "")
            top_k = command.get("top_k", 10)
            results = self.db.semantic_search(query, top_k)
            return {"status": "success", "result": [{"key": k, "similarity": s} for k, s in results]}

        if cmd_type == "QUERY_INDEX":
            field = command.get("field")
            value = command.get("value")
            try:
                keys = self.db.query_index(field, value)
                return {"status": "success", "result": keys}
            except KeyError as e:
                return {"status": "error", "message": str(e)}

        if cmd_type == "CREATE_INDEX":
            field = command.get("field")
            self.db.create_index(field)
            return {"status": "success", "result": f"Index created on field '{field}'"}

        # Write commands require leader
        if self.raft.role != "LEADER":
            leader = self.raft.leader_id
            return {"status": "error", "message": "Not Leader", "redirect": leader}

        success = await self.raft.propose_command(command)
        if success:
            if cmd_type == "INCR":
                val = self.db.get(key)
                return {"status": "success", "result": val}
            return {"status": "success", "result": "OK"}
        else:
            return {"status": "error", "message": "Raft Proposal Failed (Timeout or Leadership lost)"}

    async def start(self):
        await self.batcher.start()
        await self.raft.start()
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port, reuse_address=True)
        print(f"[{self.raft.node_id}] Server listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

    async def stop(self):
        await self.raft.stop()
        await self.batcher.stop()
        self.db.close()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--id", type=str, default="node1")
    parser.add_argument("--peers", type=str, default="", help="Comma separated list of peer host:port")
    args = parser.parse_args()
    peers = [p for p in args.peers.split(",") if p]
    db_file = f"kv_store_{args.id}.json"
    server = AsyncTCPServer(host="0.0.0.0", port=args.port, node_id=args.id, peers=peers, db_file=db_file)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    def signal_handler():
        print("\nReceived signal, stopping...")
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    server_task = asyncio.create_task(server.start())
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    print("Shutting down...")
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass
    await server.stop()
    print("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
