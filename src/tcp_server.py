import asyncio
import json
import os
import signal
from typing import Any, Dict, List, Optional
from src.kv_store import KeyValueStore, KeyNotFoundError

class BatchWALWriter:
    """
    Batches write operations and flushes them to disk using Group Commit.
    """
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
        await self.queue.put((None, None)) # Sentinel to unblock get()
        if self.task:
            await self.task

    async def submit(self, entry: Dict[str, Any]) -> None:
        """Submit an entry to be persisted and wait for it."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self.queue.put((entry, future))
        await future

    async def _process_batches(self):
        loop = asyncio.get_running_loop()
        while self.running:
            # Wait for at least one item
            entry, future = await self.queue.get()
            if entry is None: # Sentinel received
                break
            
            batch_entries = [entry]
            futures = [future]
            
            # Smart Batching: 
            # With pipelined processing, the queue fills up naturally under load.
            # Explicit sleep hurts latency for single clients (like chaos test).
            # if self.queue.empty():
            #    await asyncio.sleep(0.005)

            # Drain the queue to form a batch
            while not self.queue.empty():
                try:
                    entry, future = self.queue.get_nowait()
                    batch_entries.append(entry)
                    futures.append(future)
                    # Limit batch size
                    if len(batch_entries) >= 2000:
                        break
                except asyncio.QueueEmpty:
                    break
            
            # Flush batch to disk (Blocking I/O in thread pool)
            if batch_entries:
                try:
                    await loop.run_in_executor(None, self.db.write_batch, batch_entries)
                    # Notify all clients
                    for f in futures:
                        if not f.done():
                            f.set_result(None)
                except Exception as e:
                    print(f"Batch write failed: {e}")
                    for f in futures:
                        if not f.done():
                            f.set_exception(e)

class AsyncTCPServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8000, db_file: str = "kv_store.json"):
        self.host = host
        self.port = port
        self.db = KeyValueStore(db_file)
        self.batcher = BatchWALWriter(self.db)
        self.running = True

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        buffer = b""
        try:
            while True:
                data = await reader.read(65536) # 64KB buffer for larger batches
                if not data:
                    break
                
                buffer += data
                
                tasks = []
                while b"\n" in buffer:
                    line_bytes, buffer = buffer.split(b"\n", 1)
                    line = line_bytes.decode('utf-8').strip()
                    if not line:
                        continue
                    tasks.append(self.process_command(line))
                
                if tasks:
                    # Execute all parsed commands concurrently
                    # This allows the batcher to see all of them at once!
                    responses = await asyncio.gather(*tasks)
                    
                    # Send responses in order
                    for response in responses:
                        writer.write(json.dumps(response).encode('utf-8') + b"\n")
                    await writer.drain()
            
            # await writer.drain() # Moved inside
                    
        except ConnectionResetError:
            pass
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def process_command(self, command_str: str) -> Dict[str, Any]:
        """Process a single JSON command asynchronously."""
        try:
            command = json.loads(command_str)
            cmd_type = command.get("command")
            key = command.get("key")

            if not cmd_type or not key:
                return {"status": "error", "message": "Missing 'command' or 'key'"}

            loop = asyncio.get_running_loop()

            if cmd_type == "SET":
                value = command.get("value")
                # Update memory immediately, get entry to persist
                entry = self.db.set(key, value, sync=False)
                if entry:
                    # Wait for durability
                    await self.batcher.submit(entry)
                return {"status": "success", "result": "OK"}
            
            elif cmd_type == "GET":
                # GET is strictly in-memory
                value = self.db.get(key)
                if value is None:
                    return {"status": "error", "message": "Key not found"}
                return {"status": "success", "result": value}
            
            elif cmd_type == "DELETE":
                try:
                    entry = self.db.delete(key, sync=False)
                    if entry:
                        await self.batcher.submit(entry)
                    return {"status": "success", "result": "OK"}
                except KeyNotFoundError:
                    return {"status": "error", "message": "Key not found"}
            
            elif cmd_type == "INCR":
                amount = command.get("amount", 1)
                try:
                    new_value, entry = self.db.incr(key, amount, sync=False)
                    if entry:
                        await self.batcher.submit(entry)
                    return {"status": "success", "result": new_value}
                except ValueError as e:
                    return {"status": "error", "message": str(e)}

            else:
                return {"status": "error", "message": f"Unknown command: {cmd_type}"}

        except json.JSONDecodeError:
            return {"status": "error", "message": "Invalid JSON"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def start(self):
        await self.batcher.start()
        
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)

        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        print(f"High-Performance Async TCP Server listening on {addrs}")

        async with server:
            await server.serve_forever()

    async def stop(self):
        await self.batcher.stop()
        self.db.close()

async def main():
    port = int(os.getenv("KV_SERVER_PORT", 8000))
    db_file = os.getenv("KV_STORE_FILE", "kv_store.json")
    
    server = AsyncTCPServer(port=port, db_file=db_file)
    
    # Handle graceful shutdown
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    
    def signal_handler():
        print("\nStopping server...")
        stop_event.set()
        asyncio.create_task(server.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await server.start()
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
