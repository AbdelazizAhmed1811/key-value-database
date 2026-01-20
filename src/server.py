from fastapi import FastAPI, HTTPException, Body
import os
from pydantic import BaseModel
from typing import Any, Optional
from src.kv_store import KeyValueStore, KeyNotFoundError

app = FastAPI(title="Simple Key-Value Database API")
DB_FILE = os.getenv("KV_STORE_FILE", "kv_store.json")
db = KeyValueStore(DB_FILE)

class SetRequest(BaseModel):
    key: str
    value: Any

@app.post("/set")
def set_key(request: SetRequest):
    """Set a key-value pair."""
    db.set(request.key, request.value)
    return {"status": "success", "message": f"Key '{request.key}' set successfully."}

@app.get("/get/{key}")
def get_key(key: str):
    """Retrieve a value by key."""
    value = db.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"key": key, "value": value}

@app.delete("/delete/{key}")
def delete_key(key: str):
    """Delete a key-value pair."""
    try:
        db.delete(key)
        return {"status": "success", "message": f"Key '{key}' deleted successfully."}
    except KeyNotFoundError:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
