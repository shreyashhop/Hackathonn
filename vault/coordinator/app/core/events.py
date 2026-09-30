import asyncio
import json
from datetime import datetime, timezone
from typing import List, Dict, Any
from fastapi import WebSocket


class ConnectionManager:
    """
    Manages active WebSocket connections from frontend clients
    and broadcasts cluster live events.
    """

    def __init__(self, max_history: int = 500):
        self.active_connections: List[WebSocket] = []
        self.history: List[Dict[str, Any]] = []
        self.max_history = max_history

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Send initial connection confirmation event
        welcome_event = {
            "event_type": "SYSTEM_CONNECTED",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "message": "Connected to Vault Coordinator Live Events stream",
                "active_clients": len(self.active_connections),
            },
        }
        await websocket.send_text(json.dumps(welcome_event))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, event_type: str, data: Dict[str, Any]):
        event = {
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        # Maintain history buffer
        self.history.append(event)
        if len(self.history) > self.max_history:
            self.history = self.history[-self.max_history:]

        payload = json.dumps(event)
        stale_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(payload)
            except Exception:
                stale_connections.append(connection)

        # Cleanup stale connections
        for stale in stale_connections:
            if stale in self.active_connections:
                self.active_connections.remove(stale)


manager = ConnectionManager()
