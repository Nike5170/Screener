import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Set

import websockets
from websockets.server import WebSocketServerProtocol

from logger import Logger
from config import SIGNAL_HUB_HOST, SIGNAL_HUB_PORT, SIGNAL_HUB_TOKEN


@dataclass(eq=False)
class ClientInfo:
    ws: WebSocketServerProtocol
    client_id: str
    authed: bool = False


class SignalHub:
    """
    WS сервер для:
      - пуша сигналов (impulse/listing/top/status)
      - принятия команд (get_config/set_config/get_top/metrics)
    """
    def __init__(self, config_getter, config_patcher, top_provider, metrics_sink=None):
        self._clients: Set[ClientInfo] = set()
        self._lock = asyncio.Lock()
        self._config_getter = config_getter
        self._config_patcher = config_patcher
        self._top_provider = top_provider
        self._metrics_sink = metrics_sink

    async def start(self):
        Logger.success(f"SignalHub WS on {SIGNAL_HUB_HOST}:{SIGNAL_HUB_PORT}")
        return await websockets.serve(
            self._handler,
            SIGNAL_HUB_HOST,
            SIGNAL_HUB_PORT,
            ping_interval=20,
            ping_timeout=20
        )

    async def broadcast(self, payload: Dict[str, Any]):
        msg = json.dumps(payload, ensure_ascii=False)

        async with self._lock:
            clients = list(self._clients)

        if not clients:
            return

        dead = []
        for c in clients:
            try:
                if not c.authed:
                    continue
                await c.ws.send(msg)
            except Exception:
                dead.append(c)

        if dead:
            async with self._lock:
                for c in dead:
                    self._clients.discard(c)

    async def _handler(self, ws: WebSocketServerProtocol):
        ci = ClientInfo(ws=ws, client_id="unknown", authed=False)

        async with self._lock:
            self._clients.add(ci)

        try:
            try:
                async for raw in ws:
                    if raw == "ping":
                        await ws.send("pong")
                        continue

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        await ws.send(json.dumps({"type": "error", "error": "bad_json"}))
                        continue

                    t = (msg.get("type") or "").lower()

                    if t == "auth":
                        token = msg.get("token")
                        ci.client_id = msg.get("client_id") or "unknown"
                        if token != SIGNAL_HUB_TOKEN:
                            await ws.send(json.dumps({"type": "error", "error": "unauthorized"}))
                            await ws.close()
                            return
                        ci.authed = True
                        await ws.send(json.dumps({"type": "ok", "ts": time.time()}))
                        continue

                    if not ci.authed:
                        await ws.send(json.dumps({"type": "error", "error": "unauthorized"}))
                        continue

                    if t == "get_config":
                        await ws.send(json.dumps({"type": "config", "data": self._config_getter()}, ensure_ascii=False))

                    elif t == "set_config":
                        patch = msg.get("patch") or {}
                        applied = self._config_patcher(patch)
                        await ws.send(json.dumps({"type": "config", "data": applied}, ensure_ascii=False))

                    elif t == "get_top":
                        mode = msg.get("mode", "volume24h")
                        n = int(msg.get("n", 5))
                        items = await self._top_provider(mode=mode, n=n)
                        await ws.send(json.dumps({"type": "top", "mode": mode, "items": items}, ensure_ascii=False))

                    elif t == "metrics":
                        if self._metrics_sink:
                            await self._metrics_sink(ci.client_id, msg.get("event"), msg.get("data"))
                        await ws.send(json.dumps({"type": "ok"}))

                    elif t == "ping":
                        await ws.send(json.dumps({"type": "pong"}))

                    else:
                        await ws.send(json.dumps({"type": "error", "error": "unknown_type"}))

            except (websockets.exceptions.ConnectionClosedOK,
                    websockets.exceptions.ConnectionClosedError):
                # Клиент отвалился/закрылся без close-frame — это нормальная ситуация
                pass

        finally:
            async with self._lock:
                self._clients.discard(ci)
