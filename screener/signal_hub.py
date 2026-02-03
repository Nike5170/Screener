import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Set, Optional, Callable

import websockets
from websockets.server import WebSocketServerProtocol

from logger import Logger
from config import SIGNAL_HUB_HOST, SIGNAL_HUB_PORT


@dataclass(eq=False)
class ClientInfo:
    ws: WebSocketServerProtocol
    client_id: str
    authed: bool = False
    user_id: Optional[str] = None


class SignalHub:
    """
    WS сервер:
      - пуш сигналов
      - команды get_config/set_config (теперь per-user)
    """

    def __init__(
        self,
        auth_resolver: Callable[[str], Optional[str]],
        config_getter_for_user: Callable[[str], Dict[str, Any]],
        config_patcher_for_user: Callable[[str, Dict[str, Any]], Dict[str, Any]],
        top_provider,
        metrics_sink=None,
    ):
        self._clients: Set[ClientInfo] = set()
        self._lock = asyncio.Lock()
        self._auth_resolver = auth_resolver
        self._config_getter_for_user = config_getter_for_user
        self._config_patcher_for_user = config_patcher_for_user
        self._top_provider = top_provider
        self._metrics_sink = metrics_sink

    async def start(self):
        Logger.success(f"SignalHub WS on {SIGNAL_HUB_HOST}:{SIGNAL_HUB_PORT}")
        return await websockets.serve(
            self._handler,
            SIGNAL_HUB_HOST,
            SIGNAL_HUB_PORT,
            ping_interval=20,
            ping_timeout=20,
        )

    async def broadcast(self, payload: Dict[str, Any]):
        """
        Для общих событий.
        Сигналы по пользователям лучше слать через send_to_user().
        """
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

    async def send_to_user(self, user_id: str, payload: Dict[str, Any]):
        msg = json.dumps(payload, ensure_ascii=False)

        async with self._lock:
            clients = [c for c in self._clients if c.authed and c.user_id == user_id]

        if not clients:
            return

        dead = []
        for c in clients:
            try:
                await c.ws.send(msg)
            except Exception:
                dead.append(c)

        if dead:
            async with self._lock:
                for c in dead:
                    self._clients.discard(c)

    async def _handler(self, ws: WebSocketServerProtocol):
        ci = ClientInfo(ws=ws, client_id="unknown", authed=False, user_id=None)

        async with self._lock:
            self._clients.add(ci)

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
                    token = str(msg.get("token") or "")
                    ci.client_id = msg.get("client_id") or "unknown"

                    uid = self._auth_resolver(token)
                    if not uid:
                        await ws.send(json.dumps({"type": "error", "error": "unauthorized"}))
                        await ws.close()
                        return

                    ci.authed = True
                    ci.user_id = uid
                    await ws.send(json.dumps({"type": "ok", "ts": time.time(), "user_id": uid}))
                    continue

                if not ci.authed or not ci.user_id:
                    await ws.send(json.dumps({"type": "error", "error": "unauthorized"}))
                    continue

                if t == "get_config":
                    data = self._config_getter_for_user(ci.user_id)
                    await ws.send(json.dumps({"type": "config", "data": data}, ensure_ascii=False))

                elif t == "set_config":
                    patch = msg.get("patch") or {}
                    applied = self._config_patcher_for_user(ci.user_id, patch)
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

        except (websockets.exceptions.ConnectionClosedOK, websockets.exceptions.ConnectionClosedError):
            pass
        finally:
            async with self._lock:
                self._clients.discard(ci)
