import asyncio
import aiohttp
import random
from logger import Logger

BINANCE_WS_URL = "wss://fstream.binance.com/ws"


class _BinanceSubWS:
    """
    Один websocket к /ws с live SUBSCRIBE/UNSUBSCRIBE.
    Держит set подписок, умеет обновлять diff'ом, восстанавливается при разрыве.
    """
    def __init__(self, name: str, event_handler, heartbeat: int = 20):
        self.name = name
        self._event_handler = event_handler
        self._heartbeat = heartbeat

        self._session = None
        self._ws = None
        self._task = None

        self._wanted = set()       # что должно быть подписано
        self._subscribed = set()   # что реально подписано (после ack)
        self._cmd_id = 0

        self._lock = asyncio.Lock()
        self._stopped = asyncio.Event()

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        await self._close()

    async def set_streams(self, streams: set[str]):
        """
        Обновить желаемые подписки.
        Реальный SUB/UNSUB делаем в батчах.
        """
        async with self._lock:
            self._wanted = set(streams)

        # если сокет уже поднят — применяем diff
        await self._apply_diff()

    async def _apply_diff(self):
        if not self._ws or self._ws.closed:
            return

        async with self._lock:
            wanted = set(self._wanted)
            subscribed = set(self._subscribed)

        to_add = sorted(wanted - subscribed)
        to_del = sorted(subscribed - wanted)

        # батчи, чтобы не спамить управляющими сообщениями
        await self._send_batched("UNSUBSCRIBE", to_del, batch=80)
        await self._send_batched("SUBSCRIBE", to_add, batch=80)

    async def _send_batched(self, method: str, params: list[str], batch: int = 80):
        if not params:
            return
        for i in range(0, len(params), batch):
            chunk = params[i:i + batch]
            await self._send_cmd(method, chunk)
            async with self._lock:
                if method == "SUBSCRIBE":
                    self._subscribed.update(chunk)
                elif method == "UNSUBSCRIBE":
                    for x in chunk:
                        self._subscribed.discard(x)

            # небольшая пауза, чтобы не упираться в rate limit управляющих сообщений
            await asyncio.sleep(0.05)

    async def _send_cmd(self, method: str, params: list[str]):
        if not self._ws or self._ws.closed:
            return
        self._cmd_id += 1
        payload = {"method": method, "params": params, "id": self._cmd_id}
        await self._ws.send_json(payload)

    async def _run(self):
        timeout = aiohttp.ClientTimeout(total=None, sock_read=60, connect=10)

        backoff = 1.0
        while not self._stopped.is_set():
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    self._session = session
                    async with session.ws_connect(BINANCE_WS_URL, heartbeat=self._heartbeat) as ws:
                        self._ws = ws
                        Logger.info(f"✅ WS[{self.name}] connected")

                        # после коннекта: считаем, что подписки надо восстановить
                        async with self._lock:
                            self._subscribed.clear()

                        await self._apply_diff()
                        backoff = 1.0

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = msg.json()

                                # ACK на команды подписки: {"result":null,"id":X}
                                if isinstance(data, dict) and "result" in data and "id" in data:
                                    # result==None означает успех
                                    continue

                                # market-stream event
                                res = self._event_handler(data)
                                if asyncio.iscoroutine(res):
                                    await res

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                Logger.error(f"WS[{self.name}] reconnect: {e}")

            await self._close()

            # backoff + jitter, чтобы не reconnect-штормило
            jitter = random.uniform(0.0, 0.3)
            await asyncio.sleep(backoff + jitter)
            backoff = min(backoff * 2.0, 30.0)

    async def _close(self):
        try:
            if self._ws and not self._ws.closed:
                await self._ws.close()
        except Exception:
            pass
        self._ws = None
        self._session = None


class WSManager:
    """
    Два коннекта:
      - trades:   <symbol>@aggTrade
      - mark:     <symbol>@markPrice@1s
    Подписки обновляются диффом (каждый час можешь менять список символов).
    """
    def __init__(self, handle_trade_callback):
        self.handle_trade_callback = handle_trade_callback
        self.handle_mark_callback = None

        self._symbols = set()

        self._trade_ws = _BinanceSubWS("aggTrade", self._on_trade_event)
        self._mark_ws = _BinanceSubWS("markPrice", self._on_mark_event)

    def set_mark_handler(self, cb):
        self.handle_mark_callback = cb

    async def start(self):
        await self._trade_ws.start()
        await self._mark_ws.start()

    async def stop(self):
        await self._trade_ws.stop()
        await self._mark_ws.stop()

    async def set_symbols(self, symbols: list[str] | set[str]):
        # symbols в твоём коде сейчас lower — оставляем lower
        self._symbols = set(s.lower() for s in symbols)

        trade_streams = {f"{s}@aggTrade" for s in self._symbols}
        mark_streams = {f"{s}@markPrice@1s" for s in self._symbols}

        await self._trade_ws.set_streams(trade_streams)
        await self._mark_ws.set_streams(mark_streams)

    async def _on_trade_event(self, data: dict):
        # Binance присылает raw event: {"e":"aggTrade", ...}
        if not isinstance(data, dict):
            return
        if data.get("e") != "aggTrade":
            return
        symbol = str(data.get("s") or "").lower()
        if not symbol:
            return
        res = self.handle_trade_callback(symbol, data)
        if asyncio.iscoroutine(res):
            await res

    async def _on_mark_event(self, data: dict):
        if not isinstance(data, dict):
            return
        if data.get("e") != "markPriceUpdate":
            return
        symbol = str(data.get("s") or "").lower()
        if not symbol:
            return
        if self.handle_mark_callback:
            res = self.handle_mark_callback(symbol, data)
            if asyncio.iscoroutine(res):
                await res
