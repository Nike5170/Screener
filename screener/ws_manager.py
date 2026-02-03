import asyncio
import aiohttp
from logger import Logger


class WSManager:
    def __init__(self, handle_trade_callback):
        self.tasks = {}
        self.handle_trade_callback = handle_trade_callback
        self.mark_tasks = {}
        self.handle_mark_callback = None

    def set_mark_handler(self, cb):
        self.handle_mark_callback = cb

    def start_task(self, symbol):
        if symbol in self.tasks:
            return
        # создаём Task на coroutine
        task = asyncio.create_task(self.handle_trades(symbol))
        self.tasks[symbol] = task

        if symbol not in self.mark_tasks:
            mt = asyncio.create_task(self.handle_mark_price(symbol))
            self.mark_tasks[symbol] = mt

    def stop_task(self, symbol):
        task = self.tasks.get(symbol)
        if task:
            task.cancel()
            del self.tasks[symbol]

        mt = self.mark_tasks.get(symbol)
        if mt:
            mt.cancel()
            del self.mark_tasks[symbol]

    async def handle_trades(self, symbol):
        url = f"wss://fstream.binance.com/ws/{symbol}@aggTrade"
        timeout = aiohttp.ClientTimeout(total=None, sock_read=60, connect=10)

        while True:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = msg.json()
                                res = self.handle_trade_callback(symbol, data)
                                if asyncio.iscoroutine(res):
                                    await res

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                Logger.error(f"WS reconnect {symbol}: {e}")
                await asyncio.sleep(5)

    async def handle_mark_price(self, symbol):
        url = f"wss://fstream.binance.com/ws/{symbol}@markPrice@1s"
        timeout = aiohttp.ClientTimeout(total=None, sock_read=60, connect=10)

        while True:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = msg.json()
                                if self.handle_mark_callback:
                                    res = self.handle_mark_callback(symbol, data)
                                    if asyncio.iscoroutine(res):
                                        await res

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                Logger.error(f"WS mark reconnect {symbol}: {e}")
                await asyncio.sleep(5)
