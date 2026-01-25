# notifier.py
import asyncio
import aiohttp
import time

from logger import Logger
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


class Notifier:
    """
    Telegram + WS outbound signals.

    Раньше send_clipboard() слал символ в LAN insert app.
    Теперь send_clipboard() = отправка события link_symbol по SignalHub (WS).
    """

    def __init__(self):
        self.TG_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        self.chat_id = TELEGRAM_CHAT_ID

        self.telegram_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2000)

        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(total=8, connect=3, sock_read=5)

        self._signal_hub = None
        self._tg_task: asyncio.Task | None = None

    def set_signal_hub(self, signal_hub):
        self._signal_hub = signal_hub

    async def start(self):
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._tg_task = asyncio.create_task(self._telegram_worker(), name="telegram_worker")

    async def close(self):
        if self._tg_task:
            self._tg_task.cancel()
            self._tg_task = None
        if self._session:
            await self._session.close()
            self._session = None

    async def send_message(self, text: str):
        try:
            self.telegram_queue.put_nowait(text)
        except asyncio.QueueFull:
            Logger.warn("Telegram queue full → message dropped")

    async def send_clipboard(self, text: str):
        """
        Было: отправка по локалке.
        Стало: WS событие link_symbol для клиента (если он подключен).
        """
        if not self._signal_hub:
            return

        try:
            await self._signal_hub.broadcast({
                "type": "link_symbol",
                "symbol": str(text).upper(),
                "ts": float(time.time()),
            })
        except Exception as e:
            Logger.error(f"SignalHub broadcast error: {e}")

    async def _telegram_worker(self):
        while True:
            text = await self.telegram_queue.get()
            try:
                await self._send_telegram(text)
            except Exception as e:
                Logger.error(f"Telegram worker error: {e}")
            finally:
                self.telegram_queue.task_done()

    async def _send_telegram(self, text: str):
        if not self._session:
            Logger.error("Telegram session not initialized")
            return

        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self._session.post(self.TG_URL, json=payload) as resp:
                if resp.status == 200:
                    Logger.info(f"📨 Telegram → OK ({self.chat_id})")
                else:
                    Logger.error(f"⚠ Telegram status {resp.status}: {await resp.text()}")
        except asyncio.TimeoutError:
            Logger.error("⏳ Telegram timeout")
        except Exception as e:
            Logger.error(f"❌ Telegram exception: {e}")
