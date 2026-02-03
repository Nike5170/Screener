# notifier.py
import asyncio
import aiohttp
import time
from typing import Optional, Tuple

from logger import Logger
from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID


class Notifier:
    def __init__(self):
        self.TG_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        self.default_chat_id = TELEGRAM_CHAT_ID  # можно оставить для админа/тестов

        self.telegram_queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue(maxsize=2000)

        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(total=8, connect=3, sock_read=5)

        self._signal_hub = None
        self._tg_task: asyncio.Task | None = None

    def set_signal_hub(self, signal_hub):
        self._signal_hub = signal_hub

    async def start(self):
        if self._tg_task:
            return  # уже запущено

        self._session = aiohttp.ClientSession(timeout=self._timeout)
        self._tg_task = asyncio.create_task(self._telegram_worker(), name="telegram_worker")


    async def close(self):
        if self._tg_task:
            self._tg_task.cancel()
            self._tg_task = None
        if self._session:
            await self._session.close()
            self._session = None

    async def send_message(self, text: str, chat_id: Optional[str] = None):
        """
        chat_id=None -> default_chat_id (как было раньше)
        """
        cid = str(chat_id or self.default_chat_id)
        if not cid:
            return
        try:
            self.telegram_queue.put_nowait((cid, text))
        except asyncio.QueueFull:
            Logger.warn("Telegram queue full → message dropped")

    async def send_clipboard(self, text: str):
        """
        WS событие link_symbol всем подключенным (или ниже сделаем targeted).
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
            chat_id, text = await self.telegram_queue.get()
            try:
                await self._send_telegram(chat_id, text)
            except Exception as e:
                Logger.error(f"Telegram worker error: {e}")
            finally:
                self.telegram_queue.task_done()

    async def _send_telegram(self, chat_id: str, text: str):
        if not self._session:
            Logger.error("Telegram session not initialized")
            return

        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self._session.post(self.TG_URL, json=payload) as resp:
                if resp.status == 200:
                    Logger.info(f"📨 Telegram → OK ({chat_id})")
                else:
                    Logger.error(f"⚠ Telegram status {resp.status}: {await resp.text()}")
        except asyncio.TimeoutError:
            Logger.error("⏳ Telegram timeout")
        except Exception as e:
            Logger.error(f"❌ Telegram exception: {e}")
